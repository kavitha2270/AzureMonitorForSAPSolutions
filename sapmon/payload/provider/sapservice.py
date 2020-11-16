# Python modules
import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta, time

# SOAP client modules
from suds.client import Client
import urllib.request
import ssl
import suds.transport.http

# Payload modules
from const import *
from helper.azure import *
from helper.context import *
from helper.tools import *
from provider.base import ProviderInstance, ProviderCheck
from typing import Dict, List

class unverifiedHttpsTransport(suds.transport.http.HttpTransport):
    def __init__(self, *args, **kwargs) -> None:
        super(unverifiedHttpsTransport, self).__init__(*args, **kwargs)

    def u2handlers(self) -> suds.transport.http.HttpTransport:
        handlers = super(unverifiedHttpsTransport, self).u2handlers()
        context = ssl.create_default_context()
        context.check_hostName = False
        context.verify_mode = ssl.CERT_NONE
        handlers.append(urllib.request.HTTPSHandler(context=context))
        return handlers

class sapServiceProviderInstance(ProviderInstance):
    def __init__(self,
                tracer: logging.Logger,
                ctx: Context,
                providerInstance: Dict[str, str],
                skipContent: bool = False,
                **kwargs) -> None:
        self.sapHostName = None
        self.sapSysNr = None
        self.sapUsername = None
        self.sapPassword = None

        retrySettings = {
         "retries": 3,
         "delayInSeconds": 1,
         "backoffMultiplier": 2
        }

        super().__init__(tracer,
                       ctx,
                       providerInstance,
                       retrySettings,
                       skipContent,
                       **kwargs)

    """
    parse provider properties and get sid, host name and instance number.
    """
    def parseProperties(self) -> bool:
        self.sapSid = self.providerProperties.get("sapSid", None)
        if not self.sapSid:
            self.tracer.error("[%s] sapSid cannot be empty" % self.fullName)
            return False
        
        self.sapHostName = self.providerProperties.get("sapHostName", None)
        if not self.sapHostName:
            self.tracer.error("[%s] sapHostName cannot be empty" % self.fullName)
            return False
        
        self.sapInstanceNr = self.providerProperties.get("sapInstanceNr", None)
        if not self.sapInstanceNr:
            self.tracer.error("[%s] sapInstanceNr cannot be empty" % self.fullName)
            return False

        return True

    def getPortFromInstanceNr(self, instanceNr: str) -> str:
        return '5%s14' % self.sapInstanceNr

    def _establishConnection(self, hostName: str = None, port: str = None) -> Client:
        if not hostName:
            hostName = self.sapHostName
        if not port:
            port = self.getPortFromInstanceNr(self.sapInstanceNr) # Default https port is of the form 5<NR>14

        try:
            url = 'https://%s:%s/?wsdl' % (hostName, port)
            client = Client(url, transport=unverifiedHttpsTransport())
            return client
        except Exception as e:
            self.tracer.error("Error while connecting to hostName: %s and port: %s: %s" % (hostName, port, e))
            raise e

    def callSoapApi(self, hostName: str, port: str, apiName: str) -> str:
        try:
            client = self._establishConnection(hostName, port)
            method = getattr(client.service, apiName)
            result = method()
            return result
        except Exception as e:
            self.tracer.error("Error while calling SOAP API: %s for hostName: %s and port: %s: %s" % (apiName, hostName, port, e))
            raise e

    def validate(self) -> bool:
        self.tracer.info("connecting to sap with host name (%s) to test SOAP API connectivity.")

        try:
            self._establishConnection()
        except Exception as e:
            self.tracer.error("Error occured while validating provider: %s " % (e))
            return False

        return True

###########################
class sapServiceProviderCheck(ProviderCheck):
    lastResult = list()

    def __init__(self,
    provider: ProviderInstance,
    **kwargs
    ):
        self.lastRunTime = None
        return super().__init__(provider, **kwargs)

    def _actionProcessSystemInstanceList(self):
        self.tracer.info("Executing get system instance list web service call.")

        # fetch list from storage. If storage does not have list, use provided
        # hostName and sysnr.
        sid = 10 # sample sid assigned, use sid from provider instance once added.
        if 'hostConfig' not in self.providerInstance.state:
            self.tracer.debug("no host config persisted yet, using user-provided host name and sysnr.")
            hosts = [(self.providerInstance.sapHostName, \
                self.providerInstance.getPortFromInstanceNr(self.providerInstance.sapSysNr), )]
        else:
            currentHostConfig = self.providerInstance.state['hostConfig']
            hosts = [(hostConfig['hostName'], str(hostConfig['httpsPort']).zfill(2), ) for hostConfig in currentHostConfig]

        isSuccess = False
        instanceList = list()
        for host in hosts:
            try:
                result = self.callSoapApi(host[0], host[1], 'GetSystemInstanceList')
                if 'item' in result:
                    for itemResult in result['item']:
                        instanceList.append(Client.dict(itemResult))
                else:
                    raise AttributeError('GetSystemInstanceList result does not contain "item" schema.')
                isSuccess = True
                break
            except Exception as e:
                self.tracer.error("Could not connect to SAP with hostName: %s and port: %s" % (host[0], host[1]))

        if not isSuccess:
            raise Exception("Could not connect to any SAP instances for provider: %s with hosts %s." % ( self.providerInstance.fullName,
                                                                                                         hosts))

        # update host config, if new list is fetched.
        # parse dictionary and add current timestamp and SID to data and log it.
        if len(instanceList) != 0:
            self.providerInstance.state['hostConfig'] = instanceList

            currentTimestamp = datetime.now().isoformat()
            for instance in instanceList:
                instance['timestamp'] = currentTimestamp
                instance['SID'] = sid

            self.lastResult.extend(instanceList)

        # Update internal state
        if not self.updateState():
            raise Exception("Failed to update state")

        self.tracer.info("successfully fetched system instance list.")

    def _get_instances(self):
        result = self.providerInstance.callSoapApi("hanaapp", "50114", "GetSystemInstanceList")
        return result.item

    def _filter_instances(self, sapInstances, eligibleFeatures):
        # Only keep instance if at least 1 feature for the instance matches the list of eligible features
        instances = [(instance, instance.features.split('|')) for instance in sapInstances]
        filtered_instances = [instance for (instance, instance_features) in instances \
            if not set(eligibleFeatures).isdisjoint(set(instance_features))]

        return filtered_instances

    def _actionExecuteWebServiceRequest(self, apiName, eligibleFeatures):
        self.tracer.info("executing web service request: %s" % (apiName))

        # TODO: Implement logic
        # Get instances list
        sapInstances = self._get_instances()
        # Filter instances based on features
        sapInstances = self._filter_instances(sapInstances, eligibleFeatures)
        # Call web service
        results = [self.providerInstance.callSoapApi(instance.hostName, instance.httpsPort, apiName) \
            for instance in sapInstances]

        # Parse result

        # Update internal state
        if not self.updateState():
            raise Exception("Failed to update state")
        self.tracer.info("successfully processed web service request: %s" % (apiName))

    def generateJsonString(self) -> str:
        self.tracer.info("[%s] converting result to json string." % self.fullName)
        resultJsonString = json.dumps(self.lastResult, sort_keys=True, indent=4, cls=JsonEncoder)
        self.tracer.debug("[%s] resultJson=%s" % (self.fullName,
                                                   str(resultJsonString)))
        return resultJsonString

    def updateState(self):
        self.tracer.info("[%s] updating internal state" % self.fullName)

        # update last run local.
        lastRunLocal = datetime.utcnow()
        self.state['lastRunLocal'] = lastRunLocal
        # update last run server.
        self.state['lastRunServer'] = self.lastRunTime
        self.tracer.info("[%s] internal state successfully updated" % self.fullName)
        return True
