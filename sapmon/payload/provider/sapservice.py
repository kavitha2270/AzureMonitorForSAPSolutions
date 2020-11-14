# Python modules
import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta, time
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

class sapServiceProviderInstance(ProviderInstance):
    def __init__(self,
                tracer: logging.Logger,
                ctx: Context,
                providerInstance: Dict[str, str],
                skipContent: bool = False,
                **kwargs):
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
    parse provider properties and get host name, sysnr, username, password.
    """
    def parseProperties(self) -> bool:
        self.sapHostName = self.providerProperties.get("sapHostName", None)
        if not self.sapHostName:
            self.tracer.error("[%s] sapHostName cannot be empty" % self.fullName)
            return False
        
        self.sapSysNr = self.providerProperties.get("sapSysNr", None)
        if not self.sapSysNr:
            self.tracer.error("[%s] sapSysNr cannot be empty" % self.fullName)
            return False

        self.sapUsername = self.providerProperties.get("sapUsername", None)
        if not self.sapUsername:
            self.tracer.error("[%s] sapUsername cannot be empty" % self.fullName)
            return False

        self.sapPassword = self.providerProperties.get("sapPassword", None)
        if not self.sapPassword:
            self.tracer.error("[%s] sapPassword cannot be empty" % self.fullName)
            return False

        return True

    def getUrlLink(self, hostname: str, sysnr: str):
        return 'https://' + hostname + ':5' + sysnr + '14/?wsdl'

    def _establish_connection(self, hostname: str, sysnr: str) -> Client:
        try:
            clientWSDLUrl = self.getUrlLink(hostname, sysnr)
            client = Client(clientWSDLUrl, transport=UnverifiedHttpsTransport())
            return client
        except Exception as e:
            self.tracer.error("Error while connecting to hostname: %s and sysnr: %s: %s" %(hostname, sysnr, e))
            raise e

    def validate(self) -> bool:
        self.tracer.info("connecting to sap with host name (%s) to test required rfc calls.")

        try:
            self._establish_connection()

        except Exception as e:
            self.tracer.error("Error occured while validating %s " % (e))
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
        # hostname and sysnr.
        sid = 10 # sample sid assigned, use sid from provider instance once added.
        if 'hostConfig' not in self.providerInstance.state:
            self.tracer.debug("no host config persisted yet, using user-provided host name and sysnr.")
            hosts = [(self.providerInstance.sapHostName, self.providerInstance.sapSysNr, )]
        else:
            currentHostConfig = self.providerInstance.state['hostConfig']
            hosts = [(hostConfig['hostname'], str(hostConfig['instanceNr']).zfill(2), ) for hostConfig in currentHostConfig]

        isSuccess = False
        instanceList = list()
        for host in hosts:
            try:
                client = self.providerInstance._establish_connection(host[0], host[1])

                # check if new client WSDL contains method 'GetSystemInstanceList'.
                if hasattr(client.service, 'GetSystemInstanceList'):
                    result = client.service.GetSystemInstanceList()
                    
                    if 'item' in result:
                        for itemResult in result['item']:
                            instanceList.append(Client.dict(itemResult))
                    else:
                        raise AttributeError('GetSystemInstanceList result does not contain "item" schema.')
                    isSuccess = True
                    break
                else:
                    self.tracer.warning("WSDL for SAP with hostname: %s and sysnr: %s does not have " % (host[0], host[1]))
            except Exception as e:
                self.tracer.error("Could not connect to SAP with hostname: %s and sysnr: %s" % (host[0], host[1]))

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

    def _actionExecuteWebServiceRequest(self, apiName):
        self.tracer.info("executing web service request: %s" % (apiName))

        # TODO: Implement logic

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

class UnverifiedHttpsTransport(suds.transport.http.HttpTransport):
    def __init__(self, *args, **kwargs):
        super(UnverifiedHttpsTransport, self).__init__(*args, **kwargs)

    def u2handlers(self):
        handlers = super(UnverifiedHttpsTransport, self).u2handlers()
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        handlers.append(urllib.request.HTTPSHandler(context=context))
        return handlers