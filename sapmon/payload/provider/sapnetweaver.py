# Python modules
import json
import logging
from datetime import datetime
from typing import Any, Callable

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
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        handlers.append(urllib.request.HTTPSHandler(context=context))
        return handlers

class sapNetWeaverProviderInstance(ProviderInstance):
    def __init__(self,
                tracer: logging.Logger,
                ctx: Context,
                providerInstance: Dict[str, str],
                skipContent: bool = False,
                **kwargs) -> None:
        self.sapSid = None
        self.sapHostName = None
        self.sapInstanceNr = None

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
    parse provider properties and get sid, host name and instance number
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
        return '5%s14' % self.sapInstanceNr # As per SAP documentation, default https port is of the form 5<NR>14

    def _establishConnection(self, hostname: str = None, port: str = None) -> Client:
        if not hostname:
            hostname = self.sapHostName
        if not port:
            port = self.getPortFromInstanceNr(self.sapInstanceNr)

        self.tracer.info("[%s] connecting to hostname: %s and port: %s" % (self.fullName, hostname, port))

        try:
            url = 'https://%s:%s/?wsdl' % (hostname, port)
            self.tracer.debug("[%s] making call to url: %s" % (self.fullName, url))
            client = Client(url, transport=unverifiedHttpsTransport())
            return client
        except Exception as e:
            self.tracer.error("[%s] error while connecting to hostname: %s and port: %s: %s" % (self.fullName, hostname, port, e))
            raise e

    def callSoapApi(self, hostname: str, port: str, apiName: str) -> str:
        self.tracer.info("[%s] executing SOAP API: %s for hostname: %s and port: %s" % (self.fullName, apiName, hostname, port))

        try:
            client = self._establishConnection(hostname, port)
            # Due to a bug in exception handling in the suds client library, it doesn't honor
            # the default fallback parameter on getattr() instead of throwing an exception
            # So we have to resort to capturing the exception instead of handling it with conditional check
            # on the method return value
            method = getattr(client.service, apiName)
            result = method()
            return result
        except Exception as e:
            self.tracer.error("[%s] error while calling SOAP API: %s for hostname: %s and port: %s: %s" % (self.fullName, apiName, hostname, port, e))
            raise e

    def validate(self) -> bool:
        self.tracer.info("[%s] connecting to sap to test SOAP API connectivity" % self.fullName)

        try:
            self._establishConnection()
        except Exception as e:
            self.tracer.error("[%s] error occured while validating provider: %s " % (self.fullName, e))
            return False

        return True

###########################
class sapNetweaverProviderCheck(ProviderCheck):
    lastResult = []

    def __init__(self,
        provider: ProviderInstance,
        **kwargs
    ):
        self.lastRunTime = None
        return super().__init__(provider, **kwargs)

    def _get_formatted_timestamp(self) -> str:
        return datetime.now().isoformat()

    def _get_hosts(self) -> list:
        # Fetch last known list from storage. If storage does not have list, use provided
        # hostname and instanceNr
        if 'hostConfig' not in self.providerInstance.state:
            self.tracer.debug("[%s] no host config persisted yet, using user-provided host name and instance nr" % self.fullName)
            hosts = [(self.providerInstance.sapHostName, \
                self.providerInstance.getPortFromInstanceNr(self.providerInstance.sapInstanceNr))]
        else:
            self.tracer.info("[%s] fetching last known host config" % self.fullName)
            currentHostConfig = self.providerInstance.state['hostConfig']
            hosts = [(hostConfig['hostname'], hostConfig['httpsPort']) for hostConfig in currentHostConfig]

        return hosts

    def _parse_result(self, apiName: str, result: object) -> list:
        return [Client.dict(result)]

    def _parse_results(self, apiName: str, results: list) -> list:
        parsed_results = []
        if 'item' in results:
            for itemResult in results['item']:
                parsed_results.append(Client.dict(itemResult))
        else:
            raise AttributeError('%s result does not contain "item" schema' % apiName)

        return parsed_results

    def _get_instances(self) -> list:
        self.tracer.info("[%s] getting list of system instances" % self.fullName)

        instanceList = []
        hosts = self._get_hosts()

        # Use last known hosts to fetch the updated list of hosts
        # Walk through the known hostnames and stop whenever any of them returns the list of all instances
        isSuccess = False
        for host in hosts:
            hostname, port = host[0], host[1]

            try:
                apiName = 'GetSystemInstanceList'
                result = self.providerInstance.callSoapApi(hostname, port, apiName)
                instanceList = self._parse_results(apiName, result)
                isSuccess = True
                break
            except Exception as e:
                self.tracer.error("[%s] could not connect to SAP with hostname: %s and port: %s" % (self.fullName, hostname, port))

        if not isSuccess:
            raise Exception("[%s] could not connect to any SAP instances for provider: %s with hosts %s" % \
                (self.fullName, self.providerInstance.fullName, hosts))

        return instanceList

    def _filter_instances(self, sapInstances: list, filterFeatures: list, filterType: str) -> list:
        self.tracer.info("[%s] filtering list of system instances based on features: %s" % (self.fullName, filterFeatures))

        instances = [(instance, instance['features'].split('|')) for instance in sapInstances]

        # Inclusion filter
        # Only keep instance if the instance supports at least 1 of the filter features
        if filterType == "include":
            filtered_instances = [instance for (instance, instance_features) in instances \
                if not set(filterFeatures).isdisjoint(set(instance_features))]
        else:
        # Exclusion filter
        # Only keep instance if the instance does not support any of the filter features
            filtered_instances = [instance for (instance, instance_features) in instances \
                if set(filterFeatures).isdisjoint(set(instance_features))]

        return filtered_instances

    def _actionGetSystemInstanceList(self) -> None:
        self.tracer.info("[%s] refreshing list of system instances" % self.fullName)

        instanceList = self._get_instances()

        # Update host config, if new list is fetched
        # Parse dictionary and add current timestamp and SID to data and log it
        if len(instanceList) != 0:
            self.providerInstance.state['hostConfig'] = instanceList

            currentTimestamp = self._get_formatted_timestamp()
            for instance in instanceList:
                instance['timestamp'] = currentTimestamp
                instance['SID'] = self.providerInstance.sapSid

        self.lastResult = instanceList

        # Update internal state
        if not self.updateState():
            raise Exception("[%s] failed to update state" % self.fullName)

        self.tracer.info("[%s] successfully fetched system instance list" % self.fullName)

    def _executeWebServiceRequest(self, apiName: str, filterFeatures: list, filterType: str, parser: Callable[[str, Any], list] = None) -> None:
        self.tracer.info("[%s] executing web service request: %s" % (self.fullName, apiName))

        if parser is None:
            parser = self._parse_results

        # Use cached list of instances if available since they don't change that frequently; else fetch afresh
        if 'hostConfig' in self.providerInstance.state:
            sapInstances = self.providerInstance.state['hostConfig']
        else:
            sapInstances = self._get_instances()

        # Filter instances down to the ones that support this API
        sapInstances = self._filter_instances(sapInstances, filterFeatures, filterType)
        if len(sapInstances) == 0:
            self.tracer.info("[%s] no instances found that support this API: %s" % (self.fullName, apiName))

        # Call web service
        all_results = []
        currentTimestamp = self._get_formatted_timestamp()
        for instance in sapInstances:
            results = self.providerInstance.callSoapApi(instance['hostname'], instance['httpsPort'], apiName)
            if len(results) != 0:
                parsed_results = parser(apiName, results)
                for result in parsed_results:
                    result['hostname'] = instance['hostname']
                    result['instanceNr'] = instance['instanceNr']
                    result['SID'] = self.providerInstance.sapSid
                    result['timestamp'] = currentTimestamp
                all_results.extend(parsed_results)

        if len(all_results) == 0:
            self.tracer.info("[%s] no results found for: %s" % (self.fullName, apiName))
        self.lastResult = all_results

        # Update internal state
        if not self.updateState():
            raise Exception("[%s] failed to update state" % self.fullName)

        self.tracer.info("[%s] successfully processed web service request: %s" % (self.fullName, apiName))

    def _actionExecuteGenericWebServiceRequest(self, apiName: str, filterFeatures: list, filterType: str) -> None:
        self._executeWebServiceRequest(apiName, filterFeatures, filterType, self._parse_results)

    def _actionExecuteEnqGetStatistic(self, apiName: str, filterFeatures: list, filterType: str) -> None:
        self._executeWebServiceRequest(apiName, filterFeatures, filterType, self._parse_result)

    def generateJsonString(self) -> str:
        self.tracer.info("[%s] converting result to json string" % self.fullName)
        resultJsonString = json.dumps(self.lastResult, sort_keys=True, indent=4, cls=JsonEncoder)
        self.tracer.debug("[%s] resultJson=%s" % (self.fullName,
                                                   str(resultJsonString)))
        return resultJsonString

    def updateState(self) -> bool:
        self.tracer.info("[%s] updating internal state" % self.fullName)

        # update last run local
        lastRunLocal = datetime.utcnow()
        self.state['lastRunLocal'] = lastRunLocal
        # update last run server
        self.state['lastRunServer'] = self.lastRunTime
        self.tracer.info("[%s] internal state successfully updated" % self.fullName)
        return True
