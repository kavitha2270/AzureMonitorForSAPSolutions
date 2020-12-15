# Python modules
import json
import logging
from datetime import datetime
from typing import Any, Callable
import requests
from requests import Session

# SOAP Client modules
from zeep import Client
from zeep import helpers
from zeep.transports import Transport
from zeep.exceptions import Fault

# Payload modules
from const import *
from helper.azure import *
from helper.context import *
from helper.tools import *
from provider.base import ProviderInstance, ProviderCheck
from typing import Dict

# Suppress SSLError warning due to missing SAP server certificate
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class sapNetweaverProviderInstance(ProviderInstance):
    def __init__(self,
                tracer: logging.Logger,
                ctx: Context,
                providerInstance: Dict[str, str],
                skipContent: bool = False,
                **kwargs) -> None:
        self.sapSid = None
        self.sapHostName = None
        self.sapInstanceNr = None
        self.sapSubdomain = None

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
        self.sapHostName = self.providerProperties.get("sapHostName", None)
        if not self.sapHostName:
            self.tracer.error("[%s] sapHostName cannot be empty" % self.fullName)
            return False

        instanceNr = self.providerProperties.get("sapInstanceNr", None)
        if not instanceNr:
            self.tracer.error("[%s] sapInstanceNr cannot be empty" % self.fullName)
            return False
        if not instanceNr.isdecimal() or int(instanceNr) < 0 or int(instanceNr) > 98:
            self.tracer.error("[%s] sapInstanceNr can only be between 00 and 98 but %s was passed" % (self.fullName, instanceNr))
            return False
        self.sapInstanceNr = instanceNr.zfill(2)
        self.sapSubdomain = self.providerProperties.get("sapSubdomain", "")
        self.sapSid = self.metadata.get("sapSid", "")
        if not self.sapSid:
            self.tracer.error("[%s] sapSid cannot be empty" % self.fullName)
            return False

        return True

    def getPortFromInstanceNr(self, instanceNr: str) -> str:
        return '5%s14' % instanceNr # As per SAP documentation, default https port is of the form 5<NR>14

    def getMessageServerPortFromInstanceNr(self, instanceNr: str) -> str:
        return '81%s' % instanceNr # As per SAP documentation, default http port is of the form 81<NR>

    def getFullyQualifiedDomainName(self, subdomain: str, hostname: str) -> str:
        if subdomain:
            return hostname + "." + subdomain
        else:
            return hostname

    def getClient(self, hostname: str = None, port: str = None, subdomain: str = None) -> Client:
        if not hostname:
            hostname = self.sapHostName
        if not subdomain:
            subdomain = self.sapSubdomain
        if not port:
            port = self.getPortFromInstanceNr(self.sapInstanceNr)

        hostname = self.getFullyQualifiedDomainName(subdomain, hostname)

        self.tracer.info("[%s] connecting to hostname: %s and port: %s" % (self.fullName, hostname, port))

        try:
            url = 'https://%s:%s/?wsdl' % (hostname, port)
            self.tracer.info("[%s] establishing connection to url: %s" % (self.fullName, url))

            session = Session()
            session.verify = False
            client = Client(url, transport=Transport(session=session))
            return client
        except Exception as e:
            self.tracer.error("[%s] error while connecting to hostname: %s and port: %s: %s" % (self.fullName, hostname, port, e))
            raise e

    def callSoapApi(self, client: Client, apiName: str) -> str:
        self.tracer.info("[%s] executing SOAP API: %s for wsdl: %s" % (self.fullName, apiName, client.wsdl.location))

        try:
            method = getattr(client.service, apiName)
            result = method()
            return result
        except Exception as e:
            self.tracer.info("[%s] error while calling SOAP API: %s for wsdl: %s" % (self.fullName, apiName, client.wsdl.location))
            raise e

    def validate(self) -> bool:
        self.tracer.info("[%s] connecting to sap to test SOAP API connectivity" % self.fullName)

        # HACK: Load content json to fetch the list of APIs in the checks
        self.initContent()

        try:
            client = self.getClient()
        except Exception as e:
            self.tracer.error("[%s] error occured while establishing connectivity to SAP server: %s " % (self.fullName, e))
            return False

        # Ensure that all APIs in the checks are valid and are marked as unprotected.
        # Some APIs are compatible with only specific instance types and throw a Fault if run against
        # an incompatible one.
        # However, here we suppress all errors except Unauthorized since the Monitor phase takes
        # care of calling the API against the right instance type. As long as we don't get an
        # Unauthorized error, we know we can safely call them during the Monitor phase.
        isValid = True
        for check in self.checks:
            apiName = check.name
            method = getattr(client.service, apiName, None) # Returning None when API not found
            if method is None:
                self.tracer.error("[%s] validation failure: api %s does not exist" % (self.fullName, apiName))
                isValid = False
            else:
                try:
                    self.callSoapApi(client, apiName)
                    self.tracer.info("[%s] validated api %s" % (self.fullName, apiName))
                except Fault as e:
                    if (e.code == "SOAP-ENV:Client" and e.message == "HTTP Error: 'Unauthorized'"):
                        isValid = False
                        self.tracer.error("[%s] api %s is not marked as unprotected: %s " % (self.fullName, apiName, e))
                    else:
                        self.tracer.error("[%s] suppressing error during validation of api %s: %s " % (self.fullName, apiName, e))
                except Exception as e:
                    self.tracer.error("[%s] suppressing error during validation of api %s: %s " % (self.fullName, apiName, e))

        return isValid

###########################
class sapNetweaverProviderCheck(ProviderCheck):
    lastResult = []

    def __init__(self,
        provider: ProviderInstance,
        **kwargs
    ):
        self.lastRunServer = None
        self.lastRunLocal = None
        return super().__init__(provider, **kwargs)

    def _getFormattedTimestamp(self) -> str:
        return datetime.now().isoformat()

    def _getHosts(self) -> list:
        # Fetch last known list from storage. If storage does not have list, use provided
        # hostname and instanceNr
        if 'hostConfig' not in self.providerInstance.state:
            self.tracer.info("[%s] no host config persisted yet, using user-provided host name and instance nr" % self.fullName)
            hosts = [(self.providerInstance.sapHostName,
                      self.providerInstance.getPortFromInstanceNr(self.providerInstance.sapInstanceNr),
                      self.providerInstance.sapSubdomain)]
        else:
            self.tracer.info("[%s] fetching last known host config" % self.fullName)
            currentHostConfig = self.providerInstance.state['hostConfig']
            hosts = [(hostConfig['hostname'], hostConfig['httpsPort'], self.providerInstance.sapSubdomain) for hostConfig in currentHostConfig]

        return hosts

    def _parseResult(self, result: object) -> list:
        return [helpers.serialize_object(result, dict)]

    def _parseResults(self, results: list) -> list:
        return helpers.serialize_object(results, dict)

    def _getInstances(self) -> list:
        self.tracer.info("[%s] getting list of system instances" % self.fullName)

        instanceList = []
        hosts = self._getHosts()

        # Use last known hosts to fetch the updated list of hosts
        # Walk through the known hostnames and stop whenever any of them returns the list of all instances
        isSuccess = False
        for host in hosts:
            hostname, port, subdomain = host[0], host[1], host[2]           
            try:
                apiName = 'GetSystemInstanceList'
                client = self.providerInstance.getClient(hostname, port, subdomain)
                result = self.providerInstance.callSoapApi(client, apiName)
                instanceList = self._parseResults(result)
                isSuccess = True
                break
            except Exception as e:
                self.tracer.error("[%s] could not connect to SAP with hostname: %s and port: %s" % (self.fullName, hostname, port))

        if not isSuccess:
            raise Exception("[%s] could not connect to any SAP instances for provider: %s with hosts %s" % \
                (self.fullName, self.providerInstance.fullName, hosts))

        return instanceList

    def _filterInstances(self, sapInstances: list, filterFeatures: list, filterType: str) -> list:
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

    def _getServerTimestamp(self, instances: list) -> datetime:
        self.tracer.info("[%s] fetching current timestamp from message server" % self.fullName)
        message_server_instances = self._filterInstances(instances, ['MESSAGESERVER'], 'include')
        date = self._getFormattedTimestamp()   

        # Get timestamp from the first message server that returns a valid date
        for instance in message_server_instances:
            hostname = instance['hostname']
            instanceNr = str(instance['instanceNr']).zfill(2)
            port = self.providerInstance.getMessageServerPortFromInstanceNr(instanceNr)
            subdomain = self.providerInstance.sapSubdomain
            hostname = self.providerInstance.getFullyQualifiedDomainName(subdomain, hostname)
            message_server_endpoint = "http://%s:%s/" % (hostname, port)

            try:
                # We only care about the date in the response header. so we ignore the response body
                response = requests.get(message_server_endpoint)
                date = datetime.strptime(response.headers['date'], '%a, %d %b %Y %H:%M:%S %Z')
                break
            except Exception as e:
                self.tracer.info("[%s] suppressing expected error while fetching server time during HTTP GET request to url %s: %s " % (self.fullName, message_server_endpoint, e))
        return date

    def _actionGetSystemInstanceList(self) -> None:
        self.tracer.info("[%s] refreshing list of system instances" % self.fullName)
        self.lastRunLocal = datetime.utcnow()
        instanceList = self._getInstances()
        self.lastRunServer = self._getServerTimestamp(instanceList)
        
        # Update host config, if new list is fetched
        # Parse dictionary and add current timestamp and SID to data and log it
        if len(instanceList) != 0:
            self.providerInstance.state['hostConfig'] = instanceList
            currentTimestamp = self._getFormattedTimestamp()
            for instance in instanceList:
                instance['timestamp'] = currentTimestamp
                instance['serverTimestamp'] = self.lastRunServer.isoformat()
                instance['SID'] = self.providerInstance.sapSid
                instance['subdomain'] = self.providerInstance.sapSubdomain

        self.lastResult = instanceList

        # Update internal state
        if not self.updateState():
            raise Exception("[%s] failed to update state" % self.fullName)

        self.tracer.info("[%s] successfully fetched system instance list" % self.fullName)

    def _executeWebServiceRequest(self, apiName: str, filterFeatures: list, filterType: str, parser: Callable[[Any], list] = None) -> None:
        self.tracer.info("[%s] executing web service request: %s" % (self.fullName, apiName))
        self.lastRunLocal = datetime.utcnow()

        if parser is None:
            parser = self._parseResults

        # Use cached list of instances if available since they don't change that frequently; else fetch afresh
        if 'hostConfig' in self.providerInstance.state:
            sapInstances = self.providerInstance.state['hostConfig']
        else:
            sapInstances = self._getInstances()

        self.lastRunServer = self._getServerTimestamp(sapInstances)
        
        # Filter instances down to the ones that support this API
        sapInstances = self._filterInstances(sapInstances, filterFeatures, filterType)
        if len(sapInstances) == 0:
            self.tracer.info("[%s] no instances found that support this API: %s" % (self.fullName, apiName))

        # Call web service
        all_results = []
        currentTimestamp = self._getFormattedTimestamp()
        for instance in sapInstances:
            client = self.providerInstance.getClient(instance['hostname'], instance['httpsPort'], self.providerInstance.sapSubdomain)
            results = self.providerInstance.callSoapApi(client, apiName)
            if len(results) != 0:
                parsed_results = parser(results)
                for result in parsed_results:
                    result['hostname'] = instance['hostname']
                    result['instanceNr'] = instance['instanceNr']
                    result['subdomain'] = self.providerInstance.sapSubdomain
                    result['timestamp'] = currentTimestamp
                    result['serverTimestamp'] = self.lastRunServer.isoformat()
                    result['SID'] = self.providerInstance.sapSid
                all_results.extend(parsed_results)

        if len(all_results) == 0:
            self.tracer.info("[%s] no results found for: %s" % (self.fullName, apiName))
        self.lastResult = all_results

        # Update internal state
        if not self.updateState():
            raise Exception("[%s] failed to update state" % self.fullName)

        self.tracer.info("[%s] successfully processed web service request: %s" % (self.fullName, apiName))

    def _actionExecuteGenericWebServiceRequest(self, apiName: str, filterFeatures: list, filterType: str) -> None:
        self._executeWebServiceRequest(apiName, filterFeatures, filterType, self._parseResults)

    def _actionExecuteEnqGetStatistic(self, apiName: str, filterFeatures: list, filterType: str) -> None:
        self._executeWebServiceRequest(apiName, filterFeatures, filterType, self._parseResult)

    def generateJsonString(self) -> str:
        self.tracer.info("[%s] converting result to json string" % self.fullName)
        resultJsonString = json.dumps(self.lastResult, sort_keys=True, indent=4, cls=JsonEncoder)
        self.tracer.debug("[%s] resultJson=%s" % (self.fullName, str(resultJsonString)))
        return resultJsonString

    def updateState(self) -> bool:
        self.tracer.info("[%s] updating internal state" % self.fullName)
        self.state['lastRunLocal'] = self.lastRunLocal
        self.state['lastRunServer'] = self.lastRunServer
        self.tracer.info("[%s] internal state successfully updated" % self.fullName)
        return True
