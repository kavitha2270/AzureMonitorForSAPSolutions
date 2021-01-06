# Python modules
import json
import logging
from datetime import datetime, timedelta
from time import time
from typing import Any, Callable
import requests
from requests import Session
from requests.auth import HTTPBasicAuth

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

        self.sapOdataUsername = self.providerProperties.get("sapOdataUsername", None)
        if not self.sapOdataUsername:
            self.tracer.error("[%s] sapOdataUsername cannot be empty" % self.fullName)
            return False

        self.sapOdataPassword = self.providerProperties.get("sapOdataPassword", None)
        if not self.sapOdataPassword:
            self.tracer.error("[%s] sapOdataPassword cannot be empty" % self.fullName)
            return False

        self.sapOdataHttpsPortPrefix = self.providerProperties.get("sapOdataHttpsPortPrefix", None)
        if not self.sapOdataHttpsPortPrefix:
            self.tracer.error("[%s] sapOdataHttpsPortPrefix cannot be empty" % self.fullName)
            return False

        return True

    def _getHttpPortFromInstanceNr(self, instanceNr: str) -> str:
        return '5%s13' % instanceNr # As per SAP documentation, default http port is of the form 5<NR>13

    def _getHttpsPortFromInstanceNr(self, instanceNr: str) -> str:
        return '5%s14' % instanceNr # As per SAP documentation, default https port is of the form 5<NR>14

    def getMessageServerPortFromInstanceNr(self, instanceNr: str) -> str:
        return '81%s' % instanceNr # As per SAP documentation, default http port is of the form 81<NR>

    def getODataHttpsPortFromInstanceNr(self, instanceNr: str) -> str:
        return '%s%s' % (self.sapOdataHttpsPortPrefix, instanceNr)

    def getFullyQualifiedDomainName(self, hostname: str) -> str:
        if self.sapSubdomain:
            return hostname + "." + self.sapSubdomain
        else:
            return hostname

    def parseResult(self, result: object) -> list:
        return [helpers.serialize_object(result, dict)]

    def parseResults(self, results: list) -> list:
        return helpers.serialize_object(results, dict)

    """
    will first attempt to create SOAP client for hostname using the HTTPS port derived from the SAP instance number,
    and if that does not succeed will then try to create client using the derived HTTP port
    (if neither hostname or instance are specified, will default to the primary hostname/instance that the
    provider was initialized with from properties)
    """
    def getDefaultClient(self,
                         hostname: str = None,
                         instance: str = None) -> Client:
        if not hostname:
            hostname = self.sapHostName
        if not instance:
            instance = self.sapInstanceNr

        httpsPort = self._getHttpsPortFromInstanceNr(instance)
        httpPort = self._getHttpPortFromInstanceNr(instance)

        portList = [(httpsPort,"https"),(httpPort,"http")]
        exceptionDetails = None
        startTime = time()
        for port,protocol in portList:
            startTime = time()
            self.tracer.info("[%s] attempting to fetch default client for hostname=%s on %s port %s" % \
                         (self.fullName, hostname, protocol, port))
            try:
                client = self.getClient(hostname, httpProtocol=protocol, port=port)
                return client
            except Exception as e:
                exceptionDetails = e
                self.tracer.info("[%s] error fetching default client hostname=%s on %s port %s: %s [%d ms]" % \
                            (self.fullName, self.sapHostName, protocol, port, e, TimeUtils.getElapsedMilliseconds(startTime)))

        self.tracer.error("[%s] error fetching default client hostname=%s on port %s : %s [%d ms]" % \
                         (self.fullName, self.sapHostName, portList, exceptionDetails, TimeUtils.getElapsedMilliseconds(startTime)))
        raise exceptionDetails

    """
    attempt to create a SOAP client for the specified hostname using specific protocol and port
    (for when we already have a known hostconfig for this hostname, and already know whether HTTPS or HTTP should be used)
    """
    def getClient(self,
                  hostname: str,
                  httpProtocol: str,
                  port: str) -> Client:

        if not hostname or not httpProtocol or not port:
            raise Exception("[%s] cannot create client with empty httpProtocol, hostname or port (%s:%s:%s)" % \
                            (self.fullName, httpProtocol, hostname, port))

        if httpProtocol != "http" and httpProtocol != "https":
            raise Exception("[%s] httpProtocol %s is not valid for hostname: %s, port: %s" % \
                            (self.fullName, httpProtocol, hostname, port))

        hostname = self.getFullyQualifiedDomainName(hostname)
        url = '%s://%s:%s/?wsdl' % (httpProtocol, hostname, port)

        self.tracer.info("[%s] connecting to wsdl url: %s" % (self.fullName, url))

        startTime = time()
        try:
            session = Session()
            session.verify = False
            client = Client(url, transport=Transport(session=session))
            self.tracer.info("[%s] initialized SOAP client url: %s [%d ms]" % \
                             (self.fullName, url, TimeUtils.getElapsedMilliseconds(startTime)))

            return client
        except Exception as e:
            self.tracer.error("[%s] error fetching wsdl url: %s: %s [%d ms]" % \
                              (self.fullName, url, e, TimeUtils.getElapsedMilliseconds(startTime)))
            raise e

    def callSoapApi(self, client: Client, apiName: str) -> str:
        self.tracer.info("[%s] executing SOAP API: %s for wsdl: %s" % (self.fullName, apiName, client.wsdl.location))

        startTime = time()
        try:
            method = getattr(client.service, apiName)
            result = method()
            self.tracer.info("[%s] successful SOAP API: %s for wsdl: %s [%d ms]" % \
                             (self.fullName, apiName, client.wsdl.location, TimeUtils.getElapsedMilliseconds(startTime)))

            return result
        except Exception as e:
            self.tracer.error("[%s] error while calling SOAP API: %s for wsdl: %s: %s [%d ms]" % \
                             (self.fullName, apiName, client.wsdl.location, e, TimeUtils.getElapsedMilliseconds(startTime)))
            raise e

    def _getHosts(self) -> list:
        # Fetch last known list from storage. If storage does not have list, use provided
        # hostname and instanceNr
        if 'hostConfig' not in self.state:
            self.tracer.info("[%s] no host config persisted yet, using user-provided host name and instance nr" % self.fullName)
            hosts = [(self.sapHostName,
                      self.sapInstanceNr,
                      None,
                      None)]
        else:
            self.tracer.info("[%s] fetching last known host config" % self.fullName)
            currentHostConfig = self.state['hostConfig']
            hosts = [(hostConfig['hostname'],
                      hostConfig['instanceNr'],
                      "https" if (hostConfig['httpsPort'] and hostConfig['httpsPort'] != "0") \
                          else "http",
                      hostConfig['httpsPort'] if (hostConfig['httpsPort'] and hostConfig['httpsPort'] != "0") \
                          else hostConfig['httpPort']) for hostConfig in currentHostConfig]

        return hosts

    def getInstances(self) -> list:
        self.tracer.info("[%s] getting list of system instances" % self.fullName)

        startTime = time()

        instanceList = []
        hosts = self._getHosts()

        # Use last known hosts to fetch the updated list of hosts
        # Walk through the known hostnames and stop whenever any of them returns the list of all instances
        isSuccess = False
        for host in hosts:
            hostname, instanceNum, httpProtocol, port = host[0], host[1], host[2], host[3]

            try:
                apiName = 'GetSystemInstanceList'

                # if we have a cached host config with already defined protocol and port, then we can initialize
                # client directly from that, otherwise we have to instantiate client using ports derived from the instance number
                # which will try the derived HTTPS port first and then fallback to derived HTTP port
                if (not httpProtocol or not port):
                    client = self.getDefaultClient(hostname=hostname, instance=instanceNum)
                else:
                    client = self.getClient(hostname, httpProtocol, port)

                result = self.callSoapApi(client, apiName)
                instanceList = self.parseResults(result)
                isSuccess = True
                break
            except Exception as e:
                self.tracer.error("[%s] could not connect to SAP with hostname: %s and port: %s" % (self.fullName, hostname, port))

        if not isSuccess:
            raise Exception("[%s] could not connect to any SAP instances for provider: %s with hosts %s [%d ms]" % \
                (self.fullName, self.fullName, hosts, TimeUtils.getElapsedMilliseconds(startTime)))

        self.tracer.info("[%s] finished getting all system instances [%d ms]" % \
                         (self.fullName, TimeUtils.getElapsedMilliseconds(startTime)))

        return instanceList

    def filterInstances(self, sapInstances: list, filterFeatures: list, filterType: str) -> list:
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

    def validate(self) -> bool:
        self.tracer.info("[%s] connecting to sap to test SOAP API connectivity" % self.fullName)

        # HACK: Load content json to fetch the list of APIs in the checks
        self.initContent()

        try:
            client = self.getDefaultClient(hostname=self.sapHostName, instance=self.sapInstanceNr)
        except Exception as e:
            self.tracer.error("[%s] error occured while establishing connectivity to SAP server: %s " % (self.fullName, e))
            return False

        # SOAP API validations
        isValid = True
        soapChecks = [check for check in self.checks if check.actions[0]['type'] in \
            ['GetSystemInstanceList', 'ExecuteGenericWebServiceRequest', 'ExecuteEnqGetStatistic']]
        for check in soapChecks:
            apiName = check.name
            self.tracer.info("[%s] validating check: %s" % (self.fullName, apiName))

            # Ensure that all SOAP APIs in the checks are valid and are marked as unprotected.
            # Some APIs are compatible with only specific instance types and throw a Fault if run against
            # an incompatible one.
            # However, here we suppress all errors except Unauthorized since the Monitor phase takes
            # care of calling the API against the right instance type. As long as we don't get an
            # Unauthorized error, we know we can safely call them during the Monitor phase.
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

        if not isValid:
            return False

        # OData API validations
        instances = self.getInstances()
        instances = self.filterInstances(instances, ['ICMAN'], 'include')
        instance = instances[0]

        odataChecks = [check for check in self.checks if check.actions[0]['type'] in ['ExecuteODataServiceRequest']]
        for check in odataChecks:
            apiName = check.name
            self.tracer.info("[%s] validating check: %s" % (self.fullName, apiName))

            firstAction = check.actions[0]
            hostname = instance['hostname']
            hostname = self.getFullyQualifiedDomainName(hostname)
            port = self.getODataHttpsPortFromInstanceNr(str(instance['instanceNr']).zfill(2))
            apiPrefix = firstAction['parameters']['apiPrefix']

            # Pick a window starting from previous day's midnight. This provides enough buffer for the window end to have elapsed
            windowStartTime = datetime.combine(datetime.now().date(), datetime.min.time()) - timedelta(days = 1)
            windowEndTime = windowStartTime + timedelta(seconds = check.frequencySecs)
            date = windowStartTime.strftime('%Y-%m-%dT%H:%M:%S')
            windowStartTime = windowStartTime.strftime('PT%HH%MM%SS')
            windowEndTime = windowEndTime.strftime('PT%HH%MM%SS')

            url = "https://%s:%s/sap/opu/odata/SAP/%s/%s?$filter=Datum eq datetime'%s' \
                and Time ge time'%s' and Time le time'%s'&$format=json" \
                % (hostname, port, apiPrefix, apiName, date, windowStartTime, windowEndTime)

            self.tracer.info("[%s] making HTTP request to OData url: %s" % (self.fullName, url))
            try:
                requests.get(url, auth=HTTPBasicAuth(self.sapOdataUsername, self.sapOdataPassword), verify = False)
                self.tracer.info("[%s] validated api %s" % (self.fullName, apiName))
            except Exception as e:
                isValid = False
                self.tracer.error("[%s] error during validation of api %s: %s " % (self.fullName, apiName, e))

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

    def _getServerTimestamp(self, instances: list) -> datetime:
        self.tracer.info("[%s] fetching current timestamp from message server" % self.fullName)
        message_server_instances = self.providerInstance.filterInstances(instances, ['MESSAGESERVER'], 'include')
        date = datetime.fromisoformat(self._getFormattedTimestamp())

        # Get timestamp from the first message server that returns a valid date
        for instance in message_server_instances:
            hostname = instance['hostname']
            instanceNr = str(instance['instanceNr']).zfill(2)
            port = self.providerInstance.getMessageServerPortFromInstanceNr(instanceNr)
            hostname = self.providerInstance.getFullyQualifiedDomainName(hostname)
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
        instanceList = self.providerInstance.getInstances()
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

        # track latency of entire method excecution with dependencies
        startTime = time()

        if parser is None:
            parser = self.providerInstance.parseResults

        # Use cached list of instances if available since they don't change that frequently; else fetch afresh
        if 'hostConfig' in self.providerInstance.state:
            sapInstances = self.providerInstance.state['hostConfig']
        else:
            sapInstances = self.providerInstance.getInstances()

        self.lastRunServer = self._getServerTimestamp(sapInstances)

        # Filter instances down to the ones that support this API
        sapInstances = self.providerInstance.filterInstances(sapInstances, filterFeatures, filterType)
        if len(sapInstances) == 0:
            self.tracer.info("[%s] no instances found that support this API: %s" % (self.fullName, apiName))

        # Call web service
        all_results = []
        currentTimestamp = self._getFormattedTimestamp()
        for instance in sapInstances:
            # default to https unless the httpsPort was not defined, in which case fallback to http
            httpProtocol = "https"
            port = instance['httpsPort']
            if ((not port) or port == "0"):
                # fallback to http port instead
                httpProtocol = "http"
                port = instance['httpPort']

            client = self.providerInstance.getClient(instance['hostname'], httpProtocol, port)
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
            raise Exception("[%s] failed to update state for web service request: %s [%d ms]" % \
                            (self.fullName, apiName, TimeUtils.getElapsedMilliseconds(startTime)))

        self.tracer.info("[%s] successfully processed web service request: %s [%d ms]" % \
                         (self.fullName, apiName, TimeUtils.getElapsedMilliseconds(startTime)))

    def _actionExecuteGenericWebServiceRequest(self, apiName: str, filterFeatures: list, filterType: str) -> None:
        self._executeWebServiceRequest(apiName, filterFeatures, filterType, self.providerInstance.parseResults)

    def _actionExecuteEnqGetStatistic(self, apiName: str, filterFeatures: list, filterType: str) -> None:
        self._executeWebServiceRequest(apiName, filterFeatures, filterType, self.providerInstance.parseResult)

    def _actionExecuteODataServiceRequest(self, apiName: str, apiPrefix: str, filterFeatures: list, filterType: str) -> None:
        self.tracer.info("[%s] executing OData web service request: %s" % (self.fullName, apiName))
        self.lastRunLocal = datetime.utcnow()

        # track latency of entire method excecution with dependencies
        startTime = time()
        currentTimestamp = self._getFormattedTimestamp()

        ############ Local functions ############
        def getInstances() -> list:
            # Use cached list of instances if available since they don't change that frequently; else fetch afresh
            if 'hostConfig' in self.providerInstance.state:
                sapInstances = self.providerInstance.state['hostConfig']
            else:
                sapInstances = self.providerInstance.getInstances()

            # Filter instances down to the ones that support this API
            sapInstances = self.providerInstance.filterInstances(sapInstances, filterFeatures, filterType)

            return sapInstances

        def getQueryWindow(sapInstances) -> str:
            lastRunTime = self.state.get('lastRunServer', None)

            # If last run server time not available
            if lastRunTime is None:
                self.tracer.info("[%s] no last run time detected, picking a new window starting point" % self.fullName)
                # Set server time as the window end time
                windowEnd = self._getServerTimestamp(sapInstances)
                # Window size is frequency of action
                windowStart = windowEnd - timedelta(seconds = self.frequencySecs)
                # Clip start time to current day's midnight if it extends into previous day
                # since API supports fetching records for 1 specific day at a time
                currentMidnight = datetime.combine(windowEnd.date(), datetime.min.time())
                windowStart = max(windowStart, currentMidnight)
            else:
                self.tracer.info("[%s] found last run time, continuing next window from that point" % self.fullName)
                windowStart = lastRunTime
                windowEnd = windowStart + timedelta(seconds=self.frequencySecs)
                # Clip end time to next day's midnight if it extends into next day
                # since API supports fetching records for 1 specific day at a time
                nextMidnight = datetime.combine(windowStart.date(), datetime.min.time()) + timedelta(days = 1)
                windowEndTime = min(windowEnd, nextMidnight)

            date = datetime.combine(windowStart.date(), datetime.min.time()).strftime('%Y-%m-%dT%H:%M:%S')
            windowStartTime = windowStart.strftime('PT%HH%MM%SS')
            windowEndTime = windowEnd.strftime('PT%HH%MM%SS')
            self.tracer.info("[%s] query window: (date=%s, start=%s, end=%s)" \
                % (self.fullName, date, windowStartTime, windowEndTime))

            return date, windowStartTime, windowEndTime, windowStart, windowEnd

        def parseSapTime(date: str, time: str) -> str:
            date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
            time = datetime.strptime(time, 'PT%HH%MM%SS').time()
            timestamp = datetime.combine(date, time).isoformat()
            return timestamp

        #########################################

        # Get relevant list of instances
        sapInstances = getInstances()

        allResults = []

        date, windowStartTime, windowEndTime, _, lastRunServer = getQueryWindow(sapInstances)
        self.lastRunServer = lastRunServer

        if len(sapInstances) == 0:
            self.tracer.info("[%s] no instances found that support this API: %s" % (self.fullName, apiName))
        else:
            # Construct url
            # Pick the first hostname since all hosts refer to the same underlying OData API
            instance = sapInstances[0]
            hostname = instance['hostname']
            hostname = self.providerInstance.getFullyQualifiedDomainName(hostname)
            port = self.providerInstance.getODataHttpsPortFromInstanceNr(str(instance['instanceNr']).zfill(2))

            url = "https://%s:%s/sap/opu/odata/SAP/%s/%s?$filter=Datum eq datetime'%s' \
                   and Time ge time'%s' and Time le time'%s'&$format=json" \
                   % (hostname, port, apiPrefix, apiName, date, windowStartTime, windowEndTime)

            self.tracer.info("[%s] making HTTP request to OData url: %s" % (self.fullName, url))
            try:
                response = requests.get(url, auth=HTTPBasicAuth(self.providerInstance.sapOdataUsername, \
                    self.providerInstance.sapOdataPassword), verify = False)
                results = response.json()['d']['results']
                self.tracer.info("[%s] succesfully called OData API: %s [%d ms]" \
                    % (self.fullName, apiName, TimeUtils.getElapsedMilliseconds(startTime)))
            except Exception as e:
                self.tracer.error("[%s] error while calling OData API: %s : %s [%d ms]" % \
                                (self.fullName, apiName, e, TimeUtils.getElapsedMilliseconds(startTime)))
                raise e

            for result in results:
                result['hostname'] = instance['hostname']
                result['instanceNr'] = instance['instanceNr']
                result['httpsPort'] = port
                result['subdomain'] = self.providerInstance.sapSubdomain
                result['timestamp'] = currentTimestamp
                result['serverTimestamp'] = parseSapTime(date, result['Time'])
                result['windowStartTime'] = parseSapTime(date, windowStartTime)
                result['windowEndTime'] = parseSapTime(date, windowEndTime)
                result['SID'] = self.providerInstance.sapSid

            allResults.extend(results)

        if len(allResults) == 0:
            self.tracer.info("[%s] no results found for %s in query window: (date=%s, start=%s, end=%s)" \
                % (self.fullName, apiName, date, windowStartTime, windowEndTime))
        self.lastResult = allResults

        # Update internal state
        if not self.updateState():
            raise Exception("[%s] failed to update state for OData web service request: %s [%d ms]" % \
                            (self.fullName, apiName, TimeUtils.getElapsedMilliseconds(startTime)))

        self.tracer.info("[%s] successfully processed OData web service request: %s [%d ms]" % \
                         (self.fullName, apiName, TimeUtils.getElapsedMilliseconds(startTime)))

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
