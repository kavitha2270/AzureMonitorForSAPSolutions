# Python modules
import json
import logging
from datetime import datetime, timedelta, timezone
from time import time
from typing import Any, Callable
import requests
from requests import Session
from threading import Lock

# SOAP Client modules
from zeep import Client
from zeep import helpers
from zeep.transports import Transport
from zeep.exceptions import Fault

# Payload modules
from const import PAYLOAD_VERSION
from const import *
from helper.azure import AzureStorageAccount
from helper.context import *
from helper.tools import *
from provider.base import ProviderInstance, ProviderCheck
from netweaver.metricclientfactory import NetWeaverMetricClient, MetricClientFactory
from netweaver.rfcsdkinstaller import PATH_RFC_SDK_INSTALL, SapRfcSdkInstaller
from typing import Dict

# Suppress SSLError warning due to missing SAP server certificate
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# wait time in between attempts to re-download and install RFC SDK package if we have a download blob
# URL defined and previous install attempt was not successful
MINIMUM_RFC_INSTALL_RETRY_INTERVAL = timedelta(minutes=30)

# timeout to use for all SOAP WSDL fetch and other API calls
SOAP_API_TIMEOUT_SECS = 5

class sapNetweaverProviderInstance(ProviderInstance):
    # static / class variables to enforce singleton behavior around rfc sdk installation attempts across all 
    # instances of SAP Netweaver provider
    _isRfcInstalled = None
    _rfcInstallerLock = Lock()

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

        # RFC SDK call settings
        self.sapUsername = None
        self.sapPassword = None
        self.sapClientId = None
        self.sapRfcSdkBlobUrl = None

        # provider instance flag for whether RFC calls should be enabled for this specific Netweaver provider instance
        self._areRfcCallsEnabled = None

        retrySettings = {
            "retries": 1,
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
        if instanceNr is None: # 0 is an acceptable value for Instance Number
            self.tracer.error("[%s] sapInstanceNr cannot be empty" % self.fullName)
            return False
        if not type(instanceNr) is int or instanceNr < 0 or instanceNr > 98:
            self.tracer.error("[%s] sapInstanceNr can only be between 00 and 98 but %s was passed" % (self.fullName, str(instanceNr)))
            return False
        self.sapInstanceNr = str(instanceNr).zfill(2)
        self.sapSubdomain = self.providerProperties.get("sapSubdomain", "")
        self.sapSid = self.metadata.get("sapSid", "")
        if not self.sapSid:
            self.tracer.error("[%s] sapSid cannot be empty" % self.fullName)
            return False

        self.sapUsername = self.providerProperties.get('sapUsername', None)
        self.sapPassword = self.providerProperties.get('sapPassword', None)
        self.sapClientId = self.providerProperties.get('sapClientId', None)
        self.sapRfcSdkBlobUrl = self.providerProperties.get('sapRfcSdkBlobUrl', None)

        return True

    def _getHttpPortFromInstanceNr(self, instanceNr: str) -> str:
        return '5%s13' % instanceNr # As per SAP documentation, default http port is of the form 5<NR>13

    def _getHttpsPortFromInstanceNr(self, instanceNr: str) -> str:
        return '5%s14' % instanceNr # As per SAP documentation, default https port is of the form 5<NR>14

    def getMessageServerPortFromInstanceNr(self, instanceNr: str) -> str:
        return '81%s' % instanceNr # As per SAP documentation, default http port is of the form 81<NR>

    def getFullyQualifiedDomainName(self, hostname: str) -> str:
        if self.sapSubdomain:
            return hostname + "." + self.sapSubdomain
        else:
            return hostname

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
            client = Client(url, transport=Transport(session=session, timeout=SOAP_API_TIMEOUT_SECS, operation_timeout=SOAP_API_TIMEOUT_SECS))
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

    def validate(self) -> bool:
        self.tracer.info("[%s] connecting to sap to test SOAP API connectivity" % self.fullName)

        # HACK: Load content json to fetch the list of APIs in the checks
        self.initContent()

        try:
            client = self.getDefaultClient(hostname=self.sapHostName, instance=self.sapInstanceNr)
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

    """
    returns flag to indicate whether provider checks should attempt to use RFC SDK client calls to fetch certain metrics.
    First time may perform fairly expensive checks to validate if RFC SDK is installed anc configured, and may attempt
    to download user provided blob to install to local system.  We only want to attempt this at most once per process,
    so first caller to this function will pay that cost and the resulting success/failure flag will be cached.
    """
    def areRfcMetricsEnabled(self) -> bool:
        if self._areRfcCallsEnabled != None:
            # the flag for whether RFC is usable has already been initialzed, so return 
            return self._areRfcCallsEnabled

        # there may be 1..N sapNetWeaverProviderInstance instances per sapmon process, and each instance
        # may choose to enable/disable RFC calls individually, but we should only attempt to install the 
        # RFC SDK at most once per process.  Use a static/class variable to determine if installation 
        # attempt has already been attempted and was success/failure, and do all this inside of 
        # a lock and cache flag for future checks
        try:
            # class singleton lock
            sapNetweaverProviderInstance._rfcInstallerLock.acquire(blocking=True)

            # check -> lock -> check
            if (self._areRfcCallsEnabled != None):
                # flag was initialized prior to obtaining the lock
                return self._areRfcCallsEnabled

            # ensure this provider instance has necessary config settings to enable RFC SDK calls
            if (not self.sapUsername or
                not self.sapPassword or
                not self.sapClientId or
                not self.sapRfcSdkBlobUrl):
                self.tracer.info("Netweaver RFC calls disabled for %s|%s because missing one or more required " +
                                 "config properties: sapUsername, sapPassword, sapClientId, and sapRfcSdkBlobUrl",
                                 self.sapSid, 
                                 self.sapHostName)
                self._areRfcCallsEnabled = False
                return False

            # only attempt to install RFC SDK once per process execution
            if (sapNetweaverProviderInstance._isRfcInstalled == None):
                sapNetweaverProviderInstance._isRfcInstalled = self._trySetupRfcSdk()
                
            self._areRfcCallsEnabled = sapNetweaverProviderInstance._isRfcInstalled
            
            return self._areRfcCallsEnabled

        except Exception as e:
            self.tracer.error("Exception trying to check if rfc sdk metrics are enabled for %s|%s, %s",
                              self.sapSid, 
                              self.sapHostName,
                              e)
            sapNetweaverProviderInstance._isRfcInstalled = False
            self._areRfcCallsEnabled = False

        finally:
            sapNetweaverProviderInstance._rfcInstallerLock.release()

        return False
    
    """
    validate that RFC SDK package has been installed and configured correctly and is usable by pyrfc module.
    If pyrfc module cannot be imported, then potentially attempt to download RFC SDK blob, install to local system,
    and configure necessary environment variables and system settings so that the libraries can be
    successfully loaded by the pyrfc module.  
    Returns flag indicating whether pyrfc module can be imnported (ie. whether RFC calls can be enabled)

    Pre-requisites for RFC SDK installation attempt:
    1.) Customer provided config property sapRfcSdkBlobUrl must be non-empty.
    2.) python module for "pynwrfc" must be installed
    3.) was the last failed SDK installation attempt more than N minutes ago (defined by MINIMUM_RFC_INSTALL_RETRY_INTERVAL)
    4.) does the sapRfcSdkBlobUrl provided by customer actually exist in the storage account
    5.) was the last_modified timestamp on the sapRfcSdkBlobUrl blob modified since the last failed installation attempt
    """
    def _trySetupRfcSdk(self) -> bool:
        try:
            # if no RFC SDK download blob url specified, treat as kill switch to disable any RFC calls
            if (not self.sapRfcSdkBlobUrl):
                self.tracer.info("No user provided RFC SDK blob url, will not leverage RFC SDK. quitting...")
                return False

            installer = SapRfcSdkInstaller(tracer=self.tracer, installPath=PATH_RFC_SDK_INSTALL)

            # environment variables must be initialized before RFC and pyrfc installation can be validated
            self.tracer.info("initializing RFC SDK environment...")
            if (not installer.initRfcSdkEnvironment()):
                self.tracer.error("failed to initialize rfc sdk environment pre-requisites")
                return False

            # if we are able to successfully import the pyrfc connector module, that means RFC SDK
            # libraries must be installed and were able to be found by pyrfc package initialization,
            # so no need to do any further checks.
            if (installer.isPyrfcModuleUsable()):
                # pyrfc package is usable, which means RFC SDK is already installed and environment configured correctly
                self.tracer.info("Pyrfc module is usable, RFC calls will be enabled")
                return True

            # if pyrfc module cannot be imported, check to see if it is even installed.  Assumption is that
            # pyrfc module is installed as part of container image, so if it is missing something is wrong
            # there is no need to even try to install the RFC SDK
            if (not installer.isPyrfcModuleInstalled()):
                self.tracer.error("Pyrfc module is not installed, RFC calls will be disabled")
                return False

            # check last sdk install attempt time so we can limit how often we retry
            # to download and install SDK on persistent failures (eg. no more than once every 30 mins)
            lastSdkInstallAttemptTime = installer.getLastSdkInstallAttemptTime()
            if (lastSdkInstallAttemptTime > (datetime.now(timezone.utc) - MINIMUM_RFC_INSTALL_RETRY_INTERVAL)):
                self.tracer.info("last RFC SDK install attempt was %s, minimum attempt retry %s, skipping...",
                                 lastSdkInstallAttemptTime, 
                                 MINIMUM_RFC_INSTALL_RETRY_INTERVAL)
                return False

            self.tracer.info("RFC SDK is not installed, so attempt installation now...")
            blobStorageAccount = AzureStorageAccount(tracer=self.tracer,
                                                     sapmonId=self.ctx.sapmonId,
                                                     msiClientId=self.ctx.msiClientId,
                                                     subscriptionId=self.ctx.vmInstance["subscriptionId"],
                                                     resourceGroup=self.ctx.vmInstance["resourceGroupName"])
    
            # first check that rfc sdk download blob exists in Azure Storage account, and if it 
            # exixts also fetch the last_modified timestamp metadata
            doesPackageExist, packageLastModifiedTime = installer.isRfcSdkAvailableForDownload(
                blobUrl=self.sapRfcSdkBlobUrl, 
                storageAccount=blobStorageAccount)

            if (not doesPackageExist):
                self.tracer.error("User provided RFC SDK blob does not exist %s, skipping...", self.sapRfcSdkBlobUrl)
                return False
            
            self.tracer.info("user provided RFC SDK blob exists for download %s, lastModified=%s",
                        self.sapRfcSdkBlobUrl, packageLastModifiedTime)
            
            # the user provided sdk blob exists, so before we download compare the last_modified timestamp
            # with the last modified time of the last download attempt.  If nothing has changed, 
            # then no need to try and download the package again
            # TODO:  confirm, should we go ahead and try to re-download previously failed packages
            #        once every 30 minutes anyway?  just in case failure was something external?
            lastInstallPackageModifiedTime = installer.getLastSdkInstallPackageModifiedTime()

            if (packageLastModifiedTime == lastInstallPackageModifiedTime):
                self.tracer.info("rfc sdk download package has not been modified since last download " +
                                 "attempt (last_modified=%s), will not download again",
                                 lastInstallPackageModifiedTime)
                return False
            
            self.tracer.info("user provided rfc sdk package last_modified (%s) has changed " + 
                             "since last install attempt (%s), attempting to re-download and install",
                             packageLastModifiedTime,
                             lastInstallPackageModifiedTime)

            # try to download user provided RFC SDK blob, install to local system and configure necessary
            # environment variables and system settings so that it can be usable by pyrfc module
            if (not installer.downloadAndInstallRfcSdk(blobUrl=self.sapRfcSdkBlobUrl, storageAccount=blobStorageAccount)):
                self.tracer.error("failed to download and install rfc sdk package, RFC calls will not be enabled...")
                return False

            # on Linux pyrfc module may not be usable upon first install attempt, as it appears that unpacking
            # libraries to the LD_LIBRARY_PATH env variable after the python process starts may not pick up the change.
            # The module should be usable on the next sapmon process run.
            if (not installer.isPyrfcModuleUsable()):
                self.tracer.error("pyrfc module still not usable after RFC SDK install (might require process restart), " + 
                                  "RFC calls will not be enabled...")
                return False

            self.tracer.info("pyrfc module is usable after RFC SDK install, RFC calls will be enabled...")
            return True

        except Exception as e:
            self.tracer.error("exception trying to setup and validate RFC SDK, RFC calls will be disabled: %s", e)

        return False


###########################
class sapNetweaverProviderCheck(ProviderCheck):
    lastResult = []

    def __init__(self,
        provider: ProviderInstance,
        **kwargs
    ):
        super().__init__(provider, **kwargs)
        self.lastRunLocal = None
        self.lastRunServer = None

    def _getFormattedTimestamp(self) -> str:
        return datetime.utcnow().isoformat()

    def _getHosts(self) -> list:
        # Fetch last known list from storage. If storage does not have list, use provided
        # hostname and instanceNr
        if 'hostConfig' not in self.providerInstance.state:
            self.tracer.info("[%s] no host config persisted yet, using user-provided host name and instance nr" % self.fullName)
            hosts = [(self.providerInstance.sapHostName,
                      self.providerInstance.sapInstanceNr,
                      None,
                      None)]
        else:
            self.tracer.info("[%s] fetching last known host config" % self.fullName)
            currentHostConfig = self.providerInstance.state['hostConfig']
            hosts = [(hostConfig['hostname'], 
                      hostConfig['instanceNr'], 
                      "https" if (hostConfig['httpsPort'] and hostConfig['httpsPort'] != "0") else "http", 
                      hostConfig['httpsPort'] if (hostConfig['httpsPort'] and hostConfig['httpsPort'] != "0") else hostConfig['httpPort']) for hostConfig in currentHostConfig]

        return hosts

    def _parseResult(self, result: object) -> list:
        return [helpers.serialize_object(result, dict)]

    def _parseResults(self, results: list) -> list:
        return helpers.serialize_object(results, dict)

    """
    query SAP SOAP API to return list of all instances in the SID, but if caller specifies that cached results are okay
    and we have cached instance list with the provider instance, then just return the cached results
    """
    def _getInstances(self, useCache: bool = True) -> list:
        self.tracer.info("[%s] getting list of system instances" % self.fullName)

        # Use cached list of instances if available since they should not change within a single monitor run;
        # but if cache is not available or if caller explicitly asks to skip cache then make the SOAP call
        if ('hostConfig' in self.providerInstance.state and useCache):
            self.tracer.info("[%s] using cached list of system instances", self.fullName)
            return self.providerInstance.state['hostConfig']

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
                    client = self.providerInstance.getDefaultClient(hostname=hostname, instance=instanceNum)
                else:
                    client = self.providerInstance.getClient(hostname, httpProtocol, port)

                result = self.providerInstance.callSoapApi(client, apiName)
                instanceList = self._parseResults(result)
                isSuccess = True
                break
            except Exception as e:
                self.tracer.error("[%s] could not connect to SAP with hostname: %s and port: %s" % (self.fullName, hostname, port))

        if not isSuccess:
            raise Exception("[%s] could not connect to any SAP instances for provider: %s with hosts %s [%d ms]" % \
                (self.fullName, self.providerInstance.fullName, hosts, TimeUtils.getElapsedMilliseconds(startTime)))

        self.tracer.info("[%s] finished getting all system instances [%d ms]" % \
                         (self.fullName, TimeUtils.getElapsedMilliseconds(startTime)))

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
                # 'Thu, 04 Mar 2021 05:02:12 GMT'
                # NOTE: we don't need to follow redirects because the redirect response itself 300-3XX
                # will have the 'date' header as well.  In some cases we were following a chain
                # of redirects that would terminate in a 404, which would not have the 'date' header
                response = requests.get(message_server_endpoint, allow_redirects=False)

                if ('date' not in response.headers):
                    raise Exception("no 'date' response header found for response status:%s/%s from:%s"
                                    % (response.status_code, response.reason, message_server_endpoint))

                date = datetime.strptime(response.headers['date'], '%a, %d %b %Y %H:%M:%S %Z')
                self.tracer.info("[%s] received message server %s header: %s, parsed time: %s",
                                 self.fullName, 
                                 message_server_endpoint, 
                                 response.headers['date'],
                                 date)
                break
            except Exception as e:
                self.tracer.info("[%s] suppressing expected error while fetching server time during HTTP GET request to url %s: %s " % (self.fullName, message_server_endpoint, e))
        return date

    def _actionGetSystemInstanceList(self) -> None:
        self.tracer.info("[%s] refreshing list of system instances" % self.fullName)
        self.lastRunLocal = datetime.utcnow()
        # when performing the actual provider check action, always fetch fressh instance list snapshot and refresh the cache
        instanceList = self._getInstances(useCache=False)
        self.lastRunServer = self._getServerTimestamp(instanceList)

        # Update host config, if new list is fetched
        # Parse dictionary and add current timestamp and SID to data and log it
        if len(instanceList) != 0:
            # update instances cache with fresh instanceList
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
            parser = self._parseResults

        # Use cached list of instances if available since they don't change that frequently; else fetch afresh
        sapInstances = self._getInstances(useCache=True)

        self.lastRunServer = self._getServerTimestamp(sapInstances)

        # Filter instances down to the ones that support this API
        sapInstances = self._filterInstances(sapInstances, filterFeatures, filterType)
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

            results = []
            try:
                client = self.providerInstance.getClient(instance['hostname'], httpProtocol, port)
                results = self.providerInstance.callSoapApi(client, apiName)
            except Exception as e:
                self.tracer.error("[%s] unable to call the Soap Api %s - %s://%s:%s, %s", self.fullName, apiName, httpProtocol, instance['hostname'], port, e)
                continue

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
        self._executeWebServiceRequest(apiName, filterFeatures, filterType, self._parseResults)

    def _actionExecuteEnqGetStatistic(self, apiName: str, filterFeatures: list, filterType: str) -> None:
        self._executeWebServiceRequest(apiName, filterFeatures, filterType, self._parseResult)

    """
    fetch cached instance list for this provider and filter down to the list 'ABAP' feature functions
    that are healthy (ie. have dispstatus attribute of 'SAPControl-GREEN').  Just return first in the list.
    """
    def _getActiveDispatcherInstance(self):
        # Use cached list of instances if available since they don't change that frequently,
        # and filter down to only healthy dispatcher instances since RFC direct application server connection
        # only works against dispatchera
        instances = self._getInstances(useCache=True)
        dispatcherInstances = self._filterInstances(instances, ['ABAP'], 'include')
        healthyInstances = [instance for instance in dispatcherInstances if 'GREEN' in instance['dispstatus']]

        if (len(healthyInstances) == 0):
            raise Exception("No healthy ABAP/dispatcher instance found for %s" % self.providerInstance.sapSid)

        # return first healthy instance in list
        return healthyInstances[0]

    """
    netweaver provider check action to query for SDF/SMON Analysis Run metrics
    """
    def _actionGetSmonAnalysisMetrics(self) -> None:
        result = []
        try:
            # initialize hostname log string here to default of SID in case we cannot identify a specific dispatcher host
            sapHostnameStr = self.providerInstance.sapSid

            if (not self.providerInstance.areRfcMetricsEnabled()):
                self.tracer.info("[%s] Skipping SMON metrics for %s because RFC SDK metrics not enabled...", 
                                 self.fullName, self.providerInstance.sapSid)
                return
            
            # RFC connections against direct application server instances can only be made to 'ABAP' instances
            dispatcherInstance = self._getActiveDispatcherInstance()

            # create simple SAP host|SID|instance string for consistent logging calls below
            sapHostnameStr = "%s|%s|%s" % (dispatcherInstance['hostname'],
                                           self.providerInstance.sapSid,
                                           dispatcherInstance['instanceNr'])

            self.tracer.info("attempting to fetch SMON metrics from %s", sapHostnameStr)
            client = MetricClientFactory.getMetricClient(tracer=self.tracer, 
                                                        logTag=self.fullName,
                                                        sapHostName=dispatcherInstance['hostname'],
                                                        sapSysNr=str(dispatcherInstance['instanceNr']),
                                                        sapSubdomain=self.providerInstance.sapSubdomain,
                                                        sapSid=self.providerInstance.sapSid,
                                                        sapClient=self.providerInstance.sapClientId,
                                                        sapUsername=self.providerInstance.sapUsername,
                                                        sapPassword=self.providerInstance.sapPassword)
            
            # get metric query window based on our last successful query where results were returned
            (startTime, endTime) = client.getQueryWindow(lastRunServerTime=self.lastRunServer, 
                                                         minimumRunIntervalSecs=self.frequencySecs)
            result = client.getSmonMetrics(startDateTime=startTime, endDateTime=endTime)

            self.tracer.info("[%s] successfully queried SMON metrics for %s", self.fullName, sapHostnameStr)
            self.lastRunLocal = datetime.now(timezone.utc)
            self.lastRunServer = endTime

            # only update state on successful query attempt
            self.updateState()

        except Exception as e:
            self.tracer.error("[%s] exception trying to fetch SMON Analysis Run metrics for %s, error: %s", 
                              self.fullName, 
                              sapHostnameStr,
                              e)
            raise
        finally:
            # base class will always call generateJsonString(), so we must always be sure to set the lastResult
            # regardless of success or failure
            self.lastResult = result
    
    """
    netweaver provider check action to query for SWNC workload statistics and decorate with ST03 metric calculations
    """
    def _actionGetSwncWorkloadMetrics(self) -> None:
        result = []
        try:
            # initialize hostname log string here to default of SID in case we cannot identify a specific dispatcher host
            sapHostnameStr = self.providerInstance.sapSid

            if (not self.providerInstance.areRfcMetricsEnabled()):
                self.tracer.info("[%s] Skipping SWNC metrics for %s because RFC SDK metrics not enabled...",
                                 self.fullName, self.providerInstance.sapSid)
                return

            # RFC connections against direct application server instances can only be made to 'ABAP' instances
            dispatcherInstance = self._getActiveDispatcherInstance()

            # create simple SAP host|SID|instance string for consistent logging calls below
            sapHostnameStr = "%s|%s|%s" % (dispatcherInstance['hostname'],
                                           self.providerInstance.sapSid,
                                           dispatcherInstance['instanceNr'])

            self.tracer.info("attempting to fetch SWNC workload metrics from %s", sapHostnameStr)
            client = MetricClientFactory.getMetricClient(tracer=self.tracer, 
                                                        logTag=self.fullName,
                                                        sapHostName=dispatcherInstance['hostname'],
                                                        sapSysNr=str(dispatcherInstance['instanceNr']),
                                                        sapSubdomain=self.providerInstance.sapSubdomain,
                                                        sapSid=self.providerInstance.sapSid,
                                                        sapClient=self.providerInstance.sapClientId,
                                                        sapUsername=self.providerInstance.sapUsername,
                                                        sapPassword=self.providerInstance.sapPassword)
            
            # get metric query window based on our last successful query where results were returned
            (startTime, endTime) = client.getQueryWindow(lastRunServerTime=self.lastRunServer, 
                                                         minimumRunIntervalSecs=self.frequencySecs)

            result = client.getSwncWorkloadMetrics(startDateTime=startTime, endDateTime=endTime)

            self.tracer.info("[%s] successfully queried SWNC workload metrics for %s", self.fullName, sapHostnameStr)
            self.lastRunLocal = datetime.now(timezone.utc)
            self.lastRunServer = endTime

            # only update state on successful query attempt
            self.updateState()

        except Exception as e:
            self.tracer.error("[%s] exception trying to fetch SWNC workload metrics for %s, error: %s",
                              self.fullName, 
                              sapHostnameStr,
                              e)
            raise
        finally:
            # base class will always call generateJsonString(), so we must always be sure to set the lastResult
            # regardless of success or failure
            self.lastResult = result


    def generateJsonString(self) -> str:
        self.tracer.info("[%s] converting result to json string" % self.fullName)
        if self.lastResult is not None and len(self.lastResult) != 0:
            for result in self.lastResult:
                result['SAPMON_VERSION'] = PAYLOAD_VERSION
        resultJsonString = json.dumps(self.lastResult, sort_keys=True, indent=4, cls=JsonEncoder)
        self.tracer.debug("[%s] resultJson=%s" % (self.fullName, str(resultJsonString)))
        return resultJsonString

    def updateState(self) -> bool:
        self.tracer.info("[%s] updating internal state" % self.fullName)
        self.state['lastRunLocal'] = self.lastRunLocal
        self.state['lastRunServer'] = self.lastRunServer
        self.tracer.info("[%s] internal state successfully updated" % self.fullName)
        return True
