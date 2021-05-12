# Python modules
import json
import logging
from datetime import datetime, timedelta, timezone
from time import time
from typing import Any, Callable
import re
import requests
from requests import Session
from threading import Lock

# SOAP Client modules
from zeep import Client
from zeep import helpers
from zeep.transports import Transport
from zeep.exceptions import Fault

# Payload modules
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

# soap client cache expiration, after which amount of time both successful + failed soap client instantiation attempts will be refreshed
SOAP_CLIENT_CACHE_EXPIRATIION = timedelta(minutes=10)

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
        self.sapLogonGroup = None

        # provider instance flag for whether RFC calls should be enabled for this specific Netweaver provider instance
        self._areRfcCallsEnabled = None

        # cache WSDL SOAP clients so we can re-use them across checks for the same provider and cut down off-box calls
        self._soapClientCache = {}

        # the RFC SDK does not allow client to specify a timeout and in fact appears to have a connection timeout of 60 secs. 
        # In cases where RFC calls timeout due to some misconfiguration, multiple retries can lead to metric gaps of several minutes.  
        # We are limiting retries here because it is extremely rare for SOAP or RFC call to fail on first attempt and succeed on retry,
        # as most of these failures are due to persistent issues.  Better to not waste limited time budget.
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
        self.sapSid = self.metadata.get("sapSid", "")
        if not self.sapSid:
            self.tracer.error("%s sapSid cannot be empty", self.fullName)
            return False

        # provider level common logging prefix
        self.logTag = "[%s][%s]" % (self.fullName, self.sapSid)

        self.sapHostName = self.providerProperties.get("sapHostName", None)
        if not self.sapHostName:
            self.tracer.error("%s sapHostName cannot be empty", self.logTag)
            return False

        instanceNr = self.providerProperties.get("sapInstanceNr", None)
        if instanceNr is None: # 0 is an acceptable value for Instance Number
            self.tracer.error("%s sapInstanceNr cannot be empty", self.logTag)
            return False
        if not type(instanceNr) is int or instanceNr < 0 or instanceNr > 98:
            self.tracer.error("%s sapInstanceNr can only be between 00 and 98 but %s was passed", self.logTag, str(instanceNr))
            return False
        self.sapInstanceNr = str(instanceNr).zfill(2)
        self.sapSubdomain = self.providerProperties.get("sapSubdomain", "")

        self.sapUsername = self.providerProperties.get('sapUsername', None)
        self.sapPassword = self.providerProperties.get('sapPassword', None)
        self.sapClientId = self.providerProperties.get('sapClientId', None)
        self.sapLogonGroup = self.providerProperties.get('sapLogonGroup',None)
        self.sapRfcSdkBlobUrl = self.providerProperties.get('sapRfcSdkBlobUrl', None)

        # if user did not specify password directly via UI, check to see if they instead
        # provided link to Key Vault secret
        if not self.sapPassword:
            sapPasswordKeyVaultUrl = self.providerProperties.get("sapPasswordKeyVaultUrl", None)
            if sapPasswordKeyVaultUrl:
                self.tracer.info("%s sapPassword key vault URL specified, attempting to fetch from %s", self.logTag, sapPasswordKeyVaultUrl)

                try:
                    keyVaultUrlPatternMatch = re.match(REGEX_EXTERNAL_KEYVAULT_URL,
                                                       sapPasswordKeyVaultUrl,
                                                       re.IGNORECASE)
                    keyVaultName = keyVaultUrlPatternMatch.group(1)
                    secretName = keyVaultUrlPatternMatch.group(2)
                except Exception as e:
                    self.tracer.error("%s invalid sapPassword Key Vault secret url format: %s", self.logTag, sapPasswordKeyVaultUrl)
                    return False
                
                try:
                    kv = AzureKeyVault(self.tracer, keyVaultName, self.ctx.msiClientId)
                    self.sapPassword = kv.getSecret(secretName, None).value

                    if not self.sapPassword:
                        raise Exception("failed to read sapPassword secret")
                except Exception as e:
                    self.tracer.error("%s error fetching sapPassword secret from keyVault url: %s, %s",
                                      self.logTag, 
                                      sapPasswordKeyVaultUrl, 
                                      e)
                    return False

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
            self.tracer.info("%s attempting to fetch default client for hostname=%s on %s port %s",
                             self.logTag, hostname, protocol, port)
            try:
                client = self.getClient(hostname, httpProtocol=protocol, port=port)
                return client
            except Exception as e:
                exceptionDetails = e
                self.tracer.info("%s error fetching default client hostname=%s on %s port %s: %s [%d ms]",
                                 self.logTag, self.sapHostName, protocol, port, e, TimeUtils.getElapsedMilliseconds(startTime))

        self.tracer.error("[%s] error fetching default client hostname=%s on port %s : %s [%d ms]",
                           self.logTag, self.sapHostName, portList, exceptionDetails, TimeUtils.getElapsedMilliseconds(startTime), exc_info=True)
        raise exceptionDetails

    """
    attempt to create a SOAP client for the specified hostname using specific protocol and port
    (for when we already have a known hostconfig for this hostname, and already know whether HTTPS or HTTP should be used)
    Store successful clients in cache so we don't make unnecessary WSDL fetchs for future API calls to the same instance
    """
    def getClient(self, 
                  hostname: str, 
                  httpProtocol: str, 
                  port: str,
                  useCache: bool = True) -> Client:

        if not hostname or not httpProtocol or not port:
            raise Exception("%s cannot create client with empty httpProtocol, hostname or port (%s:%s:%s)" % \
                            (self.logTag, httpProtocol, hostname, port))

        if httpProtocol != "http" and httpProtocol != "https":
            raise Exception("%s httpProtocol %s is not valid for hostname: %s, port: %s" % \
                            (self.logTag, httpProtocol, hostname, port))

        hostname = self.getFullyQualifiedDomainName(hostname)
        url = '%s://%s:%s/?wsdl' % (httpProtocol, hostname, port)

        if (useCache and url in self._soapClientCache):
            cacheEntry = self._soapClientCache[url]
            # respect cache expiration;  if cache is expired allow client to be refreshed below
            if (cacheEntry['expirationDateTime'] > datetime.utcnow()):
                if (cacheEntry['client']):
                    # self.tracer.info("%s using cached SOAP client for wsdl: %s", self.logTag, url)
                    return cacheEntry['client']
                else:
                    # previously cached soap client attempt was failure 
                    raise Exception("%s cached SOAP client failure for wsdl: %s" % (self.logTag, url))

        self.tracer.info("%s connecting to wsdl url: %s", self.logTag, url)

        startTime = time()
        client = None
        try:
            session = Session()
            session.verify = False
            client = Client(url, transport=Transport(session=session, timeout=SOAP_API_TIMEOUT_SECS, operation_timeout=SOAP_API_TIMEOUT_SECS))
            self.tracer.info("%s initialized SOAP client url: %s [%d ms]",
                             self.logTag, url, TimeUtils.getElapsedMilliseconds(startTime))
            return client
        except Exception as e:
            self.tracer.error("%s error fetching wsdl url: %s: %s [%d ms]",
                              self.logTag, url, e, TimeUtils.getElapsedMilliseconds(startTime), exc_info=True)
            raise e
        finally:
            # cache successsful and failed soap client attempts to reduce future API calls
            self._soapClientCache[url] = { 'client': client, 'expirationDateTime': datetime.utcnow() + SOAP_CLIENT_CACHE_EXPIRATIION }

    def callSoapApi(self, client: Client, apiName: str) -> str:
        self.tracer.info("%s executing SOAP API: %s for wsdl: %s", self.logTag, apiName, client.wsdl.location)

        startTime = time()
        try:
            method = getattr(client.service, apiName)
            result = method()
            self.tracer.info("%s successful SOAP API: %s for wsdl: %s [%d ms]",
                             self.logTag, apiName, client.wsdl.location, TimeUtils.getElapsedMilliseconds(startTime))

            return result
        except Exception as e:
            self.tracer.error("%s error while calling SOAP API: %s for wsdl: %s: %s [%d ms]",
                              self.logTag, apiName, client.wsdl.location, e, TimeUtils.getElapsedMilliseconds(startTime), exc_info=True)
            raise e

    """
    return a netweaver RFC client initialized with "MESSAGESERVER" instance we find
    for this SID.  
    """
    def getRfcClient(self, logTag: str) -> NetWeaverMetricClient:
        # RFC connections against direct application server instances can only be made to 'ABAP' instances
        dispatcherInstance = self.getMessageServerInstance()

        return MetricClientFactory.getMetricClient(tracer=self.tracer, 
                                                   logTag=logTag,
                                                   sapHostName=dispatcherInstance['hostname'],
                                                   sapSysNr=str(dispatcherInstance['instanceNr']),
                                                   sapSubdomain=self.sapSubdomain,
                                                   sapSid=self.sapSid,
                                                   sapClient=str(self.sapClientId),
                                                   sapLogonGroup = self.sapLogonGroup,
                                                   #sapLogonGroup = "Technical",
                                                   sapUsername=self.sapUsername,
                                                   sapPassword=self.sapPassword)

    def validate(self) -> bool:
        logTag = "[%s][%s][validation]" % (self.fullName, self.sapSid)

        # HACK: Load content json to fetch the list of APIs in the checks
        self.initContent()

        try:
            self._validateSoapClient()
        except Exception as e:
            self.tracer.error("%s SOAP API validation failure: %s", logTag, e, exc_info=True)
            return False

        try:
            self._validateRfcClient()
        except Exception as e:
            self.tracer.error("%s RFC client validation failure: %s", logTag, e, exc_info=True)
            return False

        return True

    """
    iterate through all SOAP API calls and attempt to validate that SOAP API client can be instantiated
    and expected APIs are callable
    """
    def _validateSoapClient(self) -> None:
        ###
        # TODO:  this entire function needs to be rethought to me more precise in terms of which instances
        #        are called for which APIs, as some APIs will not work for some function types.  
        ###

        logTag = "[%s][%s][validation]" % (self.fullName, self.sapSid)

        # hard-coded list of checks that correspond to SOAP API calls to validate
        soapApiChecks = ['GetSystemInstanceList', 
                         'GetProcessList', 
                         'ABAPGetWPTable',
                         'GetQueueStatistic',
                         'EnqGetStatistic']

        self.tracer.info("%s connecting to sap to validate SOAP API connectivity", logTag)

        try:
            client = self.getDefaultClient(hostname=self.sapHostName, instance=self.sapInstanceNr)
        except Exception as e:
            self.tracer.error("%s error occured while initializing SOAP client to SAP server: %s|%s, %s", 
                              logTag, 
                              self.sapHostName, 
                              self.sapInstanceNr, 
                              e, 
                              exc_info=True)
            raise

        # Ensure that all APIs in the checks are valid and are marked as unprotected.
        # Some APIs are compatible with only specific instance types and throw a Fault if run against
        # an incompatible one.
        # However, here we suppress all errors except Unauthorized since the Monitor phase takes
        # care of calling the API against the right instance type. As long as we don't get an
        # Unauthorized error, we know we can safely call them during the Monitor phase.
        isValid = True
        for check in self.checks:
            apiName = check.name

            if (apiName not in soapApiChecks):
                # this is not a SOAP API check
                continue

            method = getattr(client.service, apiName, None) # Returning None when API not found
            if method is None:
                self.tracer.error("%s SOAP client failure: api %s does not exist for %s", logTag, apiName, client.wsdl.location)
                isValid = False
            else:
                try:
                    self.callSoapApi(client, apiName)
                    self.tracer.info("%s validated SOAP api %s for %s", logTag, apiName, client.wsdl.location)
                except Fault as e:
                    if (e.code == "SOAP-ENV:Client" and e.message == "HTTP Error: 'Unauthorized'"):
                        isValid = False
                        self.tracer.error("%s SOAP api %s is protected for %s, %s ", logTag, apiName, client.wsdl.location, e, exc_info=True)
                    else:
                        self.tracer.error("%s suppressing error during validation of SOAP api %s for %s, %s", logTag, apiName, client.wsdl.location, e, exc_info=True)
                except Exception as e:
                    self.tracer.error("%s suppressing error during validation of SOAP api %s for %s, %s ", logTag, apiName, client.wsdl.location, e, exc_info=True)

        if (not isValid):
            raise Exception("%s one or more SOAP APIs failed validation" % (logTag))
 
    """
    if customer provided RFC SDK configuration, then validate that all required properties are specified
    and validate we can establish RFC client connections to APIs we need to call
    """
    def _validateRfcClient(self) -> None:
        logTag = "[%s][%s][validation]" % (self.fullName, self.sapSid)

        # are any RFC SDK config properties populated?
        if (not self.sapUsername or
            not self.sapPassword or
            not self.sapClientId or
            not self.sapRfcSdkBlobUrl):
            # customer has not chosen to enable RFC SDK, nothing to validate
            return

        # are ALL RFC SDK config properties populated?
        if (not self.sapUsername and
            not self.sapPassword and
            not self.sapClientId and
            not self.sapRfcSdkBlobUrl):
            # customer specified only partial set of config properties needed to enable RFC, so fail validation
            raise Exception("must specify all properties to enable RFC metric collection:  Username, Password, ClientId, and RfcSdkBlobUrl")

        if (not self.areRfcMetricsEnabled()):
            raise Exception("RFC SDK failed to install and is not usable")

        # initialize a client for the first healthy ABAP/Dispatcher instance we find
        client = self.getRfcClient(logTag=logTag)

        # update logging prefix with the specific instance details of the client
        sapHostnameStr = "%s|%s" % (client.Hostname, client.InstanceNr)
        
        # get metric query window to lookback 10 minutes to see if any results are available.  If not that probably
        # indicates customer has not enabled SMON on their SAP system
        self.tracer.info("%s attempting to fetch server timestamp from %s", logTag, sapHostnameStr)
        (startTime, endTime) = client.getQueryWindow(lastRunServerTime=None, 
                                                     minimumRunIntervalSecs=600)

        self.tracer.info("%s attempting to fetch SMON metrics from %s", logTag, sapHostnameStr)
        result = client.getSmonMetrics(startDateTime=startTime, endDateTime=endTime)
        self.tracer.info("%s successfully queried SMON metrics from %s", logTag, sapHostnameStr)

        self.tracer.info("%s attempting to fetch SWNC workload metrics from %s", logTag, sapHostnameStr)
        result = client.getSwncWorkloadMetrics(startDateTime=startTime, endDateTime=endTime)
        self.tracer.info("%s successfully queried SWNC workload metrics from %s", logTag, sapHostnameStr)

        self.tracer.info("%s attempting to fetch Short Dump metrics from %s", logTag, sapHostnameStr)
        result = client.getShortDumpsMetrics(startDateTime=startTime, endDateTime=endTime)
        self.tracer.info("%s successfully queried Short Dump metrics from %s", logTag, sapHostnameStr)

        self.tracer.info("%s successfully validated all known RFC SDK calls", logTag)

    """
    query SAP SOAP API to return list of all instances in the SID, but if caller specifies that cached results are okay
    and we have cached instance list with the provider instance, then just return the cached results
    """
    def getInstances(self, 
                     filterFeatures: list = None , 
                     filterType: str = None, 
                     useCache: bool = True) -> list:
        # Use cached list of instances if available since they should not change within a single monitor run;
        # but if cache is not available or if caller explicitly asks to skip cache then make the SOAP call
        if ('hostConfig' in self.state and useCache):
            # self.tracer.debug("%s using cached list of system instances", self.logTag)
            return self.filterInstancesByFeature(self.state['hostConfig'], filterFeatures=filterFeatures, filterType=filterType)

        self.tracer.info("%s getting list of system instances", self.logTag)
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
                instanceList = self._parseResults(result)

                # cache latest results in provider state
                self.state['hostConfig'] = instanceList

                isSuccess = True
                break
            except Exception as e:
                self.tracer.error("%s could not connect to SAP with hostname: %s and port: %s", self.logTag, hostname, port, exc_info=True)

        if not isSuccess:
            raise Exception("%s could not connect to any SAP instances with hosts %s [%d ms]" % \
                            (self.logTag, hosts, TimeUtils.getElapsedMilliseconds(startTime)))

        self.tracer.info("%s finished getting all system instances [%d ms]", self.logTag, TimeUtils.getElapsedMilliseconds(startTime))

        return self.filterInstancesByFeature(instanceList, filterFeatures=filterFeatures, filterType=filterType)

    """
    fetch cached instance list for this provider and filter down to the list 'ABAP' feature functions
    that are healthy (ie. have dispstatus attribute of 'SAPControl-GREEN').  Just return first in the list.
    """
    def getActiveDispatcherInstance(self):
        # Use cached list of instances if available since they don't change that frequently,
        # and filter down to only healthy dispatcher instances since RFC direct application server connection
        # only works against dispatchera
        dispatcherInstances = self.getInstances(filterFeatures=['ABAP'], filterType='include', useCache=True)
        healthyInstances = [instance for instance in dispatcherInstances if 'GREEN' in instance['dispstatus']]

        if (len(healthyInstances) == 0):
            raise Exception("No healthy ABAP/dispatcher instance found for %s" % self.sapSid)

        # return first healthy instance in list
        return healthyInstances[0]
    
    """
    fetch cached instance list for this provider and filter down to the list 'MESSAGESERVER' feature functions
    return the available message server
    """
    def getMessageServerInstance(self):
        # Use cached list of instances if available since they don't change that frequently,
        # and filter down to only healthy dispatcher instances since RFC direct application server connection
        # only works against dispatchera
        dispatcherInstances = self.getInstances(filterFeatures=['MESSAGESERVER'], filterType='include', useCache=True)
     
        # return first healthy instance in list
        return dispatcherInstances[0]
    
    """
    given a list of sap instances and a set of instance features (ie. functions) to include or exclude,
    apply filtering logic and return only those instances that match the filter conditions:
        'include' filter type will include any instance that matches any of the feature filters
        'exclude' filter type will exclude any instance that matches any of the feature filters
    """
    def filterInstancesByFeature(self, 
                                 sapInstances: list, 
                                 filterFeatures: list = None, 
                                 filterType: str = None) -> list:
        if (not filterFeatures or len(filterFeatures) == 0 or not sapInstances):
            return sapInstances
    
        self.tracer.info("%s filtering list of system instances based on features: %s", self.logTag, filterFeatures)

        instances = [(instance, instance['features'].split('|')) for instance in sapInstances]
       
        if filterType == "include":
            # Inclusion filter
            # Only include instances that match at least one of the filter features
            filtered_instances = [instance for (instance, instance_features) in instances \
                if not set(filterFeatures).isdisjoint(set(instance_features))]
        elif filterType == "exclude":
            # Exclusion filter
            # Only include instance that match none of the filter features
            filtered_instances = [instance for (instance, instance_features) in instances \
                if set(filterFeatures).isdisjoint(set(instance_features))]
        else:
            raise Exception("%s filterType '%s' is not supported filter type" % (self.logTag, filterType))

        return filtered_instances

    """
    helper method to deserialize result and return as list of dictionary objects
    """
    def _parseResults(self, results: list) -> list:
        return helpers.serialize_object(results, dict)

    """
    private method to return default provider hostname config (what customer provided at time netweaver provided was added)
    or a fully fleshed out list of <hostname / instance # / https:Port> tuples based on a previous cached call to getInstances()
    """
    def _getHosts(self) -> list:
        # Fetch last known list from storage. If storage does not have list, use provided
        # hostname and instanceNr
        if 'hostConfig' not in self.state:
            self.tracer.info("%s no host config persisted yet, using user-provided host name and instance nr", self.logTag)
            hosts = [(self.sapHostName,
                      self.sapInstanceNr,
                      None,
                      None)]
        else:
            self.tracer.info("%s fetching last known host config", self.logTag)
            currentHostConfig = self.state['hostConfig']
            hosts = [(hostConfig['hostname'], 
                      hostConfig['instanceNr'], 
                      "https" if (hostConfig['httpsPort'] and hostConfig['httpsPort'] != "0") else "http", 
                      hostConfig['httpsPort'] if (hostConfig['httpsPort'] and hostConfig['httpsPort'] != "0") else hostConfig['httpPort']) for hostConfig in currentHostConfig]

        return hosts

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
                self.tracer.info("%s Netweaver RFC calls disabled for because missing one or more required " +
                                 "config properties: sapUsername, sapPassword, sapClientId, and sapRfcSdkBlobUrl",
                                 self.logTag)
                self._areRfcCallsEnabled = False
                return False

            # only attempt to install RFC SDK once per process execution
            if (sapNetweaverProviderInstance._isRfcInstalled == None):
                sapNetweaverProviderInstance._isRfcInstalled = self._trySetupRfcSdk()
                
            self._areRfcCallsEnabled = sapNetweaverProviderInstance._isRfcInstalled
            
            return self._areRfcCallsEnabled

        except Exception as e:
            self.tracer.error("%s Exception trying to check if rfc sdk metrics are enabled, %s", self.logTag, e, exc_info=True)
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
                self.tracer.info("%s No user provided RFC SDK blob url, will not leverage RFC SDK. quitting...", self.logTag)
                return False

            installer = SapRfcSdkInstaller(tracer=self.tracer, installPath=PATH_RFC_SDK_INSTALL)

            # environment variables must be initialized before RFC and pyrfc installation can be validated
            self.tracer.info("%s initializing RFC SDK environment...", self.logTag)
            if (not installer.initRfcSdkEnvironment()):
                self.tracer.error("%s failed to initialize rfc sdk environment pre-requisites", self.logTag)
                return False

            # if we are able to successfully import the pyrfc connector module, that means RFC SDK
            # libraries must be installed and were able to be found by pyrfc package initialization,
            # so no need to do any further checks.
            if (installer.isPyrfcModuleUsable()):
                # pyrfc package is usable, which means RFC SDK is already installed and environment configured correctly
                self.tracer.info("%s Pyrfc module is usable, RFC calls will be enabled", self.logTag)
                return True

            # if pyrfc module cannot be imported, check to see if it is even installed.  Assumption is that
            # pyrfc module is installed as part of container image, so if it is missing something is wrong
            # there is no need to even try to install the RFC SDK
            if (not installer.isPyrfcModuleInstalled()):
                self.tracer.error("%s Pyrfc module is not installed, RFC calls will be disabled", self.logTag)
                return False

            # check last sdk install attempt time so we can limit how often we retry
            # to download and install SDK on persistent failures (eg. no more than once every 30 mins)
            lastSdkInstallAttemptTime = installer.getLastSdkInstallAttemptTime()
            if (lastSdkInstallAttemptTime > (datetime.now(timezone.utc) - MINIMUM_RFC_INSTALL_RETRY_INTERVAL)):
                self.tracer.info("%s last RFC SDK install attempt was %s, minimum attempt retry %s, skipping...",
                                 self.logTag,
                                 lastSdkInstallAttemptTime, 
                                 MINIMUM_RFC_INSTALL_RETRY_INTERVAL)
                return False

            self.tracer.info("%s RFC SDK is not installed, so attempt installation now...", self.logTag)
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
                self.tracer.error("%s User provided RFC SDK blob does not exist %s, skipping...", self.logTag, self.sapRfcSdkBlobUrl)
                return False
            
            self.tracer.info("%s user provided RFC SDK blob exists for download %s, lastModified=%s",
                             self.logTag, self.sapRfcSdkBlobUrl, packageLastModifiedTime)
            
            # the user provided sdk blob exists, so before we download compare the last_modified timestamp
            # with the last modified time of the last download attempt.  If nothing has changed, 
            # then no need to try and download the package again
            # TODO:  confirm, should we go ahead and try to re-download previously failed packages
            #        once every 30 minutes anyway?  just in case failure was something external?
            lastInstallPackageModifiedTime = installer.getLastSdkInstallPackageModifiedTime()

            if (packageLastModifiedTime == lastInstallPackageModifiedTime):
                self.tracer.info("%s rfc sdk download package has not been modified since last download " +
                                 "attempt (last_modified=%s), will not download again",
                                 self.logTag, 
                                 lastInstallPackageModifiedTime)
                return False
            
            self.tracer.info("%s user provided rfc sdk package last_modified (%s) has changed " + 
                             "since last install attempt (%s), attempting to re-download and install",
                             self.logTag,
                             packageLastModifiedTime,
                             lastInstallPackageModifiedTime)

            # try to download user provided RFC SDK blob, install to local system and configure necessary
            # environment variables and system settings so that it can be usable by pyrfc module
            if (not installer.downloadAndInstallRfcSdk(blobUrl=self.sapRfcSdkBlobUrl, storageAccount=blobStorageAccount)):
                self.tracer.error("%s failed to download and install rfc sdk package, RFC calls will not be enabled...", self.logTag)
                return False

            # on Linux pyrfc module may not be usable upon first install attempt, as it appears that unpacking
            # libraries to the LD_LIBRARY_PATH env variable after the python process starts may not pick up the change.
            # The module should be usable on the next sapmon process run.
            if (not installer.isPyrfcModuleUsable()):
                self.tracer.error("%s pyrfc module still not usable after RFC SDK install (might require process restart), " + 
                                  "RFC calls will not be enabled...", 
                                  self.logTag)
                return False

            self.tracer.info("%s pyrfc module is usable after RFC SDK install, RFC calls will be enabled...", self.logTag)
            return True

        except Exception as e:
            self.tracer.error("%s exception trying to setup and validate RFC SDK, RFC calls will be disabled: %s", self.logTag, e, exc_info=True)

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

        # provider check common logging prefix
        self.logTag = "[%s][%s]" % (self.fullName, self.providerInstance.sapSid)

    def _getFormattedTimestamp(self) -> str:
        return datetime.utcnow().isoformat()

    def _parseResult(self, result: object) -> list:
        return [helpers.serialize_object(result, dict)]

    def _parseResults(self, results: list) -> list:
        return helpers.serialize_object(results, dict)

    def _getServerTimestamp(self) -> datetime:
        self.tracer.info("%s fetching current timestamp from message server", self.logTag)

        message_server_instances = self.providerInstance.getInstances(filterFeatures=['MESSAGESERVER'], filterType='include', useCache=True)
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
                self.tracer.info("%s received message server %s header: %s, parsed time: %s",
                                 self.logTag, 
                                 message_server_endpoint, 
                                 response.headers['date'],
                                 date)
                break
            except Exception as e:
                self.tracer.info("%s suppressing expected error while fetching server time during HTTP GET request to url %s: %s ",
                                 self.logTag, message_server_endpoint, e)
        return date

    def _actionGetSystemInstanceList(self) -> None:
        self.tracer.info("%s refreshing list of system instances", self.logTag)
        self.lastRunLocal = datetime.utcnow()
        # when performing the actual provider check action, always fetch fressh instance list snapshot and refresh the cache
        instanceList = self.providerInstance.getInstances(useCache=False)
        self.lastRunServer = self._getServerTimestamp()

        # Update host config, if new list is fetched
        # Parse dictionary and add current timestamp and SID to data and log it
        if len(instanceList) != 0:
            currentTimestamp = self._getFormattedTimestamp()
            for instance in instanceList:
                instance['timestamp'] = currentTimestamp
                instance['serverTimestamp'] = self.lastRunServer.isoformat()
                instance['SID'] = self.providerInstance.sapSid
                instance['subdomain'] = self.providerInstance.sapSubdomain

        self.lastResult = instanceList

        # Update internal state
        if not self.updateState():
            raise Exception("%s failed to update state" % self.logTag)

        self.tracer.info("%s successfully fetched system instance list", self.logTag)

    def _executeWebServiceRequest(self, apiName: str, filterFeatures: list, filterType: str, parser: Callable[[Any], list] = None) -> None:
        self.tracer.info("[%s] executing web service request: %s" % (self.fullName, apiName))
        self.lastRunLocal = datetime.utcnow()

        # track latency of entire method excecution with dependencies
        startTime = time()

        if parser is None:
            parser = self._parseResults

        # Use cached list of instances if available since they don't change that frequently; else fetch afresh.
        # filter down to just the instances we need for this SOAP API type
        sapInstances = self.providerInstance.getInstances(useCache=True, filterFeatures=filterFeatures, filterType=filterType)

        self.lastRunServer = self._getServerTimestamp()

        if len(sapInstances) == 0:
            self.tracer.info("%s no instances found that support this API: %s", self.logTag, apiName)

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
                if(apiName == "GetProcessList"):
                    results = self._sanitizeGetProcessList(results)
                elif(apiName == "ABAPGetWPTable"):
                    results = self._sanitizeABAPGetWPTable(results)
            except Exception as e:
                self.tracer.error("%s unable to call the Soap Api %s - %s://%s:%s, %s", self.logTag, apiName, httpProtocol, instance['hostname'], port, e, exc_info=True)
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
            self.tracer.info("%s no results found for: %s", self.logTag, apiName)
        self.lastResult = all_results

        # Update internal state
        if not self.updateState():
            raise Exception("[%s] failed to update state for web service request: %s [%d ms]" % \
                            (self.logTag, apiName, TimeUtils.getElapsedMilliseconds(startTime)))

        self.tracer.info("%s successfully processed web service request: %s [%d ms]",
                         self.logTag, apiName, TimeUtils.getElapsedMilliseconds(startTime))

    def _actionExecuteGenericWebServiceRequest(self, apiName: str, filterFeatures: list, filterType: str) -> None:
        self._executeWebServiceRequest(apiName, filterFeatures, filterType, self._parseResults)

    def _actionExecuteEnqGetStatistic(self, apiName: str, filterFeatures: list, filterType: str) -> None:
        self._executeWebServiceRequest(apiName, filterFeatures, filterType, self._parseResult)

    """
    Method to parse the value based on the key provided and set the values with None value to empty string ''
    """
    def _getKeyValue(self, dictionary, key, apiName):
            if key not in dictionary:
                raise ValueError("Result received for api %s does not contain key: %s"% (apiName, key))
            if(dictionary[key] == None):
                dictionary[key] = ""
            return dictionary[key]

    """
    Method to parse the results from ABAPGetWPTable and set the strings with None value to empty string ''
    """
    def _sanitizeABAPGetWPTable(self, records: list) -> list:
       apiName = "ABAPGetWPTable"
       processed_results = list()
       for record in records:
            processed_result = {
                "Action": self._getKeyValue(record, 'Action', apiName),
                "Client": self._getKeyValue(record, 'Client', apiName),
                "Cpu": self._getKeyValue(record, 'Cpu', apiName),
                "Err": self._getKeyValue(record, 'Err', apiName),
                "No": self._getKeyValue(record, 'No', apiName),
                "Pid": self._getKeyValue(record, 'Pid', apiName),
                "Program": self._getKeyValue(record, 'Program', apiName),
                "Reason": self._getKeyValue(record, 'Reason', apiName),
                "Sem": self._getKeyValue(record, 'Sem', apiName),
                "Start": self._getKeyValue(record, 'Start', apiName),
                "Status": self._getKeyValue(record, 'Status', apiName),
                "Table": self._getKeyValue(record, 'Table', apiName),
                "Time": self._getKeyValue(record, 'Time', apiName),
                "Typ": self._getKeyValue(record, 'Typ', apiName),
                "User": self._getKeyValue(record, 'User', apiName)
            }
            processed_results.append(processed_result)
       return processed_results

    """
    Method to parse the results from GetProcessList and set the strings with None value to empty string ''
    """
    def _sanitizeGetProcessList(self, records: list) -> list:
       apiName = "GetProcessList"
       processed_results = list()
       for record in records:
            processed_result = {
                "description": self._getKeyValue(record, 'description', apiName),
                "dispstatus": self._getKeyValue(record, 'dispstatus', apiName),
                "elapsedtime": self._getKeyValue(record, 'elapsedtime', apiName),
                "name": self._getKeyValue(record, 'name', apiName),
                "pid": self._getKeyValue(record, 'pid', apiName),
                "starttime": self._getKeyValue(record, 'starttime', apiName),
                "textstatus": self._getKeyValue(record, 'textstatus', apiName)
            }
            processed_results.append(processed_result)
       return processed_results

    """
    netweaver provider check action to query for SDF/SMON Analysis Run metrics
    """
    def _actionGetSmonAnalysisMetrics(self) -> None:
        # base class will always call generateJsonString(), so we must always be sure to set the lastResult
        # regardless of success or failure
        self.lastResult = []

        try:
            # initialize hostname log string here to default of SID in case we cannot identify a specific dispatcher host
            sapHostnameStr = self.providerInstance.sapSid

            if (not self.providerInstance.areRfcMetricsEnabled()):
                self.tracer.info("%s Skipping SMON metrics because RFC SDK metrics not enabled...", self.logTag)
                return

            # track latency of entire method excecution with dependencies
            latencyStartTime = time()
            
            # initialize a client for the first healthy ABAP/Dispatcher instance we find
            client = self.providerInstance.getRfcClient(logTag=self.logTag)

            # update logging prefix with the specific instance details of the client
            sapHostnameStr = "%s|%s" % (client.Hostname, client.InstanceNr)
            
            # get metric query window based on our last successful query where results were returned
            (startTime, endTime) = client.getQueryWindow(lastRunServerTime=self.lastRunServer, 
                                                         minimumRunIntervalSecs=self.frequencySecs)
            self.lastResult = client.getSmonMetrics(startDateTime=startTime, endDateTime=endTime)

            self.tracer.info("%s successfully queried SMON metrics for %s [%d ms]", 
                             self.logTag, sapHostnameStr, TimeUtils.getElapsedMilliseconds(latencyStartTime))
            self.lastRunLocal = datetime.now(timezone.utc)
            self.lastRunServer = endTime

            # only update state on successful query attempt
            self.updateState()

        except Exception as e:
            self.tracer.error("%s exception trying to fetch SMON Analysis Run metrics for %s [%d ms], error: %s", 
                              self.logTag,
                              sapHostnameStr,
                              TimeUtils.getElapsedMilliseconds(latencyStartTime),
                              e,
                              exc_info=True)
            raise
    
    """
    netweaver provider check action to query for SWNC workload statistics and decorate with ST03 metric calculations
    """
    def _actionGetSwncWorkloadMetrics(self) -> None:
        # base class will always call generateJsonString(), so we must always be sure to set the lastResult
        # regardless of success or failure
        self.lastResult = []

        try:
            # initialize hostname log string here to default of SID in case we cannot identify a specific dispatcher host
            sapHostnameStr = self.providerInstance.sapSid

            if (not self.providerInstance.areRfcMetricsEnabled()):
                self.tracer.info("%s Skipping SWNC metrics because RFC SDK metrics not enabled...", self.logTag)
                return

            # track latency of entire method excecution with dependencies
            latencyStartTime = time()

            # initialize a client for the first healthy ABAP/Dispatcher instance we find
            client = self.providerInstance.getRfcClient(logTag=self.logTag)

            # update logging prefix with the specific instance details of the client
            sapHostnameStr = "%s|%s" % (client.Hostname, client.InstanceNr)
            
            # get metric query window based on our last successful query where results were returned
            (startTime, endTime) = client.getQueryWindow(lastRunServerTime=self.lastRunServer, 
                                                         minimumRunIntervalSecs=self.frequencySecs)

            self.lastResult = client.getSwncWorkloadMetrics(startDateTime=startTime, endDateTime=endTime)

            self.tracer.info("%s successfully queried SWNC workload metrics for %s [%d ms]", 
                             self.logTag, sapHostnameStr, TimeUtils.getElapsedMilliseconds(latencyStartTime))
            self.lastRunLocal = datetime.now(timezone.utc)
            self.lastRunServer = endTime

            # only update state on successful query attempt
            self.updateState()

        except Exception as e:
            self.tracer.error("%s exception trying to fetch SWNC workload metrics for %s [%d ms], error: %s",
                              self.logTag,
                              sapHostnameStr,
                              TimeUtils.getElapsedMilliseconds(latencyStartTime),
                              e,
                              exc_info=True)
            raise
    
      
    """
    netweaver provider check action to query for short dumps workload statistics
    """
    def _actionGetShortDumpsMetrics(self) -> None:
        # base class will always call generateJsonString(), so we must always be sure to set the lastResult
        # regardless of success or failure
        self.lastResult = []

        try:
            # initialize hostname log string here to default of SID in case we cannot identify a specific dispatcher host
            sapHostnameStr = self.providerInstance.sapSid

            if (not self.providerInstance.areRfcMetricsEnabled()):
                self.tracer.info("%s Skipping short dumps metrics because RFC SDK metrics not enabled...", self.logTag)
                return

            # track latency of entire method excecution with dependencies
            latencyStartTime = time()

            # initialize a client for the first healthy ABAP/Dispatcher instance we find
            client = self.providerInstance.getRfcClient(logTag=self.logTag)

            # update logging prefix with the specific instance details of the client
            sapHostnameStr = "%s|%s" % (client.Hostname, client.InstanceNr)
            
            # get metric query window based on our last successful query where results were returned
            (startTime, endTime) = client.getQueryWindow(lastRunServerTime=self.lastRunServer, 
                                                         minimumRunIntervalSecs=self.frequencySecs)

            self.lastResult = client.getShortDumpsMetrics(startDateTime=startTime, endDateTime=endTime)

            self.tracer.info("%s successfully queried short dumps workload metrics for %s [%d ms]", 
                             self.logTag, sapHostnameStr, TimeUtils.getElapsedMilliseconds(latencyStartTime))
            self.lastRunLocal = datetime.now(timezone.utc)
            self.lastRunServer = endTime

            # only update state on successful query attempt
            self.updateState()

        except Exception as e:
            self.tracer.error("%s exception trying to fetch short dumps workload metrics for %s [%d ms], error: %s",
                              self.logTag,
                              sapHostnameStr,
                              TimeUtils.getElapsedMilliseconds(latencyStartTime),
                              e,
                              exc_info=True)
            raise
    
    def generateJsonString(self) -> str:
        self.tracer.info("%s converting result to json string", self.logTag)
        if self.lastResult is not None and len(self.lastResult) != 0:
            for result in self.lastResult:
                result['SAPMON_VERSION'] = PAYLOAD_VERSION
                result['PROVIDER_INSTANCE'] = self.providerInstance.name
                result['METADATA'] = self.providerInstance.metadata
    
        resultJsonString = json.dumps(self.lastResult, sort_keys=True, indent=4, cls=JsonEncoder)
        self.tracer.debug("%s resultJson=%s", self.logTag, str(resultJsonString))
        return resultJsonString

    def updateState(self) -> bool:
        self.tracer.info("%s updating internal state", self.logTag)
        self.state['lastRunLocal'] = self.lastRunLocal
        self.state['lastRunServer'] = self.lastRunServer
        self.tracer.info("%s internal state successfully updated", self.logTag)
        return True
