# 
#       Azure Monitor for SAP Solutions - Payload
#       (to be deployed on collector VM)
#
#       License:        GNU General Public License (GPL)
#       (c) 2020        Microsoft Corp.
#

# Python modules
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import time
import os

from collections import OrderedDict
import decimal
from binascii import hexlify
import json
import logging
import logging.config
from typing import Callable, Dict, Optional

from azure.mgmt.storage import StorageManagementClient
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient, KeyVaultSecret

from netweaver.rfcsdkinstaller import SapRfcSdkInstaller
from netweaver.metricclientfactory import MetricClientFactory, NetWeaverMetricClient

#from netweaver.rfcclient import NetWeaverRfcClient

from const import *
from helper.azure import AzureStorageAccount, AzureInstanceMetadataService, AzureKeyVault
from helper.tools import JsonDecoder, JsonEncoder

# Trace levels
DEFAULT_CONSOLE_TRACE_LEVEL = logging.DEBUG
DEFAULT_FILE_TRACE_LEVEL    = logging.DEBUG
DEFAULT_QUEUE_TRACE_LEVEL   = logging.DEBUG

###############################################################################
class JsonFormatter(logging.Formatter):
   def __init__(self,
                fieldMapping: Dict[str, str] = {},
                datefmt: str = None,
                customJson: json.JSONEncoder = None):
      logging.Formatter.__init__(self, None, datefmt)
      self.fieldMapping = fieldMapping
      self.customJson = customJson

   # Overridden from the parent class to look for the asctime attribute in the fields attribute
   def usesTime(self) -> bool:
      return "asctime" in self.fieldMapping.values()

   # Formats time using a specific date format
   def _formatTime(self,
                   record: logging.LogRecord) -> None:
      if self.usesTime():
         record.asctime = self.formatTime(record, self.datefmt)

   # Combines any supplied fields with the log record msg field into an object to convert to JSON
   def _getJsonData(self,
                    record: logging.LogRecord) -> OrderedDict():
      if len(self.fieldMapping.keys()) > 0:
         # Build a temporary list of tuples with the actual content for each field
         jsonContent = []
         for f in sorted(self.fieldMapping.keys()):
            jsonContent.append((f, getattr(record, self.fieldMapping[f])))
         jsonContent.append(("msg", record.msg))

         # An OrderedDict is used to ensure that the converted data appears in the same order for every record
         return OrderedDict(jsonContent)
      else:
         return record.msg

   # Overridden from the parent class to take a log record and output a JSON-formatted string
   def format(self,
              record: logging.LogRecord) -> str:
      self._formatTime(record)
      jsonData = self._getJsonData(record)
      formattedJson = json.dumps(jsonData, cls=self.customJson)
      return formattedJson

class tracing:
   config = {
       "version": 1,
       "disable_existing_loggers": True,
       "formatters": {
           "json": {
               "class": "rfctest.JsonFormatter",
               "fieldMapping": {
                   "pid": "process",
                   "timestamp": "asctime",
                   "traceLevel": "levelname",
                   "module": "filename",
                   "lineNum": "lineno",
                   "function": "funcName",
                   # Custom (payload-specific) fields below
                   "payloadVersion": "payloadversion",
                   "sapmonId": "sapmonid"
               }
           },
           "detailed": {
               "format": "[%(process)d] %(asctime)s %(levelname).1s %(filename)s:%(lineno)d %(message)s"
           },
           "simple": {
               "format": "%(levelname)-8s %(message)s"
           }
       },
       "handlers": {
           "consolex": {
               "class": "logging.StreamHandler",
               "formatter": "detailed",
               "level": DEFAULT_CONSOLE_TRACE_LEVEL
           },
           "console": {
               "class": "logging.StreamHandler",
               "formatter": "detailed",
               "level": DEFAULT_CONSOLE_TRACE_LEVEL
           },
           "file": {
               "class": "logging.handlers.RotatingFileHandler",
               "formatter": "detailed",
               "level": DEFAULT_FILE_TRACE_LEVEL,
               "filename": FILENAME_TRACE,
               "maxBytes": 10000000,
               "backupCount": 10
           },
       },
       "root": {
           "level": logging.DEBUG,
           "handlers": ["console", "file"]
           #"handlers": ["console"]
       }
   }

   # Initialize the tracer object
   @staticmethod
   def initTracer() -> logging.Logger:
      logging.config.dictConfig(tracing.config)
      return logging.getLogger(__name__)

# Main function with argument parser
def main() -> None:

    global tracer
    print("__file__: %s", __file__)

    minimumRfcInstallAttemptInterval = timedelta(seconds=10)
    
    sapClient="300"
    sapUsername = None
    sapPassword = None
    keyVaultName = None

    accountKey = None
    msiClientId = None
    authToken = None

    if (os.name == "nt"):
        sapmonId = '6c4e81332331d0'
        sapSubdomain = 'redmond.corp.microsoft.com'
        subscriptionId = '7ba9d7af-9b3e-452b-9161-e4138f054f0c'
        resourceGroupName = 'SAP_POC_AMS'
        sdkBlobUrl = "https://sapmonsto6c4e81332331d0.blob.core.windows.net/sap-netweaver-sdk/nwrfc750P_7-70002755.win.zip"
    else:
        sapmonId = '75853a6503011a'
        sapSubdomain = 'redmond.corp.microsoft.com'
        subscriptionId = 'cf814bf2-d425-4b7f-bba2-e815cef7cba7'
        resourceGroupName = 'sapmon-rg-75853a6503011a'
        keyVaultName = "sapmon-kv-75853a6503011a"
        sdkBlobUrl = "https://sapmonsto75853a6503011a.blob.core.windows.net/sap-netweaver-rfc-sdk/nwrfc750P_7-70002752.zip"

    tracer = tracing.initTracer()
    tracer.info("Logging initialized %s", datetime.now())

    if (not accountKey):
        tracer.info("attempting to fetch MSI clientId")
        authToken, msiClientId = AzureInstanceMetadataService.getAuthToken(tracer)
        tracer.info("msiClientId=%s", msiClientId)

    blobStorageAccount = AzureStorageAccount(tracer=tracer,
                                        sapmonId=sapmonId,
                                        msiClientId=msiClientId,
                                        subscriptionId=subscriptionId,
                                        resourceGroup=resourceGroupName,
                                        accountKey=accountKey)

    if (keyVaultName and msiClientId):
        kv = AzureKeyVault(tracer, kvName=keyVaultName, msiClientId=msiClientId)
        configText = kv.getSecret(secretId='SapNetwweaver')
        configData = json.loads(configText, object_hook=JsonDecoder.datetimeHook)

        sapUsername = configData['properties']['sapOdataUsername']
        sapPassword = configData['properties']['sapOdataPassword']

    if (not sapUsername or not sapPassword):
        tracer.error("Could not load sapUsername or sapPassword, quitting")
        return
        
    if (os.name == "nt"):
        installPath = 'c:\\temp\\sdk-installpath'
    else:
        installPath = '/home/mfrei/sdk-installpath'

    installer = SapRfcSdkInstaller(tracer=tracer, installPath=installPath)

    tracer.info("initializing RFC SDK environment...")
    installer.initializeRfcSdkEnvironment()

    if (not sdkBlobUrl):
        tracer.info("No user provided RFC SDK blob url, will not leverage RFC SDK. quitting...")
        return

    if (installer.isPyrfcModuleUsable()):
        tracer.info("Pyrfc module is usable")
    else:
        # check last sdk install attempt time so we can limit how often we try
        # to reinstall on persistent failures
        lastSdkInstallAttemptTime = installer.getLastSdkInstallAttemptTime()
        if (lastSdkInstallAttemptTime > (datetime.utcnow() - minimumRfcInstallAttemptInterval)):
            tracer.info("last RFC SDK install attempt was %s, minimum attempt retry %s, skipping...",
                        lastSdkInstallAttemptTime, 
                        minimumRfcInstallAttemptInterval)
            return

        if (not installer.isRfcSdkInstalled()):
            tracer.info("RFC SDK is not installed, so attempt installation now...")

            packageExists, packageLastModifiedTime = installer.isRfcSdkAvailableForDownload(
                blobUrl=sdkBlobUrl, 
                storageAccount=blobStorageAccount)

            if (not packageExists):
                tracer.info("User provided RFC SDK blob does not exist %s, quitting...", sdkBlobUrl)
                return
            
            tracer.info("user provided RFC SDK blob exists for download %s, lastModified=%s", 
                        sdkBlobUrl, packageLastModifiedTime)
            
            # user provided sdk blob exists for download, compare the last_modified timestamp
            # with the last modified time of the last download attempt.  If nothing has changed, 
            # then no need to try again
            lastInstallPackageModifiedTime = installer.getLastSdkInstallPackageModifiedTime()

            if (packageLastModifiedTime == lastInstallPackageModifiedTime):
                tracer.info("rfc sdk download package has not been modified since last download " +
                            "attempt (last_modified=%s), will not download again",
                            lastInstallPackageModifiedTime)
                return
            
            tracer.info("user provided rfc sdk package last_modified (%s) has changed " + 
                        "since laset package download (%s), attempting to download and install",
                        packageLastModifiedTime,
                        lastInstallPackageModifiedTime)

            if (not installer.downloadAndInstallRfcSdk(blobUrl=sdkBlobUrl, storageAccount=blobStorageAccount)):
                tracer.info("failed to install rfc sdk package, quitting...")
                return
            
        if (not installer.isPyrfcModuleInstalled()):

            # check last module install attempt time so we can limit how often we try
            # to reinstall on persistent failures
            lastModuleInstallAttemptTime = installer.getLastPyrfcModuleInstallAttemptTime()
            if (lastModuleInstallAttemptTime > (datetime.utcnow() - minimumRfcInstallAttemptInterval)):
                tracer.info("last pyrfc module install attempt was %s, minimum attempt retry %s, skipping...",
                        lastModuleInstallAttemptTime, 
                        minimumRfcInstallAttemptInterval)
                return

            tracer.info("pyrfc python module not installed, attempting to install now")
            if (not installer.installPyrfcModule()):
                tracer.info("pyrfc module failed to install, quitting...")
                return

            tracer.info("pyrfc module install successful")
            if (not installer.isPyrfcModuleUsable()):
                tracer.info("pyrfc module should be usable on next run")
        
    hosts = [
         ("MSX", "sapsbx00", "30"), 
        ("AST", "adstst", "20"), 
        ("MSX", "sapsbx00", "31")]

    for hostTuple in [hosts[0]]:
        sid = hostTuple[0]
        hostname = hostTuple[1]
        instanceNr = hostTuple[2]

        tracer.info("initializing client for hostname:%s, instanceNr:%s", hostname, instanceNr)
        client = MetricClientFactory.getMetricClient(tracer=tracer, 
                                                    logTag="rfctest_logTag",
                                                    sapHostName=hostname,
                                                    sapSubdomain=sapSubdomain,
                                                    sapSysNr=instanceNr,
                                                    sapSid=sid,
                                                    sapClient=sapClient,
                                                    sapUsername=sapUsername,
                                                    sapPassword=sapPassword,
                                                    columnFilterList=None,
                                                    serverTimeZone=None)

        if (not client):
            tracer.info("client was not initialized")
            return
    
        lastRunTime = (datetime.now() - timedelta(seconds=300))
        runFrequencySecs = 60

        currentServerTime = client.getServerTime() 
        tracer.info("[host:%s] Current Server Time: %s", hostname, currentServerTime)

        (startTime, endTime) = client.getQueryWindow(lastRunTime=lastRunTime, minimumRunIntervalSecs=runFrequencySecs)
        tracer.info("[host:%s] startTime: %s, endTime: %s", hostname, startTime, endTime)

        smonMetrics = client.getSmonMetrics(startDateTime=startTime, endDateTime=endTime)
        tracer.info("[host:%s] SMON metrics:  \n%s", hostname, json.dumps(smonMetrics, indent=4, cls=JsonEncoder))

        swncMetrics = client.getSwncWorkloadMetrics(startDateTime=startTime, endDateTime=endTime)
        tracer.info("[host:%s] SWNC metrics:  \n%s", hostname, json.dumps(swncMetrics, indent=4, cls=JsonEncoder))

    print("done")

    return

if __name__ == "__main__":
    main()

