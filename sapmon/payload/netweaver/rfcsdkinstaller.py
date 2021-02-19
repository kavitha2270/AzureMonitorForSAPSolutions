# Python modules
import json
import logging
import os
import subprocess
import sys
import zipfile
from datetime import datetime, date, timedelta, time
from typing import Dict, List

from azure.mgmt.storage import StorageManagementClient
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobClient
from six import reraise

from const import *
from helper.tools import JsonEncoder, JsonDecoder
from helper.azure import AzureStorageAccount

PATH_USER_LIB_ROOT = os.path.join(PATH_ROOT, "userlibs")
PATH_RFC_SDK_ROOT  = os.path.join(PATH_USER_LIB_ROOT, "nwrfcsdk")

LINUX_LD_CONFIG_FILE = '/etc/ld.so.conf.d/nwrfcsdk.conf'
RFC_SDK_INSTALLATION_STATE_FILE = 'rfc-sdk-installation-state.json'
PYRFC_MODULE_INSTALLATION_STATE_FILE = 'pyrfc-module-installation-state.json'

# relative path to sdk extraction folder (used for SAPNWRFC_HOME environment variable)
RFC_HOME_RELATIVE_PATH = 'nwrfcsdk'

# relative path to sdk extraction folder where rfc sdk library files are found
RFC_LIBRARY_RELATIVE_PATH = os.path.join(RFC_HOME_RELATIVE_PATH, 'lib')

# expected RFC SDK library file names
if (os.name == "nt"):
    # windows
    RFC_SDK_EXPECTED_FILES = ['libicudecnumber.dll', 'libsapucum.dll', 'sapnwrfc.dll']
else:
    # linux
    RFC_SDK_EXPECTED_FILES = ['libicudecnumber.so', 'libsapucum.so', 'libsapnwrfc.so']

class SapRfcSdkInstaller:

    def __init__(self,
                 tracer: logging.Logger,
                 installPath: str):
        self.tracer = tracer
        self.installPath = installPath
        self.rfcHomePath = os.path.join(self.installPath, RFC_HOME_RELATIVE_PATH)
        self.libPath = os.path.join(self.installPath, RFC_LIBRARY_RELATIVE_PATH)
        self.sdkInstallStateFilePath = os.path.join(self.installPath, RFC_SDK_INSTALLATION_STATE_FILE)
        self.pyrfcModuleInstallStateFilepath = os.path.join(self.installPath, PYRFC_MODULE_INSTALLATION_STATE_FILE)

    """
    set process level environment variables and define linux library loading config (if needed).
    If RFC SDK files are installed on system, then setting up these environment settings will
    enable the PyRFC python module to be imported successfully.
    """
    def initializeRfcSdkEnvironment(self):
        # set SAPNWRFC_HOME environment variable (and PATH on windows)
        self._setEnvironmentVariables()

        # set linux shared library loading config path
        if (os.name != "nt"):
            if (not self._isLinuxLibraryLoadConfigDefined()):
                self._setLinuxLibraryLoadConfig()

    ###########
    # Public methods to validate pynwrfc python module is installed and usable (ie. can be imported)
    # and to attempt to invoke pip install process
    ###########

    """
    Safe check to return flag indicating whether the pyrfc module has been installed
    (having module installed does not mean it is usable for import)
    """
    def isPyrfcModuleInstalled(self) -> bool:
        try:
            import pyrfc
        except ModuleNotFoundError as e:
            # specific exception that indicates module has not been installed
            self.tracer.warn("pyrfc module does not appear to be installed: %s", e)
            return False
        except Exception as e:
            # any other exception and we assume module is already installed (but may not be usable yet)
            self.tracer.warn("exception trying to check if pyrfc module is installed (assuming yes): %s", e)
        
        return True

    """
    Safe check to return flag indicating whether the pyrfc module can be imported successfully.
    Both RFC SDK and PyRfc module must be installed successfully for this to work
    """
    def isPyrfcModuleUsable(self) -> bool:
        # prequisite to importing PyRfc module is having RFC SDK installed
        if (not self.isRfcSdkInstalled()):
            self.tracer.warn("cannot import pyrfc module if RFC SDK is not installed")
            return False

        # trying to import pyrfc module for first time will attempt to load
        # RFC SDK .dll/so files referenced from SAPNWRFC_HOME environment variable,
        # and if not found then ImportError will be thrown
        try:
            import pyrfc
            return True
        except Exception as e:
            self.tracer.warn("failed to import pyrfc module: %s", e)
        
        return False

    """
    invoke pip install to install pynwrfc module.  On linux this install will fail if
    the RFC SDK has not already been installed on system with expected environment variables set
    """
    def installPyrfcModule(self) -> bool:
        wasSuccessful = False

        try:
            completedProcess = subprocess.run("pip install pynwrfc", capture_output=True, text=True)

            self.tracer.info("pip install pynwrfc return code: %s, stdout: %s, stderr: %s",
                             completedProcess.returncode,
                             completedProcess.stdout,
                             completedProcess.stderr)

            wasSuccessful = ("Successfully installed pynwrfc" in completedProcess.stdout 
                             and completedProcess.returncode == 0)

        except Exception as e:
            self.tracer.error("exception trying to install pynwrfc module via pip: %s", e)

        try:
            # persist installation attempt timestamp and (if available) the last modified
            # time of the downloaded sdk blob so we only update installation in future if it changes
            self._writePyrfcModuleInstallationState(status='success' if wasSuccessful else 'failed', 
                                                    lastInstallAttemptTime=datetime.utcnow())
        except Exception as e:
            # failure to persist installation state file is treated as installation failure
            wasSuccessful = False
        
        return wasSuccessful

    """
    get timestamp of pip install attempt of python pyrfc module
    """
    def getLastPyrfcModuleInstallAttemptTime(self) -> datetime:
        state = self._readPyrfcModuleInstallationState()
        return state["lastInstallAttemptTime"] if state else datetime.min

    ###########
    # Public methods to validate RFC SDK is installed on system, and if not download .zip file from
    # user provided blob URL if available and install on local system.
    # download state
    ###########

    """
    check expected installation path, environment variables, and dynamic library loading configuration
    are all valid, and also whether pyrfc module can be imported successfully.  
    Returns flag if all checks are validated, indicating the pyrfc and rfc sdk should be available for use 
    """
    def isRfcSdkInstalled(self) -> bool:
        # ensure expected rfc sdk libary path exists
        if (not os.path.isdir(self.libPath)):
            self.tracer.warn("expected rfc sdk path does not exist: %s", self.libPath)
            return False
        
        # ensure all expected library files are present at ${INSTALLPATH}/nwrfcsdk/lib
        if (not self._areExpectedSdkFilesFound()):
            return False

        # validate that environment variables needed by pyrfc module are set with expected values
        if (not self._areEnvironmentVariablesSet()):
            self.tracer.warn("required environment variables are missing")
            return False

        # on linux, check to see if library loading config has mapping to rfc sdk /lib folder
        if (os.name != "nt"):
            if (not self._isLinuxLibraryLoadConfigDefined()):
                return False

        ## finally, validate that the pyrfc module is installed and can be successfully imported
        #if (not self._isPyRfcModuleAvailable()):
        #    self.tracer.warn("RFC SDK is installed but pyrfc module is not available")
        #    return False
        
        self.tracer.info("validated SAP RFC SDK is installed at path:%s", self.installPath)
        return True

    """
    get timestamp of last rfc sdk download+installation attempt 
    """
    def getLastSdkInstallAttemptTime(self) -> datetime:
        state = self._readSdkInstallationState()
        return state["lastInstallAttemptTime"] if state else datetime.min

    """
    get the last_modified timestamp of the last blob that we attempted to download and install
    """
    def getLastSdkInstallPackageModifiedTime(self) -> datetime:
        state = self._readSdkInstallationState()
        return state["packageLastModifiedTime"] if state else datetime.min

    """
    check storage account and and returns tuple[bool, datetime] with flag for whether blob exists 
    and if so the last_modified time of that blob.  This enables caller to make a determination
    on whether it would be appropriate to attempt download and install.
    """
    def isRfcSdkAvailableForDownload(self, 
                                     blobUrl: str, 
                                     storageAccount: AzureStorageAccount) -> tuple[bool, datetime]:
        try:
            blobClient = BlobClient.from_blob_url(blob_url=blobUrl, credential=storageAccount.getAccessKey())

            if (not blobClient.exists()):
                # rfc sdk blob does not exist so we have no last modified time
                self.tracer.error("error validating rfc sdk blob: blob does not exist at url:%s", blobUrl)
                return (False, datetime.min)
        
            metadata = blobClient.get_blob_properties()
            lastModifiedTime = metadata.last_modified

            self.tracer.info("found rfc sdk package blob at url:%s with last_modified:%s", 
                             blobUrl, 
                             lastModifiedTime)

            return (True, lastModifiedTime)
        except Exception as e:
            self.tracer.error("error validating rfc sdk blob: exception trying to access url:%s (%s)", 
                              blobUrl, 
                              e)
        
        return (False, datetime.min)

    """
    download specified blob URL from storage account and unzip to local folder,
    then setup system configuration
    """
    def downloadAndInstallRfcSdk(self, 
                                 blobUrl: str, 
                                 storageAccount: AzureStorageAccount) -> bool:
        # start out with default since we can only persist sdk package
        # last modified date if we were able to fetch metadata for the blob
        lastModifiedTime = datetime.min
        wasSuccessful = False

        try:
            # create download folder
            self._createSdkInstallPathIfNotExists()

            blobClient = BlobClient.from_blob_url(blob_url=blobUrl, credential=storageAccount.getAccessKey())

            if (not blobClient.exists()):
                raise Exception("rfc sdk blob does noty exist! url:%s", blobUrl)

            # extract metadata properties from blob so that we can attempt to download
            # to local file with same name as the blob itself, and also persist
            # the last modified time of the blob itself
            metadata = blobClient.get_blob_properties()
            blobName = metadata.name
            lastModifiedTime = metadata.last_modified
            downloadFilePath = os.path.join(self.installPath, blobName)

            # download sdk package to local install folder and unpack it
            self._downloadAndUnzip(blobClient, downloadFilePath)

            # setup pyrfc environment variables and dynamic library loading configuration
            self.initializeRfcSdkEnvironment()

            # finally, verify that we can succesfully import the pyrfc module
            if (not self.isRfcSdkInstalled()):
                raise Exception("RFC SDK installation attempt failed!")
            
            self.tracer.info("RFC SDK installed successfully")
            wasSuccessful = True

        except Exception as e:
            wasSuccessful = False
            self.tracer.error("exception trying to download and install RFC SDK from url: %s, " + 
                              "installPath: %s, exception: (%s)",
                              blobUrl,
                              self.installPath,
                              e)
        try:
            # persist installation attempt timestamp and (if available) the last modified
            # time of the downloaded sdk blob so we only update installation in future if it changes
            self._writeSdkInstallationState(status='success' if wasSuccessful else 'failed', 
                                         lastInstallAttemptTime=datetime.utcnow(), 
                                         packageLastModifiedTime=lastModifiedTime)
        except Exception as e:
            # failure to persist installation state file is treated as installation failure
            wasSuccessful = False
        
        return wasSuccessful

    ###########
    # private methods methods to 

    """
    download binary blob to the local install path and then unpack it to that folder
    """
    def _downloadAndUnzip(self,
                          blobClient: BlobClient,
                          downloadFilePath: str):
        try:
            # first download the sdk blob file to local install path
            self.tracer.info("starting download of rfc sdk blob: %s to path: %s", blobClient.url, downloadFilePath)
            with open(downloadFilePath, "wb+") as downloadFile:
                downloadStream = blobClient.download_blob()
                downloadFile.write(downloadStream.readall())
        
            # now unzip the sdk file into the local install path
            self.tracer.info("unzipping rfc sdk file to: %s", self.installPath)
            with zipfile.ZipFile(downloadFilePath, "r") as sdkZipFile:
                sdkZipFile.extractall(self.installPath)

            self.tracer.info("downloaded sdk blob and successfully extracted to: %s", self.installPath)
        except Exception as e:
            self.tracer.error("failed to download and unzip rfc sdk package to path: %s (%s)", downloadFilePath, e)
            reraise

    """
    validate that expected rfc sdk .dll/.so files are found install path
    """
    def _areExpectedSdkFilesFound(self) -> bool:
        # ensure all expected library files are present at ${INSTALLPATH}/nwrfcsdk/lib
        for expectedFileName in RFC_SDK_EXPECTED_FILES:
            expectedFilePath = os.path.join(self.libPath, expectedFileName)
            if (not os.path.isfile(expectedFilePath)):
                self.tracer.warn("unable to validate NWRFCSDK installation, expected file not found: %s", 
                                 expectedFilePath)
                return False

        return True

    """
    validate environment variables needed for pyrfc module contain values
    that match paths where rfc sdk libraries are installed
    """
    def _areEnvironmentVariablesSet(self) -> bool:
        # set SAPNWRFC_HOME environment variable that needed by Python RFC module (pyrfc)
        if ('SAPNWRFC_HOME' not in os.environ):
            self.tracer.warn("SAPNWRFC_HOME environment variable not defined")
            return False
        
        if (self.rfcHomePath not in os.environ['SAPNWRFC_HOME']):
            self.tracer.warn("SAPNWRFC_HOME environment variable value '%s', does not match expected value '%s'",
                             os.environ['SAPNWRFC_HOME'],
                             self.rfcHomePath)
            return False
        
        # set environment variable needed by rfc sdk dynamic library loading
        if (os.name == "nt"):
            # Windows PATH environment variable should contain libPath
            if ('PATH' not in os.environ):
                self.tracer.warn("PATH environment variable not defined")
                return False
            if (self.libPath not in os.environ['PATH']):
                self.tracer.warn("PATH environment variable does not contain expected RFC library path: %s", self.libPath)
                return False
        return True

    """
    set expected environment variables needed for the pyrfc module to load rfc sdk libraries
    """
    def _setEnvironmentVariables(self):
        # set env variable needed by python rfc module
        self.tracer.info("Setting environment variable SAPNWRFC_HOME=%s", self.rfcHomePath)
        os.environ['SAPNWRFC_HOME'] = self.rfcHomePath

        # set environment variable needed by rfc sdk .dll loading on Windows
        if (os.name == "nt"):
            self.tracer.info("Appending %s to PATH environment vairable", self.libPath)
            path = os.environ['PATH']
            os.environ['PATH'] = path + ';' + self.libPath

    """
    validate that linux library load config file for nwrfcsdk contains expected
    installation library path
    """
    def _isLinuxLibraryLoadConfigDefined(self) -> bool:
        # Linux ld.config file: /etc/ld.so.conf.d/nwrfcsdk.conf should contain libPath
        if (os.name != "nt"):
            try:
                with open(LINUX_LD_CONFIG_FILE, 'r') as ldConfigFile:
                    lines = ldConfigFile.readlines()
                    if self.libPath not in lines:
                        self.tracer.warn("%s file does not contain expected RFC library path: %s",
                                         LINUX_LD_CONFIG_FILE,
                                         self.libPath)
                        return False

            except FileNotFoundError as e:
                self.tracer.warning("ld.config file %s does not exist", LINUX_LD_CONFIG_FILE)
                return False
            except Exception as e:
                self.tracer.warning("could not read ld config file %s (%s)", LINUX_LD_CONFIG_FILE, e)
                return False
        
        self.tracer.info("%s file contains expected path: %s", LINUX_LD_CONFIG_FILE, self.libPath)
        return True

    """
    ensure linux dynamic library loading config includes /lib folder of the rfc sdk install path
    """
    def _setLinuxLibraryLoadConfig(self) -> bool:
        if (os.name != "nt"):
            # Linux ld.config file: /etc/ld.so.conf.d/nwrfcsdk.conf should contain libPath
            try:
                with open(LINUX_LD_CONFIG_FILE, 'w+') as ldConfigFile:
                    lines = ['# include sap nwrfcsdk', self.libPath]
                    ldConfigFile.writelines(lines)

                self.tracer.info("wrote ld.config file %s with path %s", LINUX_LD_CONFIG_FILE, self.libPath)

            except Exception as e:
                self.tracer.warning("failed to write rfc sdk ld config file %s with content: %s (%s)", 
                                    LINUX_LD_CONFIG_FILE, 
                                    self.libPath,
                                    e)
                reraise

    """
    ensure directory exists for us to download rfc sdk blob and unpack it
    """
    def _createSdkInstallPathIfNotExists(self):
        if (not os.path.isdir(self.installPath)):
            os.makedirs(self.installPath, mode=0o777, exist_ok=True)
    
    """
    persist rfc sdk installation state including 
    1.) status - whether last install attempt was 'success' or 'failed' 
    2.) lastInstallAttemptTime - timestamp of last install attempt to download and install
    3.) packageLastModifiedTime - last_modified timestamp (if available) of the rfc sdk blob that was downloaded.  
    These timestamps can be used by caller to determine if/when to retry installation periodically, 
    and to not attempt to download and reinstall if nothing has changed.
    """
    def _writeSdkInstallationState(self, 
                               status: str, 
                               lastInstallAttemptTime: datetime, 
                               packageLastModifiedTime: datetime = None):
        
        data = {'status': status,
                'lastInstallAttemptTime': lastInstallAttemptTime,
                'packageLastModifiedTime': packageLastModifiedTime}
        try:
            with open(self.sdkInstallStateFilePath, "w+") as installStateFile:
                jsonText = json.dumps(data, indent=4, cls=JsonEncoder)
                installStateFile.write(jsonText)
        except Exception as e:
            self.tracer.error("Error writing rfc sdk installation state file: %s (%s", 
                              self.sdkInstallStateFilePath, 
                              e)
            reraise

    """
    read rfc sdk installation state file as json dictionary, or return default values if previous
    installation state was not found
    """
    def _readSdkInstallationState(self) -> Dict:
        jsonData = {}

        try:
            # if file does not exist then return initial state iondicating no previous install attempts
            if (not os.path.isfile(self.sdkInstallStateFilePath)):
                return {'status': None, 
                        'lastInstallAttemptTime': datetime.min, 
                        'packageLastModifiedTime': datetime.min }

            # read previous installation attempt state file and deserialize from json
            with open(self.sdkInstallStateFilePath, "r") as installStateFile:
                jsonText = installStateFile.read()
            jsonData = json.loads(jsonText, object_hook=JsonDecoder.datetimeHook)

            return jsonData

        except Exception as e:
            self.tracer.error("error trying to read rfc sdk installation state file: %s (%s)", 
                              self.sdkInstallStateFilePath,
                              e)
            reraise

    """
    persist pyrfc python module install state
    1.) status - whether last install attempt was 'success' or 'failed' 
    2.) lastInstallAttemptTime - timestamp of last install attempt to download and install
    3.) packageLastModifiedTime - last_modified timestamp (if available) of the rfc sdk blob that was downloaded.  
    These timestamps can be used by caller to determine if/when to retry installation periodically, 
    and to not attempt to download and reinstall if nothing has changed.
    """
    def _writePyrfcModuleInstallationState(self, 
                                      status: str, 
                                      lastInstallAttemptTime: datetime):
        
        data = {'status': status,
                'lastInstallAttemptTime': lastInstallAttemptTime}
        try:
            with open(self.pyrfcModuleInstallStateFilepath, "w+") as installStateFile:
                jsonText = json.dumps(data, indent=4, cls=JsonEncoder)
                installStateFile.write(jsonText)
        except Exception as e:
            self.tracer.error("Error writing pyrfc module installation state file: %s (%s", 
                              self.pyrfcModuleInstallStateFilepath, 
                              e)
            reraise

    """
    read rfc sdk installation state file as json dictionary, or return default values if previous
    installation state was not found
    """
    def _readPyrfcModuleInstallationState(self) -> Dict:
        jsonData = {}

        try:
            # if file does not exist then return initial state iondicating no previous install attempts
            if (not os.path.isfile(self.sdkInstallStateFilePath)):
                return {'status': None, 
                        'lastInstallAttemptTime': datetime.min}

            # read previous installation attempt state file and deserialize from json
            with open(self.pyrfcModuleInstallStateFilepath, "r") as installStateFile:
                jsonText = installStateFile.read()
            jsonData = json.loads(jsonText, object_hook=JsonDecoder.datetimeHook)

            return jsonData

        except Exception as e:
            self.tracer.error("error trying to read pyrfc python module installation state file: %s (%s)", 
                              self.pyrfcModuleInstallStateFilepath,
                              e)
            reraise
