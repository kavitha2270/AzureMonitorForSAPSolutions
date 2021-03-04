# Python modules
import json
import logging
import os
import re
import socket
import subprocess
import sys
import zipfile
from datetime import datetime, date, timedelta, time, timezone
from typing import Dict, List

from azure.mgmt.storage import StorageManagementClient
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlockBlobService

from const import *
from helper.tools import JsonEncoder, JsonDecoder
from helper.azure import AzureStorageAccount

######
# Overview on RFC SDK installation, runtime requirements, and pynwrfc python package import prerequisites:
# Pre-requisites for AMS collector process to be able to successfully import pynwrfc package and make RFC calls:
# 1.) pynwrfc python package should be downloaded and installed on collector VM (or included in container image)
#     NOTE #1: pre-built wheel CAN BE installed via pip install BEFORE RFC SDK files being present on VM.  
#          So this step can be done before steps 2-5 below.
#     NOTE #2: cannot 'import pyrfc' from python script unless the following checks 2-4 are true.  
#          Successful import of the pyrfc module requires loading of SAP NW RFC libraries and requires
#          SAPNWRFC_HOME variable to be see to reference include files
#
# 2.a.) On Linux, LD_LIBRARY_PATH environment variable must include the path ${installPath}/nwrfcsdk/lib folder where
#     the RFC SDK .so library files are expected to be found.  The SDK RFC files can be installed to this path 
#     after the monitor program starts up.
#     NOTE #1: the LD_LIBRARY_PATH variable *MUST* be initialized prior to the python script being executed!  
#     The library loading config by python process on startup and cannot be changed by
#     setting environment variables from from within python script itself.  Any changes made using os.environ['LD_LIBRARY_PATH'])
#     will be ignored.
#     NOTE #2: there is a more complicated way of updating the library loading config on linux by editing a
#     .conf file /etc/ld.co.conf.d/ that adds a library load path mapping to the nwrfcsdk/lib folder, then you can
#     run the ldconfig tool as root to force rebuild of the library load mapping cache.  While this has convenience
#     of being persistent and applying across processes and reboots, it is more difficult to automate and
#     both the file change and rebuilding of ldconfig cache require running as root.
# or
# 2.b.) On windows, PATH environment variable must include ${installPath}\nwrfcsdk\lib but it appears this PATH
#     variable can be updated in-process after python starts.
#
# 3.) On both Linux and Windows, the environment variable SAPNWRFC_HOME must be set to ${installPath}/nwrfcsck
#     This variable must be set before attempting to import the pyrfc package.  This variable may be set in-process
#     using os.environ['SAPNWRFC_HOME'].
#
# 4.) On both Linux and Windows, the user provided NW RFC SDK .zip file must be downloaded and unzipped into
#     the ${installPath}, which will create the sub folders nwrfcsdk/lib
#
# 5.) On Linux (not confirmed on Windows), there appears to be a requirement by the underlying RFC library requirement
#     that in order to establish a connection to a remote SAP host, the actual hostname of the client (localhost) VM must be
#     DNS resolvable.  It is unclear why a client would need to resolve its own hostname to establish a connection
#     with remote server.  But this is resolved by adding the following line to the /etc/hosts file:
#            127.0.0.1 localhost
#            127.0.0.1 sapmon-vm-<sapmonId>  <== new line that must be added
#     NOTE #1: this step is required to make an outgoing connection to a remote SAP instance.  At least it was required
#           from a linux VM that was not domain joined.  It was not required from a domain joined windows device,
#           but that might be artifact of way windows handles DNS resolution of a device's own hostname.
#     NOTE #2: running from Docker container, this can be achieved by using the --add-host argument to "docker run"

# 2.) Customer downloads NW RFC SDK from SAP and uploads the .zip file to their own Azure Storage account blob container.
#     Customer's blob URL will need to be provided as optional Azure Monitor for SAP config property (via Key Vault)
#
# 3.) 
# 

# relative path to sdk extraction folder used for SAPNWRFC_HOME environment variable
RFC_HOME_RELATIVE_PATH = 'nwrfcsdk'

# relative path to sdk extraction folder where rfc sdk library files are found, ie: nwrfcsdk/lib
RFC_LIBRARY_RELATIVE_PATH = os.path.join(RFC_HOME_RELATIVE_PATH, 'lib')

# default folder where user provided libraries will be downloaded and unpacked
# /var/opt/microsoft/sapmon/$version/sapmon/userlibs
PATH_USER_LIB_ROOT = os.path.join(PATH_ROOT, "usrlibs")

# default install path where user provided RFC SDK package will downloaded and unpacked
# /var/opt/microsoft/sapmon/$version/sapmon/userlibs/nwrfcsdk
PATH_RFC_SDK_INSTALL = os.path.join(PATH_USER_LIB_ROOT, "nwrfcsdk")

LINUX_HOSTS_FILE = '/etc/hosts'
#LINUX_LD_CONFIG_FILE = '/etc/ld.so.conf.d/nwrfcsdk.conf'
RFC_SDK_INSTALLATION_STATE_FILE = 'rfc-sdk-installation-state.json'
#PYRFC_MODULE_INSTALLATION_STATE_FILE = 'pyrfc-module-installation-state.json'


# expected RFC SDK library file names
if (os.name == "nt"):
    # windows
    RFC_SDK_EXPECTED_FILES = ['libicudecnumber.dll', 'libsapucum.dll', 'sapnwrfc.dll']
else:
    # linux
    RFC_SDK_EXPECTED_FILES = ['libicudecnumber.so', 'libsapucum.so', 'libsapnwrfc.so']

# regex for extracting container and blob name from RFC SDK Blob URL
# "https://{accountName}.blob.core.windows.net/{containerName}}/{blobName}"
BLOB_URL_REGEX = re.compile(r"https?\://(?P<accountName>[^\.]+)[^/]+/(?P<containerName>[^/]+)/(?P<blobName>.+)")

class SapRfcSdkInstaller:

    def __init__(self,
                 tracer: logging.Logger,
                 installPath: str):
        self.tracer = tracer
        self.installPath = installPath
        self.rfcHomePath = os.path.join(self.installPath, RFC_HOME_RELATIVE_PATH)
        self.libPath = os.path.join(self.installPath, RFC_LIBRARY_RELATIVE_PATH)

        # persisted installation state files
        self.sdkInstallStateFilePath = os.path.join(self.installPath, RFC_SDK_INSTALLATION_STATE_FILE)
        # self.pyrfcModuleInstallStateFilepath = os.path.join(self.installPath, PYRFC_MODULE_INSTALLATION_STATE_FILE)

    """
    set process level environment variables needed to import successfully Pyrfc module and load RFC libraries
    """
    def initRfcSdkEnvironment(self) -> bool:
        self.tracer.info("initializing environment for rfc sdk...")

        # set SAPNWRFC_HOME environment variable and os-specific library load path variables
        self._setEnvironmentVariables()

        if (os.name != "nt"):
            if (not self._isLinuxHostnameInHostsFile()):
                if (not self._setLinuxHostnameInHostsFile()):
                    return False

        self.tracer.info("rfc sdk environment configured successfully")
        return True

        # NOTE:  the below does not appear to be necessary if we instead leverage the 
        # *much* easier to work with LD_LIBRARY_PATH env variable, but only downside is that
        # LD_LIBRARY_PATH env variable must be set before sapmon.py is executed
        # set linux shared library loading config path
        #if (os.name != "nt"):
        #    if (not self._isLinuxLibraryLoadConfigDefined()):
        #        self._setLinuxLibraryLoadConfig()

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

    #"""
    #invoke pip install to install pynwrfc module.  On linux this install will fail if
    #the RFC SDK has not already been installed on system with expected environment variables set
    #"""
    #def installPyrfcModule(self) -> bool:
    #    wasSuccessful = False
    #
    #    try:
    #        completedProcess = subprocess.run(["pip", "install", "pynwrfc", "--no-binary", ":all:"], capture_output=True, text=True)
    #
    #        self.tracer.info("pip install pynwrfc return code: %s, stdout: %s, stderr: %s",
    #                         completedProcess.returncode,
    #                         completedProcess.stdout,
    #                         completedProcess.stderr)
    #
    #        wasSuccessful = ("Successfully installed pynwrfc" in completedProcess.stdout 
    #                         and completedProcess.returncode == 0)
    #
    #    except Exception as e:
    #        self.tracer.error("exception trying to install pynwrfc module via pip: %s", e)
    #
    #    try:
    #        # persist installation attempt timestamp and (if available) the last modified
    #        # time of the downloaded sdk blob so we only update installation in future if it changes
    #        self._writePyrfcModuleInstallationState(status='success' if wasSuccessful else 'failed', 
    #                                                lastInstallAttemptTime=datetime.now(timezone.utc))
    #    except Exception as e:
    #        # failure to persist installation state file is treated as installation failure
    #        wasSuccessful = False
    #    
    #    return wasSuccessful

    #"""
    #get timestamp of pip install attempt of python pyrfc module
    #"""
    #def getLastPyrfcModuleInstallAttemptTime(self) -> datetime:
    #    state = self._readPyrfcModuleInstallationState()
    #    return (state["lastInstallAttemptTime"] if state else datetime.min).replace(tzinfo=timezone.utc)

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
        self.tracer.info("check that rfc sdk is installed and enviroment setup for use...")

        # ensure expected rfc sdk libary path exists
        if (not os.path.isdir(self.libPath)):
            self.tracer.warn("expected rfc sdk path does not exist: %s", self.libPath)
            return False
        
        # ensure all expected library files are present at ${INSTALLPATH}/nwrfcsdk/lib
        if (not self._areExpectedSdkFilesFound()):
            return False

        # validate that environment variables needed by pyrfc module are set with expected values
        if (not self._areEnvironmentVariablesSet()):
            return False

        if (os.name != "nt"):
            if (not self._isLinuxHostnameInHostsFile()):
                return False

        # on linux, check to see if library loading config has mapping to rfc sdk /lib folder
        #if (os.name != "nt"):
        #    if (not self._isLinuxLibraryLoadConfigDefined()):
        #        return False

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
        return (state["lastInstallAttemptTime"] if state else datetime.min).replace(tzinfo=timezone.utc)

    """
    get the last_modified timestamp of the last blob that we attempted to download and install
    """
    def getLastSdkInstallPackageModifiedTime(self) -> datetime:
        state = self._readSdkInstallationState()
        return (state["packageLastModifiedTime"] if state else datetime.min).replace(tzinfo=timezone.utc)

    """
    check storage account and and returns tuple[bool, datetime] with flag for whether blob exists 
    and if so the last_modified time of that blob.  This enables caller to make a determination
    on whether it would be appropriate to attempt download and install.
    """
    def isRfcSdkAvailableForDownload(self, 
                                     blobUrl: str, 
                                     storageAccount: AzureStorageAccount) -> tuple:
        try:
            #blobClient = BlobClient.from_blob_url(blob_url=blobUrl, credential=storageAccount.getAccessKey())
            blobService = BlockBlobService(account_name=storageAccount.accountName, 
                                           account_key=storageAccount.getAccessKey())

            (accountName, containerName, blobName) = self._getBlobContainerAndName(blobUrl)

            #if (not blobClient.exists()):
            if (not blobService.exists(container_name=containerName, blob_name=blobName)):
                # rfc sdk blob does not exist so we have no last modified time
                self.tracer.error("error validating rfc sdk blob: blob does not exist at url:%s", blobUrl)
                return (False, datetime.min)
        
            #metadata = blobClient.get_blob_properties()
            metadata = blobService.get_blob_properties(container_name=containerName, blob_name=blobName)
            lastModifiedTime = metadata.properties.last_modified

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
        self.tracer.info("begin download and install of rfc sdk blob: %s", blobUrl)

        # start out with default since we can only persist sdk package
        # last modified date if we were able to fetch metadata for the blob
        lastModifiedTime = datetime.min
        wasSuccessful = False

        try:
            # create download folder
            self._createSdkInstallPathIfNotExists()

            #blobClient = BlobClient.from_blob_url(blob_url=blobUrl, credential=storageAccount.getAccessKey())
            blobService = BlockBlobService(account_name=storageAccount.accountName, 
                                           account_key=storageAccount.getAccessKey())

            (accountName, containerName, blobName) = self._getBlobContainerAndName(blobUrl)

            #if (not blobClient.exists()):
            if (not blobService.exists(container_name=containerName, blob_name=blobName)):
                raise Exception("rfc sdk blob does noty exist! url:%s" % blobUrl)

            # extract metadata properties from blob so that we can attempt to download
            # to local file with same name as the blob itself, and also persist
            # the last modified time of the blob itself
            #metadata = blobClient.get_blob_properties()
            metadata = blobService.get_blob_properties(container_name=containerName, blob_name=blobName)
            lastModifiedTime = metadata.properties.last_modified
            downloadFilePath = os.path.join(self.installPath, blobName)

            # download sdk package to local install folder and unpack it
            self._downloadAndUnzip(blobService, containerName, blobName, downloadFilePath)

            # setup pyrfc environment variables and dynamic library loading configuration
            self.initRfcSdkEnvironment()

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
                                         lastInstallAttemptTime=datetime.now(timezone.utc), 
                                         packageLastModifiedTime=lastModifiedTime)
        except Exception as e:
            # failure to persist installation state file is treated as installation failure
            wasSuccessful = False
        
        return wasSuccessful

    ###########
    # private methods methods to manage installation and environment configuration for RFC SDK 
    ###########

    def _getBlobContainerAndName(self, blobUrl: str) -> tuple:
        # "https://sapmonsto75853a6503011a.blob.core.windows.net/sap-netweaver-rfc-sdk/nwrfc750P_7-70002752.zip"
        
        m = BLOB_URL_REGEX.match(blobUrl)

        if m:
            fields = m.groupdict()
            return (fields['accountName'], fields['containerName'], fields['blobName'])

        raise Exception("sdk blob url %s did not match expected pattern" % blobUrl)

    """
    download binary blob to the local install path and then unpack it to that folder
    """
    def _downloadAndUnzip(self,
                          #blobClient: BlobClient,
                          blobService: BlockBlobService,
                          containerName: str,
                          blobName: str,
                          downloadFilePath: str):
        try:
            # first download the sdk blob file to local install path
            #self.tracer.info("starting download of rfc sdk blob: %s to path: %s", blobClient.url, downloadFilePath)
            #with open(downloadFilePath, "wb+") as downloadFile:
            #    downloadStream = blobClient.download_blob()
            #    downloadFile.write(downloadStream.readall())
        
            self.tracer.info("starting download of rfc sdk blob: %s to path: %s", 
                             ("%s/%s" % (containerName, blobName)),
                             downloadFilePath)
            blobService.get_blob_to_path(containerName, blobName, downloadFilePath, open_mode='wb+')
            
            # now unzip the sdk file into the local install path
            self.tracer.info("unzipping rfc sdk file to: %s", self.installPath)
            with zipfile.ZipFile(downloadFilePath, "r") as sdkZipFile:
                sdkZipFile.extractall(self.installPath)

            self.tracer.info("downloaded sdk blob and successfully extracted to: %s", self.installPath)
        except Exception as e:
            self.tracer.error("failed to download and unzip rfc sdk package to path: %s (%s)", downloadFilePath, e)
            raise

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
            envVarName = 'PATH'
        else:
            envVarName = 'LD_LIBRARY_PATH'
        
        if (envVarName not in os.environ):
            self.tracer.warn("%s environment variable not defined", envVarName)
            return False

        if (self.libPath not in os.environ[envVarName]):
            self.tracer.warn("%s environment variable does not contain expected RFC library path: %s", envVarName, self.libPath)
            return False

        return True

    """
    set expected environment variables needed for the pyrfc module to load rfc sdk libraries
    """
    def _setEnvironmentVariables(self):
        # set env variable needed by python rfc module
        self.tracer.info("Setting environment variable SAPNWRFC_HOME=%s", self.rfcHomePath)
        os.environ['SAPNWRFC_HOME'] = self.rfcHomePath

        # set environment variable needed by rfc sdk library loading on Windows or Linux
        if (os.name == "nt"):
            pathVarName = 'PATH'
            pathDelimiter = ';'
        else:
            pathVarName = 'LD_LIBRARY_PATH'
            pathDelimiter = ':'
        
        self.tracer.info("Appending %s to %s environment variable", self.libPath, pathVarName)
        path = os.environ.get(pathVarName, '')
        path = self.libPath if path == '' else self.libPath + pathDelimiter + path
        os.environ[pathVarName] = path

    """
    validate that mapping for current VM hostname -> 127.0.0.1 has been added to /etc/hosts file 
    on Linux so that RFC SDK is able to resolve the current hostname IP.  This explicit hostname
    loopback appears to be required for the RFC to establish a connection to a remote server, which 
    is odd since we're trying to establish an outbound connection so unclear why we have to do DNS
    resolution on our own hostname.  However, that is what is happening in the RFC SDK stack and
    there is an attempt to do name resolution.  The actual IP address does not appear to matter, 
    so we just use the 127.0.0.1
        example:  
            127.0.0.1 localhost              # default loopback
            127.0.0.1 sapmon-vm-<sapmonId>   # needs to be added

    NOTE: if this is not correctly set, then RFC SDK error like the following may be seen:
    error establishing connection with hostname: <targetSapHostname>.<sapSubDomain>, sapSysNr: 30, error: RFC_COMMUNICATION_FAILURE (rc=1): key=RFC_COMMUNICATION_FAILURE, message=
LOCATION    CPIC (TCP/IP) on local host with Unicode
ERROR       hostname 'sapmon-vm-<sapMonId>' unknown
TIME        Fri Feb 19 16:50:09 2021
RELEASE     753
COMPONENT   NI (network interface)
VERSION     40
RC          -2
MODULE      /bas/753_REL/src/base/ni/nixxhl.cpp
LINE        198
DETAIL      NiHLGetNodeAddr: hostname cached as unknown
COUNTER     2
 [MSG: class=, type=, number=, v1-4:=;;;]
    """
    def _isLinuxHostnameInHostsFile(self) -> bool:
        if (os.name == "nt"):
            return False
        
        expectedMapping = "127.0.0.1 " + socket.gethostname()

        try:
            with open(LINUX_HOSTS_FILE, 'r') as hostsFile:
                lines = hostsFile.readlines()
                matchedHostnameMappings = [line for line in lines if expectedMapping in line]

                if (len(matchedHostnameMappings) > 0):
                    self.tracer.info("%s file contains expected mapping: %s", LINUX_HOSTS_FILE, expectedMapping)
                    return True
                
                self.tracer.warn("%s file does not contain expected hostname mapping: %s", LINUX_HOSTS_FILE, expectedMapping)
        except Exception as e:
            self.tracer.warning("could not read hsots file %s (%s)", LINUX_HOSTS_FILE, e)
        
        return False

    """
    ensure linux /etc/hosts file contains expected mapping for current VM hostname -> 127.0.0.1
    NOTE:  needs to be run under sudo/root permissions.  
    """
    def _setLinuxHostnameInHostsFile(self) -> bool:
        if (os.name == "nt"):
            return False
        
        expectedMapping = "127.0.0.1 " + socket.gethostname()
        try:
            with open(LINUX_HOSTS_FILE, 'a+') as hostsFile:
                lines = hostsFile.readlines()

                matchedHostnameMappings = [line for line in lines if expectedMapping in line]
                if (len(matchedHostnameMappings) > 0):
                    return True

                lines.append("\n%s\n" % expectedMapping)
                hostsFile.writelines(lines)

            self.tracer.info("wrote hosts file %s with mapping %s", LINUX_HOSTS_FILE, expectedMapping)
            return True

        except Exception as e:
            self.tracer.error("failed to write current hostname to hosts file %s with mapping: '%s' (%s)", 
                                LINUX_HOSTS_FILE, 
                                expectedMapping,
                                e)
            return False

    #"""
    #validate that linux library load config file for nwrfcsdk contains expected
    #installation library path
    #"""
    #def _isLinuxLibraryLoadConfigDefined(self) -> bool:
    #    if (os.name == "nt"):
    #        return False
    #    
    #    # Linux ld.config file: /etc/ld.so.conf.d/nwrfcsdk.conf should contain self.libPath
    #    try:
    #        with open(LINUX_LD_CONFIG_FILE, 'r') as ldConfigFile:
    #            lines = ldConfigFile.readlines()
    #            for line in lines:
    #                if self.libPath in line:
    #                    self.tracer.info("%s file contains expected path: %s", LINUX_LD_CONFIG_FILE, self.libPath)
    #                    return True
    #
    #            self.tracer.warn("%s file does not contain expected RFC library path: %s",
    #                                LINUX_LD_CONFIG_FILE,
    #                                self.libPath)
    #
    #    except FileNotFoundError as e:
    #        self.tracer.warning("ld.config file %s does not exist", LINUX_LD_CONFIG_FILE)
    #    except Exception as e:
    #        self.tracer.warning("could not read ld config file %s (%s)", LINUX_LD_CONFIG_FILE, e)
    #    
    #    return False

    #"""
    #query linux dynamic library mappings cache and return flag indicating whether
    #expected RFC SDK libary file paths were found
    #"""
    #def _areLinuxLibraryMappingsLoaded(self) -> bool:
    #    if (os.name == "nt"):
    #        return False
    #
    #    try:
    #        completedProcess = subprocess.run(["ldconfig", "-p"], capture_output=True, text=True)
    #
    #        self.tracer.info("rebuild dynamic library load cache 'ldconfig' return code: %s, stderr: %s",
    #                        completedProcess.returncode,
    #                        completedProcess.stdout,
    #                        completedProcess.stderr)
    #
    #        if (len(completedProcess.stderr) != 0 or completedProcess.returncode != 0):
    #            self.tracer.warn("'ldconfig -p' to query dynamic library cache failed")
    #            return False
    #
    #        # ldconfig output will look like the following:
    #        # libsapucum.so (libc6,x86-64) => /some/path/sdk-installpath/nwrfcsdk/lib/libsapucum.so
    #        # libsapnwrfc.so (libc6,x86-64) => /some/path/sdk-installpath/nwrfcsdk/lib/libsapnwrfc.so
    #        # libgssapi_krb5.so.2 (libc6,x86-64) => /usr/lib/x86_64-linux-gnu/libgssapi_krb5.so.2
    #        # libgssapi.so.3 (libc6,x86-64) => /usr/lib/x86_64-linux-gnu/libgssapi.so.3
    #        outputLines = completedProcess.stdout.splitlines()
    #
    #    except Exception as e:
    #        self.tracer.error("exception trying to run 'ldconfig -p': %s", e)
    #        return False
    #
    #    # now iterate through all dynamic library mapping output lines and 
    #    # ensure that a mapping exists for each expected RFC SDK library at the 
    #    # expected install path
    #    wasSuccessful = True
    #    for expectedFileName in RFC_SDK_EXPECTED_FILES:
    #        expectedFilePath = os.path.join(self.libPath, expectedFileName)
    #
    #        # first filter dynamic library mappings down to subset of lines
    #        # the reference the expected RFC SDK file name
    #        libraryMappings = [line for line in outputLines if expectedFileName in line]
    #
    #        if (len(libraryMappings) == 0):
    #            self.tracer.warn("no dynamic library mappings found for rfc sdk file: %s", expectedFileName)
    #            wasSuccessful = False
    #            continue
    #        else:
    #            self.tracer.info("dynamic library mappings for rfc sdk file: %s -> %s",
    #                                    expectedFileName,
    #                                    '|'.join(libraryMappings))
    #        
    #        if (len(libraryMappings) > 1):
    #            # TODO: may want to treat as critical failure as likely means that rfc sdk
    #            # libraries will be loaded from somewhere other than expected path,
    #            # but for now just be tolerant
    #            self.tracer.warn("multiple dynamic library mappings loaded for rfc sdk file: %s", expectedFileName)
    #        
    #        # finally ensure that a mapping exists for this file at the expected install path
    #        #  ${installPath}/nwrfcsdk/lib 
    #        expectedPathMappings = [line for line in libraryMappings if expectedFilePath in line]
    #
    #        if (len(expectedPathMappings) != 1):
    #            self.tracer.warn("missing expected dynamic mapping path for rfc sdk file: %s", expectedFilePath)
    #            wasSuccessful = False
    #            continue
    #    
    #    return wasSuccessful

    #"""
    #ensure linux dynamic library loading config includes /lib folder of the rfc sdk install path
    #"""
    #def _setLinuxLibraryLoadConfig(self) -> bool:
    #    if (os.name == "nt"):
    #        return False
    #    
    #    # Linux ld.config file: /etc/ld.so.conf.d/nwrfcsdk.conf should contain libPath
    #    try:
    #        with open(LINUX_LD_CONFIG_FILE, 'w+') as ldConfigFile:
    #            lines = ['# include sap nwrfcsdk\n', self.libPath + "\n"]
    #            ldConfigFile.writelines(lines)
    #
    #        self.tracer.info("wrote ld.config file %s with path %s", LINUX_LD_CONFIG_FILE, self.libPath)
    #        return True
    #
    #    except Exception as e:
    #        self.tracer.error("failed to write rfc sdk ld config file %s with content: %s (%s)", 
    #                            LINUX_LD_CONFIG_FILE, 
    #                            self.libPath,
    #                            e)
    #        return False

    #"""
    #ensure linux dynamic library mappings cache is rebuilt to reflect new nwrfcsdk .conf config file
    #"""
    #def _refreshLinuxDynamicLibraryLoadingCache(self) -> bool:
    #    if (os.name == "nt"):
    #        return False
    #    
    #    try:
    #        completedProcess = subprocess.run(["ldconfig"], capture_output=True, text=True)
    #
    #        self.tracer.info("rebuild dynamic library load cache 'ldconfig' return code: %s, stdout: %s, stderr: %s",
    #                        completedProcess.returncode,
    #                        completedProcess.stdout,
    #                        completedProcess.stderr)
    #
    #        return (len(completedProcess.stderr) == 0 and completedProcess.returncode == 0)
    #
    #    except Exception as e:
    #        self.tracer.error("exception trying to run 'ldconfig': %s", e)
    #    
    #    return False

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
            raise

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
            raise

    #"""
    #persist pyrfc python module install state
    #1.) status - whether last install attempt was 'success' or 'failed' 
    #2.) lastInstallAttemptTime - timestamp of last install attempt to download and install
    #3.) packageLastModifiedTime - last_modified timestamp (if available) of the rfc sdk blob that was downloaded.  
    #These timestamps can be used by caller to determine if/when to retry installation periodically, 
    #and to not attempt to download and reinstall if nothing has changed.
    #"""
    #def _writePyrfcModuleInstallationState(self, 
    #                                  status: str, 
    #                                  lastInstallAttemptTime: datetime):
    #    
    #    data = {'status': status,
    #            'lastInstallAttemptTime': lastInstallAttemptTime}
    #    try:
    #        with open(self.pyrfcModuleInstallStateFilepath, "w+") as installStateFile:
    #            jsonText = json.dumps(data, indent=4, cls=JsonEncoder)
    #            installStateFile.write(jsonText)
    #    except Exception as e:
    #        self.tracer.error("Error writing pyrfc module installation state file: %s (%s", 
    #                          self.pyrfcModuleInstallStateFilepath, 
    #                          e)
    #        raise

    #"""
    #read rfc sdk installation state file as json dictionary, or return default values if previous
    #installation state was not found
    #"""
    #def _readPyrfcModuleInstallationState(self) -> Dict:
    #    jsonData = {}
    #
    #    try:
    #        # if file does not exist then return initial state iondicating no previous install attempts
    #        if (not os.path.isfile(self.sdkInstallStateFilePath)):
    #            return {'status': None, 
    #                    'lastInstallAttemptTime': datetime.min}
    #
    #        # read previous installation attempt state file and deserialize from json
    #        with open(self.pyrfcModuleInstallStateFilepath, "r") as installStateFile:
    #            jsonText = installStateFile.read()
    #        jsonData = json.loads(jsonText, object_hook=JsonDecoder.datetimeHook)
    #
    #        return jsonData
    #
    #    except Exception as e:
    #        self.tracer.error("error trying to read pyrfc python module installation state file: %s (%s)", 
    #                          self.pyrfcModuleInstallStateFilepath,
    #                          e)
    #        raise
