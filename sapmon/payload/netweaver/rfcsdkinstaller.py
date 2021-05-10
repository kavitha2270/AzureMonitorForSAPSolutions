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
# Overview on RFC SDK installation, runtime requirements, and pynwrfc python package import prerequisites.
#
# Pre-requisites for AMS collector process to be able to successfully import pynwrfc package and make RFC calls:
# 1.) pynwrfc python package should be downloaded and installed on collector VM (or included in container image)
#     NOTE #1: pre-built wheel CAN BE installed via pip install BEFORE RFC SDK files being present on VM.  
#          So this step can be done before steps 2-5 below.  Example:
#          curl --location --output pynwrfc-2.3.0-cp37-cp37m-linux_x86_64.whl https://github.com/SAP/PyRFC/releases/download/2.3.0/pynwrfc-2.3.0-cp37-cp37m-linux_x86_64.whl
#          pip3 install pynwrfc-2.3.0-cp37-cp37m-linux_x86_64.whl
#     NOTE #2: CAN NOT 'import pyrfc' from python script unless the following checks 2-4 are true.  
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
#     NOTE #3: on linux it appears that any libraries can be installed to this path for the first time *after* the python 
#     process starts so long as the directory exists (empty) at startup time.  If the directory in the LD_LIBRARY_PATH
#     exists at python process startup and the libraries are extracted there during runtime, they will be successfully
#     loaded by the pyrfc module at import time and usable.  However, if the directory does NOT exist at startup,
#     then it appears the LD_LIBRARY_PATH will not be recognized and the RFC libraries will not be usable until the
#     next time python process is restarted.
# or
# 2.b.) On windows, PATH environment variable must include ${installPath}\nwrfcsdk\lib but it appears this PATH
#     variable can be updated in-process after python starts, and .dlls just have to be placed there before you 
#     attempt to load them.
#
# 3.) On both Linux and Windows, the environment variable SAPNWRFC_HOME must be set to ${installPath}/nwrfcsck
#     This variable must be set before attempting to import the pyrfc package.  This variable may be set in-process
#     using os.environ['SAPNWRFC_HOME'].  Python process does not have to be restarted forhtis to take effect.
#
# 4.) On both Linux and Windows, the user provided NW RFC SDK .zip file must be downloaded and unzipped into
#     the ${installPath}, which will create the sub folders nwrfcsdk/lib.
#
# 5.) On Linux (not confirmed on Windows), there appears to be a requirement by the underlying RFC library requirement
#     that in order to establish a connection to a remote SAP host, the actual hostname of the client VM (localhost) must be
#     DNS resolvable.  Unclear why a client would need to resolve its own hostname to establish a connection
#     with remote server.  But this is resolved by adding the following line to the /etc/hosts file:
#            127.0.0.1 localhost
#            127.0.0.1 sapmon-vm-<sapmonId>  <== new line that must be added
#     NOTE #1: this step is required to make an outgoing connection to a remote SAP instance.  At least it was required
#           from a linux VM that was not domain joined.  It was not required from a domain joined windows device,
#           but that might be artifact of way windows handles DNS resolution of a device's own hostname.
#     NOTE #2: running from Docker container, this can be achieved by using the --add-host argument to "docker run"
#     NOTE #3: tried this using HOST_ALIASES file on linux but did not appear to work, which means RFC SDK may be
#           using system calls that are not covered by that user-specific override.
#
#
# Pre-requisites for RFC SDK installation attempt:
# 1.) Customer must download NW RFC SDK for linux from SAP (ex: nwrfc750P_7-70002752.zip) and upload the .zip file
#     to a blob storage container in a storage account that allows the sapmon-vm-<sapmMonId> Managed Service Identity
#     the permissions to "List Keys" on the storage account. Suggestion is to use the storage account already created
#     by SAP monitor in the sapmon-rg-XXXXXXXX managed resource group: 
#          https://sapmonsto{sapmonId}.blob.core.windows.net/sap-netweaver-rfc-sdk/nwrfc750P_7-70002752.zip
# 
# 2.) customer must provide the following properties to be parsed from the "SapNetweaver-<SID>" secret that 
#     is placed in the sapmon-kv-XXXXXXXX key vault each time customer adds a new netweaver provider monitoring config:
#         "sapUsername": "<rfc-logon-user>",
#         "sapPassword": "<rfc-logon-password>", 
#         "sapClientId": "<rfc-logon-clientId>", 
#         "sapRfcSdkBlobUrl": "<rfc-sdk-blob-url>", 
#
# 3.) user of this installer can base the decision to attempt installation and use of pyrfc module on following factors:
#       a.) are all of above required properties specified
#       b.) does the user provided rfc sdk blob URL exist?
#       c.) is the python rfc module actually installed on the system
#       d.) was last failed installation attempt more than N minutes ago
#       e.) has user provided rfc sdk blob been modified since the last failed download attempt?

# relative path to sdk extraction folder used for SAPNWRFC_HOME environment variable
RFC_HOME_RELATIVE_PATH = 'nwrfcsdk'

# relative path to sdk extraction folder where rfc sdk library files are found, ie: nwrfcsdk/lib
RFC_LIBRARY_RELATIVE_PATH = os.path.join(RFC_HOME_RELATIVE_PATH, 'lib')

# default folder where user provided libraries will be downloaded and unpacked
# /var/opt/microsoft/sapmon/{version}/sapmon/userlibs
PATH_USER_LIB_ROOT = os.path.join(PATH_ROOT, "usrlibs")

# default install path where user provided RFC SDK package will downloaded and unpacked
# /var/opt/microsoft/sapmon/{version}/sapmon/userlibs/nwrfcsdk
PATH_RFC_SDK_INSTALL = os.path.join(PATH_USER_LIB_ROOT, "nwrfcsdk")

LINUX_HOSTS_FILE = '/etc/hosts'
RFC_SDK_INSTALLATION_STATE_FILE = 'rfc-sdk-installation-state.json'


# expected RFC SDK library file names
if (os.name == "nt"):
    # windows
    RFC_SDK_EXPECTED_FILES = ['libicudecnumber.dll', 'libsapucum.dll', 'sapnwrfc.dll']
else:
    # linux
    RFC_SDK_EXPECTED_FILES = ['libicudecnumber.so', 'libsapucum.so', 'libsapnwrfc.so']

# regex for extracting container and blob name from RFC SDK Blob URL
# "https://{accountName}.blob.core.windows.net/{containerName}}/{blobName}"
BLOB_URL_REGEX = re.compile(r"https?\://(?P<accountName>[^\.]+)[^/]+/(?P<containerName>[^/]+)/(?P<blobName>.+)", re.IGNORECASE)

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

    """
    set process level environment variables needed to import successfully Pyrfc module and load RFC libraries
    """
    def initRfcSdkEnvironment(self) -> bool:
        self.tracer.info("initializing environment for rfc sdk...")

        # set SAPNWRFC_HOME environment variable and os-specific library load path variables
        self._setEnvironmentVariables()

        # NOTE:  we have observed on some express-route subscription setups that the collector VM
        #        is unable to resolve its own hostname to an ip address, which causes RFC SDK calls
        #        to fail because the protocol evidently requires resolving the client host IP.
        #        When running in dev machine setup (as sudo) or in container (as root), uncomment
        #        the code below if RFC calls are failing due to being unable to resolve the collector VM name
        if (os.name != "nt"):
            if (not self._isLinuxHostnameInHostsFile()):
                if (not self._setLinuxHostnameInHostsFile()):
                    return False

        self.tracer.info("rfc sdk environment configured successfully")
        return True

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

        # NOTE: making the decision to not do hosts file check here since it is unclear whether 
        #       the express route scenario is common enough among SAP customers.  What we really
        #       want to do is a socket.gethostbyname(hostname) resolution attempt but this has a very long
        #       timmeout on failure so we need some additional state to ensure this check is not performed too often
        if (os.name != "nt"):
            if (not self._isLinuxHostnameInHostsFile()):
                return False
        
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
            blobService = BlockBlobService(account_name=storageAccount.accountName, 
                                           account_key=storageAccount.getAccessKey())

            (accountName, containerName, blobName) = self._getBlobContainerAndName(blobUrl)

            if (not blobService.exists(container_name=containerName, blob_name=blobName)):
                # rfc sdk blob does not exist so we have no last modified time
                self.tracer.error("error validating rfc sdk blob: blob does not exist at url:%s", blobUrl)
                return (False, datetime.min)
        
            metadata = blobService.get_blob_properties(container_name=containerName, blob_name=blobName)
            lastModifiedTime = metadata.properties.last_modified

            self.tracer.info("found rfc sdk package blob at url:%s with last_modified:%s", 
                             blobUrl, 
                             lastModifiedTime)

            return (True, lastModifiedTime)
        except Exception as e:
            self.tracer.error("error validating rfc sdk blob: exception trying to access url:%s (%s)", 
                              blobUrl, 
                              e, 
                              exc_info=True)
        
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

            blobService = BlockBlobService(account_name=storageAccount.accountName, 
                                           account_key=storageAccount.getAccessKey())

            (accountName, containerName, blobName) = self._getBlobContainerAndName(blobUrl)

            if (not blobService.exists(container_name=containerName, blob_name=blobName)):
                raise Exception("rfc sdk blob does noty exist! url:%s" % blobUrl)

            # extract metadata properties from blob so that we can attempt to download
            # to local file with same name as the blob itself, and also persist
            # the last modified time of the blob itself
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
                              e, 
                              exc_info=True)
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

    """
    parse customer provided blob URL to extract storage account name, container name, and blob name
    """
    def _getBlobContainerAndName(self, blobUrl: str) -> tuple:
        # "https://sapmonstoXXXXXXXXX.blob.core.windows.net/sap-netweaver-rfc-sdk/nwrfc750P_7-70002752.zip"
        
        m = BLOB_URL_REGEX.match(blobUrl)

        if m:
            fields = m.groupdict()
            return (fields['accountName'], fields['containerName'], fields['blobName'])

        raise Exception("sdk blob url %s did not match expected pattern" % blobUrl)

    """
    download binary blob to the local install path and then unpack it to that folder
    """
    def _downloadAndUnzip(self,
                          blobService: BlockBlobService,
                          containerName: str,
                          blobName: str,
                          downloadFilePath: str):
        try:
            # first download the sdk blob file to local install path
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
            self.tracer.error("failed to download and unzip rfc sdk package to path: %s (%s)", downloadFilePath, e, exc_info=True)
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
    resolution on our own hostname.  The actual IP address does not appear to matter, so we just use the 127.0.0.1
        example:  
            127.0.0.1 localhost              # default loopback already in hosts file
            127.0.0.1 sapmon-vm-<sapmonId>   # <== needs to be added to hosts file

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
                
                self.tracer.warn("%s hosts file does not contain expected hostname mapping: %s", LINUX_HOSTS_FILE, expectedMapping)
        except Exception as e:
            self.tracer.warning("could not read hosts file %s (%s)", LINUX_HOSTS_FILE, e)
        
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
                                e, 
                                exc_info=True)
            return False

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
                              e, 
                              exc_info=True)
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
                              e, 
                              exc_info=True)
            raise
