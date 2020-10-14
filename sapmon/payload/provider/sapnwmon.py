# Python modules
import hashlib
import json
import logging
import re
import time
from datetime import datetime

# Payload modules
from const import *
from helper.azure import *
from helper.context import *
from helper.tools import *
from provider.base import ProviderInstance, ProviderCheck
from typing import Dict, List

# SAP modules.
from pyrfc import Connection, ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError

class SAPNWMonProviderInstance(ProviderInstance):
    def __init__(self,
                tracer: logging.Logger,
                ctx: Context,
                providerInstance: Dict[str, str],
                skipContent: bool = False,
                **kwargs):
        self.sapHostName = None
        self.sapSysNr = None
        self.sapClient = None
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
    parse provider properties and get host name, sysnr, client, username, password.
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

        self.sapClient = self.providerProperties.get("sapClient", None)
        if not self.sapClient:
            self.tracer.error("[%s] sapClient cannot be empty" % self.fullName)
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

    def validate(self) -> bool:
        self.tracer.info("connecting to sap with host name (%s) to test required rfc calls.")

        # establish connection to SAP using provided credentials.
        # TODO: log times required for calls.
        try:
            with self._establish_connection_to_sap() as connection:
                # test call RFCPING.
                _ = connection.call('RFCPING')
                self.tracer.info("successfully called RFC ping.")

                smon_result = self._call_sdf_get_smon_runs(connection)
                guid = self._process_guid_using_smon_runs(smon_result)
                self.tracer.info("successfully retrieved GUID from /SDF/GET_SMON_RUNS.")

                smon_analysis_result = self._call_sdf_smon_analysis_read(connection, guid, datetime.now(), 0)
                if smon_analysis_result is None:
                    return False
        except Exception as e:
            self.tracer.error("Error occured while validating %s " % (e))
            return False

        return True

    # establish connection to sap.
    def _establish_connection_to_sap(self) -> Connection:
        try:
            connection = Connection(ashost=self.sapHostName, sysnr=self.sapSysNr, client=self.sapClient, user=self.sapUsername, passwd=self.sapPassword)
        except CommunicationError as e:
            self.tracer.error("Cannot establish connection with (%s) with hostname: %s " % (self.fullName, self.sapHostName))
            return None
        except LogonError as e:
            self.tracer.error("Credentials used to connect with hostname: %s with username: %s" % (self.fullName, self.sapUsername))
            return None
        except Exception as e:
            self.tracer.error("Error occured while establishing connection (%s) " % (e))
            return None

        return connection

    def _call_sdf_get_smon_runs(self, connection: Connection):
        from_date = datetime(1971, 5, 20).date()
        to_date = datetime(2999, 12, 31).date()
        try:
            smon_result = connection.call('/SDF/SMON_GET_SMON_RUNS', FROM_DATE=from_date, TO_DATE=to_date)
        except CommunicationError as e:
            self.tracer.error("Cannot establish connection with (%s) with hostname: %s " % (self.fullName, self.sapHostName))
            return None
        except Exception as e:
            self.tracer.error("Error occured while establishing connection (%s) " % (e))
            return None

        return smon_result

    # parse result from /SDF/SMON_GET_SMON_RUNS and return GUID.
    def _process_guid_using_smon_runs(self, result):
        if 'SMON_RUNS' in result:
            if 'GUID' in result['SMON_RUNS']:
                return result['SMON_RUNS'][0]['GUID']
            else:
                raise ValueError("GUID value does not exist in /SDF/SMON_GET_SMON_RUNS return result.")
        else:
            raise ValueError("SMON_RUNS value does not exist in /SDF/SMON_GET_SMON_RUNS return result.")

    # TODO: edge case scenario related to datetime calculation.
    def _call_sdf_smon_analysis_read(self, connection: Connection,  guid: str, lastRunTime: datetime, frequency: int):
        # calculate next run time.
        nextRunTime = lastRunTime + datetime.timedelta(seconds=frequency)

        # RFC parameters.
        datum = nextRunTime.date()
        startTime = lastRunTime.time()
        endTime = nextRunTime.time()

        result = connection.call('/SDF/SMON_ANALYSIS_READ', GUID=guid, DATUM=datum, START_TIME=startTime, END_TIME=endTime)

        return result

###########################
# implement sapnwmon check.
class SAPNWMonProviderCheck(ProviderCheck):

    def __init__(self,
    provider: ProviderInstance,
    **kwargs
    ):
        super.__init__(provider, **kwargs)

    def generateJsonString(self) -> str:
        return None

    def updateState(self):
        return

