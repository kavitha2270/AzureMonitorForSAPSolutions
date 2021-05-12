# Python modules
import hashlib
import json
import logging
import re
import time
from datetime import datetime, date, timedelta, time, tzinfo, timezone
from pandas import DataFrame
from typing import Dict, List

# SAP modules
from pyrfc import Connection, ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError

# abstract base class module
from netweaver.metricclientfactory import NetWeaverMetricClient
from helper.tools import JsonEncoder

# enforce maximum query window size so that we don't accidentally query SAP for huge
# data sets after periods of prolonged downtime/inactivity
MAX_QUERY_LOOKBACK_WINDOW = timedelta(minutes=15)

# known SWNC Workload metric results task id -> text mappings
SAP_TASK_TYPE_MAPPINGS = {
    b'\x01': 'DIALOG',
    b'\x02': 'UPDATE',
    b'\x03': 'SPOOL',
    b'\x04': 'BCKGRD',
    b'\x05': 'ENQUEUE',
    b'\x06': 'BUF.SYN',
    b'\x07': 'AUTOABA',
    b'\x08': 'UPDATE2',
    b'\x0a': 'EXT.PLUGIN',
    b'\x0b': 'AUTOTH',
    b'\x0c': 'RPCTH',
    b'\x0d': 'RFCVMC',
    b'\x0e': 'DDLOG CLEANUP',
    b'\x0f': 'DEL. THCALL',
    b'\x10': 'AUTOJAVA',
    b'\x11': 'LICENCESRV',
    b'\x12': 'AUTOCCMS',
    b'\x15': 'BGRFC SCHEDULER',
    b'\x21': 'OTHER',
    b'\x22': 'DINOGUI',
    b'\x23': 'B.INPUT',
    b'\x65': 'HTTP',
    b'\x66': 'HTTPS',
    b'\x67': 'NNTP',
    b'\x68': 'SMTP',
    b'\x69': 'FTP',
    b'\x6C': 'LCOM',
    b'\x75': 'HTTP/JSP',
    b'\x76': 'HTTPS/JSP',
    b'\xFC': 'ESI',
    b'\xFD': 'FD ALE',
    b'\xFE': 'RFC',
    b'\xFF': 'CPIC',
}

######
# Implementation of NetWeaverMetricClient abstract base class interface that relies on SAP NetWeaver RFC SDK library to
# make NW RFC calls to fetch SMON and SWNC Workload metrics (ST03).
# NOTE:  Assumes user provided RFC SDK has already been downloaded and installed on the client system
#        and that pynwrfc python module has also already been installed with expected environment settings.
#        Attempting to import this file will result in impot of pyrfc, and if any of the above are not true
#        then the import of this file will fail (due to direct pyrfc package references).
######
class NetWeaverRfcClient(NetWeaverMetricClient):
    def __init__(self,
                 tracer: logging.Logger,
                 logTag: str,
                 sapHostName: str,
                 sapSubdomain: str,
                 sapSysNr: str,
                 sapClient: str,
                 sapUsername: str,
                 sapPassword: str,
                 columnFilterList: List[str],
                 serverTimeZone: tzinfo,
                 sapSid: str,
                 sapLogonGroup: str,
                 **kwargs) -> None:
        self.tracer = tracer
        self.logTag = logTag
        self.sapSid = sapSid
        self.sapHostName = sapHostName
        self.sapSubdomain = sapSubdomain
        self.fqdn = self._getFullyQualifiedDomainName()
        self.sapSysNr = sapSysNr
        self.sapClient = sapClient
        self.sapUsername = sapUsername
        self.sapPassword = sapPassword
        self.columnFilterList = columnFilterList
        self.tzinfo = serverTimeZone 
        self.sapLogonGroup = sapLogonGroup
        self.msserv = "36%s" % self.sapSysNr

        super().__init__(tracer, logTag)

    #####
    # public property getter methods
    #####

    @property
    def Hostname(self) -> str:
        return self.sapHostName

    @property
    def InstanceNr(self) -> str:
        return str(self.sapSysNr)

    #####
    # public methods to implement abstract base class interface methods for NetWeaverMetricClient.
    #####

    """
    validate that config settings and that client can establish connection
    """
    def validate(self) -> bool:
        return True

    """
    return tuple of query start / end timestamps that are timezone aware (but in server time)
    so that they can be used to define time range for SMON/SWNC metric queries.  Factor in
    awareness of last run time so that we have some tolerance on small outages or gaps and can be
    used to fill in historical data in those cases, but at same time enforce maximum query window
    to ensure we do not query SAP for huge result sets.
    Also enforce time range logic that ensures start/end time are always the same calendar 'day',
    since that is how SAP stores SMON analysis results
    """
    def getQueryWindow(self, 
                       lastRunServerTime: datetime,
                       minimumRunIntervalSecs: int) -> tuple:

        # always start with assumption that query window will work backwards from current system time
        currentServerTime = self.getServerTime()

        # usually a query window will end with the current SAP system time and will have a
        # lookback duration of the minimum check run interval (in seconds)
        # Since SAP requirement is that start and end time must occur on the same calendar day, 
        # we must handle special that if default window crosses over the midnight 00:00:00 boundary
        # we will truncate it to terminate at 11:59:59PM 
        if lastRunServerTime is None:
            # if there was no previous check timestamp, then we start a new query
            # window that works backwards from the current SAP system time (by the minimum check run interval time).
            # If that will cross the midnight boundary into the previous calendar day, 
            # then the start time should be moved up to be 00:00:00 on the current day.
            windowEnd = currentServerTime
            windowStart = currentServerTime - timedelta(seconds=minimumRunIntervalSecs)
            currentMidnight = datetime.combine(currentServerTime.date(), time(0, 0, 0), tzinfo=self.tzinfo)
            windowStart = max(windowStart, currentMidnight)
        else:
            # if we have a previous check timestamp to use as the start of new query window,
            # and this new query window will cross this midnight into the next day,
            # then truncate the query window so that it terminates at 11:59:59PM on the current day. 
            windowStart = lastRunServerTime + timedelta(seconds=1)
            nextMidnight = datetime.combine(windowStart.date(), time(0, 0, 0), tzinfo=self.tzinfo) + timedelta(days=1)
            windowEnd = min(currentServerTime, nextMidnight - timedelta(seconds=1))

        # enforce maximum query window size so that we don't attempt to fetch a huge amount of data
        # if the system has been down for prolonged period of time.  This means that we will 
        # never attempt to backfill metric data outside of the MAX_QUERY_LOOKBACK_WINDOW
        if ((windowEnd - windowStart) > MAX_QUERY_LOOKBACK_WINDOW):
            windowStart = windowEnd - MAX_QUERY_LOOKBACK_WINDOW

        self.tracer.info("[%s] getQueryWindow query window for lastRunServerTime: %s, " +
                         "currentServerTime: %s -> [start=%s, end=%s]", 
                         self.logTag, 
                         lastRunServerTime,
                         currentServerTime,
                         windowStart, 
                         windowEnd)

        return windowStart, windowEnd

    """
    fetch current sap system time stamp and apply the server timezone (if known)
    so that it be used for timezone aware comparisons against tz-aware timestamps
    """
    def getServerTime(self) -> datetime:
        self.tracer.info("executing RFC to get SAP server time")
        with self._getMessageServerConnection() as connection:
            # read current time from SAP NetWeaver.
            timestampResult = self._rfcGetSystemTime(connection)
            systemDateTime = self._parseSystemTimeResult(timestampResult)

            systemDateTime = systemDateTime.replace(tzinfo=self.tzinfo)

            return systemDateTime

    """
    fetch all /SDF/SMON_ANALYSIS_READ metric data and return as a single json string
    """
    def getSmonMetrics(self, 
                       startDateTime: datetime,
                       endDateTime: datetime) -> str:
        self.tracer.info("executing RFC SDF/SMON_ANALYSIS_RUN check")
        
        with self._getMessageServerConnection() as connection:
            # get guid to call RFC SDF/SMON_ANALYSIS_READ.
            guidResult = self._rfcGetSmonRunIds(connection, startDateTime=startDateTime, endDateTime=endDateTime)
            guid = self._parseSmonRunIdsResult(guidResult)

            # based on last run and current time, calculate start and end time.
            #startDate, startTime, _, endTime = self.getNextRunTime(currentDate, currentTime, lastSapRunTime)
            rawResult = self._rfcGetSmonAnalysisByRunId(connection, guid, startDateTime=startDateTime, endDateTime=endDateTime)
            parsedResult = self._parseSmonAnalysisResults(rawResult)

            # add additional common metric properties
            self._decorateSmonMetrics(parsedResult)

            return parsedResult

    """
    fetch SWNC_GET_WORKLOAD_SNAPSHOT data, calculate aggregate metrics and return as json string
    """
    def getSwncWorkloadMetrics(self,
                               startDateTime: datetime,
                               endDateTime: datetime) -> str:
        self.tracer.info("executing RFC SWNC_GET_WORKLOAD_SNAPSHOT check")
        with self._getMessageServerConnection() as connection:
            snapshotResult = self._rfcGetSwncWorkloadSnapshot(connection,
                                                              startDateTime=startDateTime, 
                                                              endDateTime=endDateTime)

            parsedResult = self._parseSwncWorkloadSnapshotResult(snapshotResult)

            # add additional common metric properties
            self._decorateSwncWorkloadMetrics(parsedResult, queryWindowEnd=endDateTime)

            return parsedResult

    """
    fetch all /SDF/GET_DUMP_LOG metric data and return as a single json string
    """
    def getShortDumpsMetrics(self,
                       startDateTime: datetime,
                       endDateTime: datetime) -> str:
        self.tracer.info("executing RFC SDF/GET_DUMP_LOG check")
        parsedResult = None
        with self._getMessageServerConnection() as connection:
            # get guid to call RFC SDF/GET_DUMP_LOG.
            rawResult = self._rfcGetDumpLog(connection, startDateTime=startDateTime, endDateTime=endDateTime)
            if (rawResult != None) :
                parsedResult = self._parseGetDumpLogResults(rawResult)
                #add additional common metric properties
                self._decorateShortDumpMetrics(parsedResult)
            return parsedResult

    #####
    # private methods to initiate RFC connections and fetch server timestamp
    #####

    """
    create fully qualified hostname if subdomain was provided
    """
    def _getFullyQualifiedDomainName(self) -> str:
        if self.sapSubdomain:
            return self.sapHostName + "." + self.sapSubdomain
        else:
            return self.sapHostName
    """
    establish rfc  connection to sap.
    """
    def _getMessageServerConnection(self) -> Connection:
        try:
            # Direct application server logon:  ashost, sysnr
            # load balancing logon:  mshost, msserv, sysid, group
            connection = Connection(#ashost=self.fqdn, 
                                    sysnr=self.sapSysNr, 
                                     mshost = self.fqdn,
                                    #Group = self.sapLogonGroup,
                                    Group = "Technical",
                                    msserv = self.msserv,
                                    ssid = self.sapSid,
                                    client=self.sapClient, 
                                    user=self.sapUsername, 
                                    passwd=self.sapPassword)
            return connection
        except CommunicationError as e:
            #self.tracer.error("[%s] error establishing connection with hostname: %s, sapSysNr: %s, error: %s",
            #                  self.logTag, self.fqdn, self.sapSysNr, e)
            raise
        except LogonError as e:
            #self.tracer.error("[%s] Incorrect credentials used to connect with hostname: %s username: %s, error: %s",
            #                  self.logTag, self.fqdn, self.sapUsername, e)
            raise
        except Exception as e:
            #self.tracer.error("[%s] Error occured while establishing connection to hostname: %s, sapSysNr: %s, error: %s ",
            #                  self.logTag, self.fqdn, self.sapSysNr, e)
            raise

    """
    establish rfc  connection to sap.
    """
    def _getApplicationServerConnection(self) -> Connection:
        try:
            # Direct application server logon:  ashost, sysnr
            # load balancing logon:  mshost, msserv, sysid, group
            connection = Connection(ashost=self.fqdn, 
                                    sysnr=self.sapSysNr, 
                                    client=self.sapClient, 
                                    user=self.sapUsername, 
                                    passwd=self.sapPassword)
            return connection
        except CommunicationError as e:
            #self.tracer.error("[%s] error establishing connection with hostname: %s, sapSysNr: %s, error: %s",
            #                  self.logTag, self.fqdn, self.sapSysNr, e)
            raise
        except LogonError as e:
            #self.tracer.error("[%s] Incorrect credentials used to connect with hostname: %s username: %s, error: %s",
            #                  self.logTag, self.fqdn, self.sapUsername, e)
            raise
        except Exception as e:
            #self.tracer.error("[%s] Error occured while establishing connection to hostname: %s, sapSysNr: %s, error: %s ",
            #                  self.logTag, self.fqdn, self.sapSysNr, e)
            raise

    """
    make RFC call to get system time
    """
    def _rfcGetSystemTime(self, connection: Connection):
        rfcName = 'BDL_GET_CENTRAL_TIMESTAMP'
        timestampResult = None

        try:
            timestampResult = connection.call(rfcName)
            return timestampResult
        except CommunicationError as e:
            self.tracer.error("[%s] communication error for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.fqdn, e, exc_info=True)
        except Exception as e:
            self.tracer.error("[%s] Error occured for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.fqdn, e, exc_info=True)

        return None

    """
    parse system date and time fields from RFC response and return as a datetime.
    The RFC res[pmse will not have any timezone identifier, so we will apply the known
    server time zone provided by the caller when the client was created
    """
    def _parseSystemTimeResult(self, result: Dict[str, str]) -> datetime:
        if result is None:
            raise ValueError("Invalid result received from BDL_GET_CENTRAL_TIMESTAMP")

        currentSystemDate: date = None
        currentSystemTime: time = None

        if 'TAG' in result:
            currentSystemDate = datetime.strptime(result['TAG'], '%Y%m%d').date()
        else:
            raise ValueError("RFC BDL_GET_CENTRAL_TIMESTAMP result does not have TAG value: %s" % result)

        if 'UHRZEIT' in result:
            currentSystemTime = datetime.strptime(result['UHRZEIT'], '%H%M%S').time()
        else:
            raise ValueError("RFC BDL_GET_CENTRAL_TIMESTAMP result does not have UHRZEIT value: %s" % result)

        return datetime.combine(date=currentSystemDate, time=currentSystemTime, tzinfo=self.tzinfo)

    """
    helper to take seperate server Date and Time fields and return a datetime
    object with server timezone applied
    """
    def _datetimeFromDateAndTimeString(self, dateStr: str, timeStr: str) -> datetime:
        # expecte dateStr to have format %Y%m%d
        # expect timeStr to have format %H%M%S
        parsedDateTime = datetime.strptime(dateStr + ' ' + timeStr, '%Y%m%d %H%M%S')
        parsedDateTime = parsedDateTime.replace(tzinfo=self.tzinfo)

        return parsedDateTime

    #####
    # private methods to perform two-phase call to first fetch most recent SMON run analysis identifier (guid)
    # and then use that identifer to make a second call to fetch the full SMON analysis result set for that ID.
    # Also methods to parse the responses from those RFC calls and to decorate metric results.
    #####

    """
    make RFC call to fetch list of SDF/SMON snapshot identifiers in the given time range
    """
    def _rfcGetSmonRunIds(self, 
                          connection: Connection, 
                          startDateTime: datetime, 
                          endDateTime: datetime):
        rfcName = '/SDF/SMON_GET_SMON_RUNS'

        self.tracer.info("[%s] invoking rfc %s for hostname=%s with from_date=%s, to_date=%s",
                         self.logTag, 
                         rfcName, 
                         self.sapHostName, 
                         startDateTime.date(), 
                         endDateTime.date())

        try:
            smon_result = connection.call(rfcName, FROM_DATE=startDateTime.date(), TO_DATE=endDateTime.date())
            return smon_result
        except CommunicationError as e:
            self.tracer.error("[%s] communication error for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.sapHostName, e, exc_info=True)
        except Exception as e:
            self.tracer.error("[%s] Error occured for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.sapHostName, e, exc_info=True)

        return None

    """
    parse result from /SDF/SMON_GET_SMON_RUNS and return Run ID GUID.
    """
    def _parseSmonRunIdsResult(self, result):
        if 'SMON_RUNS' in result:
            if (len(result['SMON_RUNS']) == 0):
                raise ValueError("SMON_RUNS result list was empty!")
            if 'GUID' in result['SMON_RUNS'][0]:
                return result['SMON_RUNS'][0]['GUID']
            else:
                raise ValueError("GUID value does not exist in /SDF/SMON_GET_SMON_RUNS return result.")
        else:
            raise ValueError("SMON_RUNS value does not exist in /SDF/SMON_GET_SMON_RUNS return result.")

    """
    call RFC SDF/SMON_ANALYSIS_RUN to lookup analysis results for specific Run ID and return the result
    """
    def _rfcGetSmonAnalysisByRunId(self, 
                                   connection: Connection, 
                                   guid: str, 
                                   startDateTime: datetime, 
                                   endDateTime: datetime):
        rfcName = '/SDF/SMON_ANALYSIS_READ'
        result = None

        if not guid:
            raise ValueError(("argument error for rfc %s for hostname: %s, guid: %s, "
                              "expected start (%s) and end time (%s) to be same day")
                             % (rfcName, self.sapHostName, guid, startDateTime, endDateTime))

        # validate that start and end time are the same day, since thaty is what RFC expects
        if (startDateTime.date() != endDateTime.date()):
            raise ValueError(("argument error for rfc %s for hostname: %s, guid: %s, "
                              "expected start (%s) and end time (%s) to be same day")
                             % (rfcName, self.sapHostName, guid, startDateTime, endDateTime))

        self.tracer.info("[%s] invoking rfc %s for hostname=%s with guid=%s, datum=%s, start_time=%s, end_time=%s",
                         self.logTag, 
                         rfcName, 
                         self.sapHostName, 
                         guid, 
                         startDateTime.date(), 
                         startDateTime.time(), 
                         endDateTime.time())

        try:
            result = connection.call(rfcName, 
                                     GUID=guid, 
                                     DATUM=startDateTime.date(), 
                                     START_TIME=startDateTime.time(), 
                                     END_TIME=endDateTime.time())
            return result
        except CommunicationError as e:
            self.tracer.error("[%s] communication error for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.sapHostName, e, exc_info=True)
        except Exception as e:
            self.tracer.error("[%s] Error occured for rfc %s with hostname: %s (%s)", 
                              self.logTag, rfcName, self.sapHostName, e, exc_info=True)

        return None

    """
    return header information from sdf/smon_analysis_read
    """
    def _parseSmonAnalysisResults(self, result):
        rfcName = 'SDF/SMON_ANALYSIS_READ'
        if result is None:
            raise ValueError("empty result received for rfc %s for hostname: %s" % (rfcName, self.sapHostName))

        processedResult = None
        if 'HEADER' in result:
            # create new dictionary with only values from filterList if filter dictionary exists.
            processedResult = result['HEADER']
            self.tracer.info("[%s] rfc %s returned %d records from hostname: %s",
                             self.logTag, rfcName, len(processedResult), self.sapHostName)

            # for each item in the list, create a new dictionary.
            if self.columnFilterList:
                filteredResult = list()
                for record in processedResult:
                    filteredRow = { columnName: record[columnName] for columnName in self.columnFilterList }
                    filteredResult.append(filteredRow)
                processedResult = filteredResult
        else:
            raise ValueError("%s result does not contain HEADER key from hostname: %s" % (rfcName, self.sapHostName))

        return processedResult
    
    """
    take parsed SMON analysis result set and decorate each record with additional fixed set of 
    properties expected for metrics records
    """
    def _decorateSmonMetrics(self, records: list) -> None:
        currentTimestamp = datetime.now(timezone.utc)

        # "DATUM": "20210212",
        # "TIME": "134300",
        # "SERVER": "sapsbx00_MSX_30"
        
        # regex to extract hostname / SID / instance from SERVER property,
        # since a single SMON analysis result set will contain records for
        # host/instances across the entire SAP landscape
        serverRegex = re.compile(r"(?P<hostname>.+?)_(?P<SID>[^_]+)_(?P<instanceNr>[0-9]+)")

        for record in records:
            # parse DATUM/TIME fields into serverTimestamp
            record['serverTimestamp'] = self._datetimeFromDateAndTimeString(record['DATUM'], record['TIME'])

            # parse SERVER field into hostname/SID/InstanceNr properties
            m = serverRegex.match(record['SERVER'])
            if m:
                fields = m.groupdict()
                record['hostname'] = fields['hostname']
                record['SID'] = fields['SID']
                record['instanceNr'] = fields['instanceNr']
            else:
                self.tracer.error("[%s] SMON analysis results record had unexpected SERVER format: %s", record['SERVER'])
                record['hostname'] = ''
                record['SID'] = ''
                record['instanceNr'] = ''

            record['client'] = self.sapClient
            record['subdomain'] = self.sapSubdomain
            record['timestamp'] = currentTimestamp

    #####
    # private methods to make SWNC Workload snapshot call, parse results and return enriched ST03 metrics results
    #####

    """
    call RFC SWNC_GET_WORKLOAD_SNAPSHOT and return result records
    """
    def _rfcGetSwncWorkloadSnapshot(self, 
                                    connection: Connection, 
                                    startDateTime: datetime,
                                    endDateTime: datetime):
        rfcName = 'SWNC_GET_WORKLOAD_SNAPSHOT'

        self.tracer.info(("[%s] invoking rfc %s for hostname=%s with read_start_date=%s, read_start_time=%s, "
                          "read_end_date=%s, read_end_time=%s"),
                         self.logTag, 
                         rfcName, 
                         self.sapHostName, 
                         startDateTime.date(), 
                         startDateTime.time(), 
                         endDateTime.date(), 
                         endDateTime.time())

        try:
            swnc_result = connection.call(rfcName, 
                                          READ_START_DATE=startDateTime.date(), 
                                          READ_START_TIME=startDateTime.time(), 
                                          READ_END_DATE=endDateTime.date(), 
                                          READ_END_TIME=endDateTime.time())
            return swnc_result
        except CommunicationError as e:
            self.tracer.error("[%s] communication error for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.sapHostName, e, exc_info=True)
        except Exception as e:
            self.tracer.error("[%s] Error occured for rfc %s with hostname: %s (%s)", 
                              self.logTag, rfcName, self.sapHostName, e, exc_info=True)

        return None

    """
    parse results from SWNC_GET_WORKLOAD_SNAPSHOT and enrich with additional calculated ST03 properties
    """
    def _parseSwncWorkloadSnapshotResult(self, result):
        rfcName = 'SWNC_GET_WORKLOAD_SNAPSHOT'
        if result is None:
            raise ValueError("empty result received for rfc %s from hostname: %s"
                             % (rfcName, self.sapHostName))
        
        def GetKeyValue(dictionary, key):
            if key not in dictionary:
                raise ValueError("Result received for rfc %s from hostname: %s does not contain key: %s" 
                                 % (rfcName, self.sapHostName, key))
            return dictionary[key]

        records = GetKeyValue(result, 'TASKTIMES')
        if (len(records) == 0):
            raise ValueError("empty SWNC workload task timings list for rfc %s from hostname: %s"
                             % (rfcName, self.sapHostName))
                             
        processed_results = list()
        for record in records:
            # try to map task type hex code to more descriptive task name to make more readable, but
            # if we can't find a mapping then just echo the taskTypeId
            taskTypeId = GetKeyValue(record, 'TASKTYPE')
            taskTypeText = (SAP_TASK_TYPE_MAPPINGS[taskTypeId] 
                                if taskTypeId in SAP_TASK_TYPE_MAPPINGS 
                                else taskTypeId)

            processed_result = {
                "Task Type": taskTypeId,
                "Task Type Name": taskTypeText,
                "Total Steps": GetKeyValue(record, 'COUNT'),
                "Total Response Time": GetKeyValue(record, 'RESPTI'),
                "Total Processing Time": GetKeyValue(record, 'PROCTI'),
                "Total CPU Time": GetKeyValue(record, 'CPUTI'),
                "Total Queue Time": GetKeyValue(record, 'QUEUETI'),
                "Total Roll Wait Time": GetKeyValue(record, 'ROLLWAITTI'),
                "Total GUI Steps": GetKeyValue(record, 'GUICNT'),
                "Total GUI Time": GetKeyValue(record, 'GUITIME'),
                "Total GUI Net Time": GetKeyValue(record, 'GUINETTIME'),
                "Total DB Time": GetKeyValue(record, 'READSEQTI') + GetKeyValue(record, 'CHNGTI') + GetKeyValue(record, 'READDIRTI'),
                "Total DB Dir Read Time": GetKeyValue(record, 'READDIRTI'),
                "Total DB Seq Read Time": GetKeyValue(record, 'READSEQTI'),
                "Total DB Chg Time": GetKeyValue(record, 'CHNGTI'),
                "Total DB Proc Time": GetKeyValue(record, 'DBP_TIME'),
                "Total DB Proc Steps": GetKeyValue(record, 'DBP_COUNT'),
                "Total DB Dir Read Steps": GetKeyValue(record, 'READDIRCNT'),
                "Total DB Seq Read Steps": GetKeyValue(record, 'READSEQCNT'),
                "Total DB Change Steps": GetKeyValue(record, 'CHNGCNT'),
                "PHYREADCNT": GetKeyValue(record, 'PHYREADCNT'),
                "PHYCHNGREC": GetKeyValue(record, 'PHYCHNGREC'),
                "PHYCALLS": GetKeyValue(record, 'PHYCALLS'),
            }

            # ST03 - Average response time across all steps
            if (GetKeyValue(record, 'COUNT') != 0):
                processed_result['ST03_Avg_Resp_Time'] = GetKeyValue(record, 'RESPTI') / GetKeyValue(record, 'COUNT')
            else:
                processed_result['ST03_Avg_Resp_Time'] = 0.0

            # ST03 - CPU Time, Processing Time, Queue, Roll Wait and DB times as % of Response Time
            if (GetKeyValue(record, 'RESPTI') != 0):
                processed_result['ST03_CPU_Time'] = GetKeyValue(record, 'CPUTI') / GetKeyValue(record, 'RESPTI')
                processed_result['ST03_Processing_Time'] = GetKeyValue(record, 'PROCTI') / GetKeyValue(record, 'RESPTI')
                processed_result['ST03_DB_Time'] = processed_result['Total DB Time'] / GetKeyValue(record, 'RESPTI')
                processed_result['ST03_Queue_Time'] = GetKeyValue(record, 'QUEUETI') / GetKeyValue(record, 'RESPTI')
                processed_result['ST03_RollWait_Time'] = GetKeyValue(record, 'ROLLWAITTI') / GetKeyValue(record, 'RESPTI')
            else:
                processed_result['ST03_CPU_Time'] = 0.0
                processed_result['ST03_Processing_Time'] = 0.0
                processed_result['ST03_DB_Time'] = 0.0
                processed_result['ST03_Queue_Time'] = 0.0
                processed_result['ST03_RollWait_Time'] = 0.0
            
            # ST03 - Average DB direct read latency
            if (GetKeyValue(record, 'READDIRCNT') != 0):
                processed_result['ST03_Avg_DB_Dir_Time'] = GetKeyValue(record, 'READDIRTI') / GetKeyValue(record, 'READDIRCNT')
            else:
                processed_result['ST03_Avg_DB_Dir_Time'] = 0.0

            # ST03 - Average DB sequential read latency
            if (GetKeyValue(record, 'READSEQCNT') != 0):
                processed_result['ST03_Avg_DB_Seq_Time'] = GetKeyValue(record, 'READSEQTI') / GetKeyValue(record, 'READSEQCNT')
            else:
                processed_result['ST03_Avg_DB_Seq_Time'] = 0.0

            # ST03 - Average DB change latency
            if (GetKeyValue(record, 'CHNGCNT') != 0):
                processed_result['ST03_Avg_DB_Change_Time'] = GetKeyValue(record, 'CHNGTI') / GetKeyValue(record, 'CHNGCNT')
            else:
                processed_result['ST03_Avg_DB_Change_Time'] = 0.0
        
            # ST03 - Average DB Procedure latency
            if (GetKeyValue(record, 'DBP_COUNT') != 0):
                processed_result['ST03_Avg_DB_Procedure_Time'] = GetKeyValue(record, 'DBP_TIME') / GetKeyValue(record, 'DBP_COUNT')
            else:
                processed_result['ST03_Avg_DB_Procedure_Time'] = 0.0
            
            processed_results.append(processed_result)

        return processed_results

    """
    take parsed SWNC result set and decorate each record with additional fixed set of 
    properties needed for metrics records
    """
    def _decorateSwncWorkloadMetrics(self, records: list, queryWindowEnd: datetime) -> None:
        currentTimestamp = datetime.now(timezone.utc)

        for record in records:
            # swnc workload metrics are aggregated across the SID, so no need to include hostname/instance/subdomain dimensions
            record['timestamp'] = currentTimestamp
            record['serverTimestamp'] = queryWindowEnd
            record['SID'] = self.sapSid
            record['client'] = self.sapClient

    """
    make RFC call GET_DUMP_LOG and return result records
    """
    def _rfcGetDumpLog(self,
                          connection: Connection,
                          startDateTime: datetime,
                          endDateTime: datetime):
        rfcName = '/SDF/GET_DUMP_LOG'
        self.tracer.info("[%s] invoking rfc %s for hostname=%s with date_from=%s, time_from=%s, date_to=%s, time_to=%s",
                         self.logTag,
                         rfcName,
                         self.sapHostName,
                         startDateTime.date(),
                         startDateTime.time(),
                         endDateTime.date(),
                         endDateTime.time())
        try:
            short_dump_result = connection.call(rfcName,
                                                DATE_FROM=startDateTime.date(),
                                                TIME_FROM=startDateTime.time(),
                                                DATE_TO=endDateTime.date(),
                                                TIME_TO=endDateTime.time())

            return short_dump_result
        except CommunicationError as e:
            self.tracer.error("[%s] communication error for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.sapHostName, e, exc_info=True)

        except ABAPApplicationError as e:
            self.tracer.error("[%s] Error occured for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.sapHostName, e, exc_info=True)

        except Exception as e:
            self.tracer.error("[%s] Error occured for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.sapHostName, e, exc_info=True)

        return None
    
    """
    return header information from SDF/GET_DUMP_LOG
    """
    def _parseGetDumpLogResults(self, result):
        rfcName = 'SDF/GET_DUMP_LOG'
        if result is None:
            raise ValueError("empty result received for rfc %s for hostname: %s" % (rfcName, self.sapHostName))
        colNames = None
        processedResult = None
        if 'ES_E2E_LOG_STRUCT_DESC' in result:
            # create new dictionary with only values from filterList if filter dictionary exists.
            colNames = result['ES_E2E_LOG_STRUCT_DESC']
            self.tracer.info("[%s] rfc %s returned %d records from hostname: %s",
                             self.logTag, rfcName, len(colNames), self.sapHostName)
        else:
            raise ValueError("%s result does not contain ES_E2E_LOG_STRUCT_DESC key from hostname: %s" % (rfcName, self.sapHostName))
    
        if 'ET_E2E_LOG' in result:
            # create new dictionary with only values from filterList if filter dictionary exists.
            processedResult = result['ET_E2E_LOG']
            self.tracer.info("[%s] rfc %s returned %d records from hostname: %s",
                             self.logTag, rfcName, len(processedResult), self.sapHostName)
        else:
            raise ValueError("%s result does not contain ET_E2E_LOG key from hostname: %s" % (rfcName, self.sapHostName))

        processedResult = self._renameColumnNamesInShortDump(processedResult, colNames)
        return processedResult

    def _renameColumnNamesInShortDump(self, records: list, colNames) -> list:
       dataframe = DataFrame (records,columns=['E2E_DATE','E2E_TIME','E2E_USER','E2E_SEVERITY','E2E_HOST',
                                        'FIELD1','FIELD2','FIELD3','FIELD4','FIELD5','FIELD6','FIELD7',
                                        'FIELD8','FIELD9'])
       dataframe.rename(columns = {"FIELD1": colNames["FIELD1"],
                            "FIELD2": colNames["FIELD2"],
                            "FIELD3": colNames["FIELD3"],
                            "FIELD4": colNames["FIELD4"],
                            "FIELD5": colNames["FIELD5"],
                            "FIELD6": colNames["FIELD6"],
                            "FIELD7": colNames["FIELD7"],
                            "FIELD8": colNames["FIELD8"],
                            "FIELD9": colNames["FIELD9"]},
                             inplace=True, errors='raise')
       return dataframe.to_dict('records')

    """
    take parsed Short dump result set and decorate each record with additional fixed set of 
    properties expected for metrics records
    """
    def _decorateShortDumpMetrics(self, records: list) -> None:
        currentTimestamp = datetime.now(timezone.utc)

        # "E2E_DATE": "20210329",
        # "E2E_TIME": "121703",
        # "E2E_HOST": "sapsbx00_MSX_30"
        
        # regex to extract hostname / SID / instance from SERVER property, since 
        # every short dump analysis record will contain host/instances across the 
        # entire SAP landscape
        serverRegex = re.compile(r"(?P<hostname>.+?)_(?P<SID>[^_]+)_(?P<instanceNr>[0-9]+)")

        for record in records:
            # parse DATUM/TIME fields into serverTimestamp
            record['serverTimestamp'] = self._datetimeFromDateAndTimeString(record['E2E_DATE'], record['E2E_TIME'])

            # parse SERVER field into hostname/SID/InstanceNr properties
            m = serverRegex.match(record['E2E_HOST'])
            if m:
                fields = m.groupdict()
                record['hostname'] = fields['hostname']
                record['SID'] = fields['SID']
                record['instanceNr'] = fields['instanceNr']
            else:
                self.tracer.error("[%s] short dump results record had unexpected SERVER format: %s", record['E2E_HOST'])
                record['hostname'] = ''
                record['SID'] = ''
                record['instanceNr'] = ''

            record['client'] = self.sapClient
            record['subdomain'] = self.sapSubdomain
            record['timestamp'] = currentTimestamp