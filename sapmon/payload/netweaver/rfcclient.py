# Python modules
import hashlib
import json
import logging
import re
import time
from datetime import datetime, date, timedelta, time, tzinfo
from typing import Dict, List

# SAP modules
from pyrfc import Connection, ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError

# local abstract base class module
from netweaver.metricclientfactory import NetWeaverMetricClient
from helper.tools import JsonEncoder

# ST03 Workload metric results task id -> text mappings
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

        super().__init__(tracer, logTag)

    #####
    # abstract base class interface methods (NetWeaverMetricClient)
    #####

     # validate that config settings and that client can establish connection
    def validate(self) -> bool:
        return True

    # return tuple of query start / end time range based on last run time
    def getQueryWindow(self, 
                       lastRunTime: datetime,
                       minimumRunIntervalSecs: int) -> tuple[datetime, datetime]:

        # always start with assumption that query window will work backwards from current system time
        currentServerTime = self.getServerTime()

        # usually a query window will end with the current SAP system time and will have a length
        # will have a duration of the minimum check run interval (in seconds)
        # Since SAP requirement is that start and end time must occur on the same calendar day, 
        # we must handle special when that default window crosses over the midnight 00:00:00 boundary
        # into multiple calendar days (in which case it must be truncated)
        if lastRunTime is None:
            # if there was no previous check timestamp, then we start a new query
            # window that works backwards from the SAP system time (by the minimum check run interval time).
            # If that will cross the midnight boundary into the previous calendar day, 
            # then the start time should be moved up to be 00:00:00 on the current day.
            windowEnd = currentServerTime
            windowStart = currentServerTime - timedelta(seconds=minimumRunIntervalSecs)
            currentMidnight = datetime.combine(currentServerTime.date(), time(0, 0, 0))
            windowStart = max(windowStart, currentMidnight)
        else:
            # if we have a previous check timestamp to use as the start of new query window,
            # and this new query window will cross this midnight into the next day,
            # then truncate the query window so that it terminates at 11:59:59PM on the current day. 
            windowStart = lastRunTime + timedelta(seconds=1)
            nextMidnight = datetime.combine(windowStart.date(), time(0, 0, 0)) + timedelta(days=1)
            windowEnd = min(currentServerTime, nextMidnight - timedelta(seconds=1))

        self.tracer.info("[%s] getQueryWindow query window for lastRunTime: %s, currentServerTime: %s -> [start=%s, end=%s]", 
                         self.logTag, 
                         lastRunTime,
                         currentServerTime,
                         windowStart, 
                         windowEnd)

        return windowStart, windowEnd

    # fetch current sap system time stamp and apply the server timezone (if known)
    def getServerTime(self) -> datetime:
        self.tracer.info("executing RFC to get SAP server time")
        with self._getConnection() as connection:
            # read current time from SAP NetWeaver.
            timestampResult = self._rfcGetSystemTime(connection)
            systemDateTime = self._parseSystemTimeResult(timestampResult)

            systemDateTime = systemDateTime.replace(tzinfo=self.tzinfo)

            return systemDateTime

    # fetch all /SDF/SMON_ANALYSIS_READ metric data and return as a single json string
    def getSmonMetrics(self, 
                       startDateTime: datetime,
                       endDateTime: datetime) -> str:
        self.tracer.info("executing RFC SDF/SMON_ANALYSIS_RUN check")
        with self._getConnection() as connection:
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

    # fetch SWNC_GET_WORKLOAD_SNAPSHOT data, calculate aggregate metrics and return as json string
    def getSwncWorkloadMetrics(self,
                               startDateTime: datetime,
                               endDateTime: datetime) -> str:
        self.tracer.info("executing RFC SWNC_GET_WORKLOAD_SNAPSHOT check")
        with self._getConnection() as connection:
            snapshotResult = self._rfcGetSwncWorkloadSnapshot(connection,
                                                              startDateTime=startDateTime, 
                                                              endDateTime=endDateTime)

            parsedResult = self._parseSwncWorkloadSnapshotResult(snapshotResult)

            # add additional common metric properties
            self._decorateSwncWorkloadMetrics(parsedResult, queryWindowEnd=endDateTime)

            return parsedResult

    #####
    # internal RFC Client specific methods
    #####

    # create fully qualified hostname if subdomain was provided
    def _getFullyQualifiedDomainName(self) -> str:
        if self.sapSubdomain:
            return self.sapHostName + "." + self.sapSubdomain
        else:
            return self.sapHostName

    # establish connection to sap.
    def _getConnection(self) -> Connection:
        try:
            connection = Connection(ashost=self.fqdn, 
                                    sysnr=self.sapSysNr, 
                                    client=self.sapClient, 
                                    user=self.sapUsername, 
                                    passwd=self.sapPassword)
            return connection
        except CommunicationError as e:
            self.tracer.error("[%s] error establishing connection with hostname: %s, sapSysNr: %s, error: %s" 
                              % (self.logTag, self.fqdn, self.sapSysNr, e))
        except LogonError as e:
            self.tracer.error("[%s] Incorrect credentials used to connect with hostname: %s username: %s, error: %s" 
                              % (self.logTag, self.fqdn, self.sapUsername, e))
        except Exception as e:
            self.tracer.error("[%s] Error occured while establishing connection to hostname: %s, sapSysNr: %s, error: %s " 
                              % (self.logTag, self.fqdn, self.sapSysNr, e))
        return None

    # make RFC call to get system time
    def _rfcGetSystemTime(self, connection: Connection):
        rfcName = 'BDL_GET_CENTRAL_TIMESTAMP'
        timestampResult = None

        try:
            timestampResult = connection.call(rfcName)
            return timestampResult
        except CommunicationError as e:
            self.tracer.error("[%s] communication error for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.fqdn, e)
        except Exception as e:
            self.tracer.error("[%s] Error occured for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.fqdn, e)

        return None

    # parse system date and time fields from RFC response and return as a datetime.
    # The RFC res[pmse will not have any timezone identifier, so we will apply the known
    # server time zone provided by the caller when the client was created
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

    # helper to take seperate server Date and Time fields and return a datetime
    # object with server timezone applied
    def _datetimeFromDateAndTimeString(self, dateStr: str, timeStr: str) -> datetime:
        # expecte dateStr to have format %Y%m%d
        # expect timeStr to have format %H%M%S
        parsedDateTime = datetime.strptime(dateStr + ' ' + timeStr, '%Y%m%d %H%M%S')
        parsedDateTime = parsedDateTime.replace(tzinfo=self.tzinfo)

        return parsedDateTime

    # make RFC call to fetch list of SDF/SMON snapshot identifiers in the given time range
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
                              self.logTag, rfcName, self.sapHostName, e)
        except Exception as e:
            self.tracer.error("[%s] Error occured for rfc %s with hostname: %s (%s)",
                              self.logTag, rfcName, self.sapHostName, e)

        return None

    # parse result from /SDF/SMON_GET_SMON_RUNS and return Run ID GUID.
    def _parseSmonRunIdsResult(self, result):
        if 'SMON_RUNS' in result:
            if 'GUID' in result['SMON_RUNS'][0]:
                return result['SMON_RUNS'][0]['GUID']
            else:
                raise ValueError("GUID value does not exist in /SDF/SMON_GET_SMON_RUNS return result.")
        else:
            raise ValueError("SMON_RUNS value does not exist in /SDF/SMON_GET_SMON_RUNS return result.")

    # call RFC SDF/SMON_ANALYSIS_RUN to lookup analysis results for specific Run ID and return the result
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
                              self.logTag, rfcName, self.sapHostName, e)
        except Exception as e:
            self.tracer.error("[%s] Error occured for rfc %s with hostname: %s (%s)", 
                              self.logTag, rfcName, self.sapHostName, e)

        return None

    # return header information from sdf/smon_analysis_read
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
    
    # take parsed SMON analysis result set and decorate each record with additional fixed set of 
    # properties expected for metrics records
    def _decorateSmonMetrics(self, records: list) -> None:
        currentTimestamp = datetime.utcnow()

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

            record['subdomain'] = self.sapSubdomain
            record['timestamp'] = currentTimestamp

    # call RFC SWNC_GET_WORKLOAD_SNAPSHOT and return result records
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
                              self.logTag, rfcName, self.sapHostName, e)
        except Exception as e:
            self.tracer.error("[%s] Error occured for rfc %s with hostname: %s (%s)", 
                              self.logTag, rfcName, self.sapHostName, e)

        return None

    # parse results from SWNC_GET_WORKLOAD_SNAPSHOT and enrich with additional properties
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
        
        processed_results = list()
        for record in GetKeyValue(result, 'TASKTIMES'):
            #if (GetKeyValue(record, 'TASKTYPE') not in (b'\xfe', b'\x01')):
            #    continue

            # try to make hex task type code with more descriptive task name to make more readable
            taskTypeId = GetKeyValue(record, 'TASKTYPE')
            taskTypeText = (SAP_TASK_TYPE_MAPPINGS[taskTypeId] 
                            if taskTypeId in SAP_TASK_TYPE_MAPPINGS 
                            else '')

            print("Raw ST03 result record:")
            print(json.dumps(record, indent=4, cls=JsonEncoder))

            processed_result = {
                "Task Type": taskTypeId,
                "Task Type Name": taskTypeText,
                "Number of Steps": GetKeyValue(record, 'COUNT'),
                "Response Time": GetKeyValue(record, 'RESPTI'),
                "CPU Time": GetKeyValue(record, 'CPUTI'),
                "Total DB Time": GetKeyValue(record, 'READSEQTI') + GetKeyValue(record, 'CHNGTI') + GetKeyValue(record, 'READDIRTI'),
                "STO3_RSPNSE": GetKeyValue(record, 'RESPTI') / GetKeyValue(record, 'COUNT'),
                "STO3_CPUTIME": GetKeyValue(record, 'CPUTI') / GetKeyValue(record, 'RESPTI'),
                "STO3_DB": (GetKeyValue(record, 'READSEQTI') + GetKeyValue(record, 'CHNGTI') + GetKeyValue(record, 'READDIRTI')) / GetKeyValue(record, 'RESPTI')
            }

            if (GetKeyValue(record, 'READDIRCNT') != 0):
                processed_result['STO3_Avg_Dir_DB_Time'] = GetKeyValue(record, 'READDIRTI') / GetKeyValue(record, 'READDIRCNT')
            else:
                processed_result['STO3_Avg_Dir_DB_Time'] = 0.0

            if (GetKeyValue(record, 'READSEQCNT') != 0):
                processed_result['ST03_Avg_Seq_DB_Time'] = GetKeyValue(record, 'READSEQTI') / GetKeyValue(record, 'READSEQCNT')
            else:
                processed_result['ST03_Avg_Seq_DB_Time'] = 0.0

            if (GetKeyValue(record, 'CHNGCNT') != 0):
                processed_result['ST03_Avg_chng_DB_Time'] = GetKeyValue(record, 'CHNGTI') / GetKeyValue(record, 'CHNGCNT')
            else:
                processed_result['ST03_Avg_chng_DB_Time'] = 0.0
            
            processed_results.append(processed_result)

        return processed_results

    # take parsed SWNC result set and decorate each record with additional fixed set of 
    # properties needed for metrics records
    def _decorateSwncWorkloadMetrics(self, records: list, queryWindowEnd: datetime) -> None:
        currentTimestamp = datetime.utcnow()

        for record in records:
            record['serverTimestamp'] = queryWindowEnd
            record['hostname'] = self.sapHostName
            record['SID'] = self.sapSid
            record['instanceNr'] = self.sapSysNr
            record['subdomain'] = self.sapSubdomain
            record['timestamp'] = currentTimestamp
