# Python modules
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
import logging
from typing import Callable, Dict, List, Optional

##########
# Abstract base class to represent interface for SAP NetWeaver metric extraction client implementations
##########
class NetWeaverMetricClient(ABC):

    def __init__(self, 
                 tracer: logging.Logger,
                 logTag: str):
        self.tracer = tracer
        self.logTag = logTag
    
    # validate that config settings and that client can establish connection
    @abstractmethod
    def validate(self) -> bool:
        pass

    # determine appropriate query window start / end time range
    @abstractmethod
    def getQueryWindow(self, 
                       lastRunTime: datetime,
                       minimumRunIntervalSecs: int) -> tuple[datetime, datetime]:
        pass

    # query sap instance to get current server time
    @abstractmethod
    def getServerTime(self) -> datetime:
        pass

    # fetch all /SDF/SMON_ANALYSIS_READ metric data and return as a single json string
    @abstractmethod
    def getSmonMetrics(self, startDateTime: datetime, endDateTime: datetime) -> str:
        pass

    # fetch SWNC_GET_WORKLOAD_SNAPSHOT data, calculate aggregate metrics and return as json string
    @abstractmethod
    def getSwncWorkloadMetrics(self, startDateTime: datetime, endDateTime: datetime) -> str:
        pass

##########
# helper class to instantiate SAP NetWeaver Metric clients
##########
class MetricClientFactory:

    @staticmethod
    def getMetricClient(tracer: logging.Logger, 
                        logTag: str, 
                        **kwargs) -> NetWeaverMetricClient:
        try:
            import pyrfc
            from netweaver.rfcclient import NetWeaverRfcClient
            return NetWeaverRfcClient(tracer=tracer,
                                   logTag=logTag,
                                   sapHostName=kwargs.get("sapHostName", None),
                                   sapSubdomain=kwargs.get("sapSubdomain", None),
                                   sapSysNr=kwargs.get("sapSysNr", None),
                                   sapClient=kwargs.get("sapClient", None),
                                   sapUsername=kwargs.get("sapUsername", None),
                                   sapPassword=kwargs.get("sapPassword", None),
                                   sapSid=kwargs.get("sapSid", None),
                                   columnFilterList=None,
                                   serverTimeZone=None)

        except ImportError as importEx:
            print("failed to import pyrfc module, unable to initialie RfcMetricClient: ", importEx)

        except Exception as ex:
            print("Unexpected failure trying to create RfcMetricClient: ", ex)
            
        return None
