# Python modules
from abc import ABC, abstractmethod, abstractproperty
from datetime import date, datetime, timedelta
import logging
from typing import Callable, Dict, List, Optional

##########
# Abstract base class to represent interface for SAP NetWeaver SMON and SWNC Workload metric extraction client implementations
##########
class NetWeaverMetricClient(ABC):
    #__metaclass__ = ABCMeta

    def __init__(self, 
                 tracer: logging.Logger,
                 logTag: str):
        self.tracer = tracer
        self.logTag = logTag

    @abstractproperty
    def Hostname(self) -> str:
        pass

    @abstractproperty
    def InstanceNr(self) -> str:
        pass
    
    # validate that config settings and that client can establish connection
    @abstractmethod
    def validate(self) -> bool:
        pass

    # determine appropriate query window start / end time range
    @abstractmethod
    def getQueryWindow(self, 
                       lastRunTime: datetime,
                       minimumRunIntervalSecs: int) -> tuple:
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

    # fetch GET_DUMP_LOG metrics and return as json string
    @abstractmethod
    def getShortDumpsMetrics(self, startDateTime: datetime, endDateTime: datetime) -> str:
        pass

##########
# helper class to instantiate SAP NetWeaver Metric clients while only requiring clients to be aware of interface
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
                                   sapLogonGroup=kwargs.get("sapLogonGroup", None),
                                   sapSid=kwargs.get("sapSid", None),
                                   columnFilterList=None,
                                   serverTimeZone=kwargs.get("serverTimeZone", None))

        except ImportError as importEx:
            tracer.error("failed to import pyrfc module, unable to initialize NetWeaverRfcClient: ", importEx, exc_info=True)
            raise
        except Exception as ex:
            tracer.error("Unexpected failure trying to create NetWeaverRfcClient: ", ex, exc_info=True)
            raise
