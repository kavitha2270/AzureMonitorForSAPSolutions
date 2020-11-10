# Python modules
import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta, time

# Payload modules
from const import *
from helper.azure import *
from helper.context import *
from helper.tools import *
from provider.base import ProviderInstance, ProviderCheck
from typing import Dict, List

class sapServiceProviderInstance(ProviderInstance):
    def __init__(self,
                tracer: logging.Logger,
                ctx: Context,
                providerInstance: Dict[str, str],
                skipContent: bool = False,
                **kwargs):
        self.sapHostName = None
        self.sapSysNr = None
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
    parse provider properties and get host name, sysnr, username, password.
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

        self.sapUsername = self.providerProperties.get("sapUsername", None)
        if not self.sapUsername:
            self.tracer.error("[%s] sapUsername cannot be empty" % self.fullName)
            return False

        self.sapPassword = self.providerProperties.get("sapPassword", None)
        if not self.sapPassword:
            self.tracer.error("[%s] sapPassword cannot be empty" % self.fullName)
            return False

        return True

    def _establish_connection(self):
        pass

    def validate(self) -> bool:
        self.tracer.info("connecting to sap with host name (%s) to test required rfc calls.")

        try:
            self._establish_connection()

        except Exception as e:
            self.tracer.error("Error occured while validating %s " % (e))
            return False

        return True

###########################
class sapServiceProviderCheck(ProviderCheck):
    lastResult = list()

    def __init__(self,
    provider: ProviderInstance,
    **kwargs
    ):
        self.lastRunTime = None
        return super().__init__(provider, **kwargs)

    def _actionExecuteWebServiceRequest(self, apiName):
        self.tracer.info("executing web service request: %s" % (apiName))

        # TODO: Implement logic

        # Update internal state
        if not self.updateState():
            raise Exception("Failed to update state")
        self.tracer.info("successfully processed web service request: %s" % (apiName))

    def generateJsonString(self) -> str:
        self.tracer.info("[%s] converting result to json string." % self.fullName)
        resultJsonString = json.dumps(self.lastResult, sort_keys=True, indent=4, cls=JsonEncoder)
        self.tracer.debug("[%s] resultJson=%s" % (self.fullName,
                                                   str(resultJsonString)))
        return resultJsonString

    def updateState(self):
        self.tracer.info("[%s] updating internal state" % self.fullName)

        # update last run local.
        lastRunLocal = datetime.utcnow()
        self.state['lastRunLocal'] = lastRunLocal
        # update last run server.
        self.state['lastRunServer'] = self.lastRunTime
        self.tracer.info("[%s] internal state successfully updated" % self.fullName)
        return True
