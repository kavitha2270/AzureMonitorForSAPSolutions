# Python modules
import hashlib
import json
import logging
import re
import time

# Payload modules
from const import *
from helper.azure import *
from helper.context import *
from helper.tools import *
from provider.base import ProviderInstance, ProviderCheck
from typing import Dict, List

# SAP modules.
from pyrfc import Connection

class SAPNWMonProviderInstance(ProviderInstance):
    def __init__(self,
                tracer: logging.Logger,
                ctx: Context,
                providerInstance: Dict[str, str],
                skipContent: bool = False,
                **kwargs):
        self.sapHostName -> str = None
        self.sapSysNr -> str = None
        self.sapClient -> str = None
        self.sapUsername -> str = None
        self.sapPassword -> str = None

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

        return true

    def validate(self) -> bool:
        self.tracer.info("connecting to sap with host name (%s) to test required rfc calls.")

        # establish connection to SAP using provided credentials.
        try:
            connection =  Connection(ashost=self.sapHostName, sysnr=self.sapSysNr, client=self.sapClient, user=self.sapUsername, passwd=self.sapPassword)
        except Exception as e:
            self.tracer.error("Could not establish connection to %s with hostname %s (%s)" % (self.fullName, self.sapHostName, e))
            return False

        # call all required rfc to test if provided SAP user has access.
        try:
            result = connection.call('RFCPING')
            print(result)
        except Exception as e:
            self.tracer.error("Could not call RFCPING for hostname %s " % (hostname))

###########################

# implement sapnwmon check.