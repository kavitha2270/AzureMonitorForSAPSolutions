#!/usr/bin/env python3
#
#       Azure Monitor for SAP Solutions payload script
#       (deployed on collector VM)
#
#       License:        GNU General Public License (GPL)
#       (c) 2020        Microsoft Corp.
#

# Python modules
from datetime import datetime
import re
import sys

# Payload modules
from helper.tracing import *

# Internal context handler
class Context(object):
   azKv = None
   sapmonId = None
   vmInstance = None
   vmTage = None
   analyticsTracer = None
   tracer = None

   globalParams = {}
   instances = []
   checkLockSet = set()
   lastConfigRefreshTime = datetime(2020, 1, 1)

   def __init__(self,
                tracer,
                operation: str):
      self.tracer = tracer
      self.tracer.info("initializing context")

      # Retrieve sapmonId via IMDS
      self.vmInstance = AzureInstanceMetadataService.getComputeInstance(self.tracer,
                                                                        operation)
      vmName = self.vmInstance.get("name", None)
      if not vmName:
         self.tracer.critical("could not obtain VM name from IMDS")
         sys.exit(ERROR_GETTING_SAPMONID)

      self.sapmonSubscriptionId = self.vmInstance.get("subscriptionId", None)
      if not self.sapmonSubscriptionId:
         self.tracer.critical("could not obtain sapmon Azure SubscriptionId for collector VM")
         sys.exit(ERROR_GETTING_SAPMONID)

      self.sapmonResourceGroupName = self.vmInstance.get("resourceGroupName", None)
      if not self.sapmonResourceGroupName:
         self.tracer.critical("could not obtain sapmon Azure Resource Group Name for collector VM")
         sys.exit(ERROR_GETTING_SAPMONID)

      try:
         self.sapmonId = re.search("sapmon-vm-(.*)", vmName).group(1)
      except AttributeError:
         self.tracer.critical("could not extract sapmonId from VM name")
         sys.exit(ERROR_GETTING_SAPMONID)

      # get azure resource ID for expected sapmon Managed Identity "sapmon-msi-XXXXX"
      self.msiResourceId = AzureInstanceMetadataService.getSapmonMsiResourceId(
         self.sapmonSubscriptionId,
         self.sapmonResourceGroupName,
         self.sapmonId)

      # specify the exact "sapmon-msi-XXXXX" managed identity to use
      # just in case this VM has multiple Managed Identities assigned to it
      self.authToken, self.msiClientId = AzureInstanceMetadataService.getAuthToken(self.tracer, msiResourceId=self.msiResourceId)

      self.tracer.debug("sapmonId=%s" % self.sapmonId)
      self.tracer.debug("msiClientId=%s" % self.msiClientId)

      # Add storage queue log handler to tracer
      tracing.addQueueLogHandler(self.tracer, self)

      # Initializing tracer for emitting metrics
      self.analyticsTracer = tracing.initCustomerAnalyticsTracer(self.tracer, self)

      # Get KeyVault
      self.azKv = AzureKeyVault(self.tracer,
                                KEYVAULT_NAMING_CONVENTION % self.sapmonId,
                                msiClientId = self.msiClientId)
      if not self.azKv.exists():
         sys.exit(ERROR_KEYVAULT_NOT_FOUND)

      self.tracer.info("successfully initialized context")
