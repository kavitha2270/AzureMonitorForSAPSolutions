# 
#       Azure Monitor for SAP Solutions - Payload
#       (to be deployed on collector VM)
#
#       License:        GNU General Public License (GPL)
#       (c) 2020        Microsoft Corp.
#

# Python modules
from abc import ABC, abstractmethod
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import os
import re
import sys
import threading
from time import sleep
import traceback

# Payload modules
from const import *
from helper.azure import *
from helper.context import Context
from helper.tools import *
from helper.tracing import *
from helper.providerfactory import *
from helper.updateprofile import *
from helper.updatefactory import *

###############################################################################

def runCheck(check):
   global ctx, tracer

   try:
      # Run all actions that are part of this check
      resultJson = check.run()

      # Ingest result into Log Analytics
      ctx.azLa.ingest(check.customLog,
                        resultJson,
                        check.colTimeGenerated)

      # Persist updated internal state to provider state file
      check.providerInstance.writeState()

      # Ingest result into Customer Analytics
      enableCustomerAnalytics = ctx.globalParams.get("enableCustomerAnalytics", True)
      if enableCustomerAnalytics and check.includeInCustomerAnalytics:
            tracing.ingestCustomerAnalytics(tracer,
                                          ctx,
                                          check.customLog,
                                          resultJson)
      tracer.info("finished check %s" % (check.fullName))
   finally:
      ctx.checkLockSet.remove(check.getLockName())

###############################################################################

# Load entire config from KeyVault (global parameters and provider instances)
def loadConfig() -> bool:
   global ctx, tracer
   tracer.info("loading config from KeyVault")

   secrets = ctx.azKv.getCurrentSecrets()
   for secretName in secrets.keys():
      tracer.debug("parsing KeyVault secret %s" % secretName)
      secretValue = secrets[secretName]
      try:
         providerProperties = json.loads(secretValue)
      except json.decoder.JSONDecodeError as e:
         tracer.error("invalid JSON format for secret %s (%s)" % (secretName,
                                                                  e))
         continue
      if secretName == CONFIG_SECTION_GLOBAL:
         ctx.globalParams = providerProperties
         tracer.debug("successfully loaded global config")
      else:
         instanceName = providerProperties.get("name", None)
         providerType = providerProperties.get("type", None)
         try:
            providerInstance = ProviderFactory.makeProviderInstance(providerType,
                                                                    tracer,
                                                                    ctx,
                                                                    providerProperties,
                                                                    skipContent = False)
         except Exception as e:
            tracer.error("could not validate provider instance %s (%s)" % (instanceName,
                                                                           e))
            continue
         ctx.instances.append(providerInstance)
         tracer.debug("successfully loaded config for provider instance %s" % instanceName)
   if ctx.globalParams == {} or len(ctx.instances) == 0:
      tracer.error("did not find any provider instances in KeyVault")
      return False
   return True

# Save specific instance properties to customer KeyVault
def saveInstanceToConfig(instance: Dict[str, str]) -> bool:
   global ctx, tracer
   instanceName = instance.get("name", None)
   tracer.info("saving instance %s to customer KeyVault" % instanceName)
   try:
      secretValue = json.dumps(instance)
   except json.encoder.JSONEncodeError as e:
      tracer.error("cannot JSON encode instance properties (%s)" % e)
      return False   
   result = ctx.azKv.setSecret(instanceName, secretValue)
   return result

# Store credentials in the customer KeyVault
# (To be executed as custom script upon initial deployment of collector VM)
def onboard(args: str) -> None:
   global ctx, tracer
   tracer.info("starting onboarding")

   # Update global parameters and save them to KeyVault
   ctx.globalParams = {"logAnalyticsWorkspaceId": args.logAnalyticsWorkspaceId,
                       "logAnalyticsSharedKey": args.logAnalyticsSharedKey,
                       "enableCustomerAnalytics": args.enableCustomerAnalytics}
   if not ctx.azKv.setSecret(CONFIG_SECTION_GLOBAL,
                             json.dumps(ctx.globalParams)):
      tracer.critical("could not save global config to KeyVault")
      sys.exit(ERROR_SETTING_KEYVAULT_SECRET)

   tracer.info("onboarding successfully completed")
   return

# Used by "onboard" to set each provider instance,
# or by "provider add" to set a single provider instance
def addProvider(args: str = None,
                instanceProperties: Dict[str, str] = None) -> bool:
   global ctx, tracer

   try:
      # TODO: validate metadata
      instanceProperties = {"name": args.name,
                            "type": args.type,
                            "properties": json.loads(args.properties),
                            "metadata": json.loads(args.metadata)}
   except json.decoder.JSONDecodeError as e:
      tracer.error("invalid JSON format (%s)" % e)
      return False

   tracer.info("trying to add new provider instance (name=%s, type=%s)" % (args.name,
                                                                           args.type))

   # Instantiate provider, so we can run validation check
   try:
      newProviderInstance = ProviderFactory.makeProviderInstance(args.type,
                                                                 tracer,
                                                                 ctx,
                                                                 instanceProperties,
                                                                 skipContent = True)
   except Exception as e:
      tracer.critical("could not instantiate %s (%s)" % (args.type,
                                                         e))
      sys.exit(ERROR_ADDING_PROVIDER)

   if not newProviderInstance.validate():
      tracer.critical("validation check for provider instance %s failed" % newProviderInstance.fullName)
      sys.exit(ERROR_ADDING_PROVIDER)
   if not saveInstanceToConfig(instanceProperties):
      tracer.error("could not save provider instance %s to KeyVault" % newProviderInstance.fullName)
      sys.exit(ERROR_ADDING_PROVIDER)
   open(FILENAME_REFRESH, "w")
   tracer.info("successfully added provider instance %s to KeyVault" % newProviderInstance.fullName)
   return True

# Delete a single provider instance by name
def deleteProvider(args: str) -> None:
   global ctx, tracer
   tracer.info("retrieving provider list from KeyVault")

   # Clean up state file
   fileToDelete = "%s.state" % args.name
   found = False
   for f in os.listdir(PATH_STATE):
      if f == fileToDelete:
         os.remove(os.path.join(PATH_STATE, f))
         tracer.info("state file %s successfully deleted" % fileToDelete)
         found = True
         break
   if not found:
      tracer.error("state file %s not found" % fileToDelete)

   # Delete corresponding secret from KeyVault
   secretToDelete = args.name
   secrets = ctx.azKv.getCurrentSecrets()
   if secretToDelete not in secrets.keys():
      tracer.error("provider instance %s not found in KeyVault (already deleted?)" % secretToDelete)
   else:
      if not ctx.azKv.deleteSecret(secretToDelete):
         tracer.error("error deleting KeyVault secret %s (already marked for deletion?)" % secretToDelete)
      else:
         open(FILENAME_REFRESH, "w")
         tracer.info("provider %s successfully deleted from KeyVault" % secretToDelete)
   return

# Execute the actual monitoring payload
def monitor(args: str) -> None:
   global ctx, tracer
   tracer.info("starting monitor payload")

   pool = ThreadPoolExecutor(NUMBER_OF_THREADS)
   allChecks = []

   while True:
      now = datetime.now()
      secondsSinceRefresh = (now-ctx.lastConfigRefreshTime).total_seconds()
      refresh = False

      # check if config needs refresh
      # needs refresh if 24 hours as passed or refresh file found
      if secondsSinceRefresh > CONFIG_REFRESH_IN_SECONDS:
         tracer.info("Config has not been refreshed in %d seconds, refreshing", secondsSinceRefresh)
         refresh = True
      elif os.path.isfile(FILENAME_REFRESH):
         tracer.info("Refresh file found, refreshing")
         refresh = True

      if refresh:
         allChecks = []
         ctx.instances = []

         if not loadConfig():
            tracer.critical("failed to load config from KeyVault")
            sys.exit(ERROR_LOADING_CONFIG)
         logAnalyticsWorkspaceId = ctx.globalParams.get("logAnalyticsWorkspaceId", None)
         logAnalyticsSharedKey = ctx.globalParams.get("logAnalyticsSharedKey", None)
         if not logAnalyticsWorkspaceId or not logAnalyticsSharedKey:
            tracer.critical("global config must contain logAnalyticsWorkspaceId and logAnalyticsSharedKey")
            sys.exit(ERROR_GETTING_LOG_CREDENTIALS)
         ctx.azLa = AzureLogAnalytics(tracer,
                                      logAnalyticsWorkspaceId,
                                      logAnalyticsSharedKey)

         for i in ctx.instances:
            for c in i.checks:
               allChecks.append(c)

         ctx.lastConfigRefreshTime = datetime.now()
         if os.path.exists(FILENAME_REFRESH):
            os.remove(FILENAME_REFRESH)

      for check in allChecks:
         if check.getLockName() in ctx.checkLockSet:
            tracer.info("[%s] already queued/executing, skipping" % check.fullName)
            continue
         elif not check.isEnabled():
            tracer.info("[%s] not enabled, skipping" % check.fullName)
            continue
         elif not check.isDue():
            tracer.info("[%s] not due for execution, skipping" % check.fullName)
            continue
         else:
            tracer.info("[%s] getting queued" % check.fullName)
            ctx.checkLockSet.add(check.getLockName())
            pool.submit(runCheck, check)
      sleep(CHECK_WAIT_IN_SECONDS)

# prepareUpdate will prepare the resources like keyvault, log analytics etc for the version passed as an argument
# prepareUpdate needs to be run when a version upgrade requires specific update to the content of the resources
def prepareUpdate(args: str) -> None:
    global ctx, tracer
    tracer.info("Preparing for %s" % args.toVersion)
    try:
       updateProfileFactoryObj = updateProfileFactory()
       updateprofile = updateProfileFactoryObj.createUpdateProfile(args.toVersion)
       updateprofile.update(ctx, args.fromVersion)
    except Exception as e:
        sys.stderr.write("Could not fulfill the update requirements for %s" % args.toVersion)

# Ensures the required directory structure exists
def ensureDirectoryStructure() -> None:
   for path in [PATH_STATE, PATH_TRACE]:
      try:
         if not os.path.exists(path):
            os.makedirs(path)   
      except Exception as e:
         sys.stderr.write("could not create required directory %s; please check permissions (%s)" % (path,
                                                                                                     e))
         sys.exit(ERROR_FILE_PERMISSION_DENIED)
   return

# Main function with argument parser
def main() -> None:
   def addVerboseToParser(p: argparse.ArgumentParser) -> None:
      p.add_argument("--verbose",
                     action = "store_true",
                     dest = "verbose",
                     help = "run in verbose mode")
      return

   global ctx, tracer

   # Make sure we have all directories in place
   ensureDirectoryStructure()

   # Build the argument parser
   parser = argparse.ArgumentParser(description = "SAP Monitor Payload")
   subParsers = parser.add_subparsers(title = "actions",
                                      help = "Select action to run")
   subParsers.required = True
   subParsers.dest = "command"

   # Parsers for "provider" command
   prvParser = subParsers.add_parser("provider",
                                      description = "Configuration of monitoring providers",
                                      help = "Configure monitoring providers and their properties")
   prvSubParsers = prvParser.add_subparsers(title = "action",
                                            help = "Select provider action to run")
   prvSubParsers.required = True
   prvSubParsers.dest = "command"
   prvAddParser = prvSubParsers.add_parser("add",
                                           description = "Add a provider",
                                           help = "Add a new monitoring provider to this SAP Monitor")
   prvAddParser.add_argument("--name",
                             required = True,
                             type = str,
                             help = "Name of the monitoring provider")
   prvAddParser.add_argument("--type",
                             required = True,
                             type = str,
                             help = "Type of the monitoring provider")
   prvAddParser.add_argument("--properties",
                             required = True,
                             type = str,
                             help = "Properties of the monitoring provider")
   prvAddParser.add_argument("--metadata",
                             required = False,
                             type = str,
                             help = "Metadata of the monitoring provider",
                             default = "{}")
   addVerboseToParser(prvAddParser)
   prvAddParser.set_defaults(func = addProvider)
   prvDelParser = prvSubParsers.add_parser("delete",
                                           description = "Delete a provider",
                                           help = "Delete an existing monitoring provider from this SAP Monitor")
   prvDelParser.add_argument("--name",
                             required = True,
                             type = str,
                             help = "Name of the monitoring provider")
   addVerboseToParser(prvDelParser)
   prvDelParser.set_defaults(func = deleteProvider)

   # Parsers for "monitor" command
   monParser = subParsers.add_parser("monitor",
                                      description = "Monitoring payload",
                                      help = "Execute the monitoring payload")
   addVerboseToParser(monParser)
   monParser.set_defaults(func = monitor)

   # Parsers for "onboard" command
   onbParser = subParsers.add_parser("onboard",
                                     description = "Onboard payload",
                                     help = "Onboard payload by adding credentials into KeyVault")
   onbParser.set_defaults(func = onboard,
                          command = "onboard")
   onbParser.add_argument("--logAnalyticsWorkspaceId",
                          required = True,
                          type = str,
                          help = "Workspace ID (customer ID) of the Log Analytics Workspace")
   onbParser.add_argument("--logAnalyticsSharedKey",
                          required = True,
                          type = str,
                          help = "Shared key (primary) of the Log Analytics Workspace")
   onbParser.add_argument("--enableCustomerAnalytics",
                          required = False,
                          help = "Setting to enable sending metrics to Microsoft",
                          action = "store_true",
                          dest="enableCustomerAnalytics")
   addVerboseToParser(onbParser)
   onbParser.set_defaults(enableCustomerAnalytics=False)

   # Parsers for "update" command
   updParser = subParsers.add_parser("update",
                                     description = "Prepares resources for the given version",
                                     help = "Run this before starting the next version")
   updParser.add_argument("--toVersion",
                           required = True,
                           type = str,
                           help = "Prepare resources for this target version")
   updParser.add_argument("--fromVersion",
                           required = True,
                           type = str,
                           help = "Pass the previous version (i.e. the currently running version)")
   addVerboseToParser(updParser)
   updParser.set_defaults(func = prepareUpdate)

   args = parser.parse_args()
   tracer = tracing.initTracer(args)
   ctx = Context(tracer, args.command)
   args.func(args)
   return

ctx = None
tracer = None
if __name__ == "__main__":
   main()

