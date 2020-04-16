# Python modules
import hashlib
import json
import logging
import re
import time
import pyodbc


# Payload modules
from const import *
from helper.azure import *
from helper.tools import *
from provider.base import ProviderInstance, ProviderCheck
from typing import Dict, List

###############################################################################


###############################################################################

class MSSQLProviderInstance(ProviderInstance):
   sqlHostname = None
   sqlDbUsername = None
   sqlDbPassword = None

   def __init__(self,
                tracer: logging.Logger,
                providerInstance: Dict[str, str],
                skipContent: bool = False,
                **kwargs):
      super().__init__(tracer,
                       providerInstance,
                       skipContent,
                       **kwargs)

   # Parse provider properties and fetch DB password from KeyVault, if necessary
   def parseProperties(self):

      self.SQLHostname = self.providerProperties.get("SQLHostname", None)
      if not self.sqlHostname:
         self.tracer.error("[%s] SQLHostname cannot be empty" % self.fullName)
         return False
      self.SQLUser = self.providerProperties.get("SQLUser", None)
      if not selfSQLUser:
         self.tracer.error("[%s] SQLUser cannot be empty" % self.fullName)
         return False
      self.SQLPassword = self.providerProperties.get("SQLPassword", None)
      if not self.SQLPassword:
         sqlDbPasswordKeyVaultUrl = self.providerProperties.get("sqlDbPasswordKeyVaultUrl", None)
         passwordKeyVaultMsiClientId = self.providerProperties.get("keyVaultCredentialsMsiClientID", None)
         if not sqlDbPasswordKeyVaultUrl or not passwordKeyVaultMsiClientId:
            self.tracer.error("[%s] if no password, sqlDbPasswordKeyVaultUrl and keyVaultCredentialsMsiClientID must be given" % self.fullName)
            return False

         # Determine URL of separate KeyVault
         self.tracer.info("[%s] fetching sql credentials from separate KeyVault" % self.fullName)
         try:
            passwordSearch = re.match(REGEX_EXTERNAL_KEYVAULT_URL,sqlDbPasswordKeyVaultUrl,re.IGNORECASE)
            kvName = passwordSearch.group(1)
            passwordName = passwordSearch.group(2)
            passwordVersion = passwordSearch.group(4)
         except Exception as e:
            self.tracer.error("[%s] invalid URL format (%s)" % (self.fullName, e))
            return False

         # Create temporary KeyVault object to fetch relevant secret
         try:
            kv = AzureKeyVault(self.tracer,
                               kvName,
                               passwordKeyVaultMsiClientId)
         except Exception as e:
            self.tracer.error("[%s] error accessing the separate KeyVault (%s)" % (self.fullName,e))
            return False

         # Access the actual secret from the external KeyVault
         # todo: proper (provider-independent) handling of external KeyVaults
         try:
            self.SQLPassword = kv.getSecret(SQLPassword, None).value
         except Exception as e:
            self.tracer.error("[%s] error accessing the secret inside the separate KeyVault (%s)" % (self.fullName,e))
            return False
      return True

   # Validate that we can establish a sql connection and run queries
   def validate(self) -> bool:
      self.tracer.info("connecting to sql instance (%s) to run test query" % self.sqlHostname)

      # Try to establish a sql connection using the details provided by the user
      try:
         connection = self._establishsqlConnectionToHost()
         if not connection.isconnected():
            self.tracer.error("[%s] unable to validate connection status" % self.fullName)
            return False
      except Exception as e:
         self.tracer.error("[%s] could not establish sql connection %s (%s)" % (self.fullName,self.sqlHostname,e))
         return False

      # Try to run a query against the services view
      # This query will (rightfully) fail if the sql license is expired
      try:
         cursor.execute("SELECT * FROM M_SERVICES")
         connection.close()
      except Exception as e:
         self.tracer.error("[%s] could run validation query (%s)" % (self.fullName, e))
         return False
      return True

   def _establishsqlConnectionToHost(self,
                                     SQLHostname: str = None,
                                     SQLUser: str = None,
                                     SQLPasswd: str = None) -> pyodbc.Connection:
      if not SQLHostname:
         SQLHostname = self.sqlHostname
      if not SQLUser:
         SQLUser = self.SQLUser
      if not SQLPasswd:
         SQLUser = self.SQLPasswd

      conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=[%s];UID=[%s];PWD=[%s]" % (SQLHostname, SQLUser, SQLPasswd))

###############################################################################

# Implements a SAP sql-specific monitoring check
class MSSQLProviderCheck(ProviderCheck):
   lastResult = None
   colTimeGenerated = None

   def __init__(self,
                provider: ProviderInstance,
                **kwargs):
      return super().__init__(provider, **kwargs)

   # Obtain one working sql connection
   def _getsqlConnection(self):
      self.tracer.info("[%s] establishing connection with sql instance" % self.fullName)

      try:
        connection = self.providerInstance._establishsqlConnectionToHost()
        #Validate that we're indeed connected
        #if connection.isconnected():
      except Exception as e:
         self.tracer.warning("[%s] could not connect to sql %s (%s)" % (self.fullName,self.SQLHostname,e))
         return (None)
      return (connection)


   # Calculate the MD5 hash of a result set
   def _calculateResultHash(self,
                            resultRows: List[List[str]]) -> str:
      self.tracer.info("[%s] calculating hash of SQL query result" % self.fullName)
      if len(resultRows) == 0:
         self.tracer.debug("[%s] result set is empty" % self.fullName)
         return None
      resultHash = None
      try:
         resultHash = hashlib.md5(str(resultRows).encode("utf-8")).hexdigest()
         self.tracer.debug("resultHash=%s" % resultHash)
      except Exception as e:
         self.tracer.error("[%s] could not calculate result hash (%s)" % (self.fullName,e))
      return resultHash

   # Generate a JSON-encoded string with the last query result
   # This string will be ingested into Log Analytics and Customer Analytics
   def generateJsonString(self) -> str:
      self.tracer.info("[%s] converting SQL query result set into JSON format" % self.fullName)
      logData = []

      # Only loop through the result if there is one
      if self.lastResult:
         (colIndex, resultRows) = self.lastResult
         # Iterate through all rows of the last query result
         for r in resultRows:
            logItem = {
               "CONTENT_VERSION": self.providerInstance.contentVersion,
               "SAPMON_VERSION": PAYLOAD_VERSION,
               "PROVIDER_INSTANCE": self.providerInstance.name,
            }
            for c in colIndex.keys():
               # Unless it's the column mapped to TimeGenerated, remove internal fields
               if c != self.colTimeGenerated and (c.startswith("_") or c == "DUMMY"):
                  continue
               logItem[c] = r[colIndex[c]]
            logData.append(logItem)

      # Convert temporary dictionary into JSON string
      try:
         resultJsonString = json.dumps(logData, sort_keys=True, indent=4, cls=JsonEncoder)
         self.tracer.debug("[%s] resultJson=%s" % (self.fullName,
                                                   str(resultJsonString)))
      except Exception as e:
         self.tracer.error("[%s] could not format logItem=%s into JSON (%s)" % (self.fullName,
                                                                                logItem,
                                                                                e))
      return resultJsonString



   # Connect to sql and run the check-specific SQL statement
   def _actionExecuteSql(self, sql: str) -> bool:
      self.tracer.info("[%s] connecting to sql and executing SQL" % self.fullName)

      # Find and connect to sql server
      connection = self._getsqlConnection()
      if not connection:
         return False

      # Execute SQL statement
      try:
         self.tracer.debug("[%s] executing SQL statement %s" % (self.fullName, sql))
         # todo cursor.execute(sql)

      except Exception as e:
         self.tracer.error("[%s] could not execute SQL %s (%s)" % (self.fullName, sql, e))
         return False

      #self.lastResult = (colIndex, resultRows)
      #self.tracer.debug("[%s] lastResult.resultRows=%s " % (self.fullName, resultRows))

      # Update internal state
      if not self.updateState():
         return False

      # Disconnect from sql server to avoid memory leaks
      try:
         self.tracer.debug("[%s] closing sql connection" % self.fullName)
         connection.close()
      except Exception as e:
         self.tracer.error("[%s] could not close connection to sql instance (%s)" % (self.fullName,e))
         return False

      self.tracer.info("[%s] successfully ran SQL for check" % self.fullName)
      return True

