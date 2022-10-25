# Databricks notebook source
# MAGIC %md
# MAGIC # delta-table-optimize
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Originally Created By | [butch.johnson@neudesic.com](mailto:butch.johnson@neudesic.com)|
# MAGIC |External References |[https://neudesic.com](https://neudesic.com) |
# MAGIC |Input  |<ul><li>adlsDeltaTableListTTL: Number of days to keep records in the table ADLSDeltaTable/li><li>parrallelOptimizeNumber: Number of tables to optimize at the same time</li> |
# MAGIC |Input Data Source |<ul><li></li></ul> |
# MAGIC |Output Data Source |<ul><li></li></ul> |
# MAGIC 
# MAGIC ## History
# MAGIC 
# MAGIC | Date | Developed By | Change |
# MAGIC |:----:|--------------|--------|
# MAGIC |2022-02-22| Butch Johnson | Created |
# MAGIC |2022-02-25| Butch Johnson | Edited:  Removed secrets notebook and Modifed framework functions to not use secrets |
# MAGIC 
# MAGIC ## Other Details
# MAGIC This notebook runs optimize on adls delta tables across data layers.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize
# MAGIC Load any required notebooks/libraries

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import pyodbc

# COMMAND ----------

# MAGIC %run ./neudesic-framework-functions

# COMMAND ----------

#dbutils.widgets.removeAll()
#Main Notebook Parameters
dbutils.widgets.text(name="parentNotebookExecutionLogKey", defaultValue="-1", label="Parent Notebook Execution Log Key")

#Optimize Notebook parameters
dbutils.widgets.text(name="parrallelOptimizeNumber", defaultValue="8", label="parrallel Optimize Number")
dbutils.widgets.text(name="adlsDeltaTableListTTL", defaultValue="14", label="adlsDeltaTableListTTL")

#Main Notebook Parameters
parentNotebookExecutionLogKey = dbutils.widgets.get("parentNotebookExecutionLogKey")

#Optimize Notebook parameters
adlsDeltaTableListTTL = int(dbutils.widgets.get("adlsDeltaTableListTTL"))
parrallelOptimizeNumber = int(dbutils.widgets.get("parrallelOptimizeNumber"))

secretScopeType="Developer"

print("parentNotebookExecutionLogKey: {0}".format(parentNotebookExecutionLogKey))
print("")
print("adlsDeltaTableListTTL: {0}".format(adlsDeltaTableListTTL))
print("parrallelOptimizeNumber: {0}".format(parrallelOptimizeNumber))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Start Logging

# COMMAND ----------

#Start Logging
notebookName = get_notebookName(dbutils.notebook.entry_point.getDbutils().notebook().getContext())
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentNotebookExecutionLogKey,secretScopeType)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC #### Get Table Optimization List

# COMMAND ----------

#Retreive list of tables to Optimize
try:
  optimizeTableList = get_adls_delta_table_list(1,secretScopeType)#.collect() 
  print(optimizeTableList)
except Exception as e:
  errorCode = '401'
  errorDescription = "Optimize "
  #log_event_notebook_error(notebookExecutionLogKey,errorCode,errorDescription + " Exception: " + str(e),"")  
  #log_event_pipeline_error(pipeLineExecutionLogKey,sourceName,errorCode,errorDescription)
  raise e

# COMMAND ----------

def optimize_delta_tables(NotebookTableKey,ADLSTablePath,NoteTableName):
  print("Optimize " + NoteTableName + ": " + ADLSTablePath)
  print("")
  sql = "OPTIMIZE '" + ADLSTablePath + "'"
  optimizeResult_df = spark.sql(sql)
  #updates ADLSDeltaTableList in framework with the optimize date and time
  insert_adls_delta_table(NoteTableName,ADLSTablePath,1,1)
  display(optimizeResult_df)

# COMMAND ----------

from multiprocessing.pool import ThreadPool
pool = ThreadPool(parrallelOptimizeNumber)
timeout = 1200
pool.starmap(
optimize_delta_tables,optimizeTableList
  )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanup old records from ADLSDelteTableList

# COMMAND ----------

remove_xdays_adls_delta_table(adlsDeltaTableListTTL)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName="",scopeType=secretScopeType)
dbutils.notebook.exit("Succeeded")
