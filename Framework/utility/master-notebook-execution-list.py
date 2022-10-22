# Databricks notebook source
# MAGIC %md
# MAGIC # master-notebook-execution-list
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Originally Created By | [eddie.edgeworth@neudesic.com](mailto:eddie.edgeworth@neudesic.com)|
# MAGIC |External References |[https://neudesic.com](https://neudesic.com) |
# MAGIC |Input  |<ul><li> None |
# MAGIC |Input Data Source |<ul><li>None | 
# MAGIC |Output Data Source |<ul><li>global temporary view |
# MAGIC 
# MAGIC ## History
# MAGIC 
# MAGIC | Date | Developed By | Change |
# MAGIC |:----:|--------------|--------|
# MAGIC |2019-04-04| Eddie Edgeworth | Created |
# MAGIC |2022-01-10| Butch Johnson | Edited:  Modified for SS |
# MAGIC |2022-02-25| Butch Johnson | Edited:  Removed secrets notebook and Modifed framework functions to not use secrets |
# MAGIC ## Other Details
# MAGIC This notebook loads a list of tables to process from the framework by notebook execution group.  It then processes each table in the list by passing it to the notebook master-notebook-execution
# MAGIC   
# MAGIC Prerequisites
# MAGIC Azure SQL Framework Database Metatadata must be populated

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize
# MAGIC Load any required notebooks/libraries

# COMMAND ----------

# MAGIC %run ./neudesic-framework-functions

# COMMAND ----------

#get values passed into notebook parameters and set variables. 
dbutils.widgets.removeAll()
dbutils.widgets.text(name="notebookExecutionGroupName", defaultValue="SalesProcess", label="Notebook Execution Group Name")
dbutils.widgets.text(name="parallelNotebookNumber", defaultValue="8", label="Parallel Notebook Number")
notebookExecutionGroupName = dbutils.widgets.get("notebookExecutionGroupName")
parallelNotebookNumber = int(dbutils.widgets.get("parallelNotebookNumber"))

secretScopeType = "Developer"

print("Notebook Execution Group Name: {0}".format(notebookExecutionGroupName))
print("Parallel NotebookNumber: {0}".format(parallelNotebookNumber))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Start Logging

# COMMAND ----------

#Start Logging
notebookName = get_notebookName(dbutils.notebook.entry_point.getDbutils().notebook().getContext())
notebookExecutionLogKey = log_event_notebook_start(notebookName,-1,"None",secretScopeType)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Notebook Execution List

# COMMAND ----------

try:
  notebookExecutionList = get_notebook_execution_list(notebookExecutionGroupName,secretScopeType)#.collect() 
  #notebookExecutionGroupNames = [n.ExecutionGroupName for n in notebookExecutionGroupList]
  #print(notebookExecutionGroupNames)
except Exception as e:
  errorCode = 101
  errorDescription = "Master Notebook Execution List: Get Notebook Execution List for Execution Group: " + notebookExecutionGroupName
  log_event_notebook_error (notebookExecutionLogKey,errorCode,errorDescription + " Exception: " + e,notebookTableExecutionName,secretScopeType,notebookName)  
  raise e
print(notebookExecutionList)

# COMMAND ----------

notebook = "master-notebook-execution"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Notebook Executions and Run Individual Notebooks
# MAGIC * The ThreadPool controls how many processes run concurrently.  

# COMMAND ----------

def execute_master_notebook(functionalAreaName,notebookTableExecutionName):
    #   print(notebookExecutionGroupName + ": " + functionalAreaName + ": " + notebookTableExecutionName)
    print(functionalAreaName + ": " + notebookTableExecutionName)
    
    run_with_retry(notebook, timeout, args = {"notebookExecutionGroupName": notebookExecutionGroupName, "parentNotebookExecutionLogKey": notebookExecutionLogKey, "functionalAreaName": functionalAreaName, "notebookTableExecutionName": notebookTableExecutionName}, max_retries = 0)

# COMMAND ----------

from multiprocessing.pool import ThreadPool
import time
pool = ThreadPool(parallelNotebookNumber)
timeout = 1200
start_time = time.time()
pool.starmap(
execute_master_notebook,notebookExecutionList
  )
end_time = time.time()
print("Completed in {0} Secs".format(end_time-start_time))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completed

# COMMAND ----------

log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName=notebookExecutionGroupName,scopeType=secretScopeType)
dbutils.notebook.exit("Succeeded")

