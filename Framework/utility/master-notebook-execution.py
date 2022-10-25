# Databricks notebook source
# MAGIC %md
# MAGIC # master-notebook-execution
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
# MAGIC This notebook loads the parameters from the framework and runs the notebooks to populate one table to each zone.
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

#master notebook parameters 
# dbutils.widgets.removeAll()
dbutils.widgets.text(name="functionalAreaName", defaultValue="Sales", label="Functional Area Name:")
dbutils.widgets.text(name="notebookExecutionGroupName", defaultValue="SalesProcess", label="Notebook Execution Group Name")
dbutils.widgets.text(name="parentNotebookExecutionLogKey", defaultValue="-1", label="Parent Notebook Execution Log Key")
dbutils.widgets.text(name="notebookTableExecutionName", defaultValue="Customer_SalesProcess", label="NotebookTableExecutionName")

functionalAreaName = dbutils.widgets.get("functionalAreaName")
parentNotebookExecutionLogKey = dbutils.widgets.get("parentNotebookExecutionLogKey")
notebookTableExecutionName = dbutils.widgets.get("notebookTableExecutionName")
notebookExecutionGroupName = dbutils.widgets.get("notebookExecutionGroupName")
#dateToProcess = dbutils.widgets.get("dateToProcess")

secretScopeType = "Developer"

print("functionalAreaName: {0}".format(functionalAreaName))
print("notebookExecutionGroupName: {0}".format(notebookExecutionGroupName))
print("notebookTableExecutionName: {0}".format(notebookTableExecutionName))
print("Parent Notebook Execution Log Key: {0}".format(parentNotebookExecutionLogKey))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Start Logging

# COMMAND ----------

#Start Logging
notebookName = get_notebookName(dbutils.notebook.entry_point.getDbutils().notebook().getContext())
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentNotebookExecutionLogKey,notebookTableExecutionName,secretScopeType)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Metadata from Framework DB

# COMMAND ----------

print("functionalAreaName is :",functionalAreaName)
print("notebookTableExecutionName is :", notebookTableExecutionName)

# COMMAND ----------

try:
    p = get_notebookTable_parameters(functionalAreaName,notebookTableExecutionName,secretScopeType)
    notebookKey, notebookName, parametersString = p[0]
    parameters = {}
    
    stringifiedJson = eval(parametersString)

    for r in stringifiedJson:
        k = r['pKey']
        v = r['pValue']
        parameters[k] = v
    print(parameters)
except Exception as e:
    errorString = "Framework Database is not populated for Notebook Execution Group {0}, Notebook name {1}.".format(notebookExecutionGroupName,notebookTableExecutionName)
    sourceName = "Master Notebook Execution: Get Notebook Parameters"
    errorCode = 102
    errorDescription = e
    log_event_notebook_error (notebookExecutionLogKey, errorCode, errorDescription, notebookTableExecutionName,secretScopeType,notebookName)
    # Determine if we want to seperate the master notebooks into pipeline errors to match what would be logged from Data Factory      log_event_pipeline_error(pipeLineExecutionLogKey,sourceName,errorCode,errorDescription)
    raise ValueError(errorString)



# COMMAND ----------

#master notebook parameters
#dataFactoryName=parameters.get('dataFactoryName')


#Main Notebook Parameters
containerName = parameters.get("containerName")
functionalAreaName = parameters.get("functionalAreaName")
notebookTableExecutionName = parameters.get("notebookTableExecutionName")
optimize = parameters.get("optimize")
parentNotebookExecutionLogKey = parameters.get("parentNotebookExecutionLogKey")
primaryKeyColumns = parameters.get("primaryKeyColumns")

#Landing zone to raw zone parameters
deleteFlagColumnNameAndValue = parameters.get("deleteFlagColumnNameAndValue")
landingFileType = parameters.get("landingFileType")
landingTableName = parameters.get("landingTableName")
originalTableName = parameters.get("originalTableName")
removeLandingFile = parameters.get("removeLandingFile")
remDeletesWithoutModDate  =parameters.get('remDeletesWithoutModDate')
rowModDateColumnName = parameters.get("rowModDateColumnName")
landingToRawZoneNotebook = parameters.get('landingToRawZoneNotebook')
mode = parameters.get('mode')
if landingToRawZoneNotebook:
    landingToRawZoneNotebook = landingToRawZoneNotebook.replace("\\","")

#Client/Source specific
zspRemDeletesWithoutModDate = parameters.get("zspRemDeletesWithoutModDate")

#data vault loading parameters
dataVaultSource = functionalAreaName
dataVaultNotebook = parameters.get('dataVaultNotebook')
if dataVaultNotebook:
    dataVaultNotebook = dataVaultNotebook.replace("\\","")



parentNotebookExecutionLogKey=notebookExecutionLogKey

# COMMAND ----------

print(dataVaultSource)

# COMMAND ----------

#master notebook parameters
#print('master notebook parameters')
#print("notebookExecutionGroupName: {0}".format(notebookExecutionGroupName))
#print()
#Main Notebook Parameters
print('Main Notebook Parameters')
print("s3 bucket name: {0}".format(containerName))
print("functionalAreaName: {0}".format(functionalAreaName))
print("notebookTableExecutionName: {0}".format(notebookTableExecutionName))
print("parentNotebookExecutionLogKey: {0}".format(parentNotebookExecutionLogKey))
print("primaryKeyColumns: {0}".format(primaryKeyColumns))
print()
#landing-to-raw-zone parameters
print('Landing zone to raw zone parameters')
print("deleteFlagColumnNameAndValue: {0}".format(deleteFlagColumnNameAndValue))
print("landingTableName: {0}".format(landingTableName))
print("rowModDateColumnName: {0}".format(rowModDateColumnName))
print("remDeletesWithoutModDate: {0}".format(remDeletesWithoutModDate))
print("removeLandingFile: {0}".format(removeLandingFile))
print("landingToRawZoneNotebook: {0}".format(landingToRawZoneNotebook))
print("mode: {0}".format(mode))
print()

# Data Vault Parameters
print('Data Vault Parameters')
print('Data Vault Source: {0}'.format(dataVaultSource))
print('dataVaultNotebook: {0}'.format(dataVaultNotebook))

# COMMAND ----------

# MAGIC %md
# MAGIC #### landing-to-raw-zone Processing
# MAGIC Loading the Raw Zone of the data lake can be done via Azure Data Factory or Databricks.  
# MAGIC * ADF is recommended for on-premises sources that need to go through a corporate firewall.  
# MAGIC * Databricks is an option if sources are cloud based (though ADF can be used as well).  
# MAGIC * Databricks is recommended when streaming is required

# COMMAND ----------

#landing-to-raw-zone
try:
    if landingToRawZoneNotebook:
        print(notebookTableExecutionName)
        dbutils.notebook.run(landingToRawZoneNotebook, 3600, {"containerName": containerName,
                                                        "functionalAreaName": functionalAreaName,
                                                        "notebookTableExecutionName": notebookTableExecutionName,
                                                        "optimize": optimize,
                                                        "parentNotebookExecutionLogKey": notebookExecutionLogKey,
                                                        "primaryKeyColumns": primaryKeyColumns,
                                                        "deleteFlagColumnNameAndValue": deleteFlagColumnNameAndValue,
                                                        "landingFileType": landingFileType,
                                                        "landingTableName": landingTableName,
                                                        "originalTableName": originalTableName,                                                    
                                                        "removeLandingFile":removeLandingFile,
                                                        "rowModDateColumnName": rowModDateColumnName,
                                                        "zspRemDeletesWithoutModDate": zspRemDeletesWithoutModDate,
                                                        "mode": mode,
                                                        "notebookExecutionGroupName": notebookExecutionGroupName
                                                        })
except Exception as e:
    errorCode = '102'
    errorDescription = "Master Notebook Execution: " + landingToRawZoneNotebook + " has failed" 
    print(errorDescription)
    log_event_notebook_error(notebookExecutionLogKey,errorCode,errorDescription + " Exception: " + e,notebookTableExecutionName,secretScopeType,notebookName) 
    #log_event_pipeline_error(pipeLineExecutionLogKey,sourceName,errorCode,errorDescription)
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Vault Processing

# COMMAND ----------

# bronze-to-silver loading
try:
    if dataVaultNotebook:
        print(notebookTableExecutionName)
        dbutils.notebook.run(dataVaultNotebook, 3600, { "Source": dataVaultSource,
                                                               "notebookTableExecutionName": notebookTableExecutionName,
                                                               "parentNotebookExecutionLogKey": notebookExecutionLogKey,
                                                               "notebookExecutionGroupName": notebookExecutionGroupName

                                                        })
except Exception as e:
    errorCode = '102'
    errorDescription = "Master Notebook Execution: " + dataVaultNotebook + " has failed" 
    print(errorDescription)
    log_event_notebook_error(notebookExecutionLogKey,errorCode,errorDescription + " Exception: " + e,notebookTableExecutionName,secretScopeType,notebookName) 
    #log_event_pipeline_error(pipeLineExecutionLogKey,sourceName,errorCode,errorDescription)
    raise e


# COMMAND ----------

# MAGIC %md
# MAGIC #### raw/classificationdiscovery-to-enriched-source Processing

# COMMAND ----------

# #classificationdiscovery-to-enriched-source 
# try:
#   if rawToEnrichedSourceNotebook:
#     print(sandbox)
#     print(notebookTableExecutionName)
#     dbutils.notebook.run(rawToEnrichedSourceNotebook, 3600, {"adlsAccountName": adlsAccountName,
#                                                         "environment": environment,
#                                                         "functionalAreaName": functionalAreaName,
#                                                         "notebookTableExecutionName": notebookTableExecutionName,
#                                                         "optimize": optimize,
#                                                         "parentNotebookExecutionLogKey": notebookExecutionLogKey,
#                                                         "primaryKeyColumns": primaryKeyColumns,
#                                                         "integrationSourceTableName": integrationSourceTableName,
#                                                         "originalTableName": originalTableName,
#                                                         "sandbox": sandbox,
#                                                         "translateTableFields": translateTableFields
#                                                         })
# except Exception as e:
#   errorCode = '102'
#   errorDescription = "Master Notebook Execution: Run notebook: " + rawToEnrichedSourceNotebook + "Parameters: " + parameters  
#   print(errorDescription)
#   log_event_notebook_error(notebookExecutionLogKey,errorCode,errorDescription + " Exception: " + e,notebookTableExecutionName,secretScopeType,notebookName)  
#   #log_event_pipeline_error(pipeLineExecutionLogKey,sourceName,errorCode,errorDescription)
#   raise e
  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary Zone Processing

# COMMAND ----------

# try:
#   if integrSrcToFlatSanctionedNotebook:
#     print(notebookTableExecutionName)
#     dbutils.notebook.run(integrSrcToFlatSanctionedNotebook, 3600, {"adlsAccountName": adlsAccountName,
#                                                         "environment": environment,
#                                                         "functionalAreaName": functionalAreaName,
#                                                         "notebookTableExecutionName": notebookTableExecutionName,
#                                                         "parentNotebookExecutionLogKey": notebookExecutionLogKey,
#                                                         "integrationSourceTableName": integrationSourceTableName,
#                                                         "sanctionedTableName": sanctionedTableName,
#                                                         "sandbox": sandbox
#                                                         })
# except Exception as e:
#   errorCode = '102'
#   errorDescription = "Master Notebook Execution: Run notebook: " + integrSrcToFlatSanctionedNotebook + "Parameters: " + parameters  
#   print(errorDescription)
#   log_event_notebook_error(notebookExecutionLogKey,errorCode,errorDescription + " Exception: " + e,notebookTableExecutionName,secretScopeType,notebookName)  
#   #log_event_pipeline_error(pipeLineExecutionLogKey,sourceName,errorCode,errorDescription)
#   raise e

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sanctioned Zone Processing

# COMMAND ----------

#if sanctionedZoneNotebookPath != '':
#  run_with_retry(sanctionedZoneNotebookPath, 1800, {"parentPipeLineExecutionLogKey": pipeLineExecutionLogKey, "containerName": containerName, "schemaName": parameters['queryZoneSchemaName'], "tableName": #parameters['queryZoneTableName'], "numPartitions": numPartitions})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName=notebookExecutionGroupName,scopeType=secretScopeType)
dbutils.notebook.exit("Succeeded")

