# Databricks notebook source
# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

# MAGIC %run ../utility/neudesic-framework-functions

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# User Parameters; Need to figure out how to read values from Airflow

dbutils.widgets.text(name="CopyActivityExecutionGroupName", defaultValue ="RentalAgreementRawCopyProcess", label="Copy Activity Execution Group Name")
dbutils.widgets.text(name="SystemExecutionGroupName", defaultValue="RentalAgreement", label= "System Execution Group Name")
dbutils.widgets.text(name="MasterPipelineExecutionLogKey", defaultValue="0", label= "Master Pipeline Execution Log Key")
dbutils.widgets.text(name="PipelineScheduleName", defaultValue="Daily", label="Pipeline Schedule Name")
dbutils.widgets.text(name="ResetCopyLoop", defaultValue="yes", label="Reset Copy Loop")
dbutils.widgets.text(name="parallelCopyNumber", defaultValue="8", label="Parallel Copy Number")


# COMMAND ----------

CopyActivityExecutionGroupName = dbutils.widgets.get('CopyActivityExecutionGroupName')
SystemExecutionGroupName = dbutils.widgets.get('SystemExecutionGroupName')
MasterPipelineExecutionLogKey = dbutils.widgets.get('MasterPipelineExecutionLogKey')
ScheduleFrequency = dbutils.widgets.get('PipelineScheduleName')
ResetCopyLoop = dbutils.widgets.get('ResetCopyLoop')
parallelCopyNumber = int(dbutils.widgets.get('parallelCopyNumber'))

# COMMAND ----------

print("CopyActivityExecutionGroupName Is: ", CopyActivityExecutionGroupName)
print("SystemExecutionGroupName Is: ", SystemExecutionGroupName)
print("MasterPipelineExecutionLogKey Is: ", MasterPipelineExecutionLogKey)
print("ScheduleFrequency Is: ", ScheduleFrequency)
print("ResetCopyLoop Is: ", ResetCopyLoop)
print("parallelCopyNumber Is: ", parallelCopyNumber)

# COMMAND ----------

scopeType = 'Admin'
copyActivityExecutionListProcSQL = """EXEC dbo.uspGetCopyActivityExecutionList
   @CopyActivityExecutionGroupName='{0}',@PipelineScheduleName='{1}';
  """.format(CopyActivityExecutionGroupName, ScheduleFrequency)
copyActivityExecutionList = execute_framework_stored_procedure_with_results(copyActivityExecutionListProcSQL,scopeType)
copyActivityExecutionList

# COMMAND ----------

notebook = 'copy-activity-ingestion'

# COMMAND ----------

def execute_copy_activity_ingestion(CopyActivitySinkName,CopyActivityExecutionGroupName,MasterPipelineExecutionLogKey):
   
    print(CopyActivitySinkName + ": " + CopyActivityExecutionGroupName + ": " + MasterPipelineExecutionLogKey)  
    
    run_with_retry(notebook, timeout, args = {"CopyActivitySinkName": CopyActivitySinkName, "CopyActivityExecutionGroupName": CopyActivityExecutionGroupName, "MasterPipelineExecutionLogKey": MasterPipelineExecutionLogKey}, max_retries = 0)

# COMMAND ----------

copyActivityExecutionList = [(item[0], CopyActivityExecutionGroupName, MasterPipelineExecutionLogKey) for item in copyActivityExecutionList]

# COMMAND ----------

from multiprocessing.pool import ThreadPool
import time
pool = ThreadPool(parallelCopyNumber)
timeout = 1200
start_time = time.time()
pool.starmap(
execute_copy_activity_ingestion,copyActivityExecutionList
  )
end_time = time.time()
print("Completed in {0} Secs".format(end_time-start_time))

# COMMAND ----------

# for item in result:
    
#     itemMap = {'CopyActivitySinkName':item[0],
#                'CopyActivityExecutionGroupName':CopyActivityExecutionGroupName,               
#                'MasterPipelineExecutionLogKey':'0'
#               }
#     print(itemMap)
    
#     run_with_retry('../source-to-landing/copy-activity-ingestion',60,itemMap)
    

# COMMAND ----------

if ResetCopyLoop.lower()=='yes':
    scopeType = 'Admin'
    uspResetCopyActivityExecutionList = """EXEC dbo.uspResetCopyActivityExecutionList
    @SystemName='{0}';""".format(SystemExecutionGroupName)
    execute_framework_stored_procedure_no_results(uspResetCopyActivityExecutionList, scopeType)
    
    

# COMMAND ----------

scopeType = 'Admin'
systemCopyActivityExecutionEnd = """EXEC dbo.uspLogSystemCAExecutionEnd
   @CopyActivityExecutionGroupName='{0}',
   @SystemExecutionGroupName='{1}';
  """.format(CopyActivityExecutionGroupName,SystemExecutionGroupName)
execute_framework_stored_procedure_no_results(systemCopyActivityExecutionEnd, scopeType)
