# Databricks notebook source
# MAGIC %md
# MAGIC # neudesic-framework-functions
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
# MAGIC |2018-09-07| Eddie Edgeworth | Created |
# MAGIC |2019-06-17| Mike Sherrill | Edited:  Modified for CLA |
# MAGIC |2022-01-10| Nate Pimentel and Butch Johnson | Edited:  moved query code from functions to stored procedures so code is in one place |
# MAGIC |2022-01-28| Cristian Vasconez | Edited:  Modified Merge Statement to handle deletes |
# MAGIC |2022-02-25| Butch Johnson | Edited:  Removed secrets notebook and Modifed framework functions to not use secrets |
# MAGIC ## Other Details
# MAGIC This notebook contains all the functions and utilities used by the Neudesic Framework.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize
# MAGIC Load any required notebooks/libraries

# COMMAND ----------

# MAGIC %sh pip install pyodbc

# COMMAND ----------

import uuid
import pyodbc
from datetime import datetime as dt
from datetime import timezone 
import pandas as pd

# COMMAND ----------

#Librarires for AWS
import boto3
import base64
from botocore.exceptions import ClientError
import json

# COMMAND ----------

frame_conn = ""
secretScopeType = ""

# COMMAND ----------

# --No longer needed--
#file_type = "csv"
#first_row_header = "true"
#delimiter = ","

#uploaded the s3 keys csv file 
#source_path = "/FileStore/new_user_credentials.csv"

#retreiving the s3 access key and secret key
#aws_s3_keys_df = spark.read.format(file_type).option("header",first_row_header).option("sep",delimiter).load(source_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Framework DB Functions

# COMMAND ----------

# DBTITLE 1,Required to Connect SQL Server
# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

def build_framework_sql_odbc_connection(connection,scopeType):
    
    ACCESS_KEY = dbutils.secrets.get(scope = 'dpaframework', key = 'ACCESS_KEY')
    SECRET_KEY = dbutils.secrets.get(scope = 'dpaframework', key = 'SECRET_KEY')
    
    secret_name = 'metadatacreds'
    region_name = 'us-east-1'
    
    session = boto3.session.Session(ACCESS_KEY, SECRET_KEY)
    client = session.client(service_name = 'secretsmanager', region_name = region_name)
    
    get_secret = client.get_secret_value(SecretId = secret_name)
    secret_value = json.loads(get_secret['SecretString'])
    
    if scopeType == "Admin":
        frameworkDbServer = secret_value['host']
        frameworkDbName = secret_value['databaseName']
        frameworkUserName = secret_value['username']
        frameworkPwd = secret_value['password']
    else:
        frameworkDbServer = secret_value['host']
        frameworkDbName = secret_value['databaseName']
        frameworkUserName = secret_value['username']
        frameworkPwd = secret_value['password']
        
    jdbcPort = 1433
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+frameworkDbServer+';DATABASE='+frameworkDbName+';UID='+frameworkUserName+';PWD='+ frameworkPwd)
    return conn

# COMMAND ----------

#--Not needed for AWS--
#def build_sqldw_jdbc_url_and_connectionProperties():
#    DwServerName = dbutils.secrets.get(scope = "DataPlatformSecretsAdmin", key = "SynapseSQLPoolServerName")
#    DwDatabaseName = dbutils.secrets.get(scope = "DataPlatformSecretsAdmin", key = "SynapseSQLPoolDBName")
#    DwUsername = dbutils.secrets.get(scope = "DataPlatformSecretsAdmin", key = "SQLDWUserName")
#    DwPassword = dbutils.secrets.get(scope = "DataPlatformSecretsAdmin", key = "SQLDWPassword")
#    sqlDwUrlSmall = "jdbc:sqlserver://" + DwServerName + ":1433;database=" + DwDatabaseName + ";user=" + DwUsername+";password={" + DwPassword +"}"
#    return sqlDwUrlSmall

# COMMAND ----------

def execute_framework_stored_procedure_no_results(execsp,scopeType):
    try:
        conn = build_framework_sql_odbc_connection(frame_conn,scopeType)
        cursor = conn.cursor()
        conn.autocommit = True
        cursor.execute(execsp)
        print("Executed SP")
        print(execsp)
        conn.close()
    except Exception as e:
        print(e)
        

# COMMAND ----------

def execute_framework_stored_procedure_with_results(execsp,scopeType):
  conn = build_framework_sql_odbc_connection(frame_conn,scopeType)
  cursor = conn.cursor()
  conn.autocommit = True
  cursor.execute(execsp)
  rc = cursor.fetchall()
  conn.close()
  return rc

# COMMAND ----------

def getMasking (notebookTableExecutionName,scopeType, primaryKeyColumns):
  sql = """EXEC dbo.uspGetMaskingList
   @notebookTableExecutionName='{0}';
  """.format(notebookTableExecutionName)
  conn = build_framework_sql_odbc_connection(frame_conn,scopeType)
  maskSet = pd.read_sql(sql, conn)
  
  #Check that necessary columns are not being masked
  if maskSet.empty == False:
    maskingDF = spark.createDataFrame(maskSet)
    #Remove ModifiedDateTime, PartitionColumn, and edwRowDeleted from masking process
    maskingDFFilteredMDT = maskingDF.where(maskingDF.OriginalFieldName != edwRowModifiedDateTime)
    maskingDFFilteredRD = maskingDFFilteredMDT.where(maskingDFFilteredMDT.OriginalFieldName != edwRowDeleteColumn)
    maskingDFFilteredAll = maskingDFFilteredRD.where(maskingDFFilteredRD.OriginalFieldName != edwPartitionColumn)
    if maskingDFFilteredAll.rdd.isEmpty() == False:
      #Remove Primary Keys from masking list
      for i in primaryKeyColumns:
        maskingDFFilteredAll = maskingDFFilteredAll.where(maskingDFFilteredAll.OriginalFieldName != i)
  else:
    maskingDFFilteredAll = maskSet
  return maskingDFFilteredAll


# COMMAND ----------

def maskData(candidateDF, maskingDF):
  mutatedDF = candidateDF
  for candidateColumn in candidateDF.columns:
    #print(candidateColumn)
    rowDF = maskingDF.filter(maskingDF.OriginalFieldName == candidateColumn).select('MaskingRule', 'MaskingFormat')
    if not rowDF.rdd.isEmpty():
      #display(rowDF)
      row = rowDF.collect()
      #print(row)
      maskingRule = row[0][0]#.toString
      maskingFormat = row[0][1]#.toString
      #print(maskingRule)
      #print(maskingFormat)
      
      mutatedDF = mutatedDF.withColumn(candidateColumn, regexp_replace(candidateColumn, maskingRule, maskingFormat))
  return mutatedDF

# COMMAND ----------

def getColumnDictionary (notebookTableExecutionName,scopeType):
  sql = """EXEC dbo.uspGetFieldNameTranslationList
   @NotebookTableExecutionName='{0}';
  """.format(notebookTableExecutionName)
  conn = build_framework_sql_odbc_connection(frame_conn,scopeType)
  dictionary = pd.read_sql(sql, conn)
  return dictionary

# COMMAND ----------

def getSelectedColumns (notebookTableExecutionName,translateFields,scopeType):
  if translateFields:
    fields = 1
  else:
    fields = 0
  sql = """EXEC dbo.uspGetFilterColumnList
   @NotebookTableExecutionName='{0}'
   ,@TranslateFields={1};
  """.format(notebookTableExecutionName,fields)
  conn = build_framework_sql_odbc_connection(frame_conn,scopeType)
  selectedColumns = pd.read_sql(sql, conn)
  return selectedColumns

# COMMAND ----------

def log_event_notebook_start (notebookName, parentPipelineExecutionLogKey,notebookTableExecutionName,scopeType):
  pipeLineTriggerID = str(uuid.uuid4())
  pipeLineRunId = str(uuid.uuid4())
  pipeLineTriggerName = pipeLineTriggerID
  pipeLineTriggerType = "PipelineActivity"
  notebookStartDate = str(dt.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
 
  logEventNotebookStart = """EXEC dbo.uspLogEventNotebookStart
   @NotebookName='{0}'
  ,@PipeLineTriggerID='{1}'
  ,@PipeLineRunId='{2}'
  ,@DataFactoryName=''
  ,@PipeLineTriggerName='{3}'
  ,@PipeLineTriggerType='{4}'
  ,@NotebookStartDate='{5}'
  ,@ParentPipeLineExecutionLogKey={6}
  ,@NotebookTableExecutionName={7}
  """.format(notebookName, pipeLineTriggerID, pipeLineRunId, pipeLineTriggerName, pipeLineTriggerType, notebookStartDate, parentPipelineExecutionLogKey,notebookTableExecutionName)
  #print(logEventNotebookStart)
  rc = execute_framework_stored_procedure_with_results(logEventNotebookStart,scopeType)
  notebookExecutionLogKey = rc[0][0]
  return notebookExecutionLogKey

# COMMAND ----------

def log_event_notebook_error (notebookExecutionLogKey, errorCode, errorDescription, notebookTableExecutionName,scopeType,notebookName):  
  logEventNotebookError = """EXEC dbo.uspLogEventNotebookError
   @NotebookExecutionLogKey={0}
  ,@ErrorCode={1}
  ,@ErrorDescription='{2}'
  ,@NotebookTableExecutionName='{3}'
  ,@NotebookName='{4}';
  """.format(notebookExecutionLogKey, errorCode, errorDescription, notebookTableExecutionName,notebookName)
  execute_framework_stored_procedure_no_results(logEventNotebookError,scopeType)

# COMMAND ----------

def log_event_notebook_end (notebookExecutionLogKey, notebookStatus, notebookName, notebookExecutionGroupName,scopeType):  
  logEventNotebookEnd = """EXEC dbo.uspLogEventNotebookEnd
   @NotebookExecutionLogKey={0}
  ,@NotebookStatus='{1}'
  ,@NotebookName='{2}'
  ,@NotebookExecutionGroupName='{3}';
  """.format(notebookExecutionLogKey, notebookStatus, notebookName, notebookExecutionGroupName)
  execute_framework_stored_procedure_no_results(logEventNotebookEnd,scopeType)

# COMMAND ----------

#%scala
#def build_sql_jdbc_url_and_connectionProperties_Scala(dbserver:String, dbname:String, username:String, pwd:String):(String, java.util.Properties) = {
#   val port = 1433
#   val jdbcUrl = s"jdbc:sqlserver://${dbserver}:${port};database=${dbname}"
#   import java.util.Properties
#   val connectionProperties = new Properties()
#   connectionProperties.put("user",s"${username}")
#   connectionProperties.put("password",s"${pwd}")
#   val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
#   connectionProperties.setProperty("Driver", driverClass)
#   (jdbcUrl, connectionProperties)
#}

# COMMAND ----------

def log_event_pipeline_start (pipeLineName, parentPipelineExecutionLogKey,scopeType):
  pipeLineTriggerID = str(uuid.uuid4())
  pipeLineRunId = str(uuid.uuid4())
  pipeLineTriggerName = pipeLineTriggerID
  pipeLineTriggerType = "PipelineActivity"
  pipeLineStartDate = str(datedttime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
  
  logEventPipelineStart = """EXEC dbo.uspLogEventPipelineStart
   @PipeLineName='{0}'
  ,@PipeLineTriggerID='{1}'
  ,@PipeLineRunId='{2}'
  ,@DataFactoryName=''
  ,@PipeLineTriggerName='{3}'
  ,@PipeLineTriggerType='{4}'
  ,@PipeLineStartDate='{5}'
  ,@ParentPipeLineExecutionLogKey={6};
  """.format(pipeLineName, pipeLineTriggerID, pipeLineRunId, pipeLineTriggerName, pipeLineTriggerType, pipeLineStartDate, parentPipelineExecutionLogKey)
  
  rc = execute_framework_stored_procedure_with_results(logEventPipelineStart,scopeType)
  pipeLineExecutionLogKey = rc[0][0]
  return pipeLineExecutionLogKey

# COMMAND ----------

def log_event_pipeline_end (pipeLineExecutionLogKey, pipeLineStatus, pipeLineName, pipeLineExecutionGroupName,scopeType):  
  logEventPipelineEnd = """EXEC dbo.uspLogEventPipelineEnd
   @PipeLineExecutionLogKey={0}
  ,@PipeLineStatus='{1}'
  ,@PipeLineName='{2}'
  ,@PipeLineExecutionGroupName='{3}';
  """.format(pipeLineExecutionLogKey, pipeLineStatus, pipeLineName, pipeLineExecutionGroupName)
  execute_framework_stored_procedure_no_results(logEventPipelineEnd,scopeType)

# COMMAND ----------

#def get_notebook_parameters_for_all_notebooks(scopeType):
#  jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(frameworkDbServer, frameworkDbName, frameworkUserName, frameworkPwd)
#  query = """(
#    SELECT 
#     n.NotebookKey
#    ,n.NotebookName 
#    ,t.Parameters
#    FROM dbo.Notebook n
#    CROSS APPLY
#    (
#      SELECT p.NotebookParameterName AS pKey, p.NotebookParameterValue AS pValue
#      FROM dbo.NotebookParameter p
#      JOIN dbo.Notebook nb ON p.NotebookKey=nb.NotebookKey
#      WHERE nb.NotebookKey = n.NotebookKey
#      FOR JSON AUTO, WITHOUT_ARRAY_WRAPPER
#    ) t (Parameters)
#  ) t"""
#  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
#  return df

# COMMAND ----------

def get_notebookTable_parameters(functionalAreaName,notebookTableExecutionName,scopeType):
  exSQL = "dbo.uspGetNotebookParametersDatabricks @FunctionalAreaName='" + functionalAreaName + "', @NotebookTableExecutionName='" + notebookTableExecutionName + "'"
  df = execute_framework_stored_procedure_with_results(exSQL,scopeType)
  return df

# COMMAND ----------

def get_notebook_execution_list(notebookExecutionGroupName,scopeType):
  exSQL = "dbo.uspGetNotebookExecutionList @notebookExecutionGroupName='" + notebookExecutionGroupName + "'"
  df = execute_framework_stored_procedure_with_results(exSQL,scopeType)
  return df

# COMMAND ----------

def get_adls_delta_table_list(optimizeOnly,scopeType):
  exSQL = "dbo.uspGetAdlsDeltaTableListForOptimize @optimizeOnly=" + str(optimizeOnly) 
  df = execute_framework_stored_procedure_with_results(exSQL,scopeType)
  return df

# COMMAND ----------

def remove_xdays_adls_delta_table(numberDays,scopeType):
  exSQL = """dbo.uspDeleteADLSDeltaTableList @KeepXDays={0}""".format(numberDays)
  print(exSQL)
  execute_framework_stored_procedure_no_results(exSQL,scopeType)


# COMMAND ----------

def insert_adls_delta_table(notebookTableExecutionName,filePath,optimize,UpdateOptimizedDateOnly,scopeType):
  if optimize==True:
    optimizeBit = 1
  else:
    optimizeBit = 0    
  exSQL = """dbo.uspInsertADLSDeltaTableList @NotebookTableExecutionName='{0}'
    ,@ADLSTablePath='{1}'
    ,@OptimizeTable={2}
    ,@UpdateOptimizedDateOnly={3}
    """.format(notebookTableExecutionName,filePath,optimizeBit,UpdateOptimizedDateOnly)
  #print(exSQL)
  execute_framework_stored_procedure_no_results(exSQL,scopeType)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notebook Utility Functions

# COMMAND ----------

def file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

def get_files_with_extension(directoryPath,fileExtension):
  """recursively list path of all files in path directory with fileExtension """
  files = []
  files_to_treat = dbutils.fs.ls(directoryPath)
  while files_to_treat:
    path = files_to_treat.pop(0).path
    if path.endswith('/'):
      files_to_treat += dbutils.fs.ls(path)
    elif path.endswith(fileExtension):
      files.append(path)
  return files

# COMMAND ----------

def StrDateTimeToPrecionX(strDatetime,precision):
  #strDatetime = '2022-01-25 21:31:25.936512'
  #precision='day'
  if strDatetime == '':
    strDatetime=dt.now()
  precision = precision.lower()
  if (precision == "year" or precision == "quarter" or precision == "month" or precision == "day") == False:
    precision='quarter'

  datetimeValue = dt.fromisoformat(strDatetime)
  year = datetimeValue.strftime("%Y")
  month = datetimeValue.strftime("%m")
  intMonth = int(month)
  day = datetimeValue.strftime("%d")
  if intMonth >=1 and intMonth <=3:
    quarter = '01'
  elif intMonth >=4 and intMonth <=6:
    quarter = '02'
  elif intMonth >=7 and intMonth <=9:
    quarter = '03'
  else: 
    quarter = '04'

  if precision == "year": 
      x = year
  elif precision == "quarter": 
    x = year + quarter
  elif precision == "month": 
    x = year + month
  else:
    x = year + month + day

  return int(x)
  #print(x)


# COMMAND ----------

def run_with_retry(notebook, timeout, args = {}, max_retries = 3):
  num_retries = 0
  while True:
    try:
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      if num_retries > max_retries:
        raise e
      else:
        print ("Retrying error", e)
        num_retries += 1

# COMMAND ----------

def get_notebookName(notebookContext):
  fullPathAndName = notebookContext.notebookPath().get()
  return fullPathAndName.split("/")[-1]


# COMMAND ----------

def get_column_pk_list(primaryKeys_list):
  quotes = ["a.`"+c+"`" for c in primaryKeys_list]
  return ", ".join(quotes)

# COMMAND ----------

def get_pk_on_clause(primaryKeys_list):
  quotes = ["a.`"+c+"`=b.`"+c+"`" for c in primaryKeys_list]
  return " and ".join(quotes)

# COMMAND ----------

#Function to build Merge Statement. Merge deletes, updates and inserts
def build_merge_SQL_Statement(deltaTableName,tableName,columns_list, primaryKeys_list, isDeleteEnabled, deleteFlagColumnName,scopeType):
  def get_column_insert_list(columns_list):
    quotes = ["`"+c+"`" for c in columns_list]
    return ",".join(quotes)
  def get_column_insert_values_list(columns_list, deltaTableName):
    quotes = ["b.`"+c+"`" for c in columns_list]
    return ",".join(quotes)
  def get_column_update_clause(columns_list):
    quotes = ["a.`"+c+"`= b.`"+c+"`" for c in columns_list]
    return ",".join(quotes)
  
  column_insert_list = get_column_insert_list(columns_list)
  column_insert_values_list = get_column_insert_values_list(columns_list, deltaTableName)
  column_update_clause = get_column_update_clause(columns_list)
  pk_on_clause = get_pk_on_clause(primaryKeys_list)
  delete_flag_column_name = "b."+deleteFlagColumnName
  
  if isDeleteEnabled == True:
    sql = """MERGE INTO {1} AS a USING {0} AS b ON {5}
    WHEN MATCHED AND {6} = 1 THEN DELETE
    WHEN MATCHED THEN UPDATE SET {4}
    WHEN NOT MATCHED AND {6} = 0 THEN INSERT ({2}) VALUES({3})
  """.format(deltaTableName,tableName,column_insert_list,column_insert_values_list,column_update_clause, pk_on_clause, delete_flag_column_name)
  else:
    sql = """MERGE INTO {1} AS a USING {0} AS b ON {5}
    WHEN MATCHED THEN UPDATE SET {4}
    WHEN NOT MATCHED THEN INSERT ({2}) VALUES({3})
  """.format(deltaTableName,tableName,column_insert_list,column_insert_values_list,column_update_clause, pk_on_clause)
  return sql

# COMMAND ----------

def remove_empty_directories(root):
  #"""recursively list path of all folers in path directory """
  folders = []
  folders_to_treat = dbutils.fs.ls(root)
  while folders_to_treat:
    path = folders_to_treat.pop(0).path
    if path.endswith('/'):
      folders_to_treat += dbutils.fs.ls(path)
      folders.append(path)

  reversedFolders =  reversed(folders)
  for folder in reversedFolders:
    if len(dbutils.fs.ls(folder) ) == 0 :
      dbutils.fs.rm(folder)
      

# COMMAND ----------

def get_framework_current_datetime():
  return dt.utcnow()
  

# COMMAND ----------

adlsBaseFolderName = 'idw'

# COMMAND ----------

# MAGIC %md # Copy Activity Stored Procedures

# COMMAND ----------

def log_event_CopyActivity_start (sinkName):
  CopyActivityTriggerID = str(uuid.uuid4())
  ServerExecutionId = 0
  MachineName = 'neudevwu2daiadb01'
  UserName= 'Databricks'
  CopyActivityStartDate = str(dt.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
  ParentCopyActivityExecutionLogKey = 0
  CopyActivityCreationDate = str(dt.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
  CopyActivityCreatedBy = UserName
  MasterPipelineExecutionLogKey = 0
 
  CopyActivityStart = """EXEC dbo.uspLogEventCopyActivityStart
   @CopyActivitySinkName='{0}'
  ,@CopyActivityTriggerID='{1}'
  ,@ServerExecutionId={2}
  ,@MachineName='{3}'
  ,@UserName='{4}'
  ,@CopyActivityStartDate='{5}'
  ,@ParentCopyActivityExecutionLogKey={6}
  ,@CopyActivityCreationDate='{7}'
  ,@CopyActivityCreatedBy='{8}'
  ,@MasterPipelineExecutionLogKey={9}
  """.format(sinkName, CopyActivityTriggerID, ServerExecutionId, MachineName, UserName, CopyActivityStartDate, ParentCopyActivityExecutionLogKey, CopyActivityCreationDate, CopyActivityCreatedBy, MasterPipelineExecutionLogKey)
  #print(logEventNotebookStart)
  rc = execute_framework_stored_procedure_with_results(CopyActivityStart,scopeType)
  CopyActivityExecutionKey = rc[0][0]
  return CopyActivityExecutionKey

# COMMAND ----------

# Function to get the source and sink types from CopyActivitySinkName column value
def get_source_target_types(CopyActivitySinkName):
    sqlStatement = """EXEC dbo.uspGetSourceTargetTypes @CopyActivitySinkName = {0};""".format(CopyActivitySinkName)
    types = execute_framework_stored_procedure_with_results(sqlStatement,scopeType)
    types = types[0]
    sourceType = types[1]
    targetType = types[2]
    return [CopyActivitySinkName, sourceType, targetType]

# COMMAND ----------

# Fetch the metadata for ingestion by executing the stored proc based on sourceType and targetType

def source_sink_metadata(CopyActivitySinkName, sourceType, targetType):
    resultMap = dict()
    
    # if source is sql and target is s3 bucket
    if sourceType == 'sql' and targetType == 'adls':
        sqlStatement = """EXEC dbo.uspGetCopyActivitySinkSQLToADlS @CopyActivitySinkName = {0};""".format(CopyActivitySinkName)
        queryResult = execute_framework_stored_procedure_with_results(sqlStatement,scopeType)
        queryResult = queryResult[0]
        resultMap['sql_script'] = queryResult[2]
        resultMap['container_name'] = queryResult[4]
        resultMap['landing_path'] = queryResult[5]        
        resultMap['file_name'] = queryResult[6]
    
    # if source is file and target is s3 bucket
    elif sourceType.lower() == 'file' and targetType.lower() == 'adls':
        sqlStatement = """EXEC dbo.uspGetCopyActivitySinkFileToADLS @CopyActivityName = {0};""".format(CopyActivitySinkName)
        queryResult = execute_framework_stored_procedure_with_results(sqlStatement,scopeType)
        queryResult = queryResult[0]
       
        resultMap['sourceFolderPath'] = queryResult[2]
        resultMap['sourceFileName'] = queryResult[3]
        resultMap['sourceFileType'] = queryResult[4]
        resultMap['targetContainerName'] = queryResult[7]
        resultMap['targetFolderPath'] = queryResult[8]
        resultMap['targetFileName'] = queryResult[9]
    
    return resultMap

# COMMAND ----------

import boto3

def get_aws_session(ACCESS_KEY, SECRET_KEY):
    session = boto3.session.Session(ACCESS_KEY, SECRET_KEY)
    return session

# COMMAND ----------

def get_aws_secretvalue(session, region_name, secret_name):
    client = session.client(service_name='secretsmanager',
                       region_name=region_name)
    get_secret = client.get_secret_value(SecretId=secret_name)
    secret_value = json.loads(get_secret['SecretString'])
    
    return secret_value
    

# COMMAND ----------

def copy_file_s3bucket(session, sourceObject, targetBucket, targetObject):
    s3 = session.resource('s3')    
    
    bucket = s3.Bucket(targetBucket)
    
    bucket.copy(sourceObject, targetObject)
    
    # Printing the Information That the File Is Copied.
    print('Single File is copied')
    

# COMMAND ----------

def log_event_CopyActivity_error(CAExecutionLogKey, errorCode, errorDescription):
  CopyActivityError = """EXEC dbo.uspLogEventCopyActivityError
     @CopyActivityExecutionLogKey={0}
    ,@SourceName='Databricks'
    ,@ErrorCode={1}
    ,@ErrorDescription='{2}'""".format(CAExecutionLogKey, errorCode, errorDescription)
  execute_framework_stored_procedure_no_results(CopyActivityError,scopeType)

# COMMAND ----------

def log_event_CopyActivity_end(CAExecutionLogKey, sinkName, CAExecutionGroupName, RowsCopied, CopyDuration):
  CopyActivityEnd = """EXEC dbo.uspLogEventCopyActivityEnd
   @CopyActivityExecutionLogKey={0}
  ,@CopyActivityStatus='Succeeded'
  ,@CopyActivitySinkName='{1}'
  ,@CopyActivityExecutionGroupName='{2}'
  ,@CopyActivityDataRead=0
  ,@CopyActivityDataWrite=0
  ,@CopyActivityFilesRead=1
  ,@CopyActivityFilesWrite=1
  ,@CopyActivityRowsCopied={3}
  ,@CopyActivityRowsSkipped=0
  ,@CopyActivityThroughput=0
  ,@CopyActivityCopyDuration={4}
  ,@CopyActivityUsedCloudDataMovementUnits=0
  ,@CopyActivityUsedParallelCopies=1;""".format(CAExecutionLogKey, sinkName, CAExecutionGroupName,RowsCopied,CopyDuration)
  execute_framework_stored_procedure_no_results(CopyActivityEnd,scopeType)

# COMMAND ----------

def uspGenerateHydration():
    GenerateHydration = 'EXEC dbo.uspGenerateHydration'
    execute_framework_stored_procedure_no_results(GenerateHydration, scopeType)

# COMMAND ----------

def mount_s3(ACCESS_KEY,SECRET_KEY,s3_bucket_name,mount_path):
    ACCESS_KEY = urllib.parse.quote(ACCESS_KEY, "")
    SECRET_KEY = urllib.parse.quote(SECRET_KEY, "")
    try :
        dbutils.fs.mount(f"s3a://{ACCESS_KEY}:{SECRET_KEY}@{s3_bucket_name}","/mnt/external")
        print("Mount Successful :" ,s3_bucket_name )
    except Exception as e:
        print("Mount Unsuccessful :",s3_bucket_name)
        raise f"{e}"
    

# COMMAND ----------

def unmount_s3(mount_path):
    dbutils.fs.unmount(mount_path)
    print("Unmounted : ", mount_path)
    

# COMMAND ----------

# MAGIC %md ### Call other notebook using dbutils run command

# COMMAND ----------

def run_with_retry(notebook, timeout, args = {}, max_retries = 3):
    num_retries = 0
    while True:
        try:
            return dbutils.notebook.run(notebook, timeout, args)
        except Exception as e:
            if num_retries > max_retries:
                raise e
            else:
                print("Retrying error", e)
                num_retries += 1
