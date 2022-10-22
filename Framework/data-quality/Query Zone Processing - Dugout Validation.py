# Databricks notebook source
# MAGIC %md
# MAGIC md
# MAGIC # Query Zone Processing - Overwrite dugout Validations
# MAGIC ###### Author: Jason Bian 11/19/2021
# MAGIC 
# MAGIC Data Lake pattern for tables with change feeds of new or updated records.  Takes a file from the raw data path and applies the updates to the Query zone.      
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Raw Data must exist in the Data Lake /raw zone in JSON format.  
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run ../Framework/Neudesic_Framework_Functions

# COMMAND ----------

#dbutils.widgets.removeAll()


# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="dugout", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")

# filePath is to pick up the schema of an actual file in the case of subfolders

dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="dugout", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="TableValidation", label="Table Name")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="", label="Vacuum Retention Hours")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
fullPathPrefix = "abfss://" + containerName + "@" + adlsGen2StorageAccountName + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName

currentStateDestinationPath = fullPathPrefix + "/Query/Enriched/" + schemaName + "/" +  tableName

badRecordsPath = "/BadRecords/" + schemaName + "/" + tableName
fullBadRecordsPath = fullPathPrefix + badRecordsPath
databaseTableName = containerName + "." + tableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")

# COMMAND ----------

notebookName = "Query Zone Processing - Dim Validation"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Current State Destination Path: {0}".format(currentStateDestinationPath))
print("Bad Records Path: {0}".format(fullBadRecordsPath))
print("Database Table Name: {0}".format(databaseTableName))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read STG Data from Query Zone (CurrentState}

# COMMAND ----------

sql = """
CREATE DATABASE IF NOT EXISTS {0}
""".format(schemaName)
spark.sql(sql)

# COMMAND ----------

def droptable(schema_name,table_name):
  sql = """
  DROP TABLE IF EXISTS {0}.{1}
  """.format(schema_name,table_name)
  spark.sql(sql)



# COMMAND ----------

def createtable(schema_name,table_name):
  sql = """
  CREATE TABLE IF NOT EXISTS {0}.{1}
  USING delta
  LOCATION '{2}'
  """.format(schema_name,table_name, fullPathPrefix + "/Query/Enriched/dugout/" + table_name)
  #print(sql)
  spark.sql(sql)



# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType,DateType
from pyspark.sql.functions import *

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
from datetime import timedelta

schema = StructType([StructField("table_name" , StringType()), \
                    StructField("total_rows" , IntegerType()), \
                    StructField("total_columns" , IntegerType()), \
                    StructField("no_of_null_columns" , IntegerType()), \
                    StructField("null_columns" , StringType()), \
                    StructField("no_of_duplicates_on_pky" , IntegerType()),\
                    StructField("primary_keys" , StringType())
                    ])

schema2 = StructType([StructField("table_name" , StringType()), \
                    StructField("total_rows" , IntegerType()), \
                    StructField("total_columns" , IntegerType()), \
                    StructField("no_of_null_columns" , IntegerType()), \
                    StructField("null_columns" , StringType()), \
                    StructField("no_of_duplicates_on_pky" , IntegerType()), \
                    StructField("primary_keys" , StringType()),\
                    StructField("run_date" , StringType())                    
                    ])

# COMMAND ----------

from datetime import datetime
now = datetime.now()
print(now.strftime("%m-%d-%y %H:%M:%S"))

# COMMAND ----------

def isDuplicatePky(df,pky_name):
  """Test 1: Function to return duplicate records in the primary key within the df"""
  
  pky_name = pky_name.split(",")
  
  df = df \
    .groupby(*pky_name) \
    .agg(count(pky_name[0]).alias('count')) \
    .where('count > 1') \
    .sort('count', ascending=False)
  
  return (df)
  

def getNullColumns(df):
  """Test 2: Function to return list of null columns in the df"""
  
  total_count = df.count()
  null_cols = [x for x in df.schema.fieldNames() if df.filter(col(x).isNull()).count() == total_count]
  return null_cols

def getMatchPercent(df1,df2,column1,column2):
  """Test 3: Function to to check for match %s in PK relationships between two dfs"""
  
  df1 = df1.select(column1).dropDuplicates([column1])
  total_count = df1.count()
  df2 = df2.select(column2).dropDuplicates([column2])
  matched_count = df1.join(df2,df1[column1]==df2[column2]).count()
  
  percent = (matched_count/total_count)
  return percent
  
def run_tests_single_table(Table, pk):
  """Function to run Tests 1 and 2"""
  sql = """	  
  select *
  from
  {0}
  """.format(Table)	
  df =(spark.sql(sql))
  
  res_1 = isDuplicatePky(df, pk)
  res_2 = getNullColumns(df)
  
  return res_1, res_2

# COMMAND ----------

def check_column_data_from_table(schema_name,table_name,pky_names):
  
  table_name = schema_name + '.' + table_name

  sql = """
  select * from {0}
  """.format(table_name)
  
  table_df = spark.sql(sql)
  total_rows = table_df.count()
  
  total_columns = len(table_df.schema.names)
  
  df_d,null_columns = run_tests_single_table(table_name,pky_names)

  no_of_null_columns = len(null_columns)
  df_val = spark.createDataFrame([], schema)
  no_of_duplicates_on_pky = df_d.count()
  
  newRow = spark.createDataFrame([(table_name,total_rows,total_columns,no_of_null_columns,str(null_columns),no_of_duplicates_on_pky,pky_names)])
  df_val = df_val.union(newRow)
  
  return df_val

# COMMAND ----------

# Add DIMs and FACTs here

from pyspark.sql.functions import lit
dugout_pkys_dict = {
                    'DIM_Date':'Date',
                    'FACT_AdvanceSales':'EventId',
                    'FACT_Ticket_Transaction':'acct_id',
                    'FACT_TicketMaster_AttendanceScans':'event_id'
                }
dugouttables = list(dugout_pkys_dict.keys())


# COMMAND ----------

df_vals = spark.createDataFrame([], schema2)
now = datetime.now()
run_date = now.strftime("%m-%d-%y %H:%M:%S")

for dugouttable in dugouttables:
  
  try:
    print(dugouttable) 
    createtable("dugout",dugouttable)
    df = check_column_data_from_table("dugout",dugouttable,dugout_pkys_dict[dugouttable])
    df1 = df.withColumn("run_date",lit(run_date))
    df_vals = df_vals.union(df1)
  except Exception as e:
    print ("Errors with: " + dugouttable) 
    print (e)

# COMMAND ----------

display(df_vals) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone (Enriched)

# COMMAND ----------

try:
  queryTableExists = (spark.table(tableName) is not None)
  metadata = spark.sql("DESCRIBE DETAIL " + databaseTableName)
  format = metadata.collect()[0][0]
  if format != "delta":
    sourceName = "Query Zone Processing - Overwrite Delta Lake: Validate Query Table"
    errorCode = "400"
    errorDescription = "Table {0}.{1} exists but is not in Databricks Delta format.".format(schemaName, tableName)
    log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
    raise ValueError(errorDescription)
except:
  queryTableExists = False

# COMMAND ----------

try:
    (df_vals \
      .write \
      .mode("overwrite") \
      .format("delta") \
      .save(currentStateDestinationPath)
    )
    sql = """
    CREATE TABLE IF NOT EXISTS {0}
    USING delta
    LOCATION '{1}'
    """.format(databaseTableName, currentStateDestinationPath)
    spark.sql(sql)
except Exception as e:
  sourceName = "Query Zone Processing - Overwrite Delta Lake: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

if vacuumRetentionHours != '':
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
  spark.sql("VACUUM " + databaseTableName + " RETAIN " + vacuumRetentionHours + " HOURS")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

try:
  dbutils.fs.ls(badRecordsPath)
  sourceName = "Query Zone Processing - Overwrite Delta Lake: Bad Records"
  errorCode = "500"
  errorDescription = "Processing completed, but rows were written to badRecordsPath: {0}.  Raw records do not comply with the current schema for table {1}.{2}.".format(badRecordsPath, schemaName, tableName)
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise ValueError(errorDescription)
except:
  print("success")

# COMMAND ----------

blob_storage_account_name = adlsGen2StorageAccountName
blob_storage_container_name = containerName

tempDir = "abfss://{}@{}.dfs.core.windows.net/".format(blob_storage_container_name, blob_storage_account_name) + "temp/" + dbutils.widgets.get("tableName")

# COMMAND ----------

sqlDwUrlSmall, connectionProperties = build_sqldw_jdbc_url_and_connectionProperties(sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword)

# COMMAND ----------

connectionProperties

# COMMAND ----------

try:
  df_vals \
    .repartition(numPartitions) \
    .write \
    .format("com.databricks.spark.sqldw") \
    .mode("append") \
    .option("url", sqlDwUrlSmall) \
    .option("dbtable", fullyQualifiedTableName) \
    .option("useAzureMSI","True") \
    .option("maxStrLength",2048) \
    .option("tempdir", tempDir) \
    .save()
except Exception as e:
  sourceName = "Sanctioned Zone Processing - Load Azure SQL DW: Load Synapse SQL Data Warehouse"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop Temporary Table and Views

# COMMAND ----------

dbutils.fs.rm(tempDir,True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Completed
# MAGIC val logMessage = "Completed"
# MAGIC val notebookContext = ""
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 
