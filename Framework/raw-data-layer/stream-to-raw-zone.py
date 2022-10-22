# Databricks notebook source
# MAGIC %md
# MAGIC # kafka-stream-to-raw-zone
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Originally Created By | [nate.pimentel@neudesic.com](mailto:nate.pimentel@neudesic.com)|
# MAGIC |External References |[https://neudesic.com](https://neudesic.com) |
# MAGIC |Input  |<ul><li>**Main Notebook Parameters**</li><li>adlsAccountName: name of the storage account</li><li>environment: dev/qa/prod matches base adls container</li><li>functionalAreaName: subfolder for data layers</li><li>mode: how data is going to be processed: merge, append or keep history</li><li>notebookTableExecutionName: The unique name of the table to be processed from the table NotebookTable</li><li>optimize: true/false - whether or not to add this table to the optimize delta table process</li><li>parentNotebookExecutionLogKey: Holds the NotebookExecution log key from the calling notebook</li><li>primaryKeyColumns: Primary Keys of the specified table <li>**Landing zone to raw zone parameters**</li><li>confluentTopicName: This is the name of the confluent topic </li><li>deleteFlagColumnName,Value: Holds the column name that flags the record as deleted and the associated value that marks a delete</li><li>landingTableName: The name of the table in the Landing Zone</li><li>originalTableName: This is the raw and current state table name </li><li>rowModDateColumnName: Column that contains the datetime that that row was updated/created </li><li> **Client/Source Specific Notebook Parameters**</li> |
# MAGIC |Input Data Source |<ul><li>Confluent Kafka topic stream| 
# MAGIC |Output Data Source |<ul><li> Azure Data Lake Store Delta Table|
# MAGIC 
# MAGIC ## History
# MAGIC 
# MAGIC | Date | Developed By | Change |
# MAGIC |:----:|--------------|--------|
# MAGIC |2021-12-14| Nate Pimentel | Created |
# MAGIC |2022-01-10| Butch Johnson | Edited:  Changed Parameters to align with framework db fields |
# MAGIC |2022-01-28| Cristian Vasconez | Edited:  Modified Merge Process and Merge data code to handle deletes |
# MAGIC |2022-02-10| Butch Johnson | Edited:  Added Parameters and added support for DeltaTableOptimize |
# MAGIC |2022-02-25| Butch Johnson | Edited:  Removed secrets notebook and Modifed framework functions to not use secrets |
# MAGIC |2022-05-15| Nate Pimentel | Edited:  Added soft delete functionality
# MAGIC |2022-05-26| Cristian Vasconez | Edited:  Added mode parameter to provide merge, append and keep history dynamic functionality |
# MAGIC   
# MAGIC ## Other Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize
# MAGIC Load any required notebooks/libraries

# COMMAND ----------

# MAGIC %run ../utility/neudesic-framework-functions

# COMMAND ----------

from pathlib import Path
import json
from datetime import datetime as dt
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import *
from delta.tables import *
from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql.avro.functions import from_avro

# COMMAND ----------

# #dbutils.widgets.removeAll()
# #Main Notebook Parameters
# dbutils.widgets.text(name="adlsAccountName", defaultValue="", label="ADLS Account Name:")
# dbutils.widgets.text(name="environment", defaultValue="", label="Environment:")
# dbutils.widgets.text(name="functionalAreaName", defaultValue="", label="Functional Area Name:")
# dbutils.widgets.text(name="mode", defaultValue="", label="Mode:")
# dbutils.widgets.text(name="notebookTableExecutionName", defaultValue="", label="Notebook Table Execution Name:")
# dbutils.widgets.text(name="optimize", defaultValue="", label="Optimize:")
# dbutils.widgets.text(name="parentNotebookExecutionLogKey", defaultValue="-1", label="Parent Notebook Execution Log Key")
# dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Columns:")

# #Landing zone to raw zone parameters
# dbutils.widgets.text(name="deleteFlagColumnNameAndValue", defaultValue="", label="delete Flag Column Name,Value:")
# dbutils.widgets.text(name="landingTableName", defaultValue="", label="landing Table Name:")
# dbutils.widgets.text(name="originalTableName", defaultValue="", label="original Table Name:")
# dbutils.widgets.text(name="rowModDateColumnName", defaultValue="", label="row Mod Date Column Name:")
# #dbutils.widgets.text(name="keepHistory", defaultValue="", label="keep History:")
# dbutils.widgets.text(name="topicName", defaultValue="", label="confluent Topic Name:")

# #Client/Source specific
# #dbutils.widgets.text(name="zspRemDeletesWithoutModDate", defaultValue="", label="zsp remove Deletes Without Mod Date:")

# #Main Notebook Parameters
# adlsAccountName = dbutils.widgets.get("adlsAccountName")
# functionalAreaName = dbutils.widgets.get("functionalAreaName")
# environment = dbutils.widgets.get("environment")
# mode = dbutils.widgets.get("mode")
# notebookTableExecutionName = dbutils.widgets.get("notebookTableExecutionName")
# optimize = (dbutils.widgets.get("optimize").lower() == 'true')
# parentNotebookExecutionLogKey = dbutils.widgets.get("parentNotebookExecutionLogKey")
# primaryKeyColumns = dbutils.widgets.get("primaryKeyColumns").split(",")

# #Landing zone to raw zone parameters
# deleteFlagColumnNameAndValue = dbutils.widgets.get("deleteFlagColumnNameAndValue")
# landingTableName = dbutils.widgets.get("landingTableName")
# originalTableName = dbutils.widgets.get("originalTableName")
# rowModDateColumnName = dbutils.widgets.get("rowModDateColumnName")
# tableHasModifiedColumn = rowModDateColumnName != ''
# confluentTopicName = dbutils.widgets.get("topicName")


# #kafka cluster parameters
# confluentClusterName =  dbutils.secrets.get(scope = "SunstateDataPlatformSecrets", key = "confluentClusterName")
# confluentBootstrapServers = dbutils.secrets.get(scope = "SunstateDataPlatformSecrets", key = "confluentBootstrapServers")
# confluentApiKey = dbutils.secrets.get(scope = "SunstateDataPlatformSecrets", key = "confluentApiKey")
# confluentSecret = dbutils.secrets.get(scope = "SunstateDataPlatformSecrets", key = "confluentSecret")
# confluentRegistryApiKey = dbutils.secrets.get(scope = "SunstateDataPlatformSecrets", key = "confluentRegistryApiKey")
# confluentRegistrySecret = dbutils.secrets.get(scope = "SunstateDataPlatformSecrets", key = "confluentRegistrySecret")
# schemaRegistryUrl = dbutils.secrets.get(scope = "SunstateDataPlatformSecrets", key = "schemaRegistryUrl")


# #Client/Source specific
# #zspRemDeletesWithoutModDate = (dbutils.widgets.get("zspRemDeletesWithoutModDate").lower() == 'true')

# #Notebook Variables
# edwSourceRowModifiedDateTime = 'edwSourceRowModifiedDateTime'
# edwRowCreatedDateTime = 'edwRowCreatedDateTime'
# edwRowModifiedDateTime = 'edwRowModifiedDateTime'
# edwPartitionColumn = 'PartitionColumn'
# edwRowDeleteColumn = 'edwRowDeleted'
# deleteFlagList = deleteFlagColumnNameAndValue.split(',')
# if len(deleteFlagList) >=2:
#   deleteFlagColumnName = deleteFlagList[0]
#   deleteFlagColumnValue = deleteFlagList[1]
# else:
#   deleteFlagColumnName = ""
#   deleteFlagColumnValue = "";

# #Path Settings
# fullPathPrefix = "abfss://" + environment + "@" + adlsAccountName + ".dfs.core.windows.net/" 
# rawZonePath = fullPathPrefix + adlsBaseFolderName +  "/raw-data-layer/raw-zone/" + functionalAreaName + "/" + originalTableName
# landingPath = fullPathPrefix + adlsBaseFolderName +  "/raw-data-layer/landing-zone/" + functionalAreaName + "/" + landingTableName + "/"
# landingPathParquet = fullPathPrefix + adlsBaseFolderName +  "/raw-data-layer/landing-zone/" + functionalAreaName + "/" + "archive/" + landingTableName + "/"
# checkpointLocation = fullPathPrefix + adlsBaseFolderName +  "/raw-data-layer/raw-zone/" + functionalAreaName + "/" + "_stream_checkpoint" + "/" + originalTableName
# schemasPath = fullPathPrefix + adlsBaseFolderName +  "/raw-data-layer/raw-zone/" + functionalAreaName + "/_stream_schema/" + originalTableName

# secretScopeType = "Admin"

# print("adlsAccountName: {0}".format(adlsAccountName))
# print("functionalAreaName: {0}".format(functionalAreaName))
# print("environment: {0}".format(environment))
# print("mode: {0}".format(mode))
# print("notebookTableExecutionName: {0}".format(notebookTableExecutionName))
# print("optimize: {0}".format(optimize))
# print("parentNotebookExecutionLogKey: {0}".format(parentNotebookExecutionLogKey))
# print("primaryKeyColumns: {0}".format(primaryKeyColumns))
# print("")
# print("deleteFlagColumnNameAndValue: {0}".format(deleteFlagColumnNameAndValue))
# print("confluentTopicName: {0}".format(confluentTopicName))
# print("tableHasModifiedColumn: {0}".format(tableHasModifiedColumn))
# print("")
# print("landingTableName: {0}".format(landingTableName))
# print("originalTableName: {0}".format(originalTableName))
# print("rowModDateColumnName: {0}".format(rowModDateColumnName))
# print("")
# print("")
# print("landingPath: {0}".format(landingPath))
# print("landingPathParquet: {0}".format(landingPathParquet))
# print("rawZonePath: {0}".format(rawZonePath))
# print("schemasPath: {0}".format(schemasPath))
# print("checkpointLocation: {0}".format(checkpointLocation))

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

#Main Notebook Parameters
dbutils.widgets.text(name="containerName", defaultValue="", label="s3 Bucket Name")
dbutils.widgets.text(name="functionalAreaName", defaultValue="Sales", label="Functional Area Name:")
dbutils.widgets.text(name="mode", defaultValue="append", label="Mode:")
dbutils.widgets.text(name="notebookTableExecutionName", defaultValue="", label="Notebook Table Execution Name:")
dbutils.widgets.text(name="optimize", defaultValue="true", label="Optimize:")
dbutils.widgets.text(name="parentNotebookExecutionLogKey", defaultValue="-1", label="Parent Notebook Execution Log Key")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Columns:")
dbutils.widgets.text(name="notebookExecutionGroupName", defaultValue="", label="Notebook Execution Group Name")

#Landing zone to raw zone parameters
dbutils.widgets.text(name="deleteFlagColumnNameAndValue", defaultValue="", label="delete Flag Column Name,Value:")
dbutils.widgets.text(name="landingFileType", defaultValue="csv", label="landing File Type:")
dbutils.widgets.text(name="landingTableName", defaultValue="", label="landing Table Name:")
dbutils.widgets.text(name="numberlandingfilesinbatch", defaultValue="100", label="number of landing files in batch:")  #number of files or full load
dbutils.widgets.text(name="originalTableName", defaultValue="", label="original Table Name:") 
dbutils.widgets.text(name="removeLandingFile", defaultValue="false", label="remove Landing File:")
dbutils.widgets.text(name="rowModDateColumnName", defaultValue="", label="row Mod Date Column Name:")

#Client/Source specific
dbutils.widgets.text(name="zspRemDeletesWithoutModDate", defaultValue="false", label="zsp remove Deletes Without Mod Date:")

#Main Notebook Parameters
containerName = dbutils.widgets.get("containerName") 
functionalAreaName = dbutils.widgets.get("functionalAreaName")
mode = dbutils.widgets.get("mode")
notebookTableExecutionName = dbutils.widgets.get("notebookTableExecutionName")
optimize = (dbutils.widgets.get("optimize").lower() == 'true')
parentNotebookExecutionLogKey = dbutils.widgets.get("parentNotebookExecutionLogKey")
primaryKeyColumns = dbutils.widgets.get("primaryKeyColumns").split(",")
notebookExecutionGroupName = dbutils.widgets.get("notebookExecutionGroupName")

#Landing zone to raw zone parameters
if dbutils.widgets.get("deleteFlagColumnNameAndValue").lower()=='none':
    deleteFlagColumnNameAndValue=''
else:
    deleteFlagColumnNameAndValue = dbutils.widgets.get("deleteFlagColumnNameAndValue")

landingFileType = dbutils.widgets.get("landingFileType").lower().replace(" ","")
    
if "multiline" in landingFileType:
    multiLine = 1
    landingFileType = landingFileType.replace("multiline","")
else:
    multiLine = 0

landingTableName = dbutils.widgets.get("landingTableName")

numberlandingfilesinbatch = dbutils.widgets.get("numberlandingfilesinbatch").lower().replace(" ","")
    
if numberlandingfilesinbatch=="fullload":
    numberlandingfilesinbatch=1000000
    fullLoad=1
else:
    fullLoad=0
    
originalTableName = dbutils.widgets.get("originalTableName")
removeLandingFile = (dbutils.widgets.get("removeLandingFile").lower() == 'true')

if dbutils.widgets.get("rowModDateColumnName").lower()=='none':
    rowModDateColumnName=''
else:
    rowModDateColumnName = dbutils.widgets.get("rowModDateColumnName")
    
tableHasModifiedColumn = rowModDateColumnName != ''

#Client/Source specific
zspRemDeletesWithoutModDate = (dbutils.widgets.get("zspRemDeletesWithoutModDate").lower() == 'true')

#Notebook Variables
edwSourceRowModifiedDateTime = 'edwSourceRowModifiedDateTime'
edwRowCreatedDateTime = 'edwRowCreatedDateTime'
edwRowModifiedDateTime = 'edwRowModifiedDateTime'
edwPartitionColumn = 'PartitionColumn'
edwRowDeleteColumn = 'edwRowDeleted'
deleteFlagList = deleteFlagColumnNameAndValue.split(',')
if len(deleteFlagList) >=2:
    deleteFlagColumnName = deleteFlagList[0]
    deleteFlagColumnValue = deleteFlagList[1]
else:
    deleteFlagColumnName = ""
    deleteFlagColumnValue = "" 

# COMMAND ----------

#Path Settings
#rawPathPrefix = "s3a://dpa-hanv-dpdl-1-s3-reg-raw-0001"
#landingPathPrefix = "s3a://dpa-hanv-dpdl-1-s3-int-landing-0001"
#mnt_path = "dbfs:/mnt/dpa-hanv-dpdl-1-s3-reg-raw-0001"
rawZonePath = "s3a://dpa-hanv-dpdl-1-s3-reg-raw-0001/stream_test"
landingPath = "s3a://dpa-hanv-dpdl-1-s3-int-landing-0001/dash/2022/09/13/22/"
#landingPathArchive = mnt_path +  "/landing-zone/" + functionalAreaName + "/archive/" + landingTableName + "/"
#landingPathError = mnt_path +  "/landing-zone/" + functionalAreaName + "/error/" + landingTableName + "/"
schemasPath = rawZonePath + "/schema/" + originalTableName
#dataFactoryQueuePath = mnt_path +  "/landing-zone/datafactoryqueue/"

secretScopeType = "Admin"

print("s3 Bucket: {0}".format(containerName))
print("functionalAreaName: {0}".format(functionalAreaName))
print("mode: {0}".format(mode))
print("notebookTableExecutionName: {0}".format(notebookTableExecutionName))
print("optimize: {0}".format(optimize))
print("parentNotebookExecutionLogKey: {0}".format(parentNotebookExecutionLogKey))
print("notebookExecutionGroupName: {0}".format(notebookExecutionGroupName))
print("primaryKeyColumns: {0}".format(primaryKeyColumns))
print("")
print("deleteFlagColumnNameAndValue: {0}".format(deleteFlagColumnNameAndValue))
print("tableHasModifiedColumn: {0}".format(tableHasModifiedColumn))
print("")
print("landingFileType: {0}".format(landingFileType))
print("multiLine: {0}".format(multiLine))
print("landingTableName: {0}".format(landingTableName))
print("numberlandingfilesinbatch: {0}".format(numberlandingfilesinbatch))
print("originalTableName: {0}".format(originalTableName))
print("rowModDateColumnName: {0}".format(rowModDateColumnName))
print("removeLandingFile: {0}".format(removeLandingFile))
print("")
print("zspRemDeletesWithoutModDate: {0}".format(zspRemDeletesWithoutModDate))
print("")
print("landingPath: {0}".format(landingPath))
print("rawZonePath: {0}".format(rawZonePath))
#print("landingPathArchive: {0}".format(landingPathArchive))
print("Schemas Path: {0}".format(schemasPath))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Start Logging

# COMMAND ----------

##Start Logging
notebookName = get_notebookName(dbutils.notebook.entry_point.getDbutils().notebook().getContext())
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentNotebookExecutionLogKey,notebookTableExecutionName,secretScopeType)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up the client for the Schema Registry

# COMMAND ----------

# from confluent_kafka.schema_registry import SchemaRegistryClient
# import ssl

# schema_registry_conf = {
#     'url': schemaRegistryUrl,
#     'basic.auth.user.info': '{}:{}'.format(confluentRegistryApiKey, confluentRegistrySecret)}

# schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Infer schema

# COMMAND ----------

# #Read schema of a topic from Kakfa schema registry
# topicSchemaResponse = schema_registry_client.get_latest_version(confluentTopicName + "-value").schema
# topicSchema = topicSchemaResponse.schema_str

# COMMAND ----------

# #Maps kafka topic values to topic avro schema to provide a structured schema for Delta table creation when the Delta table does not exist
# def get_schema_from_kafka_topic(topic, schema):
#     from_avro_options= {"mode":"PERMISSIVE"}
#     # UDF that will decode the magic byte and schema identifier at the front of the Avro data
#     # Initially binary_to_int was being used.  However for some reason the value that was returned was not
#     # being interpreted by a distinct() call correctly in the foreachBatch function.  Changing this
#     # value to a string and casting it to an int later enabled distinct() to get the set of schema IDs
#     #binary_to_int = fn.udf(lambda x: int.from_bytes(x, byteorder='big'), IntegerType())
#     binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())

#     # Set up the Readstream, include the authentication to Confluent Cloud for the Kafka topic.
#     # Note the specific kafka.sasl.jaas.config value - on Databricks you have to use kafkashaded.org... for that setting or else it will not find the PlainLoginModule
#     # The below is pulling from only one topic, but can be configured to pull from multiple with a comma-delimited set of topic names in the "subscribe" option
#     # The below is also starting from a specific offset in the topic.  You can specify both starting and ending offsets.  If not specified then "latest" is the default for streaming.
#     # The full syntax for the "startingOffsets" and "endingOffsets" options are to specify an offset per topic per partition.  
#     # Examples: 
#     #    .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")  The -2 means "earliest" and -1 means "latest"
#     #    .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")  The -1 means "latest", -2 not allowed for endingOffsets
#     return (
#             spark
#             .read
#             .format("kafka")
#             .option("kafka.bootstrap.servers", confluentBootstrapServers)
#             .option("kafka.security.protocol", "SASL_SSL")
#             .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
#             .option("kafka.ssl.endpoint.identification.algorithm", "https")
#             .option("kafka.sasl.mechanism", "PLAIN")
#             .option("subscribe", topic)
#             .option("startingOffsets", "earliest")
#             .option("endingOffsets", "latest")
#             .option("failOnDataLoss", "false")
#             .load()
#             .withColumn('key', fn.col("key").cast(StringType()))
#             .withColumn('fixedValue', fn.expr("substring(value, 6, length(value)-5)"))
#             .select(from_avro(fn.expr("fixedValue"), schema, from_avro_options).alias("value"))
#             .selectExpr("value.*") 
#           )

# COMMAND ----------

def get_table_schema(schemasPath,tableName,file,landingFileType,multiLine):
  fileSchemasPath = schemasPath + "/Schema.json"
  try:
    head = dbutils.fs.head(fileSchemasPath, 256000)
  except Exception as e:
    create_table_schema(schemasPath, tableName, file, landingFileType,multiLine)
    head = dbutils.fs.head(fileSchemasPath, 256000)
  return StructType.fromJson(json.loads(head))  

# COMMAND ----------

#schemaDF = spark.read.option('multiline','true').json('s3://dpa-hanv-dpdl-1-s3-int-landing-0001/dash/2022/09/13/22/T19046285_0002_000012170.json')
#RA_schema = schemaDF.schema
#print(type(RA_schema))

# COMMAND ----------

def create_table_schema(schemasPath,tableName,file,landingFileType,multiLine):
  schemaFile = schemasPath + "/Schema.json"
  if landingFileType == "json":    
    if multiLine == 0:
      createSchemaDF=spark.read.json(file)
    else:
      createSchemaDF=spark.read.option("multiline", "true").json(file) 
  else:
    createSchemaDF = spark.read.format("csv").option("header","true").load(file)
  schema = createSchemaDF.schema
  schema_json = createSchemaDF.schema.json()
  if not file_exists(schemasPath):
    dbutils.fs.mkdirs(schemasPath)
  dbutils.fs.put(schemaFile, schema_json, True)

# COMMAND ----------

#check if table exists
try:
  rawZone_df = spark.read \
    .format("delta") \
    .load(rawZonePath)
  #display(rawZone_df)
  doesTableExist = True
except Exception as e:
  doesTableExist = False
print(doesTableExist)

# COMMAND ----------

def add_framework_fields(source_dataframe):
  #display(source_dataframe)
  if tableHasModifiedColumn == True:
    rowModColType = dict(source_dataframe.dtypes)[rowModDateColumnName]
    rowmod_df = source_dataframe.withColumn(rowModDateColumnName, F.when(F.col(rowModDateColumnName)=="",None).otherwise(F.col(rowModDateColumnName)))

    source_dataframe.createOrReplaceTempView("min_mod_date_df")
    minModDate = spark.sql("select min(" + rowModDateColumnName + ") from min_mod_date_df where " + rowModDateColumnName + " <> '' and "  + rowModDateColumnName + " is not null").collect()[0][0]
    if not minModDate:
      minModDate = get_framework_current_datetime() - timedelta(minutes = 1)
    else:
      #Convert moddate to actual timestamp and substract a minute from the oldest mod datetime from the source
      minModDate = dt.strptime(minModDate, '%Y-%m-%d %H:%M:%S.%f') - timedelta(minutes = 1)
    if rowModColType == "string": #might need to add more type checks
      #convert to datetime and populate any missing modified dates with today's date 
      rowmod_df = rowmod_df.withColumn(edwSourceRowModifiedDateTime, F.when(F.col(rowModDateColumnName).isNull(),minModDate).otherwise(to_timestamp(F.col(rowModDateColumnName))))
    else:
      rowmod_df = rowmod_df.withColumn(edwSourceRowModifiedDateTime, F.when(F.col(rowModDateColumnName).isNull(),minModDate).otherwise(F.col(rowModDateColumnName)))
  else:
    rowmod_df = source_dataframe.withColumn(edwSourceRowModifiedDateTime, F.lit(get_framework_current_datetime()))
  #Add Column that holds create and update date of when the record was inserted/modified within ADLS 
  frameworkCurrentDatetime = get_framework_current_datetime()
  rowmod_df = rowmod_df.withColumn(edwRowCreatedDateTime, F.lit(frameworkCurrentDatetime))
  rowmod_df = rowmod_df.withColumn(edwRowModifiedDateTime, F.lit(frameworkCurrentDatetime))
  #display(rowmod_df)
  #Add and Populate PartionColumn  
  partition_df = rowmod_df.withColumn(edwPartitionColumn,F.concat(F.year(F.col(edwSourceRowModifiedDateTime)),F.quarter(F.col(edwSourceRowModifiedDateTime)))) 
  #Add and Populate DeletedFlagColumn
  if deleteFlagColumnName != "" and deleteFlagColumnValue != "":
    delete_df = partition_df.withColumn(edwRowDeleteColumn, F.when(F.col(deleteFlagColumnName) == deleteFlagColumnValue,1).otherwise(0))
  else:
    delete_df = partition_df.withColumn(edwRowDeleteColumn,lit(0))
  #display(delete_df)
  #display(delete_df)
  if(mode == 'keep history'):
    delete_df = delete_df.withColumn("expired_datetime", lit(None).cast(StringType()))
    delete_df = delete_df.withColumn("effective_datetime", col(edwSourceRowModifiedDateTime))
    delete_df = delete_df.withColumn("is_current", lit(1))
  return delete_df

# COMMAND ----------

##check if delta table exists in raw. if not create table from kafka topic schema and adds audit columns
##Initial Load - Iterate over landing df and update expired field for records that are expired
if doesTableExist == False:
  stream_df = get_schema_from_kafka_topic(confluentTopicName, topicSchema)   ## retrieve schema from source table to create Delta table in raw zone
  landing_df = add_framework_fields(kafka_df)

  #Create 
  (spark
 .createDataFrame([], landing_df.schema)
 .write
 .partitionBy(edwPartitionColumn)
 .option("mergeSchema", "true")
 .format("delta")
 .mode("overwrite")
 .save(rawZonePath))
else:
  print("Table Exists")

# COMMAND ----------

new_schemaDF = add_framework_fields(schemaDF)
new_df_schema = new_schemaDF.schema

# COMMAND ----------

(spark
.createDataFrame([], new_df_schema)
.write
.option('mergeSchema','true')
.format('delta')
.mode('overwrite')
.save(rawZonePath))

# COMMAND ----------

schemaLoc = rawZonePath + '/schema/RentalAgreement/'
print(schemaLoc)

# COMMAND ----------

checkpointLocation = rawZonePath + '/checkpoint'
print(checkpointLocation)

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","json")
    .option('multiline', 'true')
    .option("cloudFiles.schemaLocation", schemaLoc)
    .load(landingPath)
    .createOrReplaceTempView("stream_test"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream_test

# COMMAND ----------

(spark.table("stream_test")
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpointLocation)
    .outputMode("append")
    .option("path", rawZonePath)
    .start())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Kafka Stream

# COMMAND ----------

# def read_stream_kafka_topic(topic, schema):
#     from_avro_options= {"mode":"PERMISSIVE"}
#     # UDF that will decode the magic byte and schema identifier at the front of the Avro data
#     # Initially binary_to_int was being used.  However for some reason the value that was returned was not
#     # being interpreted by a distinct() call correctly in the foreachBatch function.  Changing this
#     # value to a string and casting it to an int later enabled distinct() to get the set of schema IDs
#     #binary_to_int = fn.udf(lambda x: int.from_bytes(x, byteorder='big'), IntegerType())
#     binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())

#     # Set up the Readstream, include the authentication to Confluent Cloud for the Kafka topic.
#     # Note the specific kafka.sasl.jaas.config value - on Databricks you have to use kafkashaded.org... for that setting or else it will not find the PlainLoginModule
#     # The below is pulling from only one topic, but can be configured to pull from multiple with a comma-delimited set of topic names in the "subscribe" option
#     # The below is also starting from a specific offset in the topic.  You can specify both starting and ending offsets.  If not specified then "latest" is the default for streaming.
#     # The full syntax for the "startingOffsets" and "endingOffsets" options are to specify an offset per topic per partition.  
#     # Examples: 
#     #    .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")  The -2 means "earliest" and -1 means "latest"
#     #    .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")  The -1 means "latest", -2 not allowed for endingOffsets
#     return (
#             spark
#             .readStream
#             .format("kafka")
#             .option("kafka.bootstrap.servers", confluentBootstrapServers)
#             .option("kafka.security.protocol", "SASL_SSL")
#             .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
#             .option("kafka.ssl.endpoint.identification.algorithm", "https")
#             .option("kafka.sasl.mechanism", "PLAIN")
#             .option("subscribe", topic)
#             .option("startingOffsets", "earliest")
#             .option("failOnDataLoss", "false")
#             .load()
#             .withColumn('key', fn.col("key").cast(StringType()))
#             .withColumn('fixedValue', fn.expr("substring(value, 6, length(value)-5)"))
#             #.withColumn('valueSchemaId', binary_to_string(fn.expr("substring(value, 2, 4)")))
#             #.select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', 'valueSchemaId','fixedValue')
#             #.orderBy('key')
#             #.select(
#             #            # offset must be the first field, due to aggregation
#             #            expr("offset as kafka_offset"),
#             #            expr("timestamp as kafka_ts"),
#             #            expr("key as kafka_key"),
#             #            "fixedValue"
#             #          )
#             #.select("kafka_key", expr("struct(*) as r"))
#             #.groupBy("kafka_key")
#             #.agg(expr("max(r) r"))
#             #.withColumn('value', from_avro(fn.expr("r.fixedValue"), schema, from_avro_options))
#             #.select('value.*')
#             .select(from_avro(fn.expr("fixedValue"), schema, from_avro_options).alias("value"))
#             .selectExpr("value.*") 
#           )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process and Merge data

# COMMAND ----------

#UDF to be called under write stream
#Add and Populate PartionColumn and RowModifiedDate Column
#Filter for the latest changes
#Merge Kafka topics events with RAW Layer Delta table

def merge_data_process(sourcePath, destinationPath): 
  ##Find all records that need to be processed from SQL Server logs
  source_df = spark.read.format("delta").load(sourcePath)   ## define the data to be processed DataFrame
  ## define the data to be processed DataFrame  
  #Add framework columns
  landing_df = add_framework_fields(source_df)
  ##Target Table
  ##Gets table that will be processed with data from the source.
  rawZone_df = spark.read \
          .format("delta") \
          .load(destinationPath)
  landing_df.count()
  ## Note: For nested structs, max on struct is computed as
  ## max on first struct field, if equal fall back to second fields, and so on.
  ColumnID = ', '.join(map(str,primaryKeyColumns))
  pk_columns = get_column_pk_list(primaryKeyColumns)
  pk_on_clause = get_pk_on_clause(primaryKeyColumns)
  Columns = ', '.join(map(str,landing_df.columns))
  print(ColumnID)
  #Creates final output of transactions that need to be merged against delta table
  ##Upserts
  if mode == 'merge':
    #Filter for the latest changes
    landinglatesttransaction_df = landing_df \
    .selectExpr("struct(" + ColumnID + ") as ColumnID ", "" + edwRowModifiedDateTime + " as edwRowModifiedDateTime " ) \
    .groupBy("ColumnID") \
    .agg(max("edwRowModifiedDateTime").alias("edwRowModifiedDateTime")) \
    .selectExpr("ColumnID.*", "edwRowModifiedDateTime")## define the latest transaction to be processed
    
    #Creates views to be joined and remove all records that do not need to be merged, avoiding duplicate keys as part of modfied records
    landing_df.createOrReplaceTempView("landingallrecords_df")
    landinglatesttransaction_df.createOrReplaceTempView("landinglatesttransaction_df")
    
    sql = ("""
      select """ + Columns + """ 
      from ( 
        select row_number() over(partition by """ + pk_columns + """, targetTable.""" + edwRowModifiedDateTime + """ order by """ + edwRowDeleteColumn + """ desc) as rownum
        , targetTable.* 
        from landingallrecords_df targetTable 
          join  landinglatesttransaction_df sourceTable on """ + pk_on_clause + """ and targetTable.""" + edwRowModifiedDateTime + """ = sourceTable.edwRowModifiedDateTime
        ) 
        where rownum = 1 """)
    join_df=spark.sql(sql)
    join_df.createOrReplaceTempView("landing_df")
    rawZone_df.createOrReplaceTempView("rawZone_df")
    ##Build Merge Statement - not using sqlInsert
    sqlMerge,sqlInsert = build_merge_SQL_Statement("landing_df","rawZone_df",list(landing_df.columns),primaryKeyColumns,mode,edwRowDeleteColumn,secretScopeType, 0,edwRowCreatedDateTime,edwRowModifiedDateTime)
    try:
      spark.sql(sqlMerge)
    except Exception as e:
      print(e)
      errorCode = "111"
      errorDescription = "kafka-stream-to-raw-zone notebook: " + notebookName + " - error while applying merge" 
      log_event_notebook_error (notebookExecutionLogKey,errorCode,errorDescription,notebookTableExecutionName,secretScopeType,notebookName)
  ##All records
  elif mode == 'append':
   #Inserts all records from landing
    #Creates views 
    landing_df.createOrReplaceTempView("landing_df")
    rawZone_df.createOrReplaceTempView("rawZone_df")
    ##Build Insert Statement - using sqlInsert 
    sqlMerge,sqlInsert = build_merge_SQL_Statement("landing_df","rawZone_df",list(landing_df.columns),primaryKeyColumns,mode,edwRowDeleteColumn,secretScopeType, 0,edwRowCreatedDateTime,edwRowModifiedDateTime)
    #print(sqlInsert)
    try:
      spark.sql(sqlInsert)
    except Exception as e:
      print(e)
      errorCode = "121"
      errorDescription = "kafka-stream-to-raw-zone notebook: " + notebookName + " - error while applying merge" 
      log_event_notebook_error (notebookExecutionLogKey,errorCode,errorDescription,notebookTableExecutionName,secretScopeType,notebookName)
      
  ##Soft Deletes (History of records with deletes and expired records)
  else:    
    landing_df.createOrReplaceTempView("landingallrecords_df")
    ##Partition landing so we can update raw one batch at a time
    sql = ("""
      select """ + Columns + """ , rownum
      from ( 
        select row_number() over(partition by """ + pk_columns + """ order by """ + edwRowModifiedDateTime + """, """ + edwRowDeleteColumn + """ desc ) as rownum
        , targetTable.* 
        from landingallrecords_df targetTable 
          
        )""")
    join_df=spark.sql(sql)
    join_df.createOrReplaceTempView("landing_df")
    rawZone_df.createOrReplaceTempView("rawZone_df")
    
    ##GET MAX ROWNUM
    maxRowNum = spark.sql("select max(rownum) from landing_df").collect()[0][0]
    
    ##Get sql statements
    rownum_df = spark.sql("select * from landing_df")
    sqlMerge,sqlInsert = build_merge_SQL_Statement("rownum_df","rawZone_df",list(landing_df.columns),primaryKeyColumns,mode,edwRowDeleteColumn,secretScopeType, 0,edwRowCreatedDateTime,edwRowModifiedDateTime)
    try:
      ##ITERATE FROM 1 TO MAX ROW NUM
      for i in range(1, maxRowNum+1):
        ##Build df to apply sql statements to where maxrownum = i
        rownum_df = spark.sql("select * from landing_df where rownum = {}".format(i))
        #display(rownum_df)
        rownum_df.createOrReplaceTempView("rownum_df")
        ##apply sql statements
        spark.sql(sqlMerge)
        spark.sql(sqlInsert)
    except Exception as e:
      print(e)
      errorCode = "131"
      errorDescription = "kafka-stream-to-raw-zone notebook: " + notebookName + " - error while applying update or insert" 
      log_event_notebook_error (notebookExecutionLogKey,errorCode,errorDescription,notebookTableExecutionName,secretScopeType,notebookName)

# COMMAND ----------

#UDF that sets up steps to be executed as stream process is happening
def execute_merge_processes(df, epochId):   
  ##Step 1: Write kafka events into Delta table to be processed and use during merge process
  df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("mergeSchema", "true") \
  .save(landingPath) 
    
  ##Step 2: Insert all events to keep history of all the data streaming from the topic
  df.write \
  .mode('append') \
  .parquet(landingPathParquet)

  ##Step 3: Execute Delta Merge process
  merge_data_process(landingPath, rawZonePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up Read Kafka Stream

# COMMAND ----------

topicstream_df = read_stream_kafka_topic(confluentTopicName, topicSchema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Write Kafka Stream to Process Data

# COMMAND ----------

topicstream_df.writeStream \
  .foreachBatch(execute_merge_processes) \
  .outputMode("update") \
  .option("checkpointLocation", checkpointLocation) \
  .start()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName="",scopeType=secretScopeType)
dbutils.notebook.exit("Succeeded")
