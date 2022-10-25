# Databricks notebook source
# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

import pyodbc
from datetime import datetime as dt
from datetime import timezone
import pandas as pd

# COMMAND ----------

from pathlib import Path
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import *
from pyspark.sql.types import *

# COMMAND ----------

import urllib
import boto3
import json

# COMMAND ----------

# MAGIC %run ../utility/neudesic-framework-functions

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

# User Parameters; Need to figure out how to read values from Airflow

dbutils.widgets.text(name="CopyActivitySinkName", defaultValue="dboRawCustomer", label= "Copy Activity Sink Name:")
dbutils.widgets.text(name="MasterPipelineExecutionLogKey", defaultValue="-1", label= "Master Pipeline Execution Log Key:")
dbutils.widgets.text(name="CopyActivityExecutionGroupName", defaultValue="AdventureWorksRawCopyProcess", label= "Copy Activity Execution Group Name:")

# COMMAND ----------

scopeType = 'Admin'
CopyActivitySinkName = dbutils.widgets.get('CopyActivitySinkName')
MasterPipelineExecutionLogKey = dbutils.widgets.get('MasterPipelineExecutionLogKey')
CopyActivityExecutionGroupName = dbutils.widgets.get('CopyActivityExecutionGroupName')



print("CopyActivitySink : " + CopyActivitySinkName)
print("MasterPipelineExecutionLogKey: " + MasterPipelineExecutionLogKey)
print("Copy Activity Execution Group Name: " + CopyActivityExecutionGroupName)

# COMMAND ----------

# MAGIC %md ##Start of Ingestion Process

# COMMAND ----------

# Start Logging
CopyActivityExecutionLogKey = log_event_CopyActivity_start(CopyActivitySinkName)
start = dt.now()
row_count = 0
print("Copy Activity Execution Log Key: {0}".format(CopyActivityExecutionLogKey))

# COMMAND ----------

try:
    types = get_source_target_types(CopyActivitySinkName)
    sourceType = types[1].lower()
    targetType = types[2].lower()
    print(f'Source type is {sourceType}')
    print(f'Target type is {targetType}')    
except Exception as e:
    errorCode = '300'
    errorDescription = 'Failed to run CopyActivity stored procedure'
    log_event_CopyActivity_error(CopyActivityExecutionLogKey, errorCode, errorDescription)
    

# COMMAND ----------

try:
    metadata = source_sink_metadata(CopyActivitySinkName, sourceType, targetType)
    print(f'Source and Target Metadata Details')
    print(json.dumps(metadata, indent=4))
except Exception as e:
    errorCode = '300'
    errorDescription = 'Failed to run CopyActivity stored procedure'
    log_event_CopyActivity_error(CopyActivityExecutionLogKey, errorCode, errorDescription)


# COMMAND ----------

#ACCESS_KEY = dbutils.secrets.get(scope = 'dpaframework', key = 'ACCESS_KEY')
#SECRET_KEY = dbutils.secrets.get(scope = 'dpaframework', key = 'SECRET_KEY')

# todo fetch these details from metadata
#secret_name = 'adventureworks'
#region_name ='us-east-1'

# Get AWS session
#session = get_aws_session(ACCESS_KEY, SECRET_KEY)

# Get Secret value for a given secret key
#secret_value = get_aws_secretvalue(session, region_name, secret_name)

# COMMAND ----------

# Connecting to Database
#serverName = secret_value['host']
#userName = secret_value['username']
#passWord = secret_value['password']
#databaseName = secret_value['databaseName']
# print(ServerName," ", userName," ", password," ", DatabaseName)
#db_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+serverName+';DATABASE='+databaseName+';UID='+userName+';PWD='+ passWord)

# COMMAND ----------

# Reading data from table and putting it in a pandas df before converting it to Spark
if sourceType == 'sql' and targetType == 'adls':
    try:
        sql_script = metadata['sql_script']
        result = pd.read_sql_query(sql_script, db_conn)
        sparkDF = spark.createDataFrame(result)
        display(sparkDF)
    except Exception as e:
        errorCode = '301'
        errorDescription = 'Could not write pandas df to Spark'
        print(errorCode, errorDescription)
        log_event_CopyActivity_error(CopyActivityExecutionLogKey, errorCode, errorDescription)
    
    # Checking the number of partitions of the Spark DF and repartitioning if it is more than 1
    numParts = sparkDF.rdd.getNumPartitions()
    if numParts > 1:
        sparkDF = sparkDF.repartition(1)
    
    # Writing DataFrame to landing zone
    try:
        #check target mount point and create if not exists
        trgt_container_name = metadata['targetContainerName']
        trgt_mnt_path = '/mnt/' + trgt_container_name
        if not check_mount(trgt_mnt_path):
            print("'{0}' mount point doesn't exist".format(trgt_mnt_path))
            mount_s3(trgt_container_name,trgt_mnt_path)  
        else:
            print("'{0}' mount point already exists".format(trgt_mnt_path))
        
        trgt_folder_path = trgt_mnt_path + metadata['targetFolderPath']
        sparkDF.write.mode('overwrite').option('header',True).csv(trgt_folder_path)
        row_count = sparkDF.count()
    except Exception as e:
        errorCode = '302'
        errorDescription = 'Failed to write Spark DF to ADLS'
        print(e)
        log_event_CopyActivity_error(CopyActivityExecutionLogKey, errorCode, errorDescription)

# COMMAND ----------


if sourceType == 'file' and targetType == 'adls':
    
    try:
        #check src mount point and create if not exists
        src_container_name = metadata['sourceContainer']
        src_mnt_path = '/mnt/' + src_container_name
        if not check_mount(src_mnt_path):
            print("'{0}' mount point doesn't exist".format(src_mnt_path))
            mount_s3(src_container_name,src_mnt_path)
        else:
            print("'{0}' mount point already exists".format(src_mnt_path))
        
        #check target mount point and create if not exists
        trgt_container_name = metadata['targetContainerName']
        trgt_mnt_path = '/mnt/' + trgt_container_name
        if not check_mount(trgt_mnt_path):
            print("'{0}' mount point doesn't exist".format(trgt_mnt_path))
            mount_s3(trgt_container_name,trgt_mnt_path)  
        else:
            print("'{0}' mount point already exists".format(trgt_mnt_path))
        
        src_file_path = src_mnt_path + metadata['sourceFolderPath'] + metadata['sourceFileName']
        trgt_file_path = trgt_mnt_path + '/' + metadata['targetFolderPath'] + 'T19047055_0002_000012120.json'
        
        result = fileCopyWithRetry(src_file_path, trgt_file_path, 3)
        if result:
            print("Copied Source '{0}' to Target '{1}'".format(src_file_path, trgt_file_path))
    except Exception as e:
        errorCode = '301'
        errorDescription = 'Could not copy source file to target location'
        print(errorCode, errorDescription)
        log_event_CopyActivity_error(CopyActivityExecutionLogKey, errorCode, errorDescription)
    

# COMMAND ----------


# if sourceType == 'file' and targetType == 'adls':
    
#     try:
#         copy_source = dict()
#         copy_source['Bucket'] = 'dpa-hertz' 
#         copy_source['Key'] = metadata['sourceFolderPath'] + '/' + metadata['sourceFileName'] + '.' +  metadata['sourceFileType']
#         print(f'Source Path {copy_source["Key"]}')
#         target_bucket = metadata['targetContainerName']
#         target_object = metadata['targetFolderPath'] + '/' + metadata['targetFileName']
#         print(f'Target Object name {target_object}')
        
#         # Invoke the func to copy file from source bucket to target bucket
#         copy_file_s3bucket(session, copy_source, target_bucket, target_object)
#     except Exception as e:
#         errorCode = '301'
#         errorDescription = 'Could not copy source file to target location'
#         print(errorCode, errorDescription)
#         log_event_CopyActivity_error(CopyActivityExecutionLogKey, errorCode, errorDescription)
    

# COMMAND ----------

end = dt.now()
duration = end - start
CopyDuration = int(duration.total_seconds())

log_event_CopyActivity_end(CopyActivityExecutionLogKey, CopyActivitySinkName, CopyActivityExecutionGroupName, row_count, CopyDuration)

# COMMAND ----------

dbutils.notebook.exit("Notebook Completed Successfully")
