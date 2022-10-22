# Databricks notebook source
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
from pyspark.sql.functions import current_date

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

scopeType = 'Admin'

# COMMAND ----------

# MAGIC %sql
# MAGIC Create database if not exists mdlbronze;
# MAGIC use database mdlbronze;

# COMMAND ----------

#Notebook Parameters
dbutils.widgets.text(name='caSink', defaultValue='', label='CopyActivitySinkName')
caSink = dbutils.widgets.get('caSink')
print(caSink)

# COMMAND ----------

# Retrieving all path information
def getSourceInfo(CopyActivitySink):
    sql = """EXEC.dbo.uspGetFileInfo 
        @CopyActivitySinkName='{0}';""".format(CopyActivitySink)
    sp_exec = execute_framework_stored_procedure_with_results(sql,scopeType)
    return sp_exec[0]

# COMMAND ----------

source_info = getSourceInfo(caSink)
sourceContainer = source_info[0]
sourceFolder = source_info[1]
sourceFileType = source_info[2]
targetContainer = source_info[3]
targetFolder = source_info[4]
source = source_info[5]


s3PathPrefix = 's3a://'
s3SourcePath = s3PathPrefix + sourceContainer + sourceFolder
s3TargetPath = s3PathPrefix + targetContainer + '/' + targetFolder
schemaLocation = s3TargetPath + 'schema/'
checkpointLocation = s3TargetPath + 'checkpoint/'

print('Source Container: {0}'.format(sourceContainer))
print('Source Folder Path: {0}'.format(s3SourcePath))
print('Source File Type: {0}'.format(sourceFileType))
print('Target Container: {0}'.format(targetContainer))
print('Target Folder Path: {0}'.format(s3TargetPath))
print('Schema Location: {0}'.format(schemaLocation))
print('Checkpoint Location: {0}'.format(checkpointLocation))

# COMMAND ----------

if sourceFileType == 'json': 
    df=(spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format','json')
        .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
        .option('multiline','true')
        .option('cloudFiles.schemaLocation', schemaLocation)
        .load(s3SourcePath))
else:
    df=(spark.readStream
        .format("cloudFiles")
        .option('cloudFiles.format','csv')
        .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
        .option('cloudFiles.schemaLocation', schemaLocation)
        .load(s3SourcePath))

# COMMAND ----------

(df.writeStream
    .format('delta')
    .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
    .option('mergeSchema','true')
    .option('checkpointLocation',checkpointLocation)
    .outputMode('append')
    .option('path', s3TargetPath)
    .start())
