# Databricks notebook source
# MAGIC %md
# MAGIC # Landing to Silver Reservation
# MAGIC 
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Originally Created By | [shourya.addepalli@neudesic.com](mailto:shourya.addepalli@neudesic.com)|
# MAGIC |curated_path |<ul><li>Silver S3 Bucket Path |
# MAGIC |Metadata Path|<ul><li>Metadata Sheet Path/RDS SQL SERVER CONNECTION |
# MAGIC 
# MAGIC ## Other Details
# MAGIC This notebook is used to Load the Data Vault Tables For Reservation

# COMMAND ----------

#%run /Framework/utility/neudesic-framework-functions

# COMMAND ----------

# MAGIC %run /Framework/utility/dv_functions

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# dbutils.widgets.text(name='Source', defaultValue="",label='Source')
dbutils.widgets.text(name='curated', defaultValue="/mnt/curated", label='Curated Path')
dbutils.widgets.text(name='metadata', defaultValue="/mnt/dpa-hertz/metadata/FrameworkHydration - Updated.xlsx", label='Metadata Path')
# dbutils.widgets.text(name='parentNotebookExecutionLogKey', defaultValue="-1",label='parentNotebookExecutionLogKey')
# dbutils.widgets.text(name='notebookTableExecutionName', defaultValue="",label='notebookTableExecutionName')
# dbutils.widgets.text(name='notebookExecutionGroupName', defaultValue="",label='notebookExecutionGroupName')

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True) #configration to set on the job cluster

# COMMAND ----------

source = "Reservation"
curated_path = dbutils.widgets.get("curated")
common_cols = ["Source","Silver_LoadDate","SourceRegion"]
metadata_path = dbutils.widgets.get("metadata")
rawZonePath = f"s3a://dpa-hanv-dpdl-1-s3-reg-raw-0001/raw-zone/Reservation"
landingPath = f"s3://dpa-hanv-dpdl-1-s3-int-landing-0001/Excalibur/"
schemaLoc = rawZonePath + '/schema/'
raw_checkpointLocation = rawZonePath + '/checkpoint/'
silver_checkpointLocation = curated_path +"/checkpoint/" + source

print(rawZonePath)
print(landingPath)
print(schemaLoc)
print(raw_checkpointLocation)
print(silver_checkpointLocation)

# COMMAND ----------

# dbutils.fs.rm(f"{silver_checkpointLocation}",recurse=True) #--remove writestream checkpoint for that source in silver
# dbutils.fs.rm(f"{curated_path}/Hub_{source}",recurse=True) #remove hub_df
# dbutils.fs.rm(f"{curated_path}/Link_{source}Association",recurse=True) #remove link_df
# dbutils.fs.rm(f"{curated_path}/Sat_{source}_PII",recurse=True) #remove PII SAT
# dbutils.fs.rm(f"{curated_path}/Sat_{source}_Non_PII",recurse=True) #remove NON PII SAT
# dbutils.fs.rm(f"{curated_path}/Sat_{source}Association",recurse=True) #remove SAT-Association
# dbutils.fs.rm(f"{rawZonePath}",recurse=True)#remove raw ,checkpoint and schema with respect landing writestream for bronze

# COMMAND ----------

# DBTITLE 1,Read from Landing
df_landing = df=(spark.readStream.format("cloudFiles")
        .option('cloudFiles.format','csv')
        .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
        .option('cloudFiles.schemaLocation', schemaLoc)
        .load(landingPath))

# COMMAND ----------

# DBTITLE 1,Write to Raw
(df_landing.writeStream
    .format('delta')
    .option('mergeSchema','true')
    .option('checkpointLocation',raw_checkpointLocation)
    .outputMode('append')
    .option('path', rawZonePath)
    .trigger(once=True)
    .start().awaitTermination())

# COMMAND ----------

df_raw = spark.readStream.format("delta").load(f"{rawZonePath}")

# COMMAND ----------

df_raw.writeStream.foreachBatch(lambda df,batch_id : foreach_batch_function(df, batch_id,source,metadata_path,common_cols,curated_path)) \
.option('checkpointLocation', silver_checkpointLocation) \
.outputMode("append") \
.trigger(once=True) \
.start() \
.awaitTermination()
