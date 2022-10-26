# Databricks notebook source
# MAGIC %md
# MAGIC # Landing to Silver Rental Agreement
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
# MAGIC This notebook is used to Load the Data Vault Tables For Rental Agreement

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

dbutils.widgets.text(name='curated', defaultValue="/mnt/curated", label='Curated Path')
dbutils.widgets.text(name='metadata', defaultValue="/FileStore/tables/FrameworkHydration.xlsx", label='Metadata Path')
dbutils.widgets.text(name='source', defaultValue="RentalAgreement", label='Source')

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True) #configration to set on the job cluster

# COMMAND ----------

source = dbutils.widgets.get("source")
curated_path = dbutils.widgets.get("curated")
common_cols = ["Source","Silver_LoadDate","SourceRegion"]
metadata_path = dbutils.widgets.get("metadata")
rawZonePath = f"s3a://dpa-hanv-dpdl-1-s3-reg-raw-0001/raw-zone/RentalAgreement"
landingPath = f"s3://dpa-hanv-dpdl-1-s3-int-landing-0001/dash/new/"
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

bronze_files = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("treatEmptyValuesAsNulls", "false") \
            .option("dataAddress","BronzeFiles"+"!A1") \
            .load(metadata_path)

# COMMAND ----------

source_entity_list = list((bronze_files.where(f"Raw_Source = '{source}'").select("Table_Name").toPandas()['Table_Name']))

# COMMAND ----------

# DBTITLE 1,Read from Landing
df_landing = (spark.readStream
                       .format('cloudFiles')
                       .option('cloudFiles.format','json')
                       .option('multiline','true')
                       .option('cloudFiles.schemaLocation', schemaLoc)
                       .load(landingPath))

# COMMAND ----------



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

# DBTITLE 1,Read From Raw
df_raw = spark.readStream.format("delta").load(f"{rawZonePath}")

# COMMAND ----------

# DBTITLE 1,Write to Silver
df_raw.writeStream.foreachBatch(lambda df,batch_id : foreach_batch_function(df, batch_id,source,metadata_path,common_cols,curated_path)) \
.option('checkpointLocation', silver_checkpointLocation) \
.outputMode("append") \
.trigger(once=True) \
.start() \
.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Raw Count
ra_raw = spark.read.format("delta").load(f"{rawZonePath}")
ra_raw.count()

# COMMAND ----------

# DBTITLE 1,Hub Count
ra_hub = spark.read.format("delta").load(f"{curated_path}/Hub_{source}")
ra_hub.count()

# COMMAND ----------

ra_hub = spark.read.format("delta").load(f"{curated_path}/Hub_{source}")
display(ra_hub)

# COMMAND ----------

# DBTITLE 1,Link Count
ra_link = spark.read.format("delta").load(f"{curated_path}/Link_{source}Association")
display(ra_link)

# COMMAND ----------

# DBTITLE 1,SAT Count
ra_sat = spark.read.format("delta").load(f"{curated_path}/Sat_{source}_Non_PII")
display(ra_sat)

# COMMAND ----------


