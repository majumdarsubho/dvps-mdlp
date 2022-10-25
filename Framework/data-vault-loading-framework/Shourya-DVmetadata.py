# Databricks notebook source
# MAGIC %run /Framework/utility/neudesic-framework-functions

# COMMAND ----------

import json
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# MAGIC %run /Framework/utility/dv_functions

# COMMAND ----------

dbutils.widgets.text(name='Source', defaultValue="",label='Source')
dbutils.widgets.text(name='curated', defaultValue="/mnt/curated", label='Curated Path')
dbutils.widgets.text(name='metadata', defaultValue="/mnt/dpa-hertz/metadata/FrameworkHydration - Updated.xlsx", label='Metadata Path')
dbutils.widgets.text(name='parentNotebookExecutionLogKey', defaultValue="-1",label='parentNotebookExecutionLogKey')
dbutils.widgets.text(name='notebookTableExecutionName', defaultValue="",label='notebookTableExecutionName')
dbutils.widgets.text(name='notebookExecutionGroupName', defaultValue="",label='notebookExecutionGroupName')

# COMMAND ----------

# DBTITLE 1,Keeping Group and Source as Same for now
source = dbutils.widgets.get("Source")
curated_path = dbutils.widgets.get("curated")
common_cols = ["Source","Silver_LoadDate","SourceRegion"]
metadata_path = dbutils.widgets.get("metadata")
print(metadata_path)
print(curated_path)
print("This is the Source : ",source)
secretScopeType = "Admin"
errorCode = None
#notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
#print(notebook_name)
parentNotebookExecutionLogKey = dbutils.widgets.get("parentNotebookExecutionLogKey")
notebookTableExecutionName = dbutils.widgets.get("notebookTableExecutionName")
notebookExecutionGroupName = dbutils.widgets.get("notebookExecutionGroupName")
checkpointLocation = curated_path +"/checkpoint/" + source
print(checkpointLocation)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# COMMAND ----------

# dbutils.fs.rm(f"{checkpointLocation}",recurse=True) #--remove writestream checkpoint for that source
# dbutils.fs.rm(f"{curated_path}/Hub_{source}",recurse=True) #remove hub_df
# # # dbutils.fs.rm(f"{curated_path}/Link_{source}_Association",recurse=True) #remove link_df
# # # dbutils.fs.rm(f"{curated_path}/Sat_{source}_Association",recurse=True) #remove sat_assoc_df
# dbutils.fs.rm(f"{curated_path}/Sat_{source}",recurse=True)
# dbutils.fs.rm(f"s3a://dpa-hanv-dpdl-1-s3-reg-raw-0001/raw-zone/{source}",recurse=True)#remove raw 

# COMMAND ----------

# notebookName = get_notebookName(dbutils.notebook.entry_point.getDbutils().notebook().getContext())
# notebookExecutionLogKey = log_event_notebook_start(notebookName,parentNotebookExecutionLogKey,notebookTableExecutionName,secretScopeType)
# # print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))
# # # log_event_notebook_start(notebookName, parentPipelineExecutionLogKey,notebookTableExecutionName,scopeType)

# COMMAND ----------

# df_schema= spark.read.format("parquet").option("inferSchema","true").load(f"s3a://dpa-hanv-dpdl-1-s3-reg-raw-0001/raw-zone/parquet/VehicleGroup/").schema
df = spark.read.format("delta").load(f"s3a://dpa-hanv-dpdl-1-s3-reg-raw-0001/raw-zone/{source}")
df.distinct().count()

# COMMAND ----------

df.writeStream.foreachBatch(lambda df,batch_id : foreach_batch_function(df, batch_id,source,metadata_path,common_cols,curated_path)).option('checkpointLocation', checkpointLocation).outputMode("append").trigger(once=True).start().awaitTermination()

# COMMAND ----------

print("Hub:",spark.read.format("delta").load(f"{curated_path}/Hub_{source}").count())
print("Sat :",spark.read.format("delta").load(f"{curated_path}/Sat_{source}").count())
# print("Link:",spark.read.format("delta").load(f"{curated_path}/Link_{source}Association").count())
# print("Sat Non PII:",spark.read.format("delta").load(f"{curated_path}/Sat_{source}_Non_PII").count())
# print("Sat_assoc:",spark.read.format("delta").load(f"{curated_path}/Sat_{source}Association").count())

# COMMAND ----------

# DBTITLE 1,End Logging
# log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName=notebookExecutionGroupName,scopeType=secretScopeType)
# dbutils.notebook.exit("Succeeded")

# COMMAND ----------

# display(spark.read.format("delta").load(f"{curated_path}/Sat_{source}_PII"))

# COMMAND ----------

populate_hive(hive_databaseName="mdlsilver",tables_folder_path=curated_path)

# COMMAND ----------

spark.read.format("delta").load(f"s3a://dpa-hanv-dpdl-1-s3-reg-raw-0001/raw-zone/{source}").select("RES_ID","TYPE_IND","CUST_NAME","PU_LOC_CD","RTRN_LOC_CD","VEH_CLS","VEH_TYPE").distinct().count()
