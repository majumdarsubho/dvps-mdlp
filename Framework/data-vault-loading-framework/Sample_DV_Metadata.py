# Databricks notebook source
# MAGIC %run /Framework/utility/neudesic-framework-functions

# COMMAND ----------

import json
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run /Framework/utility/dv_functions

# COMMAND ----------

dbutils.widgets.text(name='Source', defaultValue="",label='Source')
dbutils.widgets.text(name='curated', defaultValue="/mnt/curated", label='Curated Path')
dbutils.widgets.text(name='metadata', defaultValue="/mnt/dpa-hertz/metadata/Data Vault Metadata.xlsx", label='Metadata Path')
dbutils.widgets.text(name='parentNotebookExecutionLogKey', defaultValue="-1",label='parentNotebookExecutionLogKey')
dbutils.widgets.text(name='notebookTableExecutionName', defaultValue="",label='notebookTableExecutionName')
dbutils.widgets.text(name='notebookExecutionGroupName', defaultValue="",label='notebookExecutionGroupName')

# COMMAND ----------

# DBTITLE 1,Keeping Group and Source as Same for now
source = dbutils.widgets.get("Source")
group = dbutils.widgets.get("Source")
curated_path = dbutils.widgets.get("curated")
common_cols = ["Source","LoadDate"]
metadata_path = dbutils.widgets.get("metadata")
print(metadata_path)
print(curated_path)
print("This is the Source : ",source)
secretScopeType = "Admin"
#notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
#print(notebook_name)
parentNotebookExecutionLogKey = dbutils.widgets.get("parentNotebookExecutionLogKey")
notebookTableExecutionName = dbutils.widgets.get("notebookTableExecutionName")
notebookExecutionGroupName = dbutils.widgets.get("notebookExecutionGroupName")

# COMMAND ----------

# notebookName = get_notebookName(dbutils.notebook.entry_point.getDbutils().notebook().getContext())
# notebookExecutionLogKey = log_event_notebook_start(notebookName,parentNotebookExecutionLogKey,notebookTableExecutionName,secretScopeType)
# print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))
# # log_event_notebook_start(notebookName, parentPipelineExecutionLogKey,notebookTableExecutionName,scopeType)

# COMMAND ----------

dv_entity,hk_bk_mapping_dict=read_hashkey_metadata(metadata_path)

parent_df,all_columns = read_parquet(source,hk_bk_mapping_dict,dv_entity)

hub_df = create_hub(parent_df,source,common_cols,dv_entity)
    
link_df = create_link(parent_df,source,common_cols,dv_entity)

sat_columns_df  = create_sat_columns_metadata(metadata_path,source)
print("Source : ",source)

if source in ("Customer","Vehicle","NPS","Location","VehicleGroup"):
    sat_df = create_sat(parent_df,source,sat_columns_df,common_cols,all_columns)

elif source == "Reservation":
    sat_df,sat_assoc_df = create_sat(parent_df,source,sat_columns_df,common_cols,all_columns)

else:
    sat_df,sat_assoc_df = create_sat_rental_agreement(parent_df,all_columns,common_cols)

# COMMAND ----------

hub_df = spark.read.format("delta").load(f"{curated_path}/Hub_{source}")
link_df = spark.read.format("delta").load(f"{curated_path}/Link_{source}_Association")
sat_df = spark.read.format("delta").load(f"{curated_path}/Sat_{source}")
sat_assoc_df = spark.read.format("delta").load(f"{curated_path}/Sat_{source}_Association")

# COMMAND ----------

# DBTITLE 1,Writing Data
try:
    if source in ("Customer","Vehicle","Location","VehicleGroup"):
        hub_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("LoadDate").save(f"{curated_path}/Hub_{source}")
        sat_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("LoadDate").save(f"{curated_path}/Sat_{source}")
    elif source == "NPS":
        hub_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("LoadDate").save(f"{curated_path}/Hub_{source}")
        link_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("LoadDate").save(f"{curated_path}/Link_{source}_Association")
        sat_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("LoadDate").save(f"{curated_path}/Sat_{source}")
    else:
        hub_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("LoadDate").save(f"{curated_path}/Hub_{source}")
        link_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("LoadDate").save(f"{curated_path}/Link_{source}_Association")
        sat_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("LoadDate").save(f"{curated_path}/Sat_{source}")
        sat_assoc_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("LoadDate").save(f"{curated_path}/Sat_{source}_Association")
except Exception as e:
    print(e)
#     errorCode = "196"
#     errorDescription = "Bronze to Silver Notebook: " + notebookName + "error while writing Data"
#     log_event_notebook_error(notebookExecutionLogKey, errorCode, errorDescription, notebookTableExecutionName,secretScopeType,notebookName)


# COMMAND ----------

# DBTITLE 1,End Logging
# log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName=notebookExecutionGroupName,scopeType=secretScopeType)
# dbutils.notebook.exit("Succeeded")
