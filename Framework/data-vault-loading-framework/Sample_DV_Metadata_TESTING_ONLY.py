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
source_dv_entities = "Present"
errorCode = None
#notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
#print(notebook_name)
parentNotebookExecutionLogKey = dbutils.widgets.get("parentNotebookExecutionLogKey")
notebookTableExecutionName = dbutils.widgets.get("notebookTableExecutionName")
notebookExecutionGroupName = dbutils.widgets.get("notebookExecutionGroupName")

# COMMAND ----------

notebookName = get_notebookName(dbutils.notebook.entry_point.getDbutils().notebook().getContext())
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentNotebookExecutionLogKey,notebookTableExecutionName,secretScopeType)
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

try:
    existing_hub_df = DeltaTable.forPath(spark, f"""{curated_path}/Hub_{source}""")  
    print("Count of Existing Hub",existing_hub_df.toDF().count())
    
    if source in ("Customer","Vehicle","Location","VehicleGroup"):
        existing_sat_df = DeltaTable.forPath(spark,f"""{curated_path}/Sat_{source}""")
        print("Count of Existing SAT",existing_sat_df.count())
    
    elif source == "NPS":   
        existing_link_df = DeltaTable.forPath(spark, f"""{curated_path}/Link_{source}_Association""")
        existing_sat_df =  DeltaTable.forPath(spark,f"""{curated_path}/Sat_{source}""")
 
        print("Count of Existing Link",existing_link_df.toDF().count())
        print("Count of Existing SAT",existing_sat_df.toDF().count())
        
    else:
        existing_link_df = DeltaTable.forPath(spark, f"""{curated_path}/Link_{source}_Association""")
        existing_sat_df =  DeltaTable.forPath(spark,f"""{curated_path}/Sat_{source}""")
        existing_sat_assoc_df = DeltaTable.forPath(spark,f"""{curated_path}/Sat_{source}_Association""")
        print("Count of Existing Link",existing_link_df.toDF().count())
        print("Count of Existing SAT",existing_sat_df.toDF().count())
        print("Count of Existing SAT_ASSOC",existing_sat_assoc_df.toDF().count())
except Exception as e:
    source_dv_entities = "Not Present"
    print(f"Failed to read Silver table(s) for Source : {source}")
    

# COMMAND ----------

def hub_insert(dv_entity,source,existing_hub_df,hub_df):
    print("Inserting HUB")
    hub_hashkey  = dv_entity.where(f"Source = '{source}'and Entity_name = 'Hub_{source}' ").select("Hash_key_name").first()[0]
    existing_hub_df.alias("existing") \
    .merge(hub_df.alias("incoming"),f"""existing.{hub_hashkey} = incoming.{hub_hashkey}""") \
    .whenNotMatchedInsertAll().execute()
    return 

# COMMAND ----------

def link_insert(dv_entity,source,existing_link_df,link_df):
    
    link_hashkey  = dv_entity.where(f"Source = '{source}'and Entity_name = 'Link_{source}' ").select("Hash_key_name").first()[0]
    existing_link_df.alias("existing") \
    .merge(link_df.alias("incoming"),f"""existing.{link_hashkey} = incoming.{link_hashkey}""") \
    .whenNotMatchedInsertAll().execute()
    return 

# COMMAND ----------

def sat_insert(existing_sat_df,sat_df):
    sat_hashkey  = "HashDIFF"
    print("Inserting SAT")
    existing_sat_df.alias("existing") \
    .merge(sat_df.alias("incoming"),f"""existing.{sat_hashkey} = incoming.{sat_hashkey}""") \
    .whenNotMatchedInsertAll().execute()
    return
    

# COMMAND ----------

if source_dv_entities == "Not Present":
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
        errorCode = "196"
        errorDescription = "Bronze to Silver Notebook: " + notebookName + "error while writing Data"
        log_event_notebook_error(notebookExecutionLogKey, errorCode, errorDescription, notebookTableExecutionName,secretScopeType,notebookName)
else:
    try:
        hub_insert(dv_entity,source,existing_hub_df,hub_df)
        if source in ("Customer","Vehicle","Location","VehicleGroup"):
            sat_insert(dv_entity,source,existing_sat_df,sat_df)
        elif source == "NPS":
            print(f"{source}-Inserting LINK")
            link_insert(dv_entity,source,existing_link_df,link_df)
            print(f"{source}-Inserting SAT")
            sat_insert(dv_entity,source,existing_sat_df,sat_df)
        else:
            link_insert(dv_entity,source,existing_link_df,link_df)
            sat_insert(existing_sat_df,sat_df)
            sat_insert(existing_sat_assoc_df,sat_assoc_df)
    except Exception as e:
        errorCode = "198"
        e.with_traceback()
#         errorDescription = f"Bronze to Silver Notebook: {source}" + notebookName + "error while Inserting Data for {source}"
#         log_event_notebook_error(notebookExecutionLogKey, errorCode, errorDescription, notebookTableExecutionName,secretScopeType,notebookName)

# COMMAND ----------

# DBTITLE 1,End Logging
log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName=notebookExecutionGroupName,scopeType=secretScopeType)
dbutils.notebook.exit("Succeeded")
