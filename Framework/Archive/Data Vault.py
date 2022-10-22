# Databricks notebook source
# MAGIC %md Adding Backwards Compatibility

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# %run "../Includes/Function/GetNotebook"

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",sc.defaultParallelism)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from delta.tables import *
import os
import json

# COMMAND ----------

# MAGIC %md Get Parameters

# COMMAND ----------

# data vault load
dbutils.widgets.text("Source_System", "")
dbutils.widgets.text("DataVault_Entity_Name", "")

# deletion capture
dbutils.widgets.text("Deletion_Capture_Enabled","")
deletion_captured_enabled = dbutils.widgets.get("Deletion_Capture_Enabled").lower()

# refresh interval
dbutils.widgets.text("Is_Full_Refresh","")
is_full_refresh = dbutils.widgets.get("Is_Full_Refresh").lower()

# schema
dbutils.widgets.text("Schema","")
schema = dbutils.widgets.get("Schema")

dbutils.widgets.text("Is_Overwrite_Schema","False")
is_overwrite_schema = dbutils.widgets.get("Is_Overwrite_Schema")

dbutils.widgets.text("Is_Merge_Schema","True")
is_merge_schema = dbutils.widgets.get("Is_Merge_Schema")

# root path
dbutils.widgets.text("Root_Path", "s3")
rootPath = dbutils.widgets.get("Root_Path")

# etl logging
dbutils.widgets.text("ETL_ID","")
dbutils.widgets.text("Data_Factory_Name","")
dbutils.widgets.text("Pipeline_Name","")
dbutils.widgets.text("Pipeline_Run_Id","")

etlID = dbutils.widgets.get("ETL_ID")
dataFactoryName = dbutils.widgets.get("Data_Factory_Name")
pipelineName = dbutils.widgets.get("Pipeline_Name")
pipelineRunId = dbutils.widgets.get("Pipeline_Run_Id")

#query
dbutils.widgets.text("query_folder","/dbfs/mnt/....")
query_folder = dbutils.widgets.get("query_folder")

# #logging info
# workSpaceName = GetNotebook().workspace()
# notebookName = GetNotebook().notebook()
# outputFileLoc = "/databricks-results/"
# outputFile = os.path.join(outputFileLoc, workSpaceName, notebookName)
# stepName = "Load Data Vault"

# save data
dbutils.widgets.text("Write_Mode", "append")
write_mode = dbutils.widgets.get("Write_Mode")

# COMMAND ----------

# Generate Hashkey Function

# COMMAND ----------

# Create Data Vault Entity Function 

# COMMAND ----------

# Notebook Parameters
datavaultEntityName = dbutils.widgets.get("DataVault_Entity_Name")
sourceSystem = dbutils.widgets.get("Source_System")

# Set Spark DB
hubDatabase = 'hub'
satDatabase = 'sat'
linkDatabase = 'link'
refDatabase = 'ref'

hubRootPath = rootPath + hubDatabase
satRootPath = rootPath + satDatabase
lnkRootPath = rootPath + linkDatabase
refRootPath = rootPath + refDatabase 

# Set Dates
date_time = current_timestamp()
date = current_date()

current_user = spark.sql("select current_user()").first()[0]

# Set count variables
newSatMembersCount = 0
totalSatRowCount = 0
isSateEntity = 0

# COMMAND ----------

# Load Data Vault Logic

try: 
    objectsDF = spark.sql(f"Select * from v_Load_DV where DataVault_Entity_Name = '{datavaultEntityName}'")
    
    row = objectsDF.first()
    
    dataVaultEntityId = row["DataVault_Entity_ID"]
    datavault_Entity_Name = row["DataVault_Entity_Name"]
    query_file_path = row["Logic_Definition"]
    hubBKName = row["BK_DV_Object"]
    hubName = row["Main_Entity_Name"]
    sourceSystemName = row["Source_System_Name"]
    entityLoad = row["Entity_Load"]
    
    query_full_file_path - os.path.join(query_folder, query_file_path)
    
    with open(query_full_file_path, 'r') as f:
        query_dv = f.read()
except Exception as Error:
    
    status = "Error"
    error_msg = Error
    str_error_msg = str(error_msg)
    
    raise Exception( f"'{status}' in '{outputFile}' : '{datavaultEntityName}' failed to load metadata with error message '{str_error_msg}' in ")
        

# COMMAND ----------

# List of Entities and Fields

dvEntityFieldsDF = spark.sql(
    f" Select DataVault_Entity_Name, Entity_Type_Name, concat_ws(',', collect_list(Field_Name)) AS Field_Name, 'Metadata' as Source, Main_Entity_Key_Name \
                                FROM v_DataVault_Entity_Fields \
                                Where DataVault_Entity_Logic = '{datavaultEntityName}' \
                                Group by DataVault_Entity_Name, Entity_Type_Name, Main_Entity_Key_Name ORDER BY DataVault_Entity_Name DESC"). cache()

rowEntityFieldsDF = dvEntityFieldsDF.first()
hubSIDName = rowEntityFieldsDF["Main_Entity_Key_Name"]

# COMMAND ----------

# Execute query
dvEntity = spark.sql(query_dv).filter(f"{hubBKName} is not NULL)
 
dvEntityCols = sorted(dvEntity.columns)

# COMMAND ----------

# Create Reference Table

if entityLoad in ('Reference'):
    refName = hubName
    refBKNameAlias = hubBKName
    refSIDName = hubSIDName
    
    dvEntityFieldsRefDF = dvEntityFieldsDF
    
    for rowDVE in dvEntityFieldsRefDF.collect():
        # get fields from query
        
        colsEntity = [x.strip() for x in rowDVE["Field_Name"].split(,)]
        
        cols = sorted([c for c in colsEntity if c in dvEntityCols])
        
        colsCopy = cols.copy() # copy list before adding business key
        cols.insert(0, refBKNameAlias)
        
        # Adding Sequence ID
        colsRS = colsCopy
        colsRS.insert(0, refSIDName)
        
        ref = spark.read.table(refDatabase + "." + refName)
        
        refFields = (dvEntity
                         .withColumn("R_Record_Source", lit(sourceSystemName))
                         .withColumn("R_Load_Date", lit(date_time))
                         .withColumn("R_ERL_ID", lit(eTLID))
                         .withColumn("R_Hash_Diff", udfSha256Python_DF(F.array(colsRS)))
                         .withColumn("R_DQ_Status", lit("Unchecked"))
                         .withColumn("R_Is_Deleted", lit(0))).cache()
        
        newRefMembers = refFields.join(ref, how = 'left_anti', on[refBKNameAlias]) # identify new member
        
        # Check if there are new members
        newRefMembersCount = newRefMembers.count()
        newRefMembers = newRefMembers.withColumn("R_Is_Deleted", col("R_Is_Deleted").cast("boolean"))
        newRefMembers = newRefMembers.drop("Last_Modified_Date_Time", "Last_Modified_By", "S_Is_Deleted")
        
        if (newRefMembersCount > 0):
            # Add new data
            (newRefMembers.write.format('delta').mode('append').save(os.path.join(refRootPath, refName.lower())))
            
            print(f" --- {newRefMembersCount} new REF members found. --- ")
        else:
            print(" --- No new REF members have been found. ---")
            
        # Uncache dataframe
        refFields.unpersist()

# COMMAND ----------

# Creating hub

if entityLoad in ('SatHubLink', 'SatHub'):
    #Create if table does not exist
    dfCreateHub(hubName, hubSIDName, hubBKName, hubRootPath)
    
    # Read hub table
    hub = spark.read.table(hubDatabase + "." + hubName)
    
    #Add Hub standard fields
    
    hub_source_df = (dvEntity
                        .selectExpr(f"cast({hubBKName} as String) as {hubBKName}")
                        .withColumn("H_Schema", lit(schema))
                        .withColumn("H_Record_Source", lit(sourceSystemName))
                        .withColumn("H_Load_Date", lit(date_time))
                        .withColumn(hubSIDName, udfSha256Python_DF(F.array(hubBKName)))
                        .withColumn("H_ETL_ID", lit(eTLID)))
    
    # prepare variables
    hub_name - hubName.lower()
    hub_path = os.path.join(hubRootPath, hub_name)
    hub_table = DeltaTable.forName(spark, f"{hubDatabase}.{hubName}")
    
    if not hub_table.toDF().take(1) or write_mode == 'overwrite':
        
        write_mode == 'overwrite'
        start_time = datetime.now()
        print(f"Performing '{write_mode}' for data in '{hubDatabase}.{hubName}' at {start_time}.")
        
        # load new hub ros
        (hub_source_df.write.format('delta').mode(write_mode).save(hub_path))
        
        end_time = datetime.now()
        print(f"Completed '{write_mode}' for data in '{hubDatabase}.{hubName}' at {end_time}.")
        
    else:
        # Identify new member
        
        newHubMembers = hub_source_df.join(
            hub,
            how = 'left_anti'
            on = [hubBkName])
        
    # check if data exists
    if newHubMembers.take(1):
        print(f"Beginning {write_mode} of new HUB memers to '{hubDatabase}.{hubName}'.")
        
        (newHubMembers.write.format('delta').mode(write_mode).save(hub_path))
        
        print(f"Completed {write_mode} of new HUB members to '{hubDatabase}.{hubName}'.")
        
    else:
        print(f"No new HUB members identified for '{hubDatabase}.{hubName}'.")

# COMMAND ----------

# MAGIC %md ### Create Satellite

# COMMAND ----------



if entityLoad in ('SatHubLink', 'SatHub', 'SatLink', 'Sat'):
    dvEntityFieldsSateDF = dbEntityFieldsDF.filer(
        (col("Entity_Type_Name") == "Satellite") | (col("Entity_Type_Name") == "LinkSat"))
    
    for rowDVE in dvEntityFieldsSatDF.collect():
        
        # getting fields from query
        colsEntity = [x.strip() for x in rowDVE["Field_Name"].split(',')]
        entityName = rowDVE["DataVault_Entity_Name"]
        entityType = rowDVE["Entity_Type_Name"]
        source = rowDVE["Source"]
        
        # get columns for Data Vault entity
        cols = sorted([c for c in colsEntity if c in dvEntityCols])
        colsCopy = cols.copy() #copy list before adding the business key
        cols.insert(0,hubBKName)
        if not entityType == 'LinkSat':
            cols.insert(1, hubsSIDName) # Insert SID
            
        # Add SID and Record Source in the list to be used in the hash diff -----------------
        ### Note: the sequence of the fields in colsRS are very important to calculate the hash key
        
        colsRS = colsCopy
        colsRS.insert(0, hubSIDName) # Insert SID
        
        # ----------------Create a Dataframe with extra fields ----------------------
        if entityType in ("Satellite", "LinkSat"):
            sat_source_df = (dvEntity.select(*cols)
                            .withColumn("S_Schema", lit(schema))
                            .withColumn("S_Record_Source", lit(sourceSystemName))
                             .withColumn("S_Is_Deleted", col("S_Is_Deleted").cast("string"))
                             .withColumn("S_Hash_Diff", udfSha256Python_DF(F.array(colsRS))) # Create the hash key. Check if data is different for not key columns
                             .withColumn("S_Load_Date", lit(date_time)) # Add Load date
                             .withColumn("S_ETL_ID", lit(eTLID)) # Add ETL ID
                             .withColumn("S_DQ_Status", lit("Unchecked")) # Add unchecked dq status for sat
                             .withColumn("S_Load_Date_End", lit(None)) # End date as NULL
                            )
            sat_source_df = sat_source_df.withColumn(hubBKName, sat_source_df[hubBKName].cast("string")).dropDuplicates() # Casting BK_ID as String
            
            # ------- LOAD SATELLITE-------------------------
            
            # prepare variables
            entity_name = entityName.lower()
            entity_type = entityType.lower()
            sat_path = f"{satRootPath}/{entity_name}"
            sat_table = DeltaTable.forName(spark, f"{satDatabase}.{entityName}") # Current Delta table
            sat_latest_view = spark.sql(f"SELECT * FROM vw_{entityName}_latest")
            
            if not sat_table.toDF().take(1) or write_mode == "overwrite" :
                
                # match target schema
                
                sat_source_df = sat_source_df.withColumn("S_Is_Deleted", col("S_Is_Deleted"):cast("boolean"))
                
                # overwrite and log start time
                
                write_mode = "overwrite"
                start_time = datetime.now()
                print(f"Performing '{write_mode}' for data in '{satDatabase}.{entityName}' at {start_time}.")
                
                # load new satellite rows
                (sat_source_df.write
                .format("delta")
                .mode(write_mode)
                .save(sat_path)
                )
                
                end_time = datetime.now()
                
                print(f"Completed '{write_mode}' for data in '{satDatabase}.{entityName}' at {end_time}. ")
            else:
                
                # get max date pior to loading
                
                pre_max_load_date_time = spark.sql(f"SELECT MAX(S_Load_Date) AS pre_max_load_date_time FROM {satDatabase}.{entityName}").first()["pre_max_load_date_time"]
                start_time = datetime.now()
                print(f"Attempting '{write_mode}' for data in '{satDatabase}.{entityName}' at {start_time}.")
                
                # critical for only inseting where vault rows don't exist - consists of new and updated sat members.
                
                new_sat_members = (sat_source_df
                                  .join(sat_latest_view,
                                       how = "left_anti",
                                       on = [f"{hubSIDName}", "S_Hash_Diff"]))
                .withColumn("S_Is_Deleted", col("S_Is_Deleted").cast("boolean"))
                
                (new_sat_members.write
                .format("delta")
                .mode(write_mode)
                .save(sat_path)
                )
                
                end_time = datetime.now()
                print(f"Completed '{write_mode}' for data in '{satDatabase}.{entityName}' at {end_time}.")
                
                # get metrics for logging and change detection
                
                sat_metric_df = spark.sql(f"SELECT COUNT(*) AS total_row_count, MAX(S_Load_Data) AS post_max_load_date_time FROM {satDatabase}.{entityName}").first()
                for sat_metric in sat_metric_df:
                    totalSatRowCount = sat_metric_df["total_row_count"]
                    post_max_load_date_time = sat_metric_df["post_max_load_date_time"]
                    
                isSatEntity = 1
                
                # re-create latest view with streamed delta data
                
                start_time = datetime.now()
                print(f"Checking need for delta stream at {start_time}.")
                dq_run = spark.sql(f"SELECT 1 FROM {satDatabase}.{entityName} WHERE S_DQ_Status IN ('Passed', 'Failed')").take(1)
                
                # determine changed data
                
                if post_max_load_date_time > pre_max_load_date_time:
                    print(f"Checking need for delta stream at {start_time}.")
                    
                    # Get delta count of rows
                    
                    newSatMembersCount = spark.sql(f"SELECT COUNT(*) AS etl_row_count FROM {satDatabase}.{entityName} WHERE S_ETL_Id = {eTLID}").first()["etl_row_count"]
                    
                    
                    # for DQ purposes
                    if dq_run:
                        
                        # prepare stream
                        
                        entity_name = entityName.lower()
                        entity_type = entityType.lower()
                        sat_stream_loc = f"{satRootPath}/{entity_name}/stream"
                        sat_stream_table = f"stream_{entity_name}"
                        sat_checkpoint_loc = f"{sat_stream_loc}/_checkpoint"
                        sat_unchecked_view = f"vw_{entity_name}_latest_unchecked"
                        OUTPUT_MODE = "append"
                        
                        start_time = datetime.now()
                        print(f"Begging data '{OUTPUT_MODE}' to '{sat_stream_loc}' at {start_time}.")
                        
                        # read link table to stream out appended data after "pre_max_load_date_time"
                        
                        sat_read_changes_df = (spark.readStream
                                              .format("delta")
                                              .option("startingTimestamp", pre_max_load_date_time)
                                              .table(f"{satDatabase}.{entityName}")
                                              )
                        
                        # write deltas to file
                        sat_write_changes_df = (sat_read_changes_df.writeStream
                                               .format("delta")
                                               .option("checkpointLocation", sat_checkpoint_loc)
                                               .outputMode(OUTPUT_MODE)
                                               .trigger(once=True)
                                               .queryName(f"qry_delta_{entity_type}_{entity_name}")
                                               )
                        end_time = datetime.now()
                        print(f"Completed data '{OUTPUT_MODE}' to '{sat_stream_loc}' at {end_time}.")
                    else:
                        end_time = datetime.now()
                        print(f"Changes detected but Delta street not required for data in '{satDatabase}.{entityNAme}' at {end_time}.")
                    else:
                        # no changes detected
                        end_time = datetime.now()
                        print(f"No Changes detected and Delta stream not required for data in '{satDatabase}.{entityName}' at {end_time}.")
            
            
                                      

# COMMAND ----------

# MAGIC %md ### Links

# COMMAND ----------

if entityLoad in ('SatHubLink','Link','SatLink'):
    dvEntityFieldsLinkDF = dvEntityFieldsDf.filter(col("Entity_Type_Name")=="Link")
    
    for rowDVE in dvEntityFieldsLinkDF.collect():
        #---------Get the columns from Query i.e queryDV ---------------
        link_cols = [for x in rowDVE["Field_Name"].split(',')]
        entityName = rowDVE["DataVault_Entity_Name"]
        entityType = rowDVE["Entity_Type_Name"]
        linkSID = rowDVE["Main_Entity_Key_Name"]
        
        
        cols = sorted([c for c in link_cols if c in dvEntityCols]) #check if all columns in the metadata (in case of main sat and links) exists in the Dataframe
        cols.insert(0, linkSID)
        
        
        #--------------Create a Dataframe with extra fields ---------------
        link_source_df = (dvEntity.select(*cols)
                          .withColumn("L_Schema", lit(schema)) #add schema
                          .withColumn("L_Record_Source", lit(sourceSystemName)) # Add Record Source - Source System Name
                          .withColumn("L_Load_Date", lit(date_time))
                          .withColumn("L_ETL_ID", lit(eTLID))
                         )
        
        if entityType == "link":
            link_source_df = link_source_df.dropDuplicates()
            link_table = DeltaTable.forName(spark, f"{linkDatabase}.{entityName}")
            
            # prepare variables
            
            entity_name = entityName.lower()
            entity_type = entityType.lower()
            link_path = f"{lnkRootPath}/{entity_name}"
            
            if not link_table.toDF().take(1) or write_mode == "overwrite":
                
                write_mode = "overwrite"
                start_time = datetime.now()
                print(f"Performing '{write_mode}' for data in '{linkDatabase}.{entityName}' at {starttime}." )
                
                (link_source_df
                 .write
                .format("delta")
                .mode(write_mode)
                .save(link_path)
                )
                
                end_time = datetime.now()
                print(f"Completed '{write_mode}' for data in '{linkDatabase}.{entityName}' at {end_time}.")
            else:
                start_time = datetime.now()
                print(f"Performing '{write_mode}' for data in '{linkDatabase}.{entityName}' at {start_time}.")
                
                # Append link data
                (link_table.alias("linkCurrent")
                .merge(link_source_df.alias("linkUpdates"),
                      f"""linkCurrent.{hubSIDName} = linkUpdates.{hubSIDName}""")
                .whenNotMatchedInsertAll()
                .execute()
                )
                
                end_time = datetime.now()
                print(f"Completed '{write_mode}' for data in '{linkDatabase}.{entity}' at {end_time}.")
    

# COMMAND ----------

# MAGIC %md Uncache dataframes

# COMMAND ----------

if entityLoad in ('SatHubLink','SatHub','Link','SatLink','Sat'):
    objectDF.unpersist()
    dvEntityFieldsDF.unpersist()
    dvEntity.unpersist()

# COMMAND ----------

# MAGIC %md Exit notebook with row counts

# COMMAND ----------

# Create Json string to be sent to calling ADF
# ADF activity can access items as like activity('ActivityName').output.runOutput.processRowCount

output = json.dumps(
{
    "processedRowCount": newSatMembersCount,
    "totalRowCount": totalSatRowCount,
    "isSatEntity": str(isSatEntity)
    
}
)

dbutils.notebook.exit(output)
