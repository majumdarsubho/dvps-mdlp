# Databricks notebook source
# MAGIC %md
# MAGIC # DV-Functions
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Originally Created By | [shourya.addepalli@neudesic.com](mailto:shourya.addepalli@neudesic.com)|
# MAGIC |External References |[https://neudesic.com](https://neudesic.com) |
# MAGIC <!-- |Input  |<ul><li> None |
# MAGIC |Input Data Source |<ul><li>None | 
# MAGIC |Output Data Source |<ul><li>global temporary view | -->
# MAGIC 
# MAGIC ## Other Details
# MAGIC This notebook contains all the functions used by the Data Vault Loading Framework.

# COMMAND ----------

def read_hashkey_metadata(source,metadata_path):
    """
    -Read metadata into Dataframe for BK - > HK mapping
    -Needed Generate Hub and Link for a source.
    """
    dv_entity = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("treatEmptyValuesAsNulls", "false") \
        .option("dataAddress","HubLinkSetup"+"!A1") \
        .load(metadata_path).where(f"DataVault_Source = '{source}'").select('*').withColumn("hash_ready",split(col("DataVault_BusinessKeys"),",")) 
    hash_df_list = dv_entity.toPandas()
    display(hash_df_list)
    dict_hash = hash_df_list.set_index('HashKey_Name').to_dict()
    # get Hash-key mapping to generate Hash Keys for a Source
    hk_bk_mapping_dict=dict_hash["hash_ready"] 
    return dv_entity,hk_bk_mapping_dict
print("executed read_hashkey_metadata")

# COMMAND ----------

def pre_processed_df(df,source):
    
    """ This function: 
    -> Remove "ARRAY" Columns in DF for RentalAgreement and Also :
    1. RA - ZEROPAD(RART-RNT-AREA,5)+ZEROPAD(RART-RNT-LOC,3),
	2. RA-ZEROPAD(RARN-RTRN-AREA,5)+ZEROPAD(RARN-RTRN-LOC,3)
	3. RA-ZEROPAD(RARN-DUE-AREA,5)+ZEROPAD(RARN-DUE-LOC,3)
	-> NPS = ZEROPAD(Area,5)+ZEROPAD(Location,3)
	-> Location = ZEROPAD(LEGACY_REVENUE_AREA_NUMBER,5)+ZEROPAD( LGCY_REV_LOCATION_NUMBER,3)
    """
    if source == "RentalAgreement": 
        array_columns=[]
        for i in df.columns:
            if "ARRAY" in i:
                array_columns.append(i)
            else:
                pass
        df = df.drop(*array_columns)
        print("Number of Array type Columns being dropped :",len(array_columns))
        df = df.withColumn("RART-RNT-AREA",lpad(col("RART-RNT-AREA"),5,'0')).alias("RART-RNT-AREA") \
               .withColumn("RART-RNT-LOC",lpad(col("RART-RNT-LOC"),3,'0')).alias("RART-RNT-LOC") \
               .withColumn("RARN-RTRN-AREA",lpad(col("RARN-RTRN-AREA"),5,'0')).alias("RARN-RTRN-AREA") \
               .withColumn("RARN-RTRN-LOC",lpad(col("RARN-RTRN-LOC"),3,'0')).alias("RARN-RTRN-LOC") \
               .withColumn("RARN-DUE-AREA",lpad(col("RARN-DUE-AREA"),5,'0')).alias("RARN-DUE-AREA") \
               .withColumn("RARN-DUE-LOC",lpad(col("RARN-DUE-LOC"),3,'0')).alias("RARN-DUE-LOC")
    elif source == "Location": # adding zero padding to columns as the different sources might have these attrivutes with different length-which can cause hash key mismatch
        df = df.withColumn('LGCY_REV_LOCATION_NUMBER', lpad(col("LGCY_REV_LOCATION_NUMBER"),3, '0')).alias("LGCY_REV_LOCATION_NUMBER") \
               .withColumn('LEGACY_REVENUE_AREA_NUMBER', lpad(col("LEGACY_REVENUE_AREA_NUMBER"),5, '0')).alias("LEGACY_REVENUE_AREA_NUMBER")
    elif source == "NPS": #generating VehicleGroup Column, to create HK_VehicleGroup
        df = df.withColumn('AREA_CD', lpad(col("AREA_CD"),5, '0')).alias("AREA_CD") \
               .withColumn('LOCATION_CD', lpad(col("LOCATION_CD"),3, '0')).alias("LOCATION_CD")
    else:
        pass
    return df
print("executed cleaned_df")

# COMMAND ----------

def read_raw_streaming(df,source,hk_bk_mapping_dict,dv_entity):
    """ This function : 
    -Read parquet file for a Source
    -Remove " " from column names
    -Add & Remove columns based on Source
    -Replace Null with '0' for BK columns
    -Replace all other NULL values with ''(empty string)
    -Add Columns : hashkeys,HashDIFF,HK(s),Source,LoadDate(current timestamp) 
    """
    system_columns = ["loadDate"]
    #remove spaces from column names,to write the df as delta in later stages.
    df = df.select([col(columns_).alias(columns_.replace(' ', '')) for columns_ in df.columns])
    
    # cleaned/transformed DF
    df = pre_processed_df(df,source)
    columns_to_hash = list(set(df.columns) - set(system_columns))
     #creating a copy of dict which will later be used for generating hashkeys
    original_hash_dict = dict(hk_bk_mapping_dict)
    
    #get dict for removing null from BK for all sources.
    null_fill_dict = get_bk_fill_null(source,hk_bk_mapping_dict,dv_entity)
    
    #replace BK columns with 0
    df = replace_bk_null_values(df,null_fill_dict) 
    #replace all null in df with ''
    df = df.fillna('') 
    
    #add hashDIFF
    new_df= df.withColumn("HashDIFF",sha2(concat_ws("",*columns_to_hash),256))
    print("added HashDIFF Column")
    
    #get all columns
    all_columns = new_df.columns # all_columns = source_columns + "HashDiff" Column 
    print("Grabbed all columns from DataFrame")
    
    #generating the required hash keys
    for i in original_hash_dict.items():
        new_df = new_df.withColumn(i[0],sha2(concat_ws("", *i[1]), 256))
    print("Added Hashkeys to the DataFrame")
    
    #adding common_cols = Source and LoadDate(current_timestamp())
    parent_df = new_df.withColumn("Source",lit(source)) \
    .withColumn("SourceRegion",lit("USA")) \
    .withColumn("Silver_LoadDate",current_timestamp())

    return parent_df,all_columns
print("executed read_parquet streaming")

# COMMAND ----------

def create_hub(parent_df,source,common_cols,dv_entity):
    """
      -dv_entity : Metadata table which has Hub and Link information
      -Return hub for a given source
    """
    hub_object = dv_entity.where(f"DataVault_Source = '{source}'and Entity_name LIKE 'HUB%' ").first()
    hub_entity_name = hub_object[0]
    print("Creating HUB :",hub_entity_name)
    hub_business_key = hub_object[1]
    hub_hash_key = hub_object[2]
    hub_columns = (hub_hash_key + "," + hub_business_key).split(",")+common_cols
    hub_df = parent_df.select(*hub_columns).dropDuplicates()
    return hub_df
print("executed create_hub")

# COMMAND ----------

def create_link(parent_df,source,common_cols,dv_entity):
    """
      -dv_entity : Metadata table which has Hub and Link information
      -Return Link for a given source if exists in metadata
    """
    link_object = dv_entity.where(f"DataVault_Source = '{source}'and Entity_name LIKE 'LINK%' ").first()
    if link_object == None:
        print("No Link table exists for this source in the metadata")
        link_df = None
    else:
        link_entity_name = link_object[0]
        print("Creating LINK:",link_entity_name)
        link_other_columns = link_object[1]
        link_bk = link_object[2]
        link_all_columns = common_cols + (link_bk + "," + link_other_columns).split(",")
        link_df = parent_df.select(*link_all_columns).dropDuplicates()
   
    return link_df
print("executed create_link")

# COMMAND ----------

def create_sat_columns_metadata(metadata_path,source):
    """
      This function Reads the Metadata sheet and return a DataFrame
    """
    sat_columns_df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("treatEmptyValuesAsNulls", "false") \
        .option("dataAddress","SatelliteSetup"+"!A1") \
        .load(metadata_path).where(f"DataVault_Source = '{source}'")
    return sat_columns_df
print("executed create_sat_columns_metadata")

# COMMAND ----------

def get_sat_columns(sat_object,common_cols):
    sat_hash_key = sat_object[1]
    sat_included_columns = sat_object[2]
    sat_all_columns = common_cols + (sat_hash_key + "," + sat_included_columns + "," + "HashDIFF").split(",")
    return sat_all_columns

# COMMAND ----------

def create_sat(parent_df,source,sat_columns_df,common_cols,all_columns):
    """
    This function is used to create Satellite tables for Source : Rental Agreement,Reservation,NPS,LoyaltyCustomer,Vehicle,VehicleGroup,Location.
    common_cols = Source,Loaddate(current_timestamp())+SourceRegion
    parent_df  = SourceDF+ HashDIFF + HK(s) + Common_cols
    """
    #for Sources that have NO link tables & 1 sat, SAT = source_df + hashdiff+HK+Source+Loaddate (parent_df)
    #if source not in-add
    sat_entity_list = list((sat_columns_df.where(f"DataVault_Source = '{source}'").select("SAT_Name").toPandas()['SAT_Name']))
    for sat_entity_name in sat_entity_list:
        print("Creating SAT :",sat_entity_name)
        sat_object = sat_columns_df.where(f"SAT_Name = '{sat_entity_name}'").first()
        if sat_entity_name in ("SAT_Reservation_Non_PII","SAT_RentalAgreement_Non_PII"):
            sat_hash_key = sat_object[1]
            sat_excluded_columns = sat_object[3].split(",")
            remaining_sat_columns = list(set(all_columns) - set(sat_excluded_columns))
            sat_non_pii_all_columns = sat_hash_key.split(",")+ common_cols + remaining_sat_columns 
            sat_non_pii_df = parent_df.select(sat_non_pii_all_columns).dropDuplicates()
        elif sat_entity_name in ("SAT_Reservation_PII","SAT_RentalAgreement_PII"):
            sat_pii_all_columns = get_sat_columns(sat_object,common_cols)
            sat_pii_df = parent_df.select(*sat_pii_all_columns).dropDuplicates()
        elif sat_entity_name in ("SAT_RentalAgreementAssociation","SAT_ReservationAssociation"):
            sat_assoc_all_columns = get_sat_columns(sat_object,common_cols)
            sat_assoc_df = parent_df.select(*sat_assoc_all_columns).dropDuplicates()
        elif sat_entity_name == "SAT_NPS":
            sat_hash_key = sat_object[1].split(",") 
            sat_all_columns = sat_hash_key + common_cols + all_columns 
            sat_df = parent_df.select(*sat_all_columns).dropDuplicates()
        else:
            sat_df = parent_df.dropDuplicates()
    if source in ("LoyaltyCustomer","Vehicle","NPS","Location","VehicleGroup"):
        return sat_df
    else:
        return sat_pii_df,sat_non_pii_df,sat_assoc_df
print("executed create_sat")

# COMMAND ----------

def generate_dv_tables(df,source,common_cols,metadata_path,dv_entity,hk_bk_mapping_dict):

    parent_df,all_columns = read_raw_streaming(df,source,hk_bk_mapping_dict,dv_entity)

    hub_df = create_hub(parent_df,source,common_cols,dv_entity)

    link_df = create_link(parent_df,source,common_cols,dv_entity)

    sat_columns_df  = create_sat_columns_metadata(metadata_path,source)
    print("Source : ",source)

    if source not in ("LoyaltyCustomer","Vehicle","NPS","Location","VehicleGroup"):
        sat_pii_df,sat_non_pii_df,sat_assoc_df = create_sat(parent_df,source,sat_columns_df,common_cols,all_columns)
        return hub_df,link_df,sat_pii_df,sat_non_pii_df,sat_assoc_df
    else:
        sat_df = create_sat(parent_df,source,sat_columns_df,common_cols,all_columns)
        return hub_df,link_df,sat_df

# COMMAND ----------

def replace_bk_null_values(df,null_fill_dict):
    """
      This function is used to replace the NULL values with '0' for BK columns for a source
    """
    for i in null_fill_dict.items():
        new_df = df.fillna('0', subset=list(i[1]))
    print("Replaced Null values if any in BK columns")
    return new_df
print("executed replace_bk_null_values")

# COMMAND ----------

def get_bk_fill_null(source,hk_bk_mapping_dict,dv_entity):
    """
    This function is used to generate the DICt which is sued to replace null values with '0' in BK columns
    """
    if source not in ("Reservation","RentalAgreement","NPS"):
        return hk_bk_mapping_dict
    else:
        #getting the HK to remove from our bk_hk_mapping_dict, so that we have only BK columns and not HK columns to fill NULL rows with '0'.
        item_del_from_dict = dv_entity.where(f"DataVault_Source = '{source}' and Entity_name LIKE 'LINK%' ").first()[2] 
        hk_bk_mapping_dict.pop(item_del_from_dict) #here,we remove HK from dict as it has not been generated yet
        null_fill_dict = hk_bk_mapping_dict
        print("Fetch Dict to Replace Null values with '0' before creating HashDIFF and Hash keys")
    return null_fill_dict # same dict with HK key,which will be utilized to replace null values in BK columns for a source

print("executed get_bk_fill_null")

# COMMAND ----------

def hub_insert(dv_entity,source,existing_hub_df,hub_df):
    print("Inserting HUB")
    hub_hashkey  = dv_entity.where(f"DataVault_Source = '{source}'and Entity_name = 'HUB_{source}' ").select("HashKey_Name").first()[0]
    existing_hub_df.alias("existing") \
    .merge(hub_df.alias("incoming"),f"""existing.{hub_hashkey} = incoming.{hub_hashkey}""") \
    .whenNotMatchedInsertAll().execute()
    return 
print("executed hub_insert")

# COMMAND ----------

def link_insert(dv_entity,source,existing_link_df,link_df):
    link_hashkey  = dv_entity.where(f"DataVault_Source = '{source}'and Entity_name LIKE 'LINK%' ").select("HashKey_Name").first()[0]
    existing_link_df.alias("existing") \
    .merge(link_df.alias("incoming"),f"""existing.{link_hashkey} = incoming.{link_hashkey}""") \
    .whenNotMatchedInsertAll().execute()
    return 
print("executed link_insert")

# COMMAND ----------

def sat_insert(dv_entity,source,existing_sat_df,sat_df):
    sat_hashkey  = "HashDIFF"
    print("Inserting SAT")
    existing_sat_df.alias("existing") \
    .merge(sat_df.alias("incoming"),f"""existing.{sat_hashkey} = incoming.{sat_hashkey}""") \
    .whenNotMatchedInsertAll().execute()
    return
print("executed sat_insert")  

# COMMAND ----------

def foreach_batch_function(df, epoch_id,source,metadata_path,common_cols,curated_path):
# #     df.persist().count()
    source_dv_entities = "Present"
    dv_entity,hk_bk_mapping_dict=read_hashkey_metadata(source,metadata_path)
    if source not in ("LoyaltyCustomer","Vehicle","NPS","Location","VehicleGroup"):
        hub_df,link_df,sat_pii_df,sat_non_pii_df,sat_assoc_df = generate_dv_tables(df,source,common_cols,metadata_path,dv_entity,hk_bk_mapping_dict)

    else:
        hub_df,link_df,sat_df =  generate_dv_tables(df,source,common_cols,metadata_path,dv_entity,hk_bk_mapping_dict)       
        
    try:
        
        existing_hub_df = DeltaTable.forPath(spark, f"""{curated_path}/Hub_{source}""")  

        if source in ("LoyaltyCustomer","Vehicle","Location","VehicleGroup"):
            existing_sat_df = DeltaTable.forPath(spark,f"""{curated_path}/Sat_{source}""")

        elif source == "NPS":   
            existing_link_df = DeltaTable.forPath(spark, f"""{curated_path}/Link_{source}_Association""")
            existing_sat_df =  DeltaTable.forPath(spark,f"""{curated_path}/Sat_{source}""")

        else:
            
            existing_link_df = DeltaTable.forPath(spark, f"""{curated_path}/Link_{source}Association""")
            
            existing_sat_df_pii =  DeltaTable.forPath(spark,f"""{curated_path}/Sat_{source}_PII""")
            
            existing_sat_df_non_pii =  DeltaTable.forPath(spark,f"""{curated_path}/Sat_{source}_Non_PII""")
            
            existing_sat_assoc_df = DeltaTable.forPath(spark,f"""{curated_path}/Sat_{source}Association""")
           
    except Exception as e:
        print(f"Silver table(s) do not exist yet for Source  : {source}")
        source_dv_entities = "Not Present"
        
    
    if source_dv_entities == "Not Present":
        print(f"Creating/Writing silver tables for Source : {source}")
        try:
            if source in ("LoyaltyCustomer","Vehicle","Location","VehicleGroup"):
                hub_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("Silver_LoadDate").save(f"{curated_path}/Hub_{source}")
                sat_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("Silver_LoadDate").save(f"{curated_path}/Sat_{source}")
            elif source == "NPS":
                hub_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("Silver_LoadDate").save(f"{curated_path}/Hub_{source}")
                link_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("Silver_LoadDate").save(f"{curated_path}/Link_{source}Association")
                sat_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("Silver_LoadDate").save(f"{curated_path}/Sat_{source}")
            else:
                hub_df.write.format('delta').mode("overwrite").option("overwriteSchema","true").partitionBy("Silver_LoadDate").save(f"{curated_path}/Hub_{source}")
                link_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("Silver_LoadDate").save(f"{curated_path}/Link_{source}Association")
                sat_pii_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("Silver_LoadDate").save(f"{curated_path}/Sat_{source}_PII")
                sat_non_pii_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("Silver_LoadDate").save(f"{curated_path}/Sat_{source}_Non_PII")
                sat_assoc_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("Silver_LoadDate").save(f"{curated_path}/Sat_{source}Association")
        except Exception as e:
            print(e)
            errorCode = "196"
# #             errorDescription = "Bronze to Silver Notebook: " + notebookName + "error while writing Data"
# #             log_event_notebook_error(notebookExecutionLogKey, errorCode, errorDescription, notebookTableExecutionName,secretScopeType,notebookName)
    else:
        try:
            hub_insert(dv_entity,source,existing_hub_df,hub_df)
            if source in ("LoyaltyCustomer","Vehicle","Location","VehicleGroup"):
                sat_insert(dv_entity,source,existing_sat_df,sat_df)
            elif source == "NPS":
                
                link_insert(dv_entity,source,existing_link_df,link_df)
                
                sat_insert(dv_entity,source,existing_sat_df,sat_df)
            else:
                link_insert(dv_entity,source,existing_link_df,link_df)
                sat_insert(dv_entity,source,existing_sat_df_pii,sat_pii_df)
                sat_insert(dv_entity,source,existing_sat_df_non_pii,sat_non_pii_df)
                sat_insert(dv_entity,source,existing_sat_assoc_df,sat_assoc_df)
        except Exception as e:
            print(e)
            errorCode = "198"
            
#     #         errorDescription = f"Bronze to Silver Notebook: {source}" + notebookName + "error while Inserting Data for {source}"
#     #         log_event_notebook_error(notebookExecutionLogKey, errorCode, errorDescription, notebookTableExecutionName,secretScopeType,notebookName)

