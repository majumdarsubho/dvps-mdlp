# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Functions Master  
# MAGIC Description: Acts as a collection for all reusable functions to be loaded at the top of dependent notebooks    
# MAGIC 
# MAGIC ## Import Required Libraries/Packages

# COMMAND ----------

import re
import datetime
from notebookutils import mssparkutils

from pyspark.sql import functions as f, Window
from pyspark.sql.types import *

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### table_exists  
# MAGIC Checks the spark catalog for existance of the provided table name  
# MAGIC 
# MAGIC INPUT:  
# MAGIC - tbl (String) - the name of a spark table  
# MAGIC 
# MAGIC OUTPUT:
# MAGIC - (Boolean) - True if tbl is registered as a spark table, false otherwise

# COMMAND ----------

def table_exists(tbl):
    tbl_split = tbl.split('.')
    if len(tbl_split) == 2:
        db = tbl_split[0]
        tableName = tbl_split[1]
    elif len(tbl_split) == 1:
        db = 'default'
        tableName = tbl
    else:
        return False
    dbList = [i.name for i in spark.catalog.listDatabases()]
    if db in dbList:
        tableList = [i.name for i in spark.catalog.listTables(db)]
        if tableName in tableList:
            return True
        else:
            return False
    else:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ### column_naming_convention    
# MAGIC Cleans input dataframe to align with standard columns naming conventions  
# MAGIC 
# MAGIC INPUT:  
# MAGIC - df (Spark Dataframe) - Spark dataframe with original column naming  
# MAGIC 
# MAGIC OUTPUT:
# MAGIC - raw_df_columns_cleansed (Spark Dataframe) - Spark dataframe with cleaned column naming

# COMMAND ----------

def column_naming_convention(df):
    import re
    oldCols = df.columns
    newCols = []
    for col in oldCols:
        newCol = re.sub('[^\w]', '_', col)
        newCols.append(newCol)
    raw_df_columns_cleansed = df.toDF(*newCols)
    return raw_df_columns_cleansed

# COMMAND ----------

# MAGIC %md
# MAGIC ### standardize_scd_fields    
# MAGIC Formats standard SCD fields (Current_Row_Ind, Effective_Date and End_Date) to correct naming and as datetime not date  
# MAGIC 
# MAGIC INPUT:  
# MAGIC - df (Spark Dataframe) - Spark dataframe with original column naming  
# MAGIC 
# MAGIC OUTPUT:
# MAGIC - raw_df_columns_cleansed (Spark Dataframe) - Spark dataframe including all SCD fields with correct naming and data types

# COMMAND ----------

def  standardize_scd_fields(df):
    effective_date = datetime.datetime(1899,12,31)
    end_date = datetime.datetime(9999,12,31)
    drop_cols = ['Id_Key', 'Composite_Id']

    if 'Current_Row_Ind' in df.columns:
        df = df.withColumn('Current_Row_Ind', f.col('Current_Row_Ind') == 1)
    else:
        df = df.withColumn('Current_Row_Ind', f.lit(True))

    if 'Effective_Dt' in df.columns:
        df = df.withColumnRenamed('Effective_Dt', 'Effective_Date').withColumn('Effective_Date', f.col('Effective_Date').cast('timestamp'))
    elif 'Effective_Date' in df.columns:
        df = df.withColumn('Effective_Date', f.col('Effective_Date').cast('timestamp'))
    else:
        df = df.withColumn('Effective_Date', f.lit(effective_date).cast('timestamp'))

    if 'End_Dt' in df.columns:
        df = df.withColumnRenamed('End_Dt', 'End_Date').withColumn('End_Date', f.col('End_Date').cast('timestamp'))
    elif 'End_Date' in df.columns:
        df = df.withColumn('End_Date', f.col('End_Date').cast('timestamp'))
    else:
        df = df.withColumn('End_Date', f.lit(end_date).cast('timestamp'))

    return df.drop(*drop_cols)

# COMMAND ----------

def path_exists(path):
    try:
        mssparkutils.fs.ls(path)
        return True
    except:
        return False

# COMMAND ----------

def add_column_if_missing(df, colName, default_val=None):
  all_cols = [c.lower() for c in df.columns]
  if colName.lower() not in all_cols:
    df = df.withColumn(colName, f.lit(default_val))
  return df

def add_column_list_if_missing(df, colNames):
  for colName in colNames:
    df = add_column_if_missing(df, colName)
  return df

# COMMAND ----------

def get_account_name():
    account_map = {
        'fbfdmdeus2fdmsynapws': 'fbfdmdeus2fdmdls',
        'fbfdmueus2fdmsynapws': 'fbfdmueus2fdmdls',
        'fbfdmpeus2fdmsynapws': 'fbfdmpeus2fdmdls'
    }
    ws = mssparkutils.env.getWorkspaceName()
    return account_map.get(ws, 'fbfdmdeus2fdmdls')

# COMMAND ----------

def get_date_time(rdate):
    dt_match = re.match(r'^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}', rdate)
    str_match = re.match(r'^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}', rdate)
    if dt_match:
        rdate = dt_match.group().replace('-', '/')
    elif str_match:
        rdate = str_match.group()
    else:
        rdate = (datetime.datetime.now(tz=datetime.timezone.utc)).strftime('%m/%d/%Y')
    y, m, d = list(map(int, rdate.split('/')))
    rdatetime = datetime.datetime(y, m, d)
    return rdatetime

# COMMAND ----------

def find_common_merge_keys(df1, df2) -> list:
    common_keys = list(
        set([i.lower() for i in df1.drop('Current_Row_Ind', 'Effective_Date', 'End_Date').columns]) 
        & set([i.lower() for i in df2.drop('Current_Row_Ind', 'Effective_Date', 'End_Date').columns])
    )
    return common_keys

def get_keys_from_path(path, start_keys, read_format):
    drop_cols = ['Id_Key', 'Composite_Id']
    cols = (
        spark.read
        .format(read_format)
        .option('inferSchema', 'true')
        .option('path', path)
        .load().drop(*drop_cols)
        .limit(1)
        .columns
    )
    return_keys = [c for c in cols if c.lower() in [p.strip().lower() for p in start_keys]]
    return return_keys

# COMMAND ----------

def get_land_data(params:dict):
    """
    Load landing data as dataframe based on imput parameters
    """
    landingDataPath = params.get('landingDataPath', '')
    tableName = params.get('integrationZoneTableName', '')
    fileExtension = params.get('fileExtension', 'parquet')
    fileName = params.get('landingFileName', tableName)
    containername = params.get('containername', '').lower()
    db = 'ds_'+containername
    account_name = get_account_name()
    drop_cols = ['Id_Key', 'Composite_Id']

    landingDataPath = "abfss://landing@"+account_name+".dfs.core.windows.net/raw/"+landingDataPath+"/"+fileName+"."+fileExtension
    landBadRecordsPath = "abfss://landing@"+account_name+".dfs.core.windows.net/raw/"+containername+"/"+tableName+"/Bad_Records"

    if path_exists(landingDataPath):
        land_df = (
            spark.read
            .format('parquet')
            .option('inferSchema', 'true')
            .option('badRecordsPath', landBadRecordsPath)
            .option('path', landingDataPath)
            .load()
            .drop(*drop_cols)
            .cache()
        )
        return column_naming_convention(land_df)

def get_pst_data(params:dict):
    """
    Load persistence data as dataframe based on imput parameters
    """
    persistenceDataPath = params.get('persistenceDataPath', '')
    tableName = params.get('integrationZoneTableName', '')
    fileExtension = params.get('fileExtension', 'parquet')
    fileName = params.get('landingFileName', tableName)
    containername = params.get('containername', '').lower()
    db = 'ds_'+containername
    account_name = get_account_name()
    drop_cols = ['Id_Key', 'Composite_Id']

    persistenceDataPath = "abfss://persistence@"+account_name+".dfs.core.windows.net/"+persistenceDataPath
    persistBadRecordsPath = "abfss://persistence@"+account_name+".dfs.core.windows.net/"+containername+"/"+tableName+"/Bad_Records"

    if path_exists(persistenceDataPath):
        curr_df = (
            spark.read
            .format('delta')
            .option('badRecordsPath', persistBadRecordsPath)
            .option('path', persistenceDataPath)
            .load()
            .drop(*drop_cols)
            .where(f.col('Current_Row_Ind') == 1).cache()
        )
        return curr_df

def get_hash_cols(land_df, curr_df):
    """
    Get a list of common columns to use as join hash
    """
    hash_cols = list(
        set([i.lower() for i in land_df.drop('Current_Row_Ind', 'Effective_Date', 'End_Date').columns]) 
        & set([i.lower() for i in curr_df.drop('Current_Row_Ind', 'Effective_Date', 'End_Date').columns])
    )
    return hash_cols

def get_new_df(land_df, curr_df, anti_cols, effective_date):
    land_df = land_df.withColumn('join_hash', f.concat_ws('|', *anti_cols))
    curr_df = curr_df.withColumn('join_hash', f.concat_ws('|', *anti_cols))
    return standardize_scd_fields(
        land_df.join(curr_df.alias('sink_anti'), 'join_hash', 'left_anti').drop('join_hash')
    )

def get_expired_df(land_df, curr_df, effective_date, params):
    primaryKeyColumns = params.get('primaryKeyColumns', '').split(',')
    return (
        curr_df.alias('sink').join(land_df.alias('updates'), primaryKeyColumns, 'left_anti').drop('join_hash')
        .withColumn('Effective_Date', f.lit(effective_date))
    )

def format_keys(new_df, curr_df, params):
    """
    Returns primary keys with correct casing and order. 
    Uses all common columns there is no param for primaryKeyColumns
    """
    primaryKeyColumns = params.get('primaryKeyColumns', '').split(',')

    # Use all columns as primary key if none exists
    if (primaryKeyColumns == ['']) | (primaryKeyColumns == []):
        primaryKeyColumns = list(
            set([i.lower() for i in new_df.drop('Current_Row_Ind', 'Effective_Date', 'End_Date').columns]) 
            & set([i.lower() for i in curr_df.drop('Current_Row_Ind', 'Effective_Date', 'End_Date').columns])
        )
    # Format order and casing
    primaryKeyColumns = [c for c in curr_df.columns if c.lower() in [p.strip().lower() for p in primaryKeyColumns]]
    return primaryKeyColumns

def get_insert_keys(key_cols):
    """
    Returns list of merge keys in spark sql strings for inserted data.
    Uses Null values for all key fields to ensure no match for the insert
    """
    return ['NULL as mergeKey' + str(i) for i, c in enumerate(key_cols)]
    
def get_update_keys(key_cols):
    """
    Returns list of merge keys in spark sql strings for inserted data.
    Uses actual values for all key fields to ensure accurate match for the update
    """
    return [c + ' as mergeKey' + str(i) for i, c in enumerate(key_cols)]
    
def get_condition_clause():
    return "sink.Current_Row_Ind = true"

def get_equality_clause(key_cols, effective_date):
    """
    Returns the equality logic in a spark sql string for delta merge
    """
    effective_filter = effective_date.strftime('%Y-%m-%d %H:%M:%S')
    conditionClause = get_condition_clause()
    equalityClause = ' AND '.join(['(sink.'+c+'=updates.mergeKey'+str(i)+')' for i, c in enumerate(key_cols)])
    return f"{conditionClause} AND {equalityClause} AND sink.Effective_Date < '{effective_filter}'"

def get_set_clause(new_df, curr_df, key_cols):
    """
    Returns the set logic in a spark sql string for delta merge
    """
    setCols = new_df.columns + ['Current_Row_Ind', 'End_Date', 'Effective_Date']
    setCols = list(set([c.lower() for c in new_df.columns + ['Current_Row_Ind', 'End_Date', 'Effective_Date']]) & set([c.lower() for c in curr_df.columns]))
    SetClause = {c: ''.join(['updates.', c]) for c in setCols}
    return SetClause

def _get_scd2_update_df(land_df, new_df, curr_df, key_cols, effective_date, end_date = datetime.datetime(9999,12,31)):
    """
    Format and return final update dataframe used to merge inserts and updates for scd2
    """
    insert_merge_keys = get_insert_keys(key_cols)
    update_merge_keys = get_update_keys(key_cols)

    expired_df = curr_df.alias('sink').join(land_df.alias('updates'), key_cols, 'left_anti').drop('join_hash')

    update_df = (
        new_df.alias('updates')
        .join(curr_df.alias('sink'), key_cols)
        .selectExpr(*update_merge_keys, 'updates.*')
        .unionByName(new_df.selectExpr(*insert_merge_keys, '*'))
    )
    expired_cols = [i for i in update_df.columns if 'mergeKey' not in i]
    
    update_df = (
        update_df
        .unionByName(
            add_column_list_if_missing(expired_df, expired_cols)
            .selectExpr(*update_merge_keys + expired_cols)
        )

        .withColumn('End_Date', f.lit(end_date))
        .withColumn('Effective_Date', f.lit(effective_date))
        .withColumn('Current_Row_Ind', f.lit(True))
    )
    return update_df

def get_update_df(land_df, new_df, curr_df, key_cols, effective_date):
    """
    Format and return final update dataframe used to merge inserts and updates
    """
    return _get_scd2_update_df(land_df, new_df, curr_df, key_cols, effective_date)

def scd2_merge(target_path, update_df, equality_clause, condition_clause, set_clause, effective_date:datetime.datetime):
    """
    Merge logic for scd2 
    """
    #Disabling the broadcast join to resolve the time out error
    sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    effective_filter = effective_date.strftime('%Y-%m-%d %H:%M:%S')

    deltaTable = DeltaTable.forPath(spark, target_path)
    (
        deltaTable.alias("sink")
        .merge(update_df.alias("updates"), equality_clause)
        .whenMatchedUpdate(
            condition = condition_clause,
            set = {
                "Current_Row_Ind": "false",
                "End_Date": "updates.Effective_Date"
            }
        )
        .whenNotMatchedInsert(values = set_clause)
        .execute()
    )

def stage_integration_data(params, where_expr):
    """
    Stages data in the integration layer for only relevent data
    """
    staging_drop_cols = [
        'Date_Created',
        'Audit_Hash_Cd',
        'Id_Key', 
        'Core_Id',
        'Composite_Id',
        'Date_Updated_Last_Audit',
        'Updated_By',
        'Created_By',
        'join_hash'
    ]
    persistence_path = params.get('persistenceDataPath', '')
    integration_path = params.get('integrationDataPath', '')
    persistence_path = "abfss://persistence@"+get_account_name()+".dfs.core.windows.net/"+persistence_path
    integration_path = "abfss://integration@"+get_account_name()+".dfs.core.windows.net/staging/"+integration_path
    (
        standardize_scd_fields(DeltaTable.forPath(spark, persistence_path).toDF())
        .where(f.expr(where_expr))
        .withColumn('Current_Row_Ind', f.lit(True))
        .drop(*staging_drop_cols)
        .write.mode('overwrite')
        .parquet(integration_path)
    )

# Reference Documentation: https://docs.delta.io/0.4.0/delta-update.html#id7  
def delta_merge(effective_date:datetime.datetime, params:dict):
    """
    Primary function for merging data from landing to persistence.
    Preps update data and executes merge operation on pst
    """
    print("Loading landing data in progress ...")
    land_df = get_land_data(params)
    print("Loading landing data complete!")
    print("Loading persistence data in progress ...")
    curr_df = get_pst_data(params)
    print("Loading persistence data complete!")
    hash_cols = get_hash_cols(land_df, curr_df)
    print("Processing new data in progress ...")
    new_df = get_new_df(land_df, curr_df, hash_cols, effective_date)
    print("Loading new data complete!")
    expired_df = get_expired_df(land_df, curr_df, effective_date, params)
    key_columns = format_keys(new_df, curr_df, params)
    print("Loading update data in progress ...")
    update_df = get_update_df(land_df, new_df, curr_df, key_columns, effective_date)
    print("Loading update data complete!")

    print("Building merge clauses in progress ...")
    condition_clause = get_condition_clause()
    equality_clause = get_equality_clause(key_columns, effective_date)
    insert_merge_keys = get_insert_keys(key_columns)
    update_merge_keys = get_update_keys(key_columns)
    set_clause = get_set_clause(new_df, curr_df, key_columns)
    print("Building merge clauses complete!")

    print("Merging update data into persistence in progress ...")
    target_path = params.get('persistenceDataPath', '')
    target_path = "abfss://persistence@"+get_account_name()+".dfs.core.windows.net/"+target_path
    scd2_merge(target_path, update_df, equality_clause, condition_clause, set_clause, effective_date)
    print("Merging update data into persistence complete!")



# COMMAND ----------


