# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from dateutil.relativedelta import relativedelta
from delta.tables import *
from delta.tables import DeltaTable
from pyspark.sql import SQLContext

# COMMAND ----------

source_path = "s3a://dpa-hanv-dpdl-1-s3-reg-curated-0001/Sat_Location/"
target_path = "s3a://dpa-hanv-dpdl-1-s3-reg-conformed-0001/Dim_Location/"


# COMMAND ----------

def generateJoinCondition(key_cols):
    joinCondition = ""
    for col in key_cols:
        joinCondition += "updates."+ col + " = " + "target." + col + " and "
    print(joinCondition[0:len(joinCondition)-4])

# COMMAND ----------

def get_condition_clause():
    return "sink.IsActive = 1"

# COMMAND ----------

def get_equality_clause(key_cols, effective_date):
    """
    Returns the equality logic in a spark sql string for delta merge
    """
    effective_filter = effective_date.strftime('%Y-%m-%d %H:%M:%S')
    conditionClause = get_condition_clause()
    equalityClause = ' AND '.join(['(sink.'+c+'=updates.mergeKey'+str(i)+')' for i, c in enumerate(key_cols)])
    return f"{conditionClause} AND {equalityClause} AND sink.Effective_Date < '{effective_filter}'"

# COMMAND ----------

def get_set_clause(new_df, curr_df, key_cols):
    """
    Returns the set logic in a spark sql string for delta merge
    """
    setCols = new_df.columns + ['Current_Row_Ind', 'End_Date', 'Effective_Date']
    setCols = list(set([c.lower() for c in new_df.columns + ['Current_Row_Ind', 'End_Date', 'Effective_Date']]) & set([c.lower() for c in curr_df.columns]))
    SetClause = {c: ''.join(['updates.', c]) for c in setCols}
    return SetClause

# COMMAND ----------

def get_insert_keys(key_cols):
    """
    Returns list of merge keys in spark sql strings for inserted data.
    Uses Null values for all key fields to ensure no match for the insert
    """
    return ['NULL as mergeKey' + str(i) for i, c in enumerate(key_cols)]

# COMMAND ----------

def get_update_keys(key_cols):
    """
    Returns list of merge keys in spark sql strings for inserted data.
    Uses actual values for all key fields to ensure accurate match for the update
    """
    return [c + ' as mergeKey' + str(i) for i, c in enumerate(key_cols)]

# COMMAND ----------

sourceDbName = "mdlsilver"
sourceTableName = "sat_location"
sourceTableCompleteName = sourceDbName+'.'+sourceTableName


# COMMAND ----------

targetDbName = "sample_gold"
targetTableName = "dim_location"
targetTableCompleteName = targetDbName+'.'+targetTableName


# COMMAND ----------

# Read the source delta table in the databricks database as a stream
streamingSourceDf = spark.readStream.format("delta").table(sourceTableCompleteName)

# COMMAND ----------

# Read the existing target delta table in the databricks database
deltaTable = DeltaTable.forName(spark, targetTableCompleteName)

# COMMAND ----------

# equality_clause = "target.AreaNumber = mergeKey1 and target.LocationNumber = mergeKey2"
# condition_clause = "target.AirportAreaNumber <> staged_updates.OAG_CODE"
# set_clause = {
#             "AreaNumber": "staged_updates.LEGACY_REVENUE_AREA_NUMBER",
#             "LocationNumber": "staged_updates.LGCY_REV_LOCATION_NUMBER",
#             "AirportAreaNumber": "staged_updates.OAG_CODE",  # Set current to true along with the new address and its effective date.
#             "EffectiveFrom": "staged_updates.CREATE_DATE",
#             "EffectiveTo": "staged_updates.LAST_UPDATE_DATE",
#             "IsActive": 1
#             }

# COMMAND ----------


# Function to upsert microBatchOutputDF(updates) into Target Delta table using merge
def upsertToDeltaSCD2(microBatchOutputDF,params, batchId):
    primaryKeyColumns = params.get('primaryKeyColumns', '').split(',')
    generateJoinCondition
    
    # Rows to Insert (updates of existing primary keys)    
    newLocationsToInsert = microBatchOutputDF.alias("updates").join(deltaTable.toDF().alias("target"), ).where(col('updates.OAG_CODE') != col('target.AirportAreaNumber')).where(col('target.IsActive') == 'Yes')
#     (col('updates.LEGACY_REVENUE_AREA_NUMBER') == col('target.AreaNumber')) & (col('updates.LGCY_REV_LOCATION_NUMBER') == col('target.LocationNumber'))
    
    # Stage the update by unioning two sets of rows
    # 1. Rows that will be inserted in the whenNotMatched clause
    # 2. Rows that will either update the current Col Values of existing Locations or insert the new Col Values of new Locations
    stagedUpdates = (
        newLocationsToInsert
        .selectExpr("NULL as mergeKey1","NULL as mergeKey2" "updates.*")   # Rows for 1
        .union(microBatchOutputDF.selectExpr("LEGACY_REVENUE_AREA_NUMBER as mergeKey1","LGCY_REV_LOCATION_NUMBER as mergeKey2", "*"))  # Rows for 2.
    
    # Apply SCD Type 2 operation using merge
    deltaTable.alias("sink").merge(
        stagedUpdates.alias("updates"), equality_clause) \
        .whenMatchedUpdate(
        condition = condition_clause,
        set = {  # Set current row indicator (IsActive) to false and endDate (EffectiveTo) to source's effective date.
            "IsActive": 0
            "EffectiveTo": "staged_updates.CREATE_DATE"
            }
        ).whenNotMatchedInsert(
        values = set_clause
        ).execute()


# COMMAND ----------



streamingSourceDf.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDeltaSCD2) \
  .outputMode("update") \
.option(checkpointLocation,'')
  .start()

# COMMAND ----------



streamingSourceDf.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDelta) \
  .outputMode("update") \
  .start()

# COMMAND ----------

try:
    if (DeltaTable.isDeltaTable(spark, source_path)):
        source_df = spark.read.format("delta").option("inferSchema","true").load(source_path)
        display(source_df)
    
    if (DeltaTable.isDeltaTable(spark, target_path)):
        target_df = spark.read.format("delta").option("inferSchema","true").load(target_path)
        display(target_df)
except Exception as e:
    print(e)
    

# COMMAND ----------

# Rows to INSERT new addresses of existing customers
newAddressesToInsert = source_df \
  .alias("updates") \
  .join(target_df.alias("target"), (updates.LEGACY_REVENUE_AREA_NUMBER == target.AreaNumber) & (updates.LGCY_REV_LOCATION_NUMBER == target.LocationNumber)) \
  .where("source.Customer_loc <> target.Customer_loc") \
  .where("customers.Customer_expiration_date is null")

# COMMAND ----------

# # DBTITLE 1,table schema will be used for delta table
# df_data = [
#              (123,'UK,LA','2019-09-09','null'),
# 		 ]
# df=spark.createDataFrame(df_data,['Customer_id', 'Customer_loc', 'Customer_effective_Date', 'Customer_expiration_Date'])

# display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# # DBTITLE 1,update dataframe
# df_update_data = [
#              (123,'US,Las Vegas','2020-09-29','null'),(124,'US,New York City','2020-09-17','null')
# 		 ]
# df_update=spark.createDataFrame(df_update_data,['Customer_id', 'Customer_loc', 'Customer_effective_Date', 'Customer_expiration_Date'])

# display(df_update)

# COMMAND ----------

# DBTITLE 1,writing df in cmd1 as delta table
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("dbfs:/mnt/Customer_data_new")
display(spark.sql("DROP TABLE IF EXISTS Customer_data_py"))
#display(spark.sql("TRUNCATE TABLE Customer_data_final"))
display(spark.sql("CREATE TABLE Customer_data_py USING DELTA LOCATION 'dbfs:/mnt/Customer_data_new'"))

# COMMAND ----------

# DBTITLE 1,Merge typ2 operation delta table with update df
customersTable = DeltaTable.forPath(spark,"dbfs:/mnt/Customer_data_new/")  # DeltaTable with schema (customerId, address, current, effectiveDate, endDate)

updatesDF =df_update       # DataFrame with schema (customerId, address, effectiveDate)

# COMMAND ----------

# Rows to INSERT new addresses of existing customers
newAddressesToInsert = updatesDF \
  .alias("updates") \
  .join(customersTable.toDF().alias("customers"), "Customer_id") \
  .where("updates.Customer_loc <> customers.Customer_loc") \
  .where("customers.Customer_expiration_date is null")

# COMMAND ----------

# Stage the update by unioning two sets of rows
# 1. Rows that will be inserted in the whenNotMatched clause
# 2. Rows that will either update the current addresses of existing customers or insert the new addresses of new customers
stagedUpdates = (
  newAddressesToInsert
  .selectExpr("NULL as mergeKey", "updates.*")   # Rows for 1
  .union(updatesDF.selectExpr("Customer_id as mergeKey", "*"))  # Rows for 2.

# COMMAND ----------

# Apply SCD Type 2 operation using merge
customersTable.alias("customers").merge(
  stagedUpdates.alias("staged_updates"),
  "customers.Customer_id = mergeKey") \
.whenMatchedUpdate(
  condition = "customers.Customer_loc <> staged_updates.Customer_loc",
  set = {                                      # Set current to false and endDate to source's effective date.
    "Customer_expiration_Date": "staged_updates.Customer_effective_Date"
  }
).whenNotMatchedInsert(
  values = {
    "Customer_id": "staged_updates.Customer_id",
    "Customer_loc": "staged_updates.Customer_loc",
    "Customer_effective_Date": "staged_updates.Customer_effective_Date",  # Set current to true along with the new address and its effective date.
    "Customer_expiration_Date": "null"
  }
).execute()

# COMMAND ----------

# MAGIC %sql select * from  Customer_data_py
