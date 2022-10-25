# Databricks notebook source
# MAGIC %run /Framework/utility/neudesic-framework-functions

# COMMAND ----------

import json
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

dbutils.widgets.text(name='gold', defaultValue="/mnt/gold", label='Gold Path')
dbutils.widgets.text(name='gold_hydration_metadata', defaultValue="/mnt/dpa-hertz/metadata/Hertz Silver-Gold.xlsx", label='Gold Hydration Path')
dbutils.widgets.text(name='parentNotebookExecutionLogKey', defaultValue="-1",label='parentNotebookExecutionLogKey')
dbutils.widgets.text(name='notebookTableExecutionName', defaultValue="",label='notebookTableExecutionName')
dbutils.widgets.text(name='notebookExecutionGroupName', defaultValue="",label='notebookExecutionGroupName')

# COMMAND ----------

gold_path = dbutils.widgets.get("gold")
gold_hydration_path = dbutils.widgets.get("gold_hydration_metadata")
print(gold_hydration_path)
print(gold_path)
gold_hydration_sheet_name = "Columns" +"!A1"
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

hydrationTable = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("treatEmptyValuesAsNulls", "false") \
    .option("dataAddress",gold_hydration_sheet_name) \
    .load(gold_hydration_path)

# COMMAND ----------

test= hydrationTable.where("Table = 'Dim_Location'")

# COMMAND ----------

def hydrateGold(hydrationTable):
    # Get the data in the hydration table and store it as a dataframe hydration
    # Create a list of the tables to create
    tables = list(hydrationTable.select(col("Table")).dropDuplicates().toPandas()["Table"])
    # create the dataframe for each table and save them to goldBucket
    for table in tables:
        print(table)
        #Create a list of the columns to add to the dataframe
        table_cols = list(hydrationTable.filter(hydrationTable.Table == table).select(col("Column")).dropDuplicates().toPandas()["Column"])
        mySchema = StructType([StructField(c, StringType()) for c in table_cols])
        df = spark.createDataFrame(data=[], schema=mySchema)
        # Write the dataframe to delta
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}/{table}")
    print("All tables have been written")
    return  

# COMMAND ----------

hydrateGold(hydrationTable)

# COMMAND ----------

def upsertGold(row, table, keyColumns, batchDate):
	# Get the data from the hydration table and store it as dataframe hydration
	
	# Filter hydration to only the table being inserted to
	filteredHydration = hydration.filter(hydration.Table == table)

	# Get key if the table is an SCD
	surrogateColumn = filteredHyration.filter(hydration.SCD2Function == "K")
	
	# Create the new dataFrame to append to the database (called upsert)
	# loop through columns, adding appropriate data

	# if table is SCD2
	# get maximum date value
	maxDate = datetime.max
	# retrieve other instances of that primary key already exist (SLOW, but no getting around it)
	
	# if number of existing entries is greater than zero
		# filter to the one that has EffectiveTo = maxDate
		# add that row to upsert dataframe with the new batchDate as EffectiveTo and IsActive as 0
		# run delta upsert on the dataframe, matching on keyColumns and surrogateColumn
	
	# else run delta upsert on the dataframe, matching on keyColumns

# COMMAND ----------

# DBTITLE 1,End Logging
# log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName=notebookExecutionGroupName,scopeType=secretScopeType)
# dbutils.notebook.exit("Succeeded")
