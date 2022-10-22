# Databricks notebook source
# MAGIC %md
# MAGIC This notebook provides an overview of handling SCD Type 2 for records where there isn't a timestamp available.  The process involves creating a hash column of the concatenated columns and then comparing history against source.

# COMMAND ----------

#import libraries
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2, concat_ws
from datetime import datetime as dt

#Set Up Paths for Source and History
adlsgen2storageaccountname = 'neufwksipsa'
sourceContainerName = 'landing'
sourceTableName = 'customerAccountBalance'
sourceFullPathPrefix = "abfss://" + sourceContainerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 
sourceEnrichedPath = "/source/" + sourceTableName
sourceFullEnrichedPath = sourceFullPathPrefix + sourceEnrichedPath

historyContainerName = 'persistence'
historyTableName = 'customerAccountBalance'
historyFullPathPrefix = "abfss://" + historyContainerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 
historyEnrichedPath = "/source/" + historyTableName
historyFullEnrichedPath = historyFullPathPrefix + historyEnrichedPath

#Set variables for dates
maxDate = dt.strptime('9999-12-31', '%Y-%m-%d').date()
currentDate = dt.today().date()
effectiveDate = dt.strptime('2020-11-1', '%Y-%m-%d').date()

# COMMAND ----------

#Generate History DataFrame

dataHistory = [
    Row(customerID = '1', customerName = 'Joe', customerSurname = 'Kastner', city = 'Brick', state = 'New Jersey', balance = '100000', effectiveDate = effectiveDate, endDate = maxDate, currentRowInd = 1),
    Row(customerID = '1', customerName = 'Joe', customerSurname = 'Kastner', city = 'Brick', state = 'New Jersey', balance = '90000', effectiveDate = dt.strptime('2020-10-1', '%Y-%m-%d').date(), endDate = effectiveDate, currentRowInd = 0),
    Row(customerID = '2', customerName = 'Michael', customerSurname = 'Strahan', city = 'New York', state = 'New York', balance = '50000', effectiveDate = effectiveDate, endDate = maxDate, currentRowInd = 1),
    Row(customerID = '3', customerName = 'Barbara', customerSurname = 'Corcoran', city = 'Edgewater', state = 'New Jersey', balance = '2500000', effectiveDate = effectiveDate, endDate = maxDate, currentRowInd = 1),
    Row(customerID = '4', customerName = 'Michael', customerSurname = 'Scott', city = 'Scranton', state = 'Pennsylvania', balance = '15000', effectiveDate = effectiveDate, endDate = maxDate, currentRowInd = 1),
    Row(customerID = '5', customerName = 'Elon', customerSurname = 'Musk', city = 'Los Angeles', state = 'California', balance = '120000000', effectiveDate = effectiveDate, endDate = maxDate, currentRowInd = 1),
    Row(customerID = '6', customerName = 'Wes', customerSurname = 'McKinney', city = 'New York', state = 'New York', balance = '40000', effectiveDate = effectiveDate, endDate = maxDate, currentRowInd = 1)
]

dfHistory = spark.createDataFrame(dataHistory)
dfHistory.write.format('Delta').mode('overwrite').save(historyFullEnrichedPath)

#Generate Source DataFrame

dataSource = [
    Row(customerID = '1', customerName = 'Joe', customerSurname = 'Kastner', city = 'Brick', state = 'New Jersey', balance = '150000'),
    Row(customerID = '2', customerName = 'Michael', customerSurname = 'Strahan', city = 'New York', state = 'New York', balance = '50000'),
    Row(customerID = '3', customerName = 'Barbara', customerSurname = 'Corcoran', city = 'Edgewater', state = 'New Jersey', balance = '2500000'),
    Row(customerID = '4', customerName = 'Michael', customerSurname = 'Scott', city = 'Scranton', state = 'Pennsylvania', balance = '15000'),
    Row(customerID = '5', customerName = 'Elon', customerSurname = 'Musk', city = 'Los Angeles', state = 'California', balance = '1000000'),
    Row(customerID = '6', customerName = 'Wes', customerSurname = 'McKinney', city = 'New York', state = 'New York', balance = '40000'),
    Row(customerID = '7', customerName = 'Kyle', customerSurname = 'Polich', city = 'New York', state = 'New York', balance = '120000')                
]

dfSource = spark.createDataFrame(dataSource)
dfSource.write.format('Delta').mode('overwrite').save(sourceFullEnrichedPath)

# COMMAND ----------

dfSource.show()
dfHistory.show()

# COMMAND ----------

#Read tables in from datalake
dfHistory = spark.read.parquet(historyFullEnrichedPath)
dfSource = spark.read.parquet(sourceFullEnrichedPath)

#Create Hash Column for History DataFrame
dfHistory = (dfHistory
    .withColumn("hashValue", sha2(concat_ws('||', *['balance', 'city', 'customerName', 'customerSurname', 'state']), 256))
    .orderBy('customerID')
)

#Create Alias columns to distinguish ambiguous columns after joining dataframes
dfHistory = dfHistory.select([col(c).alias(c+"_hist") for c in dfHistory.columns])

#Create Hash Column and set endDate to be MaxDate for Source DataFrame
dfSource = (dfSource
    .withColumn("hashValue", sha2(concat_ws('||', *['balance', 'city', 'customerName', 'customerSurname', 'state']), 256))
    .withColumn("endDate", lit(maxDate))
    .orderBy('customerID')
    )

#Create Alias columns to distinguish ambiguous columns after joining dataframes
dfSource = dfSource.select([col(c).alias(c+"_src") for c in dfSource.columns])

#Show Dataframes for comparison
dfHistory.show()
dfSource.show()

# COMMAND ----------

x = dfHistory.select('hashValue_hist').where('customerSurname_hist == "Kastner" and currentRowInd_hist == 1').take(1)
y = dfSource.select('hashValue_src').where('customerSurname_src == "Kastner"').take(1)

x==y

# COMMAND ----------

# MAGIC %md SCD 2 Operations
# MAGIC - History File:
# MAGIC   - All Columns from Source File plus effective date, end date, currentRowInd
# MAGIC - Source File:
# MAGIC   - All Columns
# MAGIC 
# MAGIC - Logic:
# MAGIC   - Do Nothing: Source row exists in history and hash value is same
# MAGIC   - Insert New Row: 
# MAGIC     - New Record: Source row does not exist in history.  Set effectiveDate to currentDate and currentRowInd to 1.
# MAGIC     - Changed Record: Source row exists in history but hashValue is different.  Expire previous current row in history (set endDate to currentDate and currentRowInd to 0) and insert new row with effectiveDate = currentDate, endDate to 9999-12-31

# COMMAND ----------

#Create History DataFrame where currentRowInd == 0
dfMergeHistory = (
    dfHistory
    .where(dfHistory.currentRowInd_hist == 0)
  )  #Get history of non-current rows

dfMergeHistory = (
    dfMergeHistory.select(*[col(column).alias(column.replace("_hist", "")) for column in dfMergeHistory.columns if column.endswith("_hist")])
)

dfHistoryCurrentRows = dfHistory.where(dfHistory.currentRowInd_hist == 1)  #Get history of current rows

#Create DataFrame for records that have to be expired where they are in both source and history and the hash value doesn't exist
dfMergeExpire = (
    dfHistoryCurrentRows
    .join(dfSource, (dfHistoryCurrentRows.customerID_hist == dfSource.customerID_src) & (dfSource.hashValue_src != dfHistoryCurrentRows.hashValue_hist), how='inner')
    .withColumn('currentRowInd_hist', lit(0))
    .withColumn('endDate_hist', lit(currentDate))
)
dfMergeExpire = (
    dfMergeExpire.select(*[col(column).alias(column.replace("_hist", "")) for column in dfMergeExpire.columns if column.endswith("_hist")])
)


#Create DataFrame for records that have to be upserted where they are either new or changed records
dfMergeUpsert = (
    dfSource
    .join(dfHistoryCurrentRows, (dfHistoryCurrentRows.customerID_hist == dfSource.customerID_src) & (dfSource.hashValue_src == dfHistoryCurrentRows.hashValue_hist), how='left_anti')
    .withColumn('currentRowInd_src', lit(1))
    .withColumn('effectiveDate_src', lit(currentDate))
    .withColumn('endDate_src', lit(maxDate))
)

dfMergeUpsert = (
    dfMergeUpsert.select(*[col(column).alias(column.replace("_src", "")) for column in dfMergeUpsert.columns if column.endswith("_src")])
)

#Create DataFrame for records that are unchanged where the hash value and ID exists in both the history and source file
dfMergeUnchanged = (
    dfHistoryCurrentRows
    .join(dfSource, (dfHistoryCurrentRows.customerID_hist == dfSource.customerID_src) & (dfSource.hashValue_src == dfHistoryCurrentRows.hashValue_hist), how='inner')
    .withColumn('currentRowInd_hist', lit(1))
    .withColumn('effectiveDate_hist', lit(currentDate))
    .withColumn('endDate_hist', lit(maxDate))
)

dfMergeUnchanged = (
    dfMergeUnchanged
    .select(*[col(column).alias(column.replace("_hist", "")) for column in dfMergeUnchanged.columns if column.endswith("_hist")])
)

#Union the dataframes together to create new table in persistence layer
dfMergeComplete = (
    dfMergeHistory
    .unionByName(dfMergeExpire)
    .unionByName(dfMergeUpsert)
    .unionByName(dfMergeUnchanged)
    .drop(col("hashValue"))
    .orderBy(col("customerID"), col("currentRowInd"))
)

dfMergeComplete.show()

# COMMAND ----------

dfHistory.show()
dfSource.show()
dfMergeHistory.show()
dfMergeExpire.show()
dfMergeUpsert.show()
dfMergeUnchanged.show()
dfMergeComplete.show()

# COMMAND ----------

dfMergeComplete.createOrReplaceTempView("Merge_Table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM Merge_Table
# MAGIC WHERE customerSurname = 'Kastner'
