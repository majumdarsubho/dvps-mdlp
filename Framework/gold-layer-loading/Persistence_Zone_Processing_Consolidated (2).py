# Databricks notebook source
# MAGIC %md
# MAGIC ## Persistence Zone Processing Consolidated  
# MAGIC  
# MAGIC Description: Performs SCD type 2 logic and moves data from landing to persistence layers. Version 2, referencing functions notebook  
# MAGIC Reference Documentation: https://docs.delta.io/0.4.0/delta-update.html#id7  
# MAGIC 
# MAGIC Prerequisites:  Delta table exists at persistenceDataPath and input params are populated  
# MAGIC 
# MAGIC Input Variables:  
# MAGIC - primaryKeyColumns
# MAGIC - landingDataPath
# MAGIC - persistenceDataPath
# MAGIC - tableName
# MAGIC - fileName  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Notebook to Load Shared Functions

# COMMAND ----------

# MAGIC %run Persistence_Notebooks/Notebooks_Consolidated/ETL_Functions_Master

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries/Packages

# COMMAND ----------

import re
import datetime
# from notebookutils import mssparkutils

from pyspark.sql import functions as f, Window
from pyspark.sql.types import *

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Parameter Values

# COMMAND ----------

Params = "{'containername':'Informent','delimiter':'|','fileExtension':'parquet','hasHeader':'1','integrationDataPath':'informent/CDC/pv_v_loan_consumer_info','integrationZoneNotebookPath':'','integrationZoneSchemaName':'stg_ie','integrationZoneTableName':'PV_V_Loan_Consumer_Info','landingDataPath':'informent/CDC/pv_v_loan_consumer_info','landingZoneNotebookPath':'Raw_Zone_Processing','numPartitions':'80','persistenceDataPath':'informent/CDC/pv_v_loan_consumer_info','persistenceZoneNotebookPath':'Persistence_Zone_Processing_FDMSPARK','primaryKeyColumns':'bank_number,appl_id,account_number','semanticDataPath':'informent/CDC/pv_v_loan_consumer_info','semanticZoneNotebookPath':'','timestampColumns':'','vacuumRetentionHours':'0'}"
RunDate = "2022-08-02T16:59:53.6618741Z"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Params and Execute Merge

# COMMAND ----------

# effective_date = get_date_time(RunDate)
params = eval(Params)
# print('Effective Date:', effective_date)
# print()
for k, v in params.items():
    print(f'{k}:', v)

# COMMAND ----------

delta_merge(effective_date, params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage Data in Integration Container

# COMMAND ----------

stage_integration_data(params, 'Current_Row_Ind = 1')
