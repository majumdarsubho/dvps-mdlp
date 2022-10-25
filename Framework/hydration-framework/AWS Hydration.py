# Databricks notebook source
# MAGIC %run ../utility/neudesic-framework-functions

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

# pip install openpyxl

# COMMAND ----------

# pip install sqlalchemy

# COMMAND ----------

import pyodbc
from datetime import datetime as dt
from datetime import timezone
# import pyspark
import pandas as pd
import json
import openpyxl
import sqlalchemy as sa
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

dbutils.widgets.text(name='sheetNames', defaultValue="CopyActivitySetup,NotebookSetup", label='Sheet Names')
dbutils.widgets.text(name='aws_bucket_name', defaultValue="dpa-hertz", label='AWS Bucket')
dbutils.fs.ls("/FileStore/")

# COMMAND ----------

ACCESS_KEY = dbutils.secrets.get(scope = 'dpaframework', key = 'ACCESS_KEY')
SECRET_KEY = dbutils.secrets.get(scope = 'dpaframework', key = 'SECRET_KEY')

# COMMAND ----------

import urllib

#encode the secret key
#ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY,"")
aws_bucket_name = dbutils.widgets.get('aws_bucket_name')
mount_name = "dpa"

display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

scopeType = 'Admin'
ServerName = 'framework-db.cof6rbxdsl87.us-east-1.rds.amazonaws.com'
userName = 'admin'
password = 'Hertz1234!'
dbname = 'metadata'
engine = sa.create_engine(f"""mssql+pyodbc://{userName}:{password}@{ServerName}:1433/{dbname}?driver=ODBC+Driver+17+for+SQL+Server""")
sheet_names = dbutils.widgets.get('sheetNames').split(',')
# db_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+ServerName+';DATABASE=databricks-ingestion;UID='+userName+';PWD='+ password)

# COMMAND ----------

#metadata framework excel s3 path
framework_excel_s3_path = "/mnt/dpa/Hydration/FrameworkHydration.xlsx"

#flags required for reading the excel
isHeaderOn = "true"
isInferSchemaOn = "false"

# COMMAND ----------

for sheet in sheet_names:
    copyactivity_sheet_name = f"{sheet}!A1"

    framework_copyactivity_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", isHeaderOn) \
    .option("inferSchema", isInferSchemaOn) \
    .option("treatEmptyValuesAsNulls", "false") \
    .option("dataAddress",copyactivity_sheet_name) \
    .load(framework_excel_s3_path)
    
    display(framework_copyactivity_df)
    
    framework_copyactivity_df=framework_copyactivity_df.toPandas()
    framework_copyactivity_df.to_sql(sheet, engine,if_exists='replace',index=False)

# COMMAND ----------

conn = build_framework_sql_odbc_connection(frame_conn,scopeType)
conn.autocommit = True
coursor = conn.cursor()
coursor.execute('EXEC dbo.uspGenerateHydration')
# conn.close()

# COMMAND ----------

# execsp = 'EXEC dbo.uspGenerateHydration'
# execute_framework_stored_procedure_no_results(execsp, scopeType)
# # conn.close()

# COMMAND ----------

# conn.execute(execsp)

# COMMAND ----------

# uspGenerateHydration()
