# Databricks notebook source
#write_to_staging is the for each batch function
#can have as many parameters, im just input_table/tableName
#write_all is top level function which gets called in parallel in threadpool in command4
#currently write_to_staging is just being used to create global temp, but can add merge logic/dbutils.notebook.run of your merge logic in here. among other writes

# COMMAND ----------

def write_to_staging(df, batchId, tableName):
  df.createOrReplaceGlobalTempView(tableName.replace('.','_'))

def write_all(startingversion, input_table,checkpoint_location, table_type, primary_keys):
  
  if (int(startingversion)-1)==int(spark.sql('describe history '+input_table).select('version').first()[0]):
    return print('No New data available for '+input_table)
  
  try:
    df2=spark.readStream.format("delta")\
                              .option("readChangeFeed", "true")\
                              .option("startingVersion", startingversion) \
                              .table(input_table) 
    df2.writeStream\
                              .format("delta")\
                              .foreachBatch((lambda df, batchId: write_to_staging(df, batchId, input_table)))\
                              .option("checkpointLocation", checkpoint_location)\
                              .trigger(once=True)\
                              .start()\
                              .awaitTermination()
    
    if table_type=='L0':
        key_columns=[x for x in ast.literal_eval(spark.table('global_temp.'+input_table.replace('.','_')).select('_pk_').first()[0]).keys()]
        w=Window().partitionBy(*key_columns)
        spark.table('global_temp.'+input_table.replace('.','_')).filter((F.col('_change_type')=='update_postimage')|(F.col('_change_type')=='insert'))\
          .withColumn('max', F.max('_commit_version').over(w))\
          .filter(F.col('max')==F.col('_commit_version'))\
          .createOrReplaceGlobalTempView(input_table.replace('.','_'))
    elif table_type=='CORE' or table_type=='SEMANTIC' or table_type=='WORK':
        key_columns=[x.strip() for x in primary_keys.split(',')]
        w=Window().partitionBy(*key_columns)
        spark.table('global_temp.'+input_table.replace('.','_')).filter((F.col('_change_type')=='update_postimage')|(F.col('_change_type')=='insert')|(F.col('_change_type')=='delete'))\
          .withColumn('max', F.max('_commit_version').over(w))\
          .filter(F.col('max')==F.col('_commit_version'))\
          .createOrReplaceGlobalTempView(input_table.replace('.','_'))

# COMMAND ----------

def get_parameters_incremental(table_type,config_df):
  config_array=streaming_config.collect()
  return [(x.starting_version,x.table_name,x.checkpoint_location, x.table_type,x.primary_keys) for x in config_array if x.table_type in table_type.split(',')] 

# COMMAND ----------

import ast
from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from multiprocessing.pool import ThreadPool


spark.conf.set( "spark.databricks.delta.formatCheck.enabled","False")
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle","True")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "True")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "True")
tpool1 = ThreadPool(10)
points1=tpool1.starmap(write_all, get_parameters_incremental('L0,L0_OTIF',streaming_config))
points1
