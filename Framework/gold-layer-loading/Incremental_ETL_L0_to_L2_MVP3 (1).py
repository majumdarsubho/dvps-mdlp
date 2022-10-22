# Databricks notebook source
# MAGIC %md
# MAGIC ### Config file paths:

# COMMAND ----------

dbutils.widgets.text("streaming_config","","streaming_config")
dbutils.widgets.text("merge_config","","merge_config")
dbutils.widgets.text("synapse_config","","synapse_config")


import ast
from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from multiprocessing.pool import ThreadPool


spark.conf.set( "spark.databricks.delta.formatCheck.enabled","False")
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle","True")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "True")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "True")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")
spark.conf.set("spark.databricks.sqldw.writeSemantics", "COPY")
spark.conf.set("spark.sql.storeAssignmentPolicy","LEGACY" )


synapse_config=spark.read.csv('abfss://landing@'+ adlsName +'.dfs.core.windows.net/'+'config/l0_incremental/MVP3/Synapse_config',header=True)
#dbutils.widgets.get('synapse_config'), header=True)
databricks_config=spark.read.csv('abfss://landing@'+ adlsName +'.dfs.core.windows.net/'+'config/l0_incremental/MVP3/Databricks_Merge_config',header=True)
#dbutils.widgets.get('merge_config'), header=True)
streaming_config=spark.table('sys.streaming_config_MVP3').withColumn('checkpoint_location',F.when(F.col('table_type')=='L0_OTIF',F.regexp_replace('checkpoint_location','\*',adlsName)).otherwise(F.col('checkpoint_location')))
#dbutils.widgets.get('streaming_config'), header=True)




import ast
def get_parameters_merge_core(table_type,config_df):
  config_array=config_df.collect()
  return [(x.merge_condition,x.merge_set,x.sql_query,x.target_table,x.primary_keys,x.incremental_tables,x.deletes,x.non_incremental_tables) for x in config_array if x.merge_type==table_type] 

def get_parameters_merge_work(table_type,config_df):
  config_array=config_df.collect()
  return [(x.merge_condition,x.merge_set,x.sql_query,x.target_table,x.primary_keys,x.incremental_tables,x.deletes,x.non_incremental_tables) for x in config_array if x.merge_type==table_type] 

def get_parameters_incremental(table_type,config_df):
  config_array=streaming_config.collect()
  return [(x.starting_version,x.table_name,x.checkpoint_location, x.table_type,x.primary_keys) for x in config_array if x.table_type in table_type.split(',')] 


def get_parameters_merge_semantic(table_type,config_df):
  config_array=config_df.collect()
  return ((x.merge_condition,x.merge_set,x.sql_query,x.target_table,x.primary_keys,x.incremental_tables,x.deletes,x.non_incremental_tables) for x in config_array if x.merge_type==table_type) 

def get_parameters_synapse(table_type,config_df):
  config_array=config_df.collect()
  return [(x.incremental_view,x.dwhStagingTable,x.dwhTargetTable,x.lookupColumns,x.deltaName,x.dwhStagingDistributionColumn,x.insert_overwrite) for x in config_array if x.merge_type==table_type] 

def get_parameters_non_incremental(df):
    x1=df.withColumn('non_incremental_tables', F.explode(F.split('non_incremental_tables',','))).filter('non_incremental_tables!="NO"').select('non_incremental_tables').distinct().collect()
    return list(set([x.non_incremental_tables for x in x1]))

# COMMAND ----------

display(streaming_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet Table Check for non incremental tables :
# MAGIC #### -> Notebook will exit upon interruption of data due to Main Data Pipeline Insert Overwrite

# COMMAND ----------

def check_table(table):
  try:
      df=spark.table(table).persist()
      df.createOrReplaceGlobalTempView(table.replace('.','_'))
      return print(str(spark.sql('select * from global_temp.'+table.replace('.','_')).count())+'  '+table)
  except Exception as e:
      dbutils.notebook.exit('exiting notebook because dependant table '+table+' not available. Will Try again next run. ERROR:'+str(e))
      
      
tpool = ThreadPool(15)


points1=tpool.map(check_table, get_parameters_non_incremental(databricks_config))

points1

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Functions :

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
        
   
    
    
    return print('global_temp.'+input_table.replace('.','_')+" micro batch table overwritten")
  except Exception as e:
        if 'Table or view not found' in str(e):
          return print('No New data available for '+input_table)
        return print('stream failed for '+input_table+(str(e)))
  
  
def get_SQL(SQL,incremental_tables,non_incremental_tables):
    import re
    if incremental_tables!='NO' and non_incremental_tables=='NO':
        incremental_tables=[x.strip() for x in incremental_tables.split(',')]
        d1={incremental_tables[i]: 'global_temp.'+incremental_tables[i].replace('.','_') for i in range(0, len(incremental_tables))}
        d2={incremental_tables[i].split('.')[1]+'.': incremental_tables[i].replace('.','_')+'.' for i in range(0, len(incremental_tables))}
        my_dict={**d1, **d2}
    elif incremental_tables=='NO' and non_incremental_tables!='NO':
        non_incremental_tables=[x.strip() for x in non_incremental_tables.split(',')]
        d3={non_incremental_tables[i]: 'global_temp.'+non_incremental_tables[i].replace('.','_') for i in range(0, len(non_incremental_tables))}
        d4={non_incremental_tables[i].split('.')[1]+'.': non_incremental_tables[i].replace('.','_')+'.' for i in range(0, len(non_incremental_tables))}
        my_dict={**d3, **d4}
    elif incremental_tables=='NO' and non_incremental_tables=='NO':
        return SQL 
    else:
        incremental_tables=[x.strip() for x in incremental_tables.split(',')]
        non_incremental_tables=[x.strip() for x in non_incremental_tables.split(',')]
        d1={incremental_tables[i]: 'global_temp.'+incremental_tables[i].replace('.','_') for i in range(0, len(incremental_tables))}
        d2={incremental_tables[i].split('.')[1]+'.': incremental_tables[i].replace('.','_')+'.' for i in range(0, len(incremental_tables))}
        d3={non_incremental_tables[i]: 'global_temp.'+non_incremental_tables[i].replace('.','_') for i in range(0, len(non_incremental_tables))}
        d4={non_incremental_tables[i].split('.')[1]+'.': non_incremental_tables[i].replace('.','_')+'.' for i in range(0, len(non_incremental_tables))}
        my_dict={**d1,**d2,**d3, **d4}

    rep = dict((re.escape(k), v) for k, v in my_dict.items())
    pattern = re.compile("|".join(rep.keys()))

    return pattern.sub(lambda m: rep[re.escape(m.group(0))], SQL)
  



def append_to_log(target_table,target_table_type, sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,log):
  
     
  list1=[[target_table,target_table_type, sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,log]]
  df=spark.createDataFrame(list1,['target_table','target_table_type','sqlQuery', 'condition', 'primary_keys', 'deletes', 'incremental_tables', 'non_incremental_tables','log'])
  df.withColumn('timestamp',F.from_utc_timestamp(F.current_timestamp(), 'America/New_York')).write.format("delta").mode("append").saveAsTable('sys.incremental_log')
  
  return print('Log successfully written to table for:' +target_table)
  
  
  
  
def merge_core(condition,mergeSet,sqlQuery,target_table,primary_keys,incremental_tables,deletes, non_incremental_tables):
  #for x in [x.strip() for x in incremental_tables.split(',')]:
     #spark.table('global_temp.'+x).filter('`_change_type`=="insert" OR `_change_type`=="update_postimage"').createOrReplaceTempView(x)
  #get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)   
  #get_SQL(dbutils.notebook.run(deletes,60),incremental_tables, non_incremental_tables)
  
  
  
  if dbutils.notebook.run(sqlQuery,60,arguments={"insert_overwrite": "NO"})=="":
    return print('No New data available for '+target_table+' '+condition[-5:])
  
 
  gold_delta = DeltaTable.forName(spark, target_table)
  if mergeSet!='ALL':  
     try:
        gold_delta.alias("gold") \
        .merge(spark.sql(get_SQL(dbutils.notebook.run(sqlQuery,60,arguments={"insert_overwrite": "NO"}),incremental_tables, non_incremental_tables)).alias("silver"), condition) \
        .whenMatchedUpdate(set = ast.literal_eval(mergeSet)) \
        .whenNotMatchedInsert(values = ast.literal_eval(mergeSet)) \
        .execute()
        
        if deletes!='NO':    
            primarykeys=[x.strip() for x in primary_keys.split(',')]
            if dbutils.notebook.run(deletes,60)!="":
                spark.sql(get_SQL(dbutils.notebook.run(deletes,60),incremental_tables, non_incremental_tables)).createOrReplaceTempView('deleteview')
                spark.sql('DELETE FROM '+target_table+' AS gold WHERE EXISTS (SELECT '+','.join(primarykeys)+' FROM deleteview WHERE '+' AND '.join(['gold.'+x+'='+x for x in primarykeys])+')')
        
        append_to_log(target_table,'CORE', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,'SUCCESS')
        return (print('success for merge condition: '+condition))
     except Exception as e:
        if 'Table or view not found' in str(e):
          append_to_log(target_table,'CORE', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,str(e))
          return print('No New data available for '+target_table+' '+condition[-5:])
        append_to_log(target_table,'CORE', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,str(e))
        return (print('error:'+(str(e))))
  else:
     try:
        gold_delta.alias("gold") \
        .merge(spark.sql(get_SQL(dbutils.notebook.run(sqlQuery,60,arguments={"insert_overwrite": "NO"}),incremental_tables, non_incremental_tables)).alias("silver"), condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        
        if deletes!='NO':  
            primarykeys=[x.strip() for x in primary_keys.split(',')]   
            if dbutils.notebook.run(deletes,60)!="":
                spark.sql(get_SQL(dbutils.notebook.run(deletes,60),incremental_tables, non_incremental_tables)).createOrReplaceTempView('deleteview')
                spark.sql('DELETE FROM '+target_table+' AS gold WHERE EXISTS (SELECT '+','.join(primarykeys)+' FROM deleteview WHERE '+' AND '.join(['gold.'+x+'='+x for x in primarykeys])+')')
        append_to_log(target_table,'CORE', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,'SUCCESS')
        return (print('success for merge condition: '+condition))
     except Exception as e:
        if 'Table or view not found' in str(e):
          append_to_log(target_table,'CORE', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,str(e))
          return print('No New data available for '+target_table+' '+condition[-5:])
        append_to_log(target_table,'CORE', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,str(e))
        return (print('error:'+(str(e))))
  
   
      
def upsertDWH(incremental_view,dwhStagingTable,dwhTargetTable,lookupColumns,deltaName,dwhStagingDistributionColumn,insert_overwrite):
    #STEP1: Derive dynamic delete statement to delete existing record from TARGET if the source record is newer
    lookupCols =[x.strip() for x in lookupColumns.split(",")]
    whereClause=""
    for col in lookupCols:
      whereClause= whereClause + dwhStagingTable  +"."+ col  + "="+ dwhTargetTable +"." + col + " and "
 
    if deltaName is not None and  len(deltaName) >0:
      #Check if the last updated is greater than existing record
      whereClause= whereClause + dwhStagingTable  +"."+ deltaName  + ">="+ dwhTargetTable +"." + deltaName
    else:
      #remove last "and"
      remove="and"
      reverse_remove=remove[::-1]
      whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]
 
    deleteSQL = "delete from " + dwhTargetTable + " where exists (select 1 from " + dwhStagingTable + " where " +whereClause +");"
 
    #STEP2: Delete existing records but outdated records from SOURCE
    whereClause=""
    for col in lookupCols:
      whereClause= whereClause + dwhTargetTable  +"."+ col  + "="+ dwhStagingTable +"." + col + " and "
 
    if deltaName is not None and  len(deltaName) >0:
      #Check if the last updated is lesser than existing record
      whereClause= whereClause + dwhTargetTable  +"."+ deltaName  + "> "+ dwhStagingTable +"." + deltaName
    else:
      #remove last "and"
      remove="and"
      reverse_remove=remove[::-1]
      whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]
 
    #deleteOutdatedSQL = "delete from " + dwhStagingTable + " where exists (select 1 from " + dwhTargetTable + " where " + whereClause + " );"
    #print("deleteOutdatedSQL={}".format(deleteOutdatedSQL))
 
    #STEP3: Insert SQL
    insertSQL ="Insert Into " + dwhTargetTable + " select * from " + dwhStagingTable +";"
    #print("insertSQL={}".format(insertSQL))
 
    #consolidate post actions SQL
    postActionsSQL = deleteSQL + insertSQL
    print("postActionsSQL={}".format(postActionsSQL))
 
    sqldwJDBC = "Your JDBC Connection String for Azure Synapse Analytics. Preferably from Key Vault"
    tempSQLDWFolder = "A temp folder in Azure Datalake Storage or Blob Storage for temp polybase files "
     
     
    #Use Hash Distribution on STG table where possible
    if dwhStagingDistributionColumn is not None and len(dwhStagingDistributionColumn) > 0:
      stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH (" +  dwhStagingDistributionColumn + ")"
    else:
      stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN"
        
    
      
      
    #insert_overwrite
    if insert_overwrite=='YES':
      insert_overwrite_SQL="truncate table " + dwhTargetTable +"; Insert into "+ dwhTargetTable +" select * from "+ dwhStagingTable+";"
      spark.table(incremental_view).write.format("com.databricks.spark.sqldw") \
                            .mode("overwrite") \
                            .option("url", jdbcUrl) \
                            .option("dbtable", dwhStagingTable) \
                            .option("tableOptions",stgTableOptions)\
                            .option("useAzureMSI", "true") \
                            .option("tempDir", tempDir) \
                            .option("maxStrLength", 4000)\
                            .option("postActions", insert_overwrite_SQL)\
                            .save()
      return print('Success for insert/overwrite: '+dwhTargetTable+'   '+insert_overwrite_SQL)
    
    
  
  
    try:    
        df=spark.table('global_temp.'+incremental_view.replace('.','_')).filter('`_change_type`=="insert" OR `_change_type`=="update_postimage"').select(spark.table(dwhTargetTable).columns)
        df1=spark.table('global_temp.'+incremental_view.replace('.','_')).filter('`_change_type`=="delete"').select(spark.table(dwhTargetTable).columns)
    except Exception as e:
        if 'Table or view not found' in str(e):
          return print('No New data available for '+dwhTargetTable)
    
    
      
    #Upsert/Merge to Target using STG postAction
    
    try:
        #updates and inserts into Synapse  
        df.write.format("com.databricks.spark.sqldw") \
                            .mode("overwrite") \
                            .option("url", jdbcUrl) \
                            .option("dbtable", dwhStagingTable) \
                            .option("tableOptions",stgTableOptions)\
                            .option("useAzureMSI", "true") \
                            .option("tempDir", tempDir) \
                            .option("maxStrLength", 4000)\
                            .option("postActions", postActionsSQL)\
                            .save()
        print('Success for update/insert: '+dwhTargetTable+'   '+postActionsSQL)
        
        if df1.count()!=0:
            df1.write.format("com.databricks.spark.sqldw") \
                                .mode("overwrite") \
                                .option("url", jdbcUrl) \
                                .option("dbtable", dwhStagingTable) \
                                .option("tableOptions",stgTableOptions)\
                                .option("useAzureMSI", "true") \
                                .option("tempDir", tempDir) \
                                .option("maxStrLength", 4000)\
                                .option("postActions", deleteSQL)\
                                .save()
            print('Success for delete : '+dwhTargetTable+'   '+deleteSQL)
    except Exception as e:
        print(str(e))
        
        
        
        
def merge_work(condition,mergeSet,sqlQuery,gold_table, primary_keys,incremental_tables,deletes,non_incremental_tables):
  #gold_delta = DeltaTable.forPath(spark, target_table_path)
  #get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)   
  #get_SQL(dbutils.notebook.run(deletes,60),incremental_tables, non_incremental_tables) 
  gold_delta = DeltaTable.forName(spark, gold_table)

  #try:
      #for x in [(x.strip()).replace('.','_') for x in incremental_tables.split(',') if x !='NO']:
         #spark.table('global_temp.'+x).filter('`_change_type`=="insert" OR `_change_type`=="update_postimage"').createOrReplaceGlobalTempView(x)
         
      
      
  if mergeSet!='ALL':  
     try:
        for x in [(x.strip()).replace('.','_') for x in incremental_tables.split(',') if x !='NO']:
          spark.table('global_temp.'+x).filter('`_change_type`=="insert" OR `_change_type`=="update_postimage"').createOrReplaceGlobalTempView(x)
        gold_delta.alias("gold") \
        .merge(spark.sql(get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)).alias('silver'), condition) \
        .whenMatchedUpdate(set = ast.literal_eval(mergeSet)) \
        .whenNotMatchedInsert(values = ast.literal_eval(mergeSet)) \
        .execute()
        
        if deletes=='YES':    
            for x in [(x.strip()).replace('.','_') for x in incremental_tables.split(',')]:
               spark.table('global_temp.'+x).filter('`_change_type`=="delete"').createOrReplaceGlobalTempView(x)
            primarykeys=[x.strip() for x in primary_keys.split(',')]   
            spark.sql(get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)).createOrReplaceTempView('deleteview')
            spark.sql('DELETE FROM '+gold_table+' AS gold WHERE EXISTS (SELECT '+','.join(primarykeys)+' FROM deleteview WHERE '+' AND '.join(['gold.'+x+'='+x for x in primarykeys])+')')
    

        return (print('success for merge condition: '+condition))
     except Exception as e:
        if 'Table or view not found' in str(e):
          return print('No New data available for '+gold_table)
        return (print('error:'+(str(e))))
  else:
     try:
        for x in [(x.strip()).replace('.','_') for x in incremental_tables.split(',') if x !='NO']:
          spark.table('global_temp.'+x).filter('`_change_type`=="insert" OR `_change_type`=="update_postimage"').createOrReplaceGlobalTempView(x)
        gold_delta.alias("gold") \
        .merge(spark.sql(get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)).alias("silver"), condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        
        if deletes=='YES':    
            for x in [(x.strip()).replace('.','_') for x in incremental_tables.split(',')]:
               spark.table('global_temp.'+x).filter('`_change_type`=="delete"').createOrReplaceGlobalTempView(x)
            primarykeys=[x.strip() for x in primary_keys.split(',')]   
            spark.sql(get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)).createOrReplaceTempView('deleteview')
            spark.sql('DELETE FROM '+gold_table+' AS gold WHERE EXISTS (SELECT '+','.join(primarykeys)+' FROM deleteview WHERE '+' AND '.join(['gold.'+x+'='+x for x in primarykeys])+')')
        
        return (print('success for merge condition: '+condition))
     except Exception as e:
        if 'Table or view not found' in str(e):
          return print('No New data available for '+gold_table)
        return (print('error:'+(str(e))))
      
def merge_semantic(condition,mergeSet,sqlQuery,gold_table, primary_keys,incremental_tables,deletes,non_incremental_tables):
  #gold_delta = DeltaTable.forPath(spark, target_table_path)
  #get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)   
  #get_SQL(dbutils.notebook.run(deletes,60),incremental_tables, non_incremental_tables) 
  gold_delta = DeltaTable.forName(spark, gold_table)

  #for x in [(x.strip()).replace('.','_') for x in incremental_tables.split(',') if x !='NO']:
     #spark.table('global_temp.'+x).filter('`_change_type`=="insert" OR `_change_type`=="update_postimage"').createOrReplaceGlobalTempView(x)
  if condition=='overwrite':
    spark.sql('INSERT OVERWRITE '+gold_table+' PARTITION(SRC_SYS_CD)'+get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables))
    append_to_log(gold_table,'SEMANTIC', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,'SUCCESS')
    return print('Success for Insert Overwrite :'+ gold_table)
              
      
      
  if mergeSet!='ALL':  
     try:
        for x in [(x.strip()).replace('.','_') for x in incremental_tables.split(',') if x !='NO']:
          spark.table('global_temp.'+x).filter('`_change_type`=="insert" OR `_change_type`=="update_postimage"').createOrReplaceGlobalTempView(x)
        gold_delta.alias("gold") \
        .merge(spark.sql(get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)).alias('silver'), condition) \
        .whenMatchedUpdate(set = ast.literal_eval(mergeSet)) \
        .whenNotMatchedInsert(values = ast.literal_eval(mergeSet)) \
        .execute()
        
        append_to_log(gold_table,'SEMANTIC', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,'SUCCESS')
        
        count=0
        if deletes=='YES':    
            for x in [(x.strip()).replace('.','_') for x in incremental_tables.split(',')]:
                spark.table('global_temp.'+x).filter('`_change_type`=="delete"').createOrReplaceGlobalTempView(x)
                count= count + spark.table('global_temp.'+x).count()
            if count!=0:
                primarykeys=[x.strip() for x in primary_keys.split(',')]   
                spark.sql(get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)).createOrReplaceTempView('deleteview')
                spark.sql('DELETE FROM '+gold_table+' AS gold WHERE EXISTS (SELECT '+','.join(primarykeys)+' FROM deleteview WHERE '+' AND '.join(['gold.'+x+'='+x for x in primarykeys])+')')
            
       

        return (print('success for merge condition: '+condition))
     except Exception as e:
        if 'Table or view not found' in str(e):
          append_to_log(gold_table,'SEMANTIC', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,str(e))
          return print('No New data available for '+gold_table)
        append_to_log(gold_table,'SEMANTIC', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,str(e))
        return (print('error:'+(str(e))))
  else:
     try:
        for x in [(x.strip()).replace('.','_') for x in incremental_tables.split(',') if x !='NO']:
          spark.table('global_temp.'+x).filter('`_change_type`=="insert" OR `_change_type`=="update_postimage"').createOrReplaceGlobalTempView(x)
        gold_delta.alias("gold") \
        .merge(spark.sql(get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)).alias("silver"), condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        
        append_to_log(gold_table,'SEMANTIC', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,'SUCCESS')
        
        count=0
        if deletes=='YES':    
            for x in [(x.strip()).replace('.','_') for x in incremental_tables.split(',')]:
                spark.table('global_temp.'+x).filter('`_change_type`=="delete"').createOrReplaceGlobalTempView(x)
                count= count + spark.table('global_temp.'+x).count()
            if count!=0:
                primarykeys=[x.strip() for x in primary_keys.split(',')]   
                spark.sql(get_SQL(dbutils.notebook.run(sqlQuery,60),incremental_tables, non_incremental_tables)).createOrReplaceTempView('deleteview')
                spark.sql('DELETE FROM '+gold_table+' AS gold WHERE EXISTS (SELECT '+','.join(primarykeys)+' FROM deleteview WHERE '+' AND '.join(['gold.'+x+'='+x for x in primarykeys])+')')
            
        
        return (print('success for merge condition: '+condition))
     except Exception as e:
        if 'Table or view not found' in str(e):
          append_to_log(gold_table,'SEMANTIC', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,str(e))
          return print('No New data available for '+gold_table)
        append_to_log(gold_table,'SEMANTIC', sqlQuery, condition, primary_keys, deletes, incremental_tables, non_incremental_tables,str(e))
        return (print('error:'+(str(e))))

# COMMAND ----------

# MAGIC %md
# MAGIC #### streaming_config:

# COMMAND ----------

display(streaming_config)

# COMMAND ----------

# MAGIC %md
# MAGIC #### databricks_config:

# COMMAND ----------

display(databricks_config)

# COMMAND ----------

# MAGIC %md
# MAGIC #### synapse_config:

# COMMAND ----------

display(synapse_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Incrementals from L0:

# COMMAND ----------


  
tpool1 = ThreadPool(10)
points1=tpool1.starmap(write_all, get_parameters_incremental('L0,L0_OTIF',streaming_config))
points1
  
  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform Merge on Joined incremental data- L1 core tables updated:
# MAGIC 
# MAGIC ##### - Parameters are sqlQuery, mergeSet and condition

# COMMAND ----------



tpool2=ThreadPool(20)
points2=tpool2.starmap(merge_core, get_parameters_merge_core('CORE',databricks_config))
points2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get incremental data from L1 Updates:
# MAGIC 
# MAGIC ##### - This prepares us for pushing L1 changes forward to work/semantic layers

# COMMAND ----------



tpool3 = ThreadPool(10)
points3=tpool3.starmap(write_all, get_parameters_incremental('CORE',streaming_config))
points3



# COMMAND ----------

# MAGIC %md
# MAGIC ### Synapse L1 Merge + Delete :

# COMMAND ----------



tpool4=ThreadPool(10)
points4=tpool4.starmap(upsertDWH, get_parameters_synapse('CORE',synapse_config))


points4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Work table updates:

# COMMAND ----------

tpool5 = ThreadPool(10)


points5=tpool5.starmap(merge_work, get_parameters_merge_work('WORK', databricks_config))
points5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get work incrementals:
# MAGIC #### -> This prepares us for pushing work table changes forward to semantic layers

# COMMAND ----------

tpool6 = ThreadPool(10)


points6=tpool6.starmap(write_all, get_parameters_incremental('WORK',streaming_config))
points6


# COMMAND ----------

# MAGIC %md ## Work table update Synapse

# COMMAND ----------

tpool10=ThreadPool(10)
points10=tpool10.starmap(upsertDWH, get_parameters_synapse('WORK',synapse_config))


points10

# COMMAND ----------

# MAGIC %md
# MAGIC ### L2 MERGE:

# COMMAND ----------

tpool7=ThreadPool(10)
points7=tpool7.starmap(merge_semantic, get_parameters_merge_semantic('SEMANTIC',databricks_config))


points7

# COMMAND ----------

# MAGIC %md
# MAGIC ### L2 changes for Synapse

# COMMAND ----------

tpool8 = ThreadPool(10)
points8=tpool8.starmap(write_all, get_parameters_incremental('SEMANTIC',streaming_config))
points8


# COMMAND ----------

# MAGIC %md
# MAGIC ### Synapse L2 Merge Using Insert Update logic

# COMMAND ----------

tpool9=ThreadPool(10)
points9=tpool9.starmap(upsertDWH, get_parameters_synapse('SEMANTIC',synapse_config))


points9
