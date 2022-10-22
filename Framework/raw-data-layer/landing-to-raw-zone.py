# Databricks notebook source
# MAGIC %md
# MAGIC # landing-to-raw-zone
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Originally Created By | [nate.pimentel@neudesic.com](mailto:nate.pimentel@neudesic.com)|
# MAGIC |External References |[https://neudesic.com](https://neudesic.com) |
# MAGIC |Input  |<ul><li>**Main Notebook Parameters**</li><li>s3 Bucket Namee: name of the s3 Bucket</li><li>environment: dev/qa/prod matches base adls container</li><li>functionalAreaName: subfolder for data layers</li><li>mode: how data is going to be processed: merge, append or keep history</li><li>notebookTableExecutionName: The unique name of the table to be processed from the table NotebookTable</li><li>optimize: true/false - whether or not to add this table to the optimize delta table process</li><li>parentNotebookExecutionLogKey: Holds the NotebookExecution log key from the calling notebook</li><li>primaryKeyColumns: Primary Keys of the specified table <li>**Landing zone to raw zone parameters**</li><li>deleteFlagColumnName,Value: Holds the column name that flags the record as deleted and the associated value that marks a delete</li><li>landingFileType: json, json multiline,csv File type of landing flat file</li><li>landingTableName: The name of the table in the Landing Zone</li><li>originalTableName: This is the raw and current state table name </li><li>removeLandingFile: true/false - Remove Source Landing files</li><li>rowModDateColumnName: Column that contains the datetime that that row was updated/created</li><li>**Client/Source Specific Notebook Parameters**</li><li>zspRemDeletesWithoutModDate: true/false - Remove source rows if the source file contains deletes without a delete date</li>|
# MAGIC |Input Data Source |<ul><li>json, json multiline or csv flat file in Azure Data Lake Store | 
# MAGIC |Output Data Source |<ul><li> Azure Data Lake Store Delta Table|
# MAGIC 
# MAGIC ## History
# MAGIC 
# MAGIC | Date | Developed By | Change |
# MAGIC |:----:|--------------|--------|
# MAGIC |2021-12-14| Nate Pimentel | Created |
# MAGIC |2022-01-10| Butch Johnson | Edited:  Changed Parameters to align with framework db fields |
# MAGIC |2022-01-28| Cristian Vasconez | Edited:  Modified Merge Process and Merge data code to handle deletes |
# MAGIC |2022-02-10| Butch Johnson | Edited:  Added Parameters and added support for DeltaTableOptimize |
# MAGIC |2022-02-25| Butch Johnson | Edited:  Removed secrets notebook and Modifed framework functions to not use secrets |
# MAGIC |2022-05-26| Cristian Vasconez | Edited:  Added mode parameter to provide merge, append and keep history dynamic functionality |
# MAGIC |2022-07-05| Butch Johnson | Edited:  Added json multiline support |  
# MAGIC |2022-07-07| Butch Johnson | Edited:  Added ability to support incremential loads across layers using adls modified date |  
# MAGIC |2022-08-04| Butch Johnson | Edited:  Added ability to load all landing files in one batch | 
# MAGIC |2022-08-04| Butch Johnson | Edited:  Added function to pass list of files to delete to Data Factory |
# MAGIC   
# MAGIC ## Other Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize
# MAGIC Load any required notebooks/libraries

# COMMAND ----------

# MAGIC %run ../utility/neudesic-framework-functions

# COMMAND ----------

from pathlib import Path
import json
from datetime import datetime as dt
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import *
from delta.tables import *
from pyspark.sql.types import *

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

#Main Notebook Parameters
dbutils.widgets.text(name="containerName", defaultValue="", label="s3 Bucket Name")
dbutils.widgets.text(name="functionalAreaName", defaultValue="Sales", label="Functional Area Name:")
dbutils.widgets.text(name="mode", defaultValue="append", label="Mode:")
dbutils.widgets.text(name="notebookTableExecutionName", defaultValue="", label="Notebook Table Execution Name:")
dbutils.widgets.text(name="optimize", defaultValue="true", label="Optimize:")
dbutils.widgets.text(name="parentNotebookExecutionLogKey", defaultValue="-1", label="Parent Notebook Execution Log Key")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Columns:")
dbutils.widgets.text(name="notebookExecutionGroupName", defaultValue="", label="Notebook Execution Group Name")

#Landing zone to raw zone parameters
dbutils.widgets.text(name="deleteFlagColumnNameAndValue", defaultValue="", label="delete Flag Column Name,Value:")
dbutils.widgets.text(name="landingFileType", defaultValue="csv", label="landing File Type:")
dbutils.widgets.text(name="landingTableName", defaultValue="", label="landing Table Name:")
dbutils.widgets.text(name="numberlandingfilesinbatch", defaultValue="100", label="number of landing files in batch:")  #number of files or full load
dbutils.widgets.text(name="originalTableName", defaultValue="", label="original Table Name:") 
dbutils.widgets.text(name="removeLandingFile", defaultValue="false", label="remove Landing File:")
dbutils.widgets.text(name="rowModDateColumnName", defaultValue="", label="row Mod Date Column Name:")

#Client/Source specific
dbutils.widgets.text(name="zspRemDeletesWithoutModDate", defaultValue="false", label="zsp remove Deletes Without Mod Date:")

#Main Notebook Parameters
containerName = dbutils.widgets.get("containerName") 
functionalAreaName = dbutils.widgets.get("functionalAreaName")
mode = dbutils.widgets.get("mode")
notebookTableExecutionName = dbutils.widgets.get("notebookTableExecutionName")
optimize = (dbutils.widgets.get("optimize").lower() == 'true')
parentNotebookExecutionLogKey = dbutils.widgets.get("parentNotebookExecutionLogKey")
primaryKeyColumns = dbutils.widgets.get("primaryKeyColumns").split(",")
notebookExecutionGroupName = dbutils.widgets.get("notebookExecutionGroupName")

#Landing zone to raw zone parameters
if dbutils.widgets.get("deleteFlagColumnNameAndValue").lower()=='none':
    deleteFlagColumnNameAndValue=''
else:
    deleteFlagColumnNameAndValue = dbutils.widgets.get("deleteFlagColumnNameAndValue")

landingFileType = dbutils.widgets.get("landingFileType").lower().replace(" ","")
    
if "multiline" in landingFileType:
    multiLine = 1
    landingFileType = landingFileType.replace("multiline","")
else:
    multiLine = 0

landingTableName = dbutils.widgets.get("landingTableName")

numberlandingfilesinbatch = dbutils.widgets.get("numberlandingfilesinbatch").lower().replace(" ","")
    
if numberlandingfilesinbatch=="fullload":
    numberlandingfilesinbatch=1000000
    fullLoad=1
else:
    fullLoad=0
    
originalTableName = dbutils.widgets.get("originalTableName")
removeLandingFile = (dbutils.widgets.get("removeLandingFile").lower() == 'true')

if dbutils.widgets.get("rowModDateColumnName").lower()=='none':
    rowModDateColumnName=''
else:
    rowModDateColumnName = dbutils.widgets.get("rowModDateColumnName")
    
tableHasModifiedColumn = rowModDateColumnName != ''

#Client/Source specific
zspRemDeletesWithoutModDate = (dbutils.widgets.get("zspRemDeletesWithoutModDate").lower() == 'true')

#Notebook Variables
edwSourceRowModifiedDateTime = 'edwSourceRowModifiedDateTime'
edwRowCreatedDateTime = 'edwRowCreatedDateTime'
edwRowModifiedDateTime = 'edwRowModifiedDateTime'
edwPartitionColumn = 'PartitionColumn'
edwRowDeleteColumn = 'edwRowDeleted'
deleteFlagList = deleteFlagColumnNameAndValue.split(',')
if len(deleteFlagList) >=2:
    deleteFlagColumnName = deleteFlagList[0]
    deleteFlagColumnValue = deleteFlagList[1]
else:
    deleteFlagColumnName = ""
    deleteFlagColumnValue = "" 



# COMMAND ----------

#Path Settings
fullPathPrefix = "s3://"
mnt_path = "dbfs:/mnt/dpa-hanv-dpdl-1-s3-reg-raw-0001"
rawZonePath = mnt_path +  "/raw-zone/" + functionalAreaName + "/"
landingPath = mnt_path +  "/landing-zone/" + functionalAreaName + "/"
landingPathArchive = mnt_path +  "/landing-zone/" + functionalAreaName + "/archive/" + landingTableName + "/"
landingPathError = mnt_path +  "/landing-zone/" + functionalAreaName + "/error/" + landingTableName + "/"
schemasPath = mnt_path +  "/landing-zone/" + functionalAreaName + "/schema/" + originalTableName
dataFactoryQueuePath = mnt_path +  "/landing-zone/datafactoryqueue/"

secretScopeType = "Admin"

print("s3 Bucket: {0}".format(containerName))
print("functionalAreaName: {0}".format(functionalAreaName))
print("mode: {0}".format(mode))
print("notebookTableExecutionName: {0}".format(notebookTableExecutionName))
print("optimize: {0}".format(optimize))
print("parentNotebookExecutionLogKey: {0}".format(parentNotebookExecutionLogKey))
print("notebookExecutionGroupName: {0}".format(notebookExecutionGroupName))
print("primaryKeyColumns: {0}".format(primaryKeyColumns))
print("")
print("deleteFlagColumnNameAndValue: {0}".format(deleteFlagColumnNameAndValue))
print("tableHasModifiedColumn: {0}".format(tableHasModifiedColumn))
print("")
print("landingFileType: {0}".format(landingFileType))
print("multiLine: {0}".format(multiLine))
print("landingTableName: {0}".format(landingTableName))
print("numberlandingfilesinbatch: {0}".format(numberlandingfilesinbatch))
print("originalTableName: {0}".format(originalTableName))
print("rowModDateColumnName: {0}".format(rowModDateColumnName))
print("removeLandingFile: {0}".format(removeLandingFile))
print("")
print("zspRemDeletesWithoutModDate: {0}".format(zspRemDeletesWithoutModDate))
print("")
print("landingPath: {0}".format(landingPath))
print("rawZonePath: {0}".format(rawZonePath))
print("landingPathArchive: {0}".format(landingPathArchive))
print("Schemas Path: {0}".format(schemasPath))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Start Logging

# COMMAND ----------

#Start Logging
notebookName = get_notebookName(dbutils.notebook.entry_point.getDbutils().notebook().getContext())
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentNotebookExecutionLogKey,notebookTableExecutionName,secretScopeType)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Landing Zone Functions

# COMMAND ----------

def createYearQuarterFolderStructure(path):
  today = dt.now()
  year = today.strftime("%Y")
  dateTime = today.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
  #print(dateTime)
  quarter = str(StrDateTimeToPrecionX(dateTime, "quarter"))[5:]
  #print(path)
  newPath = path + year + "/Q" + quarter + "/"
  #print(newPath)
  if not file_exists(newPath):
    dbutils.fs.mkdirs(newPath)
  return newPath

# COMMAND ----------

def fileCopyWithRetry(file, path, maxRetries):
  numRetries = 1
  while True:
    try:
      x = dbutils.fs.cp(file,path)
      return x
    except Exception as e:
      if numRetries > maxRetries:
        raise e
      else:
        print ("Retrying error", e)
        numRetries += 1

# COMMAND ----------

def build_error_file(files,errorFilePath):
  today = get_framework_current_datetime()
  dateTime = today.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
  errorFileName = "error_" + dateTime + "_" + originalTableName + ".txt"
  dbutils.fs.put(errorFilePath + errorFileName, files)
  return errorFilePath + errorFileName

# COMMAND ----------

#Logs notebook error
#Copies landing file to error folder
#Removes landing file
#Revmoves emptly folders
def handle_error_exception(notebookExecutionLogKey,errorCode,errorDescription,notebookTableExecutionName,landingPath,landingPathError,removeLandingFile,fileListString,e):
  log_event_notebook_error(notebookExecutionLogKey,errorCode,errorDescription,notebookTableExecutionName,secretScopeType,notebookName)
  if not file_exists(landingPathError):
    print("'error path' does not exist, creating now.")
    dbutils.fs.mkdirs(landingPathError)
  #yearQuarterErrorPath = createYearQuarterFolderStructure(landingPathError)
  #copy list of landing files to error folder
  errorFile = build_error_file(fileListString,landingPathError)
  #dbutils.fs.cp(file,yearQuarterErrorPath + newFileNameFinal)  
  print("Error " + errorDescription + "Exception: " +str(e))
  print("Error file: {0} created".format(errorFile))
  
  #Remove Landing Source File
  #if removeLandingFile == True:
  #  if file_exists(file):
  #    dbutils.fs.rm(file,recurse=False)

# COMMAND ----------

def create_archive_path(archiveFilePath):
  today = get_framework_current_datetime()
  year = today.strftime("%Y")
  dateTime = today.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
  #print(dateTime)
  quarter = str(StrDateTimeToPrecionX(dateTime, "quarter"))[5:]
  #print(path)
  yearQuarterArchivePath = archiveFilePath + year + "/Q" + quarter + "/"
  #print(newPath)
  if not file_exists(yearQuarterArchivePath):
    dbutils.fs.mkdirs(yearQuarterArchivePath)
  return yearQuarterArchivePath
  

# COMMAND ----------

def get_archive_filename(sourceFilename):
  today = get_framework_current_datetime()
  dateTime = today.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
  originalFileName=Path(sourceFilename).name
  newFileName = dateTime + "_" + originalFileName 
  #print(newFileName)
  return newFileName

# COMMAND ----------

def build_data_factory_queque_file(filesToProcess,tablename,archiveFilePath,queuePath,functionalAreaName,fullPathPrefix):
  yearQuarterArchivePath = create_archive_path(archiveFilePath)
  destinationPath = yearQuarterArchivePath.replace(fullPathPrefix,"")
  sourceDestFiles = ''
  fileCreated = ''
  comma = ''
  fileCount = 0
  for file in filesToProcess:
    fileCount = fileCount + 1
    #add file to queue contents
    destinationFilename = get_archive_filename(file)
    sourceFilename = Path(file).name
    sourceFullPath = file.replace(sourceFilename,"")    
    sourcePath = sourceFullPath.replace(fullPathPrefix,"") 
    sourceDestFiles = sourceDestFiles + (comma + ' {"sourcePath":"' + sourcePath + '","sourceFilename":"' + sourceFilename + '","destinationPath":"' + destinationPath + '","destinationFilename":"' + destinationFilename + '"}')
    comma = ','
    if (fileCount >=4998 and len(sourceDestFiles)>=1024*1024*3):  #lookup in Data Factory is limited to files of 5000 rows and 4 mb
      #write the file 
      print("fileCount: {0}".format(fileCount))
      print("length: {0}".format(len(sourceDestFiles)))
      today = get_framework_current_datetime()
      dateTime = today.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
      queueFileName = functionalAreaName + "_" + dateTime + "_" + tablename + ".json"
      jsonBody = '{"queueFilename":"' + queueFileName + '","files": [' + sourceDestFiles + ']}'
      #print(sourceDestList)
      #print(jsonBody)
      dbutils.fs.put(queuePath + queueFileName, jsonBody)
      if fileCreated=='':
        fileCreated = queuePath + queueFileName
      else:
        fileCreated = fileCreated + comma + queuePath + queueFileName
      comma = ''
      fileCount = 0
      sourceDestFiles = ''
    
  if fileCount > 0:
    today = get_framework_current_datetime()
    dateTime = today.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    queueFileName = functionalAreaName + "_" + dateTime + "_" + tablename + ".json"
    jsonBody = '{"queueFilename":"' + queueFileName + '","files": [' + sourceDestFiles + ']}'
    #print(sourceDestList)
    #print(jsonBody)
    dbutils.fs.put(queuePath + queueFileName, jsonBody)
    fileCreated = fileCreated + comma + queuePath + queueFileName
  return fileCreated

# COMMAND ----------

def copy_landingfile_to_archive_archiveiscsv(sourceFilename,tablename,archiveFilePath,sourceFile_df):
  #Convert landing file to csv if needed
  if landingFileType != "csv":
    csvFolderName = sourceFilename.replace("." + landingFileType, ".csv") 
  else:
    csvFolderName = sourceFilename
  
  yearQuarterArchivePath = create_archive_path(archiveFilePath)
  destinationFilename = yearQuarterArchivePath + get_archive_filename(csvFolderName)
  
  if not file_exists(archiveFilePath):
    print("'archive path' does not exist, creating now.")
    dbutils.fs.mkdirs(archiveFilePath)

  if landingFileType == "csv":
    fileCopyWithRetry(sourceFilename, yearQuarterArchivePath + newFileNameFinal, 3)
  else:
    sourceFile_df.write.option("header",True).csv(csvFolderName)
    #Copy partfile from folder to a File and remove a Folder
    #This remove all CRC files
    counter=0
    for csvFile in get_files_with_extension(csvFolderName + "/",".csv"):
      file_name = Path(csvFile).name
      counter = counter + 1      
      fileCopyWithRetry(csvFile, yearQuarterArchivePath + newFileNameFinalbase + '_' + str(counter), 3)
    dbutils.fs.rm(csvFolderName,recurse=True)

# COMMAND ----------

def copy_landingfile_to_archive_archiveSameTypeAsSource(sourceFilename,tablename,archiveFilePath):
  yearQuarterArchivePath = create_archive_path(archiveFilePath)
  destinationFilename = yearQuarterArchivePath + get_archive_filename(sourceFilename)
  fileCopyWithRetry(sourceFilename, destinationFilename, 3)

# COMMAND ----------

def get_table_schema(schemasPath,tableName,file,landingFileType,multiLine):
  fileSchemasPath = schemasPath + "/Schema.json"
  try:
    head = dbutils.fs.head(fileSchemasPath, 256000)
  except Exception as e:
    create_table_schema(schemasPath, tableName, file, landingFileType,multiLine)
    head = dbutils.fs.head(fileSchemasPath, 256000)
  return StructType.fromJson(json.loads(head))  

# COMMAND ----------

def create_table_schema(schemasPath,tableName,file,landingFileType,multiLine):
  schemaFile = schemasPath + "/Schema.json"
  if landingFileType == "json":    
    if multiLine == 0:
      createSchemaDF=spark.read.json(file)
    else:
      createSchemaDF=spark.read.option("multiline", "true").json(file) 
  else:
    createSchemaDF = spark.read.format("csv").option("header","true").load(file)
  schema = createSchemaDF.schema
  schema_json = createSchemaDF.schema.json()
  if not file_exists(schemasPath):
    dbutils.fs.mkdirs(schemasPath)
  dbutils.fs.put(schemaFile, schema_json, True)

# COMMAND ----------

def add_framework_fields(source_dataframe):
  #display(source_dataframe)
  if tableHasModifiedColumn == True:
    rowModColType = dict(source_dataframe.dtypes)[rowModDateColumnName]
    rowmod_df = source_dataframe.withColumn(rowModDateColumnName, F.when(F.col(rowModDateColumnName)=="",None).otherwise(F.col(rowModDateColumnName)))

    source_dataframe.createOrReplaceTempView("min_mod_date_df")
    minModDate = spark.sql("select min(" + rowModDateColumnName + ") from min_mod_date_df where " + rowModDateColumnName + " <> '' and "  + rowModDateColumnName + " is not null").collect()[0][0]
    if not minModDate:
      minModDate = get_framework_current_datetime() - timedelta(minutes = 1)
    else:
      #Convert moddate to actual timestamp and substract a minute from the oldest mod datetime from the source
      minModDate = dt.strptime(minModDate, '%Y-%m-%d %H:%M:%S.%f') - timedelta(minutes = 1)
    if rowModColType == "string": #might need to add more type checks
      #convert to datetime and populate any missing modified dates with today's date 
      rowmod_df = rowmod_df.withColumn(edwSourceRowModifiedDateTime, F.when(F.col(rowModDateColumnName).isNull(),minModDate).otherwise(to_timestamp(F.col(rowModDateColumnName))))
    else:
      rowmod_df = rowmod_df.withColumn(edwSourceRowModifiedDateTime, F.when(F.col(rowModDateColumnName).isNull(),minModDate).otherwise(F.col(rowModDateColumnName)))
  else:
    rowmod_df = source_dataframe.withColumn(edwSourceRowModifiedDateTime, F.lit(get_framework_current_datetime()))
  #Add Column that holds create and update date of when the record was inserted/modified within ADLS 
  frameworkCurrentDatetime = get_framework_current_datetime()
  rowmod_df = rowmod_df.withColumn(edwRowCreatedDateTime, F.lit(frameworkCurrentDatetime))
  rowmod_df = rowmod_df.withColumn(edwRowModifiedDateTime, F.lit(frameworkCurrentDatetime))
  #display(rowmod_df)
  #Add and Populate PartionColumn  
  partition_df = rowmod_df.withColumn(edwPartitionColumn,F.concat(F.year(F.col(edwSourceRowModifiedDateTime)),F.quarter(F.col(edwSourceRowModifiedDateTime)))) 
  #Add and Populate DeletedFlagColumn
  if deleteFlagColumnName != "" and deleteFlagColumnValue != "":
    delete_df = partition_df.withColumn(edwRowDeleteColumn, F.when(F.col(deleteFlagColumnName) == deleteFlagColumnValue,1).otherwise(0))
  else:
    delete_df = partition_df.withColumn(edwRowDeleteColumn,lit(0))
  #display(delete_df)
  #display(delete_df)
  if(mode == 'keep history'):
    delete_df = delete_df.withColumn("expired_datetime", lit(None).cast(StringType()))
    delete_df = delete_df.withColumn("effective_datetime", col(edwSourceRowModifiedDateTime))
    delete_df = delete_df.withColumn("is_current", lit(1))
  return delete_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process and Merge data

# COMMAND ----------

#check if raw table exists
try:
  rawZone_df = spark.read \
    .format("delta") \
    .load(rawZonePath)
  #display(rawZone_df)
  doesTableExist = True
  rawZone_df = spark.read \
              .format("delta") \
              .load(rawZonePath)
except Exception as e:
  doesTableExist = False
  
print("Raw table exists: {0}".format(doesTableExist))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Source Specific Functions

# COMMAND ----------

#Filter out Deletes without mod date
def filter_deletes_no_mod_date(source_dataframe,mode,tableHasModifiedColumn,zspRemDeletesWithoutModDate):
  if (mode == 'merge' and tableHasModifiedColumn == True and zspRemDeletesWithoutModDate == True):   
    #Filter out deletes if rowModDateColumnName is = '' since it will be '' for a full load
    source_dataframe.createOrReplaceTempView("landingallrecords_df")        
    sql = ("""select * from landingallrecords_df where !({0} = '{1}' and {2}=='')""").format(deleteFlagColumnName,deleteFlagColumnValue,rowModDateColumnName) 
    #sql = ("""select * from landingallrecords_df where !({0} =='D' and {1}=='')""").format(deleteFlagColumnName,rowModDateColumnName)
    #print(sql)
    source_dataframe = spark.sql(sql) 
  return source_dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC #### Begin Main Data Process

# COMMAND ----------

#Read in a batch of source files
filesProcessed = []
filesProcessedString = ""
filesCount = 0
try:
    dbutils.fs.ls(landingPath)
    landingPathExists = 1
except:
    landingPathExists = 0
  
if landingPathExists == 1:
    filesToProcess = get_files_with_extension(landingPath,"." + landingFileType)
    for file in filesToProcess:
        filesCount = filesCount + 1
        if filesCount==1:
            landingSchema = get_table_schema(schemasPath, notebookTableExecutionName, file, landingFileType,multiLine)
        filesProcessed.append(file)
        filesProcessedString = filesProcessedString + ", " + file
    if filesCount>0:
        if landingFileType=="json" and multiLine == 0:
            source_df1=spark.read.schema(landingSchema).json(filesToProcess)
        elif landingFileType=="json" and multiLine == 1:
            source_df1=spark.read.option("multiline", "true").schema(landingSchema).json(filesToProcess)
        else:
            source_df1=spark.read.format("csv").option("header","true").schema(landingSchema).csv(filesToProcess)
            #display(source_df1)
        RecordCount = source_df1.count()
        print('Landing file list loaded.  {0} Files ready to process. {1} Records ready to process'.format(filesCount,RecordCount))
    else:
        print('Zero Landing files found')

# COMMAND ----------

#Walk through table directory to find source files
#Load X number of source files to dataframe
#Filter out Deletes without mod date  (This is for initial loads that process all deletes first - raw should be deleted instead of processing all deletes)
#Add Framework fields
#Filter for the latest changes
#Merge landing files into RAW Layer Delta table
#Save landing files to archive folder

addToAdlsTableList = False
errorCode=''
originalSourceEmpty = False

#print(len(filesToProcess))
#Load a batch of Landing Files
try:  
  print('Processing {0} landing files'.format(filesCount))
  if filesCount > 0: 
    #display(source_df1)
    source_df=source_df1 
    #display(source_df)
    originalSourceEmpty = True;
    if source_df.rdd.isEmpty() == False:
      originalSourceEmpty = False;
      #display(source_df)

      #Source Specific Function
      source_df = filter_deletes_no_mod_date(source_df,mode,tableHasModifiedColumn,zspRemDeletesWithoutModDate)  
      #display(source_df)
    #If source_df is empty skip file since there is no data    
    if source_df.rdd.isEmpty() == False:
      #Add framework columns
      landing_df = add_framework_fields(source_df) 

      #check if delta table exists in raw. if not create table from schema dataframe.
      if doesTableExist == False: 
         #Create 
        (spark
       .createDataFrame([], landing_df.schema)
       .write
       .partitionBy(edwPartitionColumn)
       .option("mergeSchema", "true")
       .format("delta")
       .mode("overwrite")
       .save(rawZonePath))

      #Load table table after creating it from schema dataframe
        rawZone_df = spark.read \
          .format("delta") \
          .load(rawZonePath) 
        doesTableExist = True
        errorCode=''
      #end doesTableExist
      ColumnID = ', '.join(map(str,primaryKeyColumns)) 
      pk_columns = get_column_pk_list(primaryKeyColumns)
      pk_on_clause = get_pk_on_clause(primaryKeyColumns) 
      Columns = ', '.join(map(str,landing_df.columns))
      #Filter for the latest changes
      ##Merge
      if mode == 'merge':
        #Filter for the latest changes
        landinglatesttransaction_df = landing_df \
        .selectExpr("struct(" + ColumnID + ") as ColumnID ", "" + edwSourceRowModifiedDateTime + " as " + edwSourceRowModifiedDateTime ) \
        .groupBy("ColumnID") \
        .agg(max(edwSourceRowModifiedDateTime).alias(edwSourceRowModifiedDateTime)) \
        .selectExpr("ColumnID.*", "" + edwSourceRowModifiedDateTime + "")## define the latest transaction to be processed

        #Creates views to be joined and remove all records that do not need to be merged, avoiding duplicate keys as part of modfied records
        landing_df.createOrReplaceTempView("landingallrecords_df")
        landinglatesttransaction_df.createOrReplaceTempView("landinglatesttransaction_df")
        sql = ("""
          select """ + Columns + """ 
          from ( 
            select row_number() over(partition by """ + pk_columns + """, targetTable.""" + edwSourceRowModifiedDateTime + """ order by """ + edwRowDeleteColumn + """ desc) as rownum
            , targetTable.* 
            from landingallrecords_df targetTable 
              join  landinglatesttransaction_df sourceTable on """ + pk_on_clause + """ and targetTable.""" + edwSourceRowModifiedDateTime + """ = sourceTable.""" + edwSourceRowModifiedDateTime + """
            ) 
            where rownum = 1 """)
        join_df=spark.sql(sql)
        join_df.createOrReplaceTempView("landing_df")
        rawZone_df.createOrReplaceTempView("rawZone_df")
        ##Build Merge Statement - not using sqlInsert
        sqlMerge,sqlInsert = build_merge_SQL_Statement("landing_df","rawZone_df",list(landing_df.columns),primaryKeyColumns,mode,edwRowDeleteColumn,secretScopeType,0,edwRowCreatedDateTime,edwRowModifiedDateTime)  
        #print(sqlMerge)
        #print(sqlInsert)
        try:
          spark.sql(sqlMerge)
        except Exception as e:
          print(e)
          errorCode = "111"
          errorDescription = "Landing to Raw Zone notebook: " + notebookName + " - error while applying merge" 
          log_event_notebook_error (notebookExecutionLogKey,errorCode,errorDescription,notebookTableExecutionName,secretScopeType,notebookName)
      ##All records
      elif mode == 'append':
        #Inserts all records from landing
        #Creates views
        landing_df.createOrReplaceTempView("landing_df")
        rawZone_df.createOrReplaceTempView("rawZone_df")
        ##Build Insert Statement - using sqlInsert 
        sqlMerge,sqlInsert = build_merge_SQL_Statement("landing_df","rawZone_df",list(landing_df.columns),primaryKeyColumns,mode,edwRowDeleteColumn,secretScopeType,0,edwRowCreatedDateTime,edwRowModifiedDateTime)
        try:
          spark.sql(sqlInsert)
        except Exception as e:
          print(e)
          errorCode = "121"
          errorDescription = "Landing to Raw Zone notebook: " + notebookName + " - error while appending data" 
          log_event_notebook_error (notebookExecutionLogKey,errorCode,errorDescription,notebookTableExecutionName,secretScopeType,notebookName)   
      ##Soft Deletes (History of records with deletes and expired records)
      else:    
        landing_df.createOrReplaceTempView("landingallrecords_df")
        ##Partition landing so we can update raw one batch at a time
        sql = ("""
          select """ + Columns + """ , rownum
          from ( 
            select row_number() over(partition by """ + pk_columns + """ order by """ + edwSourceRowModifiedDateTime + """, """ + edwRowDeleteColumn + """ desc ) as rownum
            , targetTable.* 
            from landingallrecords_df targetTable 

            )""")
        join_df=spark.sql(sql)
        join_df.createOrReplaceTempView("landing_df")
        rawZone_df.createOrReplaceTempView("rawZone_df")

        ##GET MAX ROWNUM
        maxRowNum = spark.sql("select max(rownum) from landing_df").collect()[0][0]

        ##Get sql statements
        rownum_df = spark.sql("select * from landing_df")
        sqlMerge,sqlInsert = build_merge_SQL_Statement("rownum_df","rawZone_df",list(landing_df.columns),primaryKeyColumns,mode,edwRowDeleteColumn,secretScopeType, 0,edwRowCreatedDateTime,edwRowModifiedDateTime)
        try:
          ##ITERATE FROM 1 TO MAX ROW NUM
          for i in range(1, maxRowNum+1):
            ##Build df to apply sql statements to where maxrownum = i
            rownum_df = spark.sql("select * from landing_df where rownum = {}".format(i))
            #display(rownum_df)
            rownum_df.createOrReplaceTempView("rownum_df")
            ##apply sql statements
            spark.sql(sqlMerge)
            spark.sql(sqlInsert)
        except Exception as e:
          print(e)
          errorCode = "131"
          errorDescription = "Landing to Raw Zone notebook: " + notebookName + " - error while merging landing file content with Raw zone delta table, file name: " + filesProcessedString 
          handle_error_exception(notebookExecutionLogKey,errorCode,errorDescription,notebookTableExecutionName,landingPath,landingPathError,removeLandingFile,filesProcessedString,e) 
  if (filesCount > 0 and errorCode=='' and originalSourceEmpty == False):
    print("Processed {0} files".format(len(filesProcessed)))
    ##Adds table to framework ADLS table list
    insert_adls_delta_table(notebookTableExecutionName,rawZonePath,optimize,0,secretScopeType)
  elif errorCode=='':
    print(print("Zero landing files to process for table " + notebookTableExecutionName))
except Exception as e:
  errorCode = "121"
  errorDescription = "Landing to Raw Zone notebook: " + notebookName + " - error while processing landing files: " + filesProcessedString 
  handle_error_exception(notebookExecutionLogKey,errorCode,errorDescription,notebookTableExecutionName,landingPath,landingPathError,removeLandingFile,filesProcessedString,e)
  raise(e)

# COMMAND ----------

#Remove Landing files if process was successful
if (filesCount > 0 and errorCode=='' and originalSourceEmpty == False and removeLandingFile == True):
  if filesCount > 500:
    print("Removing {0} files from Landing using DataFactory".format(filesCount))
    queueFiles = build_data_factory_queque_file(filesProcessed,originalTableName,landingPathArchive,dataFactoryQueuePath,functionalAreaName,fullPathPrefix)
    print("Data Factory queue file(s) {0} created".format(queueFiles))
  else:
    print("Removing {0} files from Landing using Databricks".format(filesCount))
    for file in filesProcessed:
      if file_exists(file):
        copy_landingfile_to_archive_archiveSameTypeAsSource(file,originalTableName,landingPathArchive)
        #print('Delete Landing File: {0}'.format(file))
        dbutils.fs.rm(file,recurse=False)
   

# COMMAND ----------

#Remove all empty folders
if landingPathExists == 1:
  remove_empty_directories(landingPath)

# COMMAND ----------

#rawZone_df = spark.read \
#              .format("delta") \
#              .load(rawZonePath)
#rawZone_df.Optimize()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName=notebookExecutionGroupName,scopeType=secretScopeType)
dbutils.notebook.exit("Succeeded")
