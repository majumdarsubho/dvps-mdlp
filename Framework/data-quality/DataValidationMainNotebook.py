# Databricks notebook source
# MAGIC %run ../data-quality/FilesystemOperations

# COMMAND ----------

# MAGIC %run ../data-quality/DatatypeConversions

# COMMAND ----------

# MAGIC %run ../data-quality/DataQualityFramework

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd
import re

# COMMAND ----------

# Parameters recieving from pipieline 
relative_path  = "test/tables"
container_name = "bronze"
account_name   = "adlsentapp58081"
# account_name   = "adlsentapp58082"
pipeline_start_time = datetime.datetime.now()
# filename = "task.parquet"
filename = "opportunity.parquet"
# filename = 'Account.parquet'
sourcename ='salesforce'
source_timezone = 'EST'
CopyActivitySinkKey = '1001'
CopyActivityExecutionLogKey = '88'

# COMMAND ----------

dbutils.widgets.text(name="SourcefilePath", defaultValue ="/FileStore/tables/Validations/Vehicle.csv", label="Source File Path")
dbutils.widgets.text(name="ValidationCSVfilePath", defaultValue ="/FileStore/tables/Validations/Vehicle_Validation.csv", label="Validation CSV file Path")
dbutils.widgets.text(name="GlobalparametersfilePath", defaultValue ="/FileStore/tables/Validations/globalparameters.csv", label="Gobalparameters CSV file Path")
dbutils.widgets.text(name="ValidationresultsfilePath", defaultValue ="/FileStore/tables/Validations/Validation.delta", label="Validationresults delta file Path")
dbutils.widgets.text(name="UpdatedSourcefilePath", defaultValue ="/FileStore/tables/Validations/Vehicle.parquet", label="UpdatedSource parquet file Path")
dbutils.widgets.text(name="Delimiter", defaultValue =",", label="Delimiter for CSV file")

SourcefilePath = dbutils.widgets.get('SourcefilePath')
ValidationCSVfilePath = dbutils.widgets.get('ValidationCSVfilePath')
GlobalparametersfilePath = dbutils.widgets.get('GlobalparametersfilePath')
ValidationresultsfilePath = dbutils.widgets.get('ValidationresultsfilePath')
UpdatedSourcefilePath = dbutils.widgets.get('UpdatedSourcefilePath')
Delimiter = dbutils.widgets.get('Delimiter')

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

def date_fileds_coversion(source_df):
    col_name = None
    dtformat1 = re.compile("[0-9][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9][0-9] [0-9][0-9].[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9] [A|P]M")
    dtformat2 = re.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]:[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]")
    dtformat3 = re.compile("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]")
    
    dateformat1 = re.compile("[0-9].*/[0-9].*/[0-9][0-9][0-9][0-9]")
    dateformat2 = re.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]")
    
    timeformat1 = re.compile("[0-9][0-9]:[0-9][0-9]")
    for c in source_df.columns:
        col_name = c
        
        where = f"{c} is not null" # or {c} != None"
        if dict(source_df.dtypes)[col_name] == 'string':
#             print (col_name)
            df = source_df.select(c).filter(where).limit(1)
            if df.count() > 0:
                value = df.rdd.collect()[0][0]
#                 print(dtformat1.match(value))
                if dtformat1.match(value) != None:   
#                     print(dtformat1.match(value))
                    source_df = source_df.withColumn(c, to_timestamp(col(c), "dd-MMM-yy hh.mm.ss.SSSSSSSSS a"))
                elif dtformat2.match(value) != None:               
                    source_df = source_df.withColumn(c, to_timestamp(col(c), "yyyy-MM-dd:HH:mm:ss.SSSSSS")) 
                elif dtformat3.match(value) != None:
                    source_df = source_df.withColumn(c, to_timestamp(col(c), "MM/dd/yyyy HH:mm:ss.SSSSSS"))
                elif dateformat1.match(value) != None:
                    source_df = source_df.withColumn(c, to_date(col(c), "M/d/yyyy"))
                elif dateformat1.match(value) != None:
                    dateformat2 = source_df.withColumn(c, to_date(col(c), "yyyy/MM/dd"))
                    
        if dict(source_df.dtypes)[col_name] == 'timestamp':
            df = source_df.select(c).filter(where).limit(1)
            if df.count() > 0:
                value1 = df.rdd.collect()[0][0]
                value = str(value1)
#                 print(col_name,value)
#                 print(timeformat1.match(value))
                if timeformat1.match(value) != None:
                    print(col_name)
                    source_df = source_df.withColumn(col_name, source_df[col_name].cast(StringType()))
    
#     display(source_df)
#     source_df.printSchema()
    return source_df

# COMMAND ----------

# Start Application
if __name__ == '__main__':

    param_dict = {}

    parquet_folder_path = 'raw/' + sourcename + '/' + filename.split('.parquet')[0] +'/'+ 'original'
    configure_folder_path = 'validations/' + sourcename 
    configure_file_name = filename.split('.parquet')[0] + '.csv'
    finalfile_path = 'raw/' + sourcename + '/' + filename.split('.parquet')[0] 
    print ('start source file read          :', datetime.datetime.now())
    # Read the source file and and load data into dataframe
    source_file_df1 = spark.read.format('csv') \
                                .option('header',True)\
                                .option('inferschema',True)\
                                .option("delimiter", Delimiter)\
                                .option("timestampFormat", "dd-MMM-yy hh.mm.ss.SSSSSSSSS a")\
                                .load(SourcefilePath)
    print ('source file read done           :', datetime.datetime.now())

    display(source_file_df1)
    source_file_df = date_fileds_coversion(source_file_df1)
    # Exit the notebook when there are no records in sourcefile
    if source_file_df.count() == 0:
        error_details = {}
        record_validation_table_dict = {}
        error_details = {'Error' : 'No records in source file ', 'code' : '0', 'row_count' : source_file_df.count(),
                         'col_count' : len(source_file_df.columns)}
        write_record_in_validation_table(param_dict,record_validation_table_dict,error_details)
        mssparkutils.notebook.exit('Norecords')

    raw_input_df1 = source_file_df
    # Read the configure csv file and load data into dataframe
    print ('validation input csv read       :', datetime.datetime.now())
#     config_file_df = get_file_data(param_dict, 'configure_parm')
    config_file_df1 = spark.read.format('csv').option('header',True).option('inferschema',True).load(ValidationCSVfilePath)
    config_file_df  = config_file_df1.filter(config_file_df1['include'] == 'Y')
    configure_file_df = config_file_df.na.fill('01/01/1000',['MinDate']).na.fill('12/31/9999',['MaxDate'])
    print ('validation input csv read done  :', datetime.datetime.now())

    print ('global parameter cvs file read  :', datetime.datetime.now())
#     global_parm_df = get_file_data(param_dict, 'global_parm')
    global_parm_df = spark.read.format('csv').option('header',True).option('inferschema',True).load(GlobalparametersfilePath)
    print ('global parameter cvs file done  :', datetime.datetime.now())

    # Add raw_filename and ingest_datetime_utc columns
    process_file = filename + '_' + str(pipeline_start_time)
    source_file_df = source_file_df.withColumn('raw_filename' , F.lit(process_file)) \
                    .withColumn('ingest_datetime_utc',F.lit(pipeline_start_time))

    # Convert the spaces to undescore in columns names
    print ('underscore conversion start     :', datetime.datetime.now())
    converted_undscr_df = covert_spaces_to_underscore(source_file_df)
    print ('underscore conversion end       :', datetime.datetime.now())

    # Conver the dates to utc format and create new column for utc dates.
    print('utc conversion start            :',datetime.datetime.now())
    converted_utc_df = convert_date_to_utc(converted_undscr_df, source_timezone)
    print('utc conversion end              :',datetime.datetime.now())

    # Data validations - column level
    # Execlude the columns
    columns_include_list = configure_file_df.filter(configure_file_df['include'] == 'Y').rdd.map(lambda x: x[0]).collect()
    raw_input_df = raw_input_df1.select(columns_include_list)

    data_val =  DataQualityFramework(raw_input_df,configure_file_df,converted_utc_df,global_parm_df)
    data_val.columns_validations()
    # record_validation_table_dict = data_val.columns_validations()
    print('column validations are done     :', datetime.datetime.now())

    # Data validations - row level
    final_df, validate_delta_dict = data_val.rows_data_validations()
    print('row validations are done        :', datetime.datetime.now())

    # Write record in column validation delta file
    error_details= {}
    error_details = {'Error' : 'None', 'code' : '0', 'row_count' : raw_input_df.count(),'col_count' : len(raw_input_df.columns)}
#     display(final_df)
#     display(validate_delta_dict)
    write_record_in_validation_table(param_dict,validate_delta_dict,error_details,ValidationresultsfilePath)
    print ('validation delta file written   :' , datetime.datetime.now())

#     Overwrite the raw file with new fileds added file
#     final_path = get_adls_path_and_filename(param_dict['finalfile_parm'])
#     path = final_path.split(filename)[0]
    filename = UpdatedSourcefilePath.split('/')[-1]
    path = UpdatedSourcefilePath.split(filename)[0] + '/'
    print(path)
    write_to_parquet(final_df, path, filename)
#     print('new consolidate file path       :', final_path)
#     print('new consolidate file path       :', UpdatedSourcefilePath)
#     display(df1)
    print('new consolidate file written    :', datetime.datetime.now())

    # Exit the notebook with proper status
    if (validate_delta_dict['StdDevErrors'] != None) or (validate_delta_dict['TimestampErrors'] == 'yes'):
        dbutils.notebook.exit('thresholdfail')
    else:
        dbutils.notebook.exit('pass')


# COMMAND ----------

updated_source_file_df = spark.read.format('parquet').load(UpdatedSourcefilePath)
display(updated_source_file_df)
Validationresults_df = spark.read.format('delta').load(ValidationresultsfilePath)
display(Validationresults_df)

# COMMAND ----------

# config_file_df1 = spark.read.format('csv').option('header',True).option('inferschema',True).load(ValidationCSVfilePath)
# display(config_file_df1)
