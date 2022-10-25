# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

def get_adls_path_and_filename(param_dict):
    """
        This function will concatenate the paramerters which are recieved from pipeline and create the path from it for further file reading purpose.
        :param param_dict: Dictonary(paramters values received from pipeline) 
        :return: path
    """
    path = f"abfss://{param_dict['container']}@{param_dict['account']}.dfs.core.windows.net/{param_dict['folder_path']}/{param_dict['filename']}" 
    return path

# COMMAND ----------

def get_file_data(param_dict, key):
    """
        This function will use the file path parameter and read the files from the location and stores data in spark dataframe.
        :param param_dict: Dictonary(paramters values received from pipeline for files location) 
        :return: dataframe
    """
    adls_file_path = get_adls_path_and_filename(param_dict[key])
    print ('file path                       :', adls_file_path)
    if param_dict[key]['file_format'] == 'parquet':
        if dbutils.fs.ls(adls_file_path):
            file_df = read_parquet_file(adls_file_path)
            return file_df
    else:   
        try:
            if dbutils.fs.ls(adls_file_path):
                if param_dict[key]['file_format'] == 'csv':
                    file_df = read_csv_file(adls_file_path)
                    return file_df
        except:
            error_details = {}
            record_validation_table_dict = {}
            print (param_dict[key]['filename']+' file       :', 'Not found')
            error_details = {'Error' : param_dict[key]['filename'] + ' is not found', 'code' : '1',
                             'row_count' :  0 ,'col_count' : 0 }
            write_record_in_validation_table(param_dict,record_validation_table_dict,error_details)
            dbutils.notebook.exit('fail')       
    return

# COMMAND ----------

def read_parquet_file(adls_file_path):
    """
        This function read the parquet file from path mentioned in adls_file_path
        :param adls_file_path: full file path
        :return: dataframe.
    """
    parquet_df = spark.read.format('parquet').load(adls_file_path)
    return parquet_df

# COMMAND ----------

def read_csv_file(adls_file_path):
    """
        This function read the csv file from path mentioned in adls_file_path
        :param adls_file_path: full file path
        :return: dataframe.
    """
    try:
        csv_df = spark.read.format('csv').option('header',True).option('inferSchema',True).option('delimiter',',').load(adls_file_path)
    except:
        error_details = {}
        record_validation_table_dict = {}
        print (param_dict[key]['filename']+' file       :', 'Unable to read')
        error_details = {'Error' : param_dict[key]['filename'] + ' Unable to read', 'code' : '1'}
        write_record_in_validation_table(param_dict,record_validation_table_dict,error_details)
        dbutils.notebook.exit('fail') 
    
    return csv_df 

# COMMAND ----------

def write_record_in_validation_table(param_dict,record_validation_table_dict,error_details,ValidationresultsfilePath):
    
#     parquet_path = get_adls_path_and_filename(param_dict['parquet_parm'])
#     configure_path = get_adls_path_and_filename(param_dict['configure_parm'])

#     delta_folder = configure_path.split(filename.split('.')[0])[0] 
#     delta_file_path = delta_folder + 'validation.delta'
    delta_file_path = ValidationresultsfilePath
    stddev_errors    = ''
    timestamp_errors = ''
    if ('StdDevErrors' in record_validation_table_dict) or ('TimeStampErrors' in record_validation_table_dict):
        if record_validation_table_dict['StdDevErrors'] != None:
            stddev_errors = 'StdDev exceeded columns : ' + str(record_validation_table_dict['StdDevErrors'])

        if record_validation_table_dict['TimestampErrors'] == 'yes':
            timestamp_errors = "{ 'Exceeded Dates Threshold' : 'please check new parquet file'}"

        if (stddev_errors != '') or (timestamp_errors != ''):
            error_details.update({'Error' : timestamp_errors + stddev_errors })


    df = spark.createDataFrame([Row(
                                    TableName = filename.split('.parquet')[0],
                                    CopyActivitySinkKey = CopyActivitySinkKey,
                                    CopyActivityExecutionLogKey = CopyActivityExecutionLogKey,
                                    IngestDateTime = pipeline_start_time,
                                    RawPathAndFileName = 'Rqwfilenamepath',  #parquet_path,
                                    ValidationPathAndFileName = 'ValidationCsvs file path' ,#configure_path,
                                    RowCount = error_details['row_count'],
                                    ColumnCount = error_details['col_count'],
                                    TableValidationErrors = error_details['Error'],
                                    IsValidationSuccessful = error_details['code'],
                                    UniqueValuesPerColumn = str(record_validation_table_dict['UniqueValuesPerColumn']) 
                                    if 'UniqueValuesPerColumn' in record_validation_table_dict else 'None', 
                                    MinMaxLengthsPerAlphanumericColumn = str(record_validation_table_dict['MinMaxLengthsPerAlphanumericColumn'])
                                    if 'MinMaxLengthsPerAlphanumericColumn' in record_validation_table_dict else 'None' ,
                                    StatisticsPerNumericColumn = str(record_validation_table_dict['StatisticsPerNumericColumn'])
                                    if 'StatisticsPerNumericColumn' in record_validation_table_dict else 'None',
                                    MinMaxDateTimesPerColumn = str(record_validation_table_dict['MinMaxDateTimesPerColumn'])
                                    if 'MinMaxDateTimesPerColumn' in record_validation_table_dict else 'None',
                                    MissingDataPercentagePerIdentifiedColumn = str(record_validation_table_dict['MissingDataPercentage'])
                                    if 'MissingDataPercentage' in record_validation_table_dict else 'None')
                                        
                            ])
    df1 = df.select('TableName','CopyActivitySinkKey','CopyActivityExecutionLogKey','IngestDateTime','RawPathAndFileName',
                    'ValidationPathAndFileName','TableValidationErrors','RowCount','ColumnCount','IsValidationSuccessful','UniqueValuesPerColumn',
                    'MinMaxLengthsPerAlphanumericColumn','StatisticsPerNumericColumn','MinMaxDateTimesPerColumn',
                    'MissingDataPercentagePerIdentifiedColumn'
                    )
    print ('Validation results delta path   :', delta_file_path)
#     display(df1)
    df1.coalesce(1).write.format('delta').mode('append').save(delta_file_path)
    return

# COMMAND ----------

def write_to_csv(df, path, filename):

    """
        This function writes the csv file to the path mentioned in path with the filename given as filename.
        It writes the file, which writes as folder and partition. The file is coalesced using 1 (only one partition).
        That partition is moved to the directory it should exist, then the original folder and partition files are deleted.
        Then, the partition file is renamed to the filename parameter and written in the path from the path parameter.
        :param df: the dataframe to write as csv
        :param path: the full path to where the file should be written
        :param filename: the name that should be used when writing the file
    """
    path_and_filename = f"{path}{filename}"
    df.coalesce(1).write.option("header",True).mode("overwrite").option('inferschema',True).csv(path_and_filename)
    file = [x for x in dbutils.fs.ls(path_and_filename) if 'part-' in x.name ]
    for x in file:
        dbutils.fs.mv(x.path, path, True)
        dbutils.fs.rm(path_and_filename, True)
        dbutils.fs.mv(path + x.name, path_and_filename, True)

# COMMAND ----------

def write_to_parquet(df, path, filename):

    """
        This function writes the parquet file to the path mentioned in path with the filename given as filename.
        It writes the file, which writes as folder and partition. The file is coalesced using 1 (only one partition).
        That partition is moved to the directory it should exist, then the original folder and partition files are deleted.
        Then, the partition file is renamed to the filename parameter and written in the path from the path parameter.
        :param df: the dataframe to write as csv
        :param path: the full path to where the file should be written
        :param filename: the name that should be used when writing the file
    """
    path_and_filename = f"{path}{filename}"
    df.coalesce(1).write.mode("overwrite").parquet(path_and_filename)
    file = [x for x in dbutils.fs.ls(path_and_filename) if 'part-' in x.name]
    for x in file:
        dbutils.fs.mv(x.path, path, True)
        dbutils.fs.rm(path_and_filename, True)
        dbutils.fs.mv(path + x.name, path_and_filename, True)

# COMMAND ----------

def read_csv_file_with_delim(param_dict,key,delim):
    """
        This function read the csv file from path mentioned in adls_file_path
        :param adls_file_path: full file path
        :return: dataframe.
    """
    adls_file_path = get_adls_path_and_filename(param_dict[key])
    try:
        if dbutils.fs.ls(adls_file_path):
            csv_df = spark.read.format('csv').option('header',True).option('inferSchema',True).option('delimiter',delim).load(adls_file_path)
    except:
        error_details = {}
        datasource_list = ['All triggered APIs']
        print (param_dict[key]['filename']+' file       :', 'Unable to locate/read')
        error_details = {'masterfiles_error' : param_dict[key]['filename'] + ' Unable to locate or read'}
        write_api_error_file(param_dict,error_details,datasource_list)
        exit_value = str(error_details)
        dbutils.notebook.exit(exit_value) 
    return csv_df 

# COMMAND ----------

def write_to_json(df, path, filename):

    """
        This function writes the csv file to the path mentioned in path with the filename given as filename.
        It writes the file, which writes as folder and partition. The file is coalesced using 1 (only one partition).
        That partition is moved to the directory it should exist, then the original folder and partition files are deleted.
        Then, the partition file is renamed to the filename parameter and written in the path from the path parameter.
        :param df: the dataframe to write as csv
        :param path: the full path to where the file should be written
        :param filename: the name that should be used when writing the file
    """
    path_and_filename = f"{path}{filename}"
    df.coalesce(1).write.format('json').mode('append').save(path_and_filename)
