# Databricks notebook source
# MAGIC %run ../data-quality/FilesystemOperations

# COMMAND ----------

# MAGIC %run ../data-quality/DataQualityFramework

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id,row_number
from pyspark.sql.window import Window
import datetime
from pyspark.sql.functions import col,isnan,when,count, lit, to_date
import ast
from pyspark.sql import Row
import re
from pyspark.sql.types import DateType
from pyspark.sql.functions import to_timestamp

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

#dbutils.fs.ls("/mnt/dpa-hanv-dpdl-1-s3-reg-raw-0001/raw-zone/Location")

# COMMAND ----------

def convert_number_statistics_to_df(number_dict):
    number_row_list = []
    if not number_dict:
        number_row_list.append(Row(column_number = 'nocol' , Min = 0 , Max = 0 , Mean = 0 , StdDevColumn = 0, Percentile25 =  0,
                                        Percentile50 =  0, Median =  0, Percentile75 =  0,MinValueCount = 0, MaxValueCount =  0  ))
        number_df= spark.createDataFrame(number_row_list)
        return number_df
    else:
        for k in number_dict:
            if number_dict[k]['Min'] == None:
                number_row_list.append(Row(column_number = k , Min = '' , Max = '' , Mean = '' , StdDevColumn = '', Percentile25 = '',
                                        Percentile50 =  '', Median =  '', Percentile75 =  '' ,MinValueCount = '', MaxValueCount =   '' ))

            else:
                number_row_list.append(Row(column_number = k , Min = number_dict[k]['Min'] , Max = number_dict[k]['Max'] , 
                                                Mean = number_dict[k]['Mean'] , StdDevColumn = number_dict[k]['StdDev'],
                                                Percentile25 =  number_dict[k]['Percentile 25, 50, 75'][0] if number_dict[k]['Percentile 25, 50, 75'] != None else None,
                                                Percentile50 =  number_dict[k]['Percentile 25, 50, 75'][1] if number_dict[k]['Percentile 25, 50, 75'] != None else None,
                                                Median =  number_dict[k]['Percentile 25, 50, 75'][1] if number_dict[k]['Percentile 25, 50, 75'] != None else None,
                                                Percentile75 =  number_dict[k]['Percentile 25, 50, 75'][2] if number_dict[k]['Percentile 25, 50, 75'] != None else None,
                                                MinValueCount = number_dict[k]['MinValueCount'],
                                                MaxValueCount =  number_dict[k]['MaxValueCount'] 
                                                
                                                ))
        number_df= spark.createDataFrame(number_row_list)
        return number_df

def convert_dates_to_df(date_input_dict):
    date_row_list = []
    if not date_input_dict:
        date_row_list.append(Row(column_date = 'nocol', MinDate = '' , MaxDate = '' ))
        date_df = spark.createDataFrame(date_row_list)
        return date_df
    
    dates_latest = ((str(date_input_dict).replace("[" , "{")).replace("]",'}')).replace(' :' , "': '")
    date_dict = ast.literal_eval(dates_latest)
    for k in date_dict:
        date_row_list.append(Row(column_date = k, MinDate = date_dict[k]['MinDate'] if date_dict[k]['MinDate'] != 'None' else None , 
                                MaxDate = date_dict[k]['MaxDate'] if date_dict[k]['MaxDate'] != 'None' else None))
    date_df = spark.createDataFrame(date_row_list)
    return date_df

def missing_percent_to_df(missing_dict):
    missing_row_list = []
    for k in missing_dict:
        missing_row_list.append(Row(column_missing = k , MissingPercentColumn = missing_dict[k]))
    missing_df= spark.createDataFrame(missing_row_list)
    return missing_df

def unique_count_to_df(unique_percent_data):
    unique_percent_latest = ((str(unique_percent_data).replace("[" , "{")).replace("]",'}')).replace(' :' , "': '")
    unique_percent_row_list = []
    unique_percent_dict = ast.literal_eval(unique_percent_latest)

    for k in unique_percent_dict:
        if k == 'TotalRecords':
            print('TotalRecords                    :' , unique_percent_dict[k])
        else:
            unique_percent_row_list.append(Row(column_unique = k, UniqueValueCount = unique_percent_dict[k]['unique']))
                                                #  Percent = unique_percent_dict[k]['percent']))

    unique_df = spark.createDataFrame(unique_percent_row_list)
    return unique_df

def min_max_valuelength_to_df(string_dict):

    string_row_list = []
    for k in string_dict:
        string_row_list.append(Row(column_string = k , MinValueLength = string_dict[k]['Min'] , MaxValuelength = string_dict[k]['Max']))

    string_df= spark.createDataFrame(string_row_list)

    return string_df


# COMMAND ----------

def date_fileds_coversion(source_df):
    col_name = None
    dtformat1 = re.compile("[0-9][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9][0-9] [0-9][0-9].[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9] [A|P]M")
    dtformat2 = re.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]:[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]")
    dtformat3 = re.compile("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9]")
    
    dateformat1 = re.compile("[0-9].*/[0-9].*/[0-9][0-9][0-9][0-9]")
    dateformat2 = re.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]")
    dateformat3 = re.compile("[0-9].*-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-[0-9][0-9]")
#     31-Dec-99
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
                elif dateformat2.match(value) != None:
                    source_df = source_df.withColumn(c, to_date(col(c), "yyyy/MM/dd"))
                elif dateformat3.match(value) != None:
                    source_df = source_df.withColumn(c, to_date(col(c), "dd/MMM/yy"))
                    
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

dbutils.widgets.text(name="SourcefilePath", defaultValue ="/FileStore/tables/Validations/Vehicle.csv", label="Source File Path")
dbutils.widgets.text(name="Outputfolder", defaultValue ="/FileStore/tables/Validations", label="Output folder Path")
dbutils.widgets.text(name="Delimiter", defaultValue =",", label="Delimiter for CSV file")

SourcefilePath = dbutils.widgets.get('SourcefilePath')
Outputfolder = dbutils.widgets.get('Outputfolder')
Delimiter = dbutils.widgets.get('Delimiter')

print(SourcefilePath)


# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# dbutils.fs.rm("/FileStore/tables/Validations/Validation.delta", recurse=True) 

# COMMAND ----------

# Start Application
if __name__ == '__main__':
 
    print('start processing                :', datetime.datetime.now())
#     source_file_df = spark.read.format('csv') \
#                                 .option('header',True)\
#                                 .option('inferschema',True)\
#                                 .option("delimiter", Delimiter)\
#                                 .load(SourcefilePath)

    source_file_df1 = spark.read.format('csv') \
                                .option('header',True)\
                                .option('inferschema',True)\
                                .option("delimiter", Delimiter)\
                                .option("timestampFormat", "dd-MMM-yy hh.mm.ss.SSSSSSSSS a")\
                                .load(SourcefilePath)
#     source_file_df1.printSchema()
    display(source_file_df1)
    source_file_df = date_fileds_coversion(source_file_df1)
    data = dict(source_file_df.dtypes)
    datatype_list = []
    for key in data:
        datatype_list.append(Row(Column = key, DataType = data[key]))

    datatype_df = spark.createDataFrame(datatype_list)
    datatype_df = datatype_df.withColumn("row_idx",row_number().over(Window.orderBy(monotonically_increasing_id())))
    print ('datatypes are done              :', datetime.datetime.now())
    
#     source_df_aft_date = date_fileds_coversion(source_file_df)
    
    configure_file_df = {}
    converted_utc_df = {}
    global_parm_df = {}
    str_min_max_dict = {}

#     data_val =  DataQualityFramework(source_df_aft_date,configure_file_df,converted_utc_df,global_parm_df)
    data_val =  DataQualityFramework(source_file_df1,source_file_df,configure_file_df,converted_utc_df,global_parm_df)

    missingpecent_column = data_val.get_empty_count_percentage()
    missingpercent_df = missing_percent_to_df(missingpecent_column)
#     display(missingpercent_df)
    print ('missing percent done            :', datetime.datetime.now())

    number_statistics_dict =  data_val.get_statistics_number()
    number_stats_df = convert_number_statistics_to_df(number_statistics_dict)
    print ('number statistics are done      :', datetime.datetime.now())

    unique_values_dict = data_val.unique_and_percentage() 
    unique_values_df = unique_count_to_df(unique_values_dict)
    print ('unique values cnt are done      :', datetime.datetime.now())

    date_min_max_dict = data_val.min_max_dates()
    date_df = convert_dates_to_df(date_min_max_dict)
#     display(date_df)
    print ('min and max dates are done      :', datetime.datetime.now())

    for column in source_file_df.columns:
        if dict(source_file_df.dtypes)[column] == 'string':
            str_min_max_dict.update(data_val.min_max_valuelength(column)) 

    minmax_valuelength_df = min_max_valuelength_to_df(str_min_max_dict)
    print ('Min and Max value length done   :', datetime.datetime.now())


    join_df = datatype_df.join(missingpercent_df, datatype_df['Column'] == missingpercent_df['column_missing'],'LEFT') \
                         .join (date_df, datatype_df['Column'] == date_df['column_date'],'LEFT') \
                         .join (number_stats_df, datatype_df['Column'] == number_stats_df['column_number'],'LEFT') \
                         .join (unique_values_df, datatype_df['Column'] == unique_values_df['column_unique'],'LEFT') \
                         .join (minmax_valuelength_df, datatype_df['Column'] == minmax_valuelength_df['column_string'],'LEFT') \
                         .sort('row_idx').drop(*('column_number','column_date','column_missing', 'column_unique','column_string','row_idx'))
    join_df = join_df.withColumn('Include' , lit('Y')).withColumn('ShouldBeUnique' , lit(''))\
                     .withColumn('ShouldNotBeNull' , lit('')).withColumn('ShouldMatchPattern' , lit('')).withColumn('ShouldBeInList' , lit(''))
    final_df = join_df.select('Column','DataType','Include','Min','Max','Mean','Median','StdDevColumn','Percentile25','Percentile50', \
                              'Percentile75','MinValueCount','MaxValueCount','UniqueValueCount','MinDate','MaxDate','MinValueLength','MaxValueLength',
                              'MissingPercentColumn','ShouldBeUnique','ShouldNotBeNull','ShouldMatchPattern','ShouldBeInList')
    
#     display(final_df)
    # # configure_csv_path = get_adls_path_and_filename(param_dict['configure_parm'])
    filename = Outputfolder.split('/')[-1]
    configure_csv_path = Outputfolder.split(filename)[0]
    
#     configure_csv_path = Outputfolder
    print('validation csv path             :' ,configure_csv_path)
#     display(final_df)
    write_to_csv(final_df, configure_csv_path,filename)
    print ('validation csv written          :', datetime.datetime.now())
   

# COMMAND ----------

Validationcsv_df = spark.read.format('csv').option('header',True).option('inferschema',True).load(Outputfolder)
display(Validationcsv_df)
