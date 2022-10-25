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

# COMMAND ----------

def convert_number_statistics_to_df(number_dict):
    number_row_list = []
    if not number_dict:
        # print ('enter')
        number_row_list.append(Row(column_number = 'nocol' , Min = 0 , Max = 0 , Mean = 0 , StdDevColumn = 0, Percentile25 =  0,
                                        Percentile50 =  0, Median =  0, Percentile75 =  0,MinValueCount = 0, MaxValueCount =  0  ))
        number_df= spark.createDataFrame(number_row_list)
        return number_df
    else:
        for k in number_dict:
            if number_dict[k]['Min'] == None:
                # print ('great')
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
    date_format_pattern = "[0-9][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9][0-9] [0-9][0-9].[0-9][0-9].[0-9][0-9].\
                            [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9] [A|P]M"
    date_format = re.compile(date_format_pattern)

    for c in source_df.columns:
        try:
            col_name = c
    #         print(type(col(c)))
            where = f"{c} is not null" # or {c} != None"
    #         print(where)
            df = source_df.select(c).filter(where).limit(1)
            if df.count() > 0:
                value = df.rdd.collect()[0][0]
                result = date_format.match(value)
                if result != None: 
                    print(c, result)
                    test_df = source_df.withColumn(c + "_A", to_timestamp(col(c), "%Y-%m-%dT%H:%M:%S"))
    #         lst = [x for x in df.rdd.collect()]
    #         print(lst)
    #         print(c, df.count())
    #         test_df = test_df.withColumn(c, col(c).cast(DateType()))
        except Exception as e:
    #         print(col_name)
    #         print(str(e))
            pass


# COMMAND ----------

# Start Application
if __name__ == '__main__':
 
    print('start processing                :', datetime.datetime.now())
    source_file_df = spark.read.format('csv') \
                                .option('header',True)\
                                .option('inferschema',True)\
                                .load("/FileStore/tables/Vehicle.csv")
       
    data = dict(source_file_df.dtypes)
#     print(source_file_df.dtypes)
    datatype_list = []
    for key in data:
        datatype_list.append(Row(Column = key, DataType = data[key]))

    datatype_df = spark.createDataFrame(datatype_list)
    datatype_df = datatype_df.withColumn("row_idx",row_number().over(Window.orderBy(monotonically_increasing_id())))
    print ('datatypes are done              :', datetime.datetime.now())
    
#     display(datatype_df)
    df_after_date_conversion = date_fileds_coversion(source_file_df)
    
    configure_file_df = {}
    converted_utc_df = {}
    global_parm_df = {}
    str_min_max_dict = {}

    data_val =  DataQualityFramework(source_file_df,configure_file_df,converted_utc_df,global_parm_df)

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
    join_df = join_df.withColumn('Include' , lit('Y')).withColumn('ShouldBeUnique' , lit('')).withColumn('ShouldNotBeNull' , lit(''))
    final_df = join_df.select('Column','DataType','Include','Min','Max','Mean','Median','StdDevColumn','Percentile25','Percentile50', \
                              'Percentile75','MinValueCount','MaxValueCount','UniqueValueCount','MinDate','MaxDate','MinValueLength','MaxValueLength',
                              'MissingPercentColumn','ShouldBeUnique','ShouldNotBeNull')
    
    display(final_df)
#     # # configure_csv_path = get_adls_path_and_filename(param_dict['configure_parm'])
#     configure_csv_path = '/FileStore/tables/validationscsvs/'
#     print('validation csv path             :' ,configure_csv_path)
#     write_to_csv(final_df, configure_csv_path, "Vehicle_Validation.csv")
#     print ('validation csv written          :', datetime.datetime.now())
   

# COMMAND ----------

test_df = source_file_df
display(test_df)
    

# COMMAND ----------

date_format = re.compile("[0-9][0-9]-NOV-21 [0-9][0-9].[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9] [A|P]M")

# COMMAND ----------

from pyspark.sql.types import DateType

col_name = None
date_format_pattern = "[0-9][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9][0-9] [0-9][0-9].[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9] [A|P]M"
date_format = re.compile(date_format_pattern)

for c in test_df.columns:
    try:
        col_name = c
#         print(type(col(c)))
        where = f"{c} is not null" # or {c} != None"
#         print(where)
        df = test_df.select(c).filter(where).limit(1)
        if df.count() > 0:
            value = df.rdd.collect()[0][0]
            result = date_format.match(value)
            if result != None: 
                print(c, result)
                test_df = test_df.withColumn(c + "_A", to_timestamp(col(c), "%Y-%m-%dT%H:%M:%S"))
#         lst = [x for x in df.rdd.collect()]
#         print(lst)
#         print(c, df.count())
#         test_df = test_df.withColumn(c, col(c).cast(DateType()))
    except Exception as e:
#         print(col_name)
#         print(str(e))
        pass

# df2.select(col("input"), 
#     to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp") 
#   ).show(truncate=False)


# COMMAND ----------



# COMMAND ----------

date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
print(date)

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([
   StructField("timestamp_string", StringType(), True),
   StructField("string", StringType(), True)])
data = [("05-NOV-21 03.01.36.422000000 PM", "a"),
        ("05-NOV-21 03.01.36.422000000 PM", "b"),
        ("05-NOV-21", "c")]
df = spark.createDataFrame(data, schema)

# COMMAND ----------

from datetime import datetime
date_input = df.rdd.collect()[1][0]
print(date_input)
date_obj = datetime.strptime(date_input, '%d-%b-%y %I.%M.%S.%f000 %p')
print(date_obj)
# print date_obj.strftime('%d-%m-%Y')

# COMMAND ----------

df2 = df.filter("string = 'c'")
df2 = df2.withColumn("timestamp_timestamp", to_timestamp("timestamp_string")) #, '%d-%M-%y')) # %K.%M.%s.%S %a'))
df2 = df2.withColumn("testing", to_date(col("timestamp_string")))
display(df2)

# COMMAND ----------

from pyspark.sql.functions import *

df=spark.createDataFrame(
        data = [ ("1","2019-06-24 12:01")],
        schema=["id","input_timestamp"])
df.printSchema()

#Timestamp String to DateType
df = df.withColumn("timestamp",to_timestamp("input_timestamp", "MM-dd-yyyy HH:mm:ss.SSSS")) #\
#   .show(truncate=False)

# Using Cast to convert TimestampType to DateType
df = df.withColumn('timestamp_string', \
         to_timestamp('timestamp').cast('string')) #\
#   .show(truncate=False)

display(df)

# COMMAND ----------

display(test_df)

# COMMAND ----------

 df =  Seq(("2019-11-16 16:50:59.406")).toDF("input_timestamp")
 df.selectExpr("input_timestamp", "extract(week FROM input_timestamp) as w").show

# COMMAND ----------

test_df.printSchema()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create SparkSession
# spark = SparkSession.builder \
#           .appName('SparkByExamples.com') \
#           .getOrCreate()

from pyspark.sql.functions import *

df=spark.createDataFrame(
        data = [ ("1","2019-06-24 12:01:19.000")],
        schema=["id","input_timestamp"])
df.printSchema()

#Timestamp String to DateType
df = df.withColumn("input_timestamp",to_timestamp("input_timestamp"))

df.printSchema()

# # Using Cast to convert TimestampType to DateType
# df.withColumn('timestamp', \
#          to_timestamp('input_timestamp').cast('string')) \
#   .show(truncate=False)
  

# df.select(to_timestamp(lit('06-24-2019 12:01:19.000'),'MM-dd-yyyy HH:mm:ss.SSSS')) \
#   .show(truncate=False)
  
# #SQL string to TimestampType
# spark.sql("select to_timestamp('2019-06-24 12:01:19.000') as timestamp")
# #SQL CAST timestamp string to TimestampType
# spark.sql("select timestamp('2019-06-24 12:01:19.000') as timestamp")
# #SQL Custom string to TimestampType
# spark.sql("select to_timestamp('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as timestamp")
