# Databricks notebook source
import pytz
from pyspark.sql.functions import to_utc_timestamp
import datetime
from functools import reduce
from pyspark.sql.functions import col,isnan,when,count, lit

# COMMAND ----------

def covert_spaces_to_underscore(source_file_df):
    """
        This function will check all the columns of source file and convets the spaces to underscore if any column name has white space.
        :param source_file_df: source file dataframe 
        :return:dataframe which does not have any spaces in column names.
    """
    oldColumns = source_file_df.columns
    newColumns = list(map(lambda item : item.replace(" ","_"),oldColumns)) 
    undscr_df = reduce(lambda source_file_df, idx: source_file_df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), source_file_df)  
    return undscr_df

# COMMAND ----------

def convert_date_to_utc(source_file_df, source_time_zone):
    """
        This function will check all colums of source file and convets the date to utc timezone and creates new column for utc date.
        :param source_file_df: source file/table dataframe 
        :return:dataframe which does not have any spaces in columns.
    """ 
    all_timezones = pytz.all_timezones
    if source_timezone not in all_timezones:
        raise Exception(f"The timezone used is not recognized. Please use one of the following: {all_timezones}")

    if source_time_zone not in ('utc' , 'UTC') : 
        for col in source_file_df.columns:
            if dict(source_file_df.dtypes)[col] in ('timestamp','date'):
                new_col = col + '_utc'
                source_file_df = source_file_df.withColumn(new_col , to_utc_timestamp(source_file_df[col], source_timezone))
    
    return source_file_df.drop('ingest_datetime_utc_utc')
