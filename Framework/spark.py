from pyspark.sql import *
spark = SparkSession.builder.getOrCreate()

def get_spark():
     return spark