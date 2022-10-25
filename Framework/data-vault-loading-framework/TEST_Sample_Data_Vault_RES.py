# Databricks notebook source
# MAGIC %run ../utility/neudesic-framework-functions

# COMMAND ----------

import json
from pyspark.sql.functions import *

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(name='Source', defaultValue="", label='Source')
dbutils.widgets.text(name='landing', defaultValue="", label='Landing Path')
dbutils.widgets.text(name='satellite', defaultValue="/mnt/dpa/idw/silver/Satellites", label='Satellite Path')
dbutils.widgets.text(name='hub', defaultValue="/mnt/dpa/idw/silver/Hubs", label='Hub Path')
dbutils.widgets.text(name='link', defaultValue="/mnt/dpa/idw/silver/Links", label='Link Path')
# dbutils.widgets.text(name='table', defaultValue="", label='Table Name')
# dbutils.widgets.text(name='pk', defaultValue="", label='Primary key')

# COMMAND ----------

dict_resv = {
    "HK_Reservation":["RES_ID"],
    "HK_Customer":["CUST_NAME"],
    "HK_PickupLocation":["PU_LOC_CD"],
    "HK_ReturnLocation":["RTRN_LOC_CD"],
    "HK_RentalReservation":["HK_Customer","HK_PickupLocation","HK_ReturnLocation","HK_Reservation"]}

# COMMAND ----------

# DBTITLE 1,Keeping Group and Source as Same for now
landing_path  = dbutils.widgets.get("landing")
source = dbutils.widgets.get("Source")
group = dbutils.widgets.get("Source")
hub_path = dbutils.widgets.get("hub")
satellite_path = dbutils.widgets.get("satellite")
link_path = dbutils.widgets.get("link")
primary_key_hub = ["RES_ID"]
primary_key_sat_resv = ["HK_Reservation","RES_ID","RES_VER"]
rental_resv_columns = ["HK_Customer","HK_PickupLocation","HK_ReturnLocation","HK_RentalReservation"]
common_cols = ["Source","LoadDate"]
hub_columns = ["HK_Reservation"]+primary_key_hub+common_cols
print("landing path: "+landing_path)
print("These are Hub columns :",hub_columns)
print("These are SAT_RESV columns :",primary_key_sat_resv)
print("These are SAT_Rental_Resv columns :",rental_resv_columns)

# COMMAND ----------

#write as parquet to raw-layer
df = spark.read.format("csv").option("header","true").load(landing_path)
df.write.format("parquet").mode("overwrite").save(f"/mnt/dpa/idw/raw-data-layer/raw-zone/{source}")
display(df)

# COMMAND ----------

df = spark.read.format("parquet").load(f"/mnt/dpa/idw/raw-data-layer/raw-zone/{source}")
display(df.limit(10))
display(df.count())

# COMMAND ----------

#generating the required hash keys
for i in dict_resv.items():
    df = df.withColumn(i[0],sha2(concat_ws("", *i[1]), 256))

# COMMAND ----------



# COMMAND ----------

source_df = df.withColumn("Source",lit(source)).withColumn("Source",lit(source)) \
.withColumn("LoadDate",current_timestamp())
all_columns = source_df.columns

# COMMAND ----------

hub_resv = source_df.select(*hub_columns)
display(hub_resv)
hub_resv.write.format("delta").mode("append").save("/mnt/dpa/idw/silver/Hubs/HUB_Reservation_test")

# COMMAND ----------

#SAT_RESV - need to create HAshDIFF column
sat_resv_columns = list(set(all_columns + primary_key_sat_resv))
sat_resv= source_df.select(*sat_resv_columns)
sat_resv_df = sat_resv.drop(*rental_resv_columns)
sat_resv_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(f"{satellite_path}/SAT_Resv_test")

# COMMAND ----------

#SAT_RENTALRESV - need to add HashDIFF column
sat_rental_resv_columns = ["HK_Reservation","HK_Customer","HK_PickupLocation","HK_ReturnLocation","HK_RentalReservation","RES_VER"]+common_cols
sat_rental_resv_df = source_df.select(*sat_rental_resv_columns)
display(sat_rental_resv_df)
sat_rental_resv_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(f"{satellite_path}/SAT_Rental_Resv_test")

# COMMAND ----------

#Link_RentalReservation
link_rental_resv_cols = ["HK_RentalReservation","HK_Reservation","HK_Customer","HK_PickupLocation","HK_ReturnLocation"]+common_cols
link_rental_resv_df = source_df.select(*link_rental_resv_cols)
display(link_rental_resv_df)
link_rental_resv_df.write.format("delta").mode("append").save(f"{link_path}Link_RentalReservation_test")

# COMMAND ----------

display(df)
