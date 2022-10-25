# Databricks notebook source
import datetime as dt
from pysparks.sql import *

# COMMAND ----------

def effectiveTo(dfData, businessKey, businessKeyName):
    dfFilter = dfdata[dfdata[businessKeyName] == businessKey]
    maxDate = dfFilter[EffectiveFrom].max
    return maxDate

# COMMAND ----------

def isActive(effectiveFrom, effectiveTo):
    return date.today() >= effectiveFrom && date.today() < effectiveTo

# COMMAND ----------

def zeroPad(string, number):
    return string.zfill(number)

# COMMAND ----------

def TmStmp(date,time):
    return dt.datetime.combine(date,time)

# COMMAND ----------

def RoundDown15M()

# COMMAND ----------

def GetValue(baseTable, valuePath, rowIdentifier, rowIdentifierField, rowIdentifierName, effectiveTime = datetime.now()):
    query = "from " + baseTable + "\n"
    if valuePath.contains("|"):
        pathSplit = valuePath.split("|")
        for subPath in pathSplit:
            if subPath.contains("="):
                query = query+"JOIN " + subPath.split("=")[1].split(".")[0] + " ON " + subPath + "\n"
            else:
                query = "Select " + subPath + "\n" + query + "Where " + effectiveTime + " >= " + subPath.split(".")[0]+".EffectiveFrom AND "+effectiveTime+" < "+subPath.split(".")[0]+".EffectiveTo\n"
    else:
        query = "SELECT "+valuePath+"\n"+query+"WHERE "+effectiveTime+" >= "+valuePath.split(".")[0]+".EffectiveFrom AND "+effectiveTime+" < "+valuePath.split(".")[0]+".EffectiveTo\n"
    query = query+"AND "+baseTable+"."+rowIdentifierField+" = "+rowIdentifier
    return spark.SQL(query)

# COMMAND ----------


