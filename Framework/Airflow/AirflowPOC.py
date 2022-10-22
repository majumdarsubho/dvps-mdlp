# Databricks notebook source
import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine

#extract tasks
@task()
def get_src_tables():
    hook = MsSqlHook(mssql_conn_id="sqlserver")
    sql=""" select t.CopyActivitySetupKey as CopyActivitySetupKey from dbo.Notebookexecutionplan """
    df = hook.get_pandas_df(sql)
    print(df)
    tbl_dict = df.to_dict('dict')
    return tbl_dict
# get the value of tbl_dict from the main pipeline notebook
@task()
def load_src_metadata(tbl_dict: dict):
    hook = MsSqlHook(mssql_conn_id="sqlserver")
    all_tbl_name = []
    for k, v in tbl_dict['CopyActivitySetupKey'].items():
        all_tbl_name.append(v)
        sql = f'select * as table_name from dbo.CopyActivitySetup where CopyActivitySetupKey = {v}'
        param_dict = df.to_dict('dict')
    return all_tbl_name

