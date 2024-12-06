
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging
import os, time, pendulum
from datetime import datetime
import os
from sql_query.incremental_raw_query import *
from sql_query.dimension_fact import *




local_tz = pendulum.timezone("Europe/London")
start_date=pendulum.yesterday(tz=local_tz)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

  
with DAG('Raw_Dim_Fact_ETL', default_args=default_args,start_date= start_date,schedule_interval='59 16 * * *') as dag:

    staging_raw_zone_task = SQLExecuteQueryOperator(
        task_id='staging_to_raw_zone_task_CDC',
        conn_id="redshift_connection",
        autocommit=True,
        sql=[
            truncate_scm_raw_zone_cities ,
            truncate_scm_raw_zone_countries ,
            truncate_scm_raw_zone_customercategories ,
            truncate_scm_raw_zone_customers ,
            truncate_scm_raw_zone_orderlines ,
            truncate_scm_raw_zone_orders ,
            truncate_scm_raw_zone_paymentmethods ,
            truncate_scm_raw_zone_people ,
            truncate_scm_raw_zone_stateprovinces ,
            truncate_scm_raw_zone_stockitems ,
            truncate_scm_raw_zone_suppliers ,
            truncate_scm_raw_zone_suppliertransactions ,
            truncate_scm_raw_zone_transactiontypes ,
            insert_scm_raw_zone_cities ,
            insert_scm_raw_zone_countries ,
            insert_scm_raw_zone_customercategories ,
            insert_scm_raw_zone_customers ,
            insert_scm_raw_zone_orderlines ,
            insert_scm_raw_zone_orders ,
            insert_scm_raw_zone_paymentmethods ,
            insert_scm_raw_zone_people ,
            insert_scm_raw_zone_stateprovinces ,
            insert_scm_raw_zone_stockitems ,
            insert_scm_raw_zone_suppliers ,
            insert_scm_raw_zone_suppliertransactions ,
            insert_scm_raw_zone_transactiontypes ,
        
        ]
    )


    dimension_fact_data_mov = SQLExecuteQueryOperator(
        task_id='dimension_fact_data',
        conn_id="redshift_connection",
        autocommit=True,
        sql=[
            truncate_processing_zone_Dim_Customer ,
            truncate_processing_zone_Dim_Location ,
            truncate_processing_zone_Dim_Product ,
            truncate_processing_zone_Dim_Datetime ,
            truncate_processing_zone_dim_Supplier ,
            truncate_processing_zone_dim_transaction_type ,
            truncate_processing_zone_dim_payment_method ,
            truncate_processing_zone_Fact_Sales ,
            truncate_processing_zone_FactSupplier_Transactions ,
            insert_processing_zone_Dim_Customer ,
            insert_processing_zone_Dim_Location ,
            insert_processing_zone_Dim_Product ,
            insert_processing_zone_Dim_Datetime ,
            insert_processing_zone_dim_Supplier ,
            insert_processing_zone_dim_transaction_type ,
            insert_processing_zone_dim_payment_method ,
            insert_processing_zone_Fact_Sales ,
            insert_processing_zone_FactSupplier_Transactions ,
        
        ]
    )



    

    staging_raw_zone_task >> dimension_fact_data_mov
