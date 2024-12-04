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
##from SQL_QUERY.incremental_raw_query import *
##from SQL_QUERY.dimension_fact import *


def export_sql_to_s3():
    # Set up the SQL and S3 hooks
    sql_hook = MsSqlHook(mssql_conn_id='sql_server_cred')
    s3_hook = S3Hook(aws_conn_id='s3_cred')
    bucket_name = 'full-dump-data-stg'
    
    # Establish connection to the SQL Server database
    sql_conn = sql_hook.get_conn()
    sql_cursor = sql_conn.cursor()

    try:
        # Fetch list of tables from the database
        sql_cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='dbo'")
        tables = sql_cursor.fetchall()

        for table in tables:
            table_name = table[0]
            logging.info(f"Processing table: {table_name}")
            
            # Fetch data from the current table
            sql_cursor.execute(f"SELECT * FROM [WideWorldImporters].dbo.{table_name}")
            
            # Fetch all data and convert it to a pandas DataFrame
            try:
                df = pd.DataFrame(sql_cursor.fetchall(), columns=[col[0] for col in sql_cursor.description])
            except Exception as e:
                logging.error(f"Error while converting SQL data to DataFrame for table {table_name}: {e}")
                continue  # Skip to the next table

            # Handle invalid datetime values
            for column in df.select_dtypes(include=['datetime64']).columns:
                df[column] = pd.to_datetime(df[column], errors='coerce')

            # Generate the file name with a timestamp
            file_name = f"{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            file_path = f"/tmp/{file_name}"
            
            # Save DataFrame to CSV file
            try:
                df.to_csv(file_path, index=False)
            except Exception as e:
                logging.error(f"Error while writing {table_name} to CSV: {e}")
                continue

            # Upload CSV to S3
            try:
                s3_hook.load_file(filename=file_path, bucket_name=bucket_name, key=f"data/{file_name}", replace=True)
                logging.info(f"Uploaded {file_name} to S3 bucket {bucket_name}")
            except Exception as e:
                logging.error(f"Error while uploading {file_name} to S3: {e}")
            
            # Remove the temporary CSV file after upload
            os.remove(file_path)

    except Exception as e:
        logging.error(f"An error occurred during the SQL to S3 export process: {e}")
    
    finally:
        # Clean up the SQL connection
        sql_cursor.close()
        sql_conn.close()


def delete_existing_files():

    try:
             
        s3_hook = S3Hook(aws_conn_id='s3_cred')
       
        keys = s3_hook.list_keys(bucket_name='full-dump-data-stg', prefix='data/')
       
        if keys:
            s3_hook.delete_objects(bucket='full-dump-data-stg', keys=keys)
    except Exception as e:
        logging.error(f"Error deleting file: {e}")


def truncate_staging_tables():
    redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
    redshift_conn = redshift_hook.get_conn()
    redshift_cursor = redshift_conn.cursor()
    
    try:
        redshift_cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'staging'")
        tables = redshift_cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            redshift_cursor.execute(f"TRUNCATE TABLE staging.{table_name}")
            logging.info(f"Truncated table staging.{table_name}")
        
        redshift_conn.commit()
    except Exception as e:
        logging.error(f"Error truncating tables in staging schema: {e}")
    finally:
        redshift_cursor.close()
        redshift_conn.close()


def load_s3_to_redshift():
    s3_hook = S3Hook(aws_conn_id='s3_cred')
    redshift_hook = PostgresHook(postgres_conn_id='redshift_connection')
    bucket_name = 'full-dump-data-stg'
    redshift_conn = redshift_hook.get_conn()
    redshift_cursor = redshift_conn.cursor()

    # List all files in the S3 bucket under the 'data/' prefix
    s3_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix='data/')
    csv_files = [key for key in s3_keys if key.endswith('.csv')]

    for csv_file in csv_files:
        # Extract the table name correctly from the filename
        table_name = os.path.basename(csv_file).rsplit('_', 1)[0]
        logging.info(f"Loading data for table: {table_name} from file: {csv_file}")

        s3_path = f"s3://{bucket_name}/{csv_file}"
        copy_query = f"""
        COPY staging.{table_name}
        FROM '{s3_path}'
        IAM_ROLE 'arn:aws:iam::443370691446:role/service-role/AmazonRedshift-CommandsAccessRole-20241123T164052'
        FORMAT AS CSV
        IGNOREHEADER 1;
        
        """
        try:
            redshift_cursor.execute(copy_query)
            redshift_conn.commit()
            logging.info(f"Data loaded into staging.{table_name} from {s3_path}")
        except Exception as e:
            logging.error(f"Error loading data into staging.{table_name} from {s3_path}: {e}")
            logging.error(f"Query: {copy_query}")
            redshift_conn.rollback()


    redshift_cursor.close()
    redshift_conn.close()


local_tz = pendulum.timezone("Europe/London")
start_date=pendulum.yesterday(tz=local_tz)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

  
with DAG('Datawarehouse_project_elt_pipline', default_args=default_args,start_date= start_date,schedule_interval='13 23 * * *') as dag:
    truncate_staging_tables = PythonOperator(
        task_id='truncate_staging_tables_data',
        python_callable=truncate_staging_tables,
    )


    task_delete_existing_files_from_S3 = PythonOperator(
        task_id="delete_existing_files_from_S3",
        python_callable=delete_existing_files
    )

    export_sql_to_s3_task = PythonOperator(
        task_id='export_sql_to_s3',
        python_callable=export_sql_to_s3,
    )

    load_s3_to_redshift_task = PythonOperator(
        task_id='load_s3_to_redshift',
        python_callable=load_s3_to_redshift,
    )

    

truncate_staging_tables  >> task_delete_existing_files_from_S3 >> export_sql_to_s3_task >> load_s3_to_redshift_task 