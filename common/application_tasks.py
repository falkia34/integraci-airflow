from airflow.decorators import task
from datetime import datetime
import json
import polars as pl
import requests
import os
import pandas as pd

@task
def extract_data_from_db(hook,target_dir=None,query_stmt = None, ti=None):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = f"{target_dir}/extract_db_data_{timestamp}.parquet"

    df = pl.read_database(query_stmt, connection=hook.get_conn())
    print(f"Data from DB: {df}")
    df.write_parquet(file_path, compression = None)
    ti.xcom_push(key="extract_path", value=file_path)
    ti.xcom_push(key = 'extract_num_rows', value = df.height) 
    print(f"Data extracted and saved to {file_path}")


@task
def data_split_worker(extract_id,target_dir, num_workers,ti=None):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    extract_path = ti.xcom_pull(task_ids=extract_id, key="extract_path")
    num_rows = ti.xcom_pull(task_ids=extract_id, key="extract_num_rows")
    df = pl.scan_parquet(extract_path)
    # Split the DataFrame into chunks
    chunk_size = num_rows // num_workers
    
    
    for i in range(num_workers):
        file_path = f"{target_dir}/data_split_worker_{i}_{timestamp}.parquet"
        start = i * chunk_size
        end = (i + 1) * chunk_size if i != num_workers - 1 else num_rows
        chunk = df.slice(start, end - start)
        chunk.sink_parquet(file_path, compression = 'uncompressed')
        ti.xcom_push(key=f"chunk_{i}_path", value=file_path)
    
    print(f"Data split into {num_workers} chunks and saved to {target_dir}")

@task
def extract_data_from_api(target_dir=None, 
                          chunk = None, 
                          api_url="https://gitlab.com/api/v4/projects/divistant.com%2Fdemo%2Finternal-demo%2Fapplications%2F{application}%2F{service_type}%2F{service_name}/languages", headers=None, params=None, ti=None):
    
    token = os.getenv("GITLAB_TOKEN")
    print(f"Token: {token}")


    headers = {"PRIVATE-TOKEN":token}
    
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = f"{target_dir}/extract_api_data_{chunk}_{timestamp}.parquet"
    chunk_path = ti.xcom_pull(key = f"chunk_{chunk}_path")
    chunk_df = pl.scan_parquet(chunk_path)
    chunk_df = chunk_df.collect()

    data_api = []
    

    for row in chunk_df.iter_rows(named=True):
        application = row["application"]
        service_name = row["service_name"]
        service_type = row["service_type"]

        url = api_url.format(application=application, service_type=service_type, service_name=service_name)
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            print(f"Data for {application}/{service_name}/{service_type}: {data}")
            if data != {}:
                new_data = {
                    "application": application,
                    "service_name": service_name,
                    "service_type": service_type,
                    "data": json.dumps(data)
                }
                data_api.append(new_data)
                    
        else:
            print(f"Failed to retrieve data for {application}/{service_name}/{service_type}. Status code: {response.status_code}")
    
    df = pl.DataFrame(data_api)
    print(f"Data from API: {df}")   
    df.write_parquet(file_path, compression = None)
    ti.xcom_push(key=f"extract_api_data_{chunk}_path", value=file_path)
    print(f"Data extracted from API and saved to {file_path}")
    

 # collect all the data from the chunks and load to db   
@task
def load_data_to_db(hook, num_workers=None, ti=None):
    file_paths = []
    for i in range(num_workers):
        file_path = ti.xcom_pull(key=f"extract_api_data_{i}_path")
    
    data = pl.read_parquet(file_path).to_dicts()

    # load to postgres with upsert batch
    # create a connection to the database
    conn = hook.get_conn()
    

    
        