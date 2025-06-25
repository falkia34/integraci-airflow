from airflow.decorators import task
from datetime import datetime
import pandas as pd
import time

@task
def extract(size,extract_dir,ti = None):
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Generate a DataFrame with random data
    num_rows = int(size * 1024 * 1024 / 8)
    # Approximate number of rows for 10 MB (assuming 8 bytes per value)
    dummy_data = pd.DataFrame({
        'id': range(num_rows),
        'name': [f"string_{i}" for i in range(num_rows)]
    })
    extract_path = f"{extract_dir}/dummy_data_{timestamp}.csv"
    dummy_data.to_csv(extract_path, index=False)
    ti.xcom_push(key='extract_file_path', value=extract_path)
    print(f"Extracting data: {dummy_data.shape[0]} rows successfully extracted.")


@task
def transform(extract_id,transform_dir,ti = None):
    data = pd.read_csv(ti.xcom_pull(key='extract_file_path',task_ids=extract_id))
    # Transforming data
    data['name_upper'] = data['name'].str.upper()
    # loading data to file system
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{transform_dir}/transformed_data_{timestamp}.csv"
    data.to_csv(file_path, index=False)
    ti.xcom_push(key='transform_file_path', value=file_path)
    print(f"Transforming data: {data.shape[0]} rows successfully transformed.")
    

@task
def load(transform_id,target_dir,ti = None):
    # read file_path
    data = pd.read_csv(ti.xcom_pull(key='transform_file_path',task_ids=transform_id))
    # loading data to file system
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{target_dir}/loaded_data_{timestamp}.csv"
    data.to_csv(file_path, index=False)
    print(f"Loading data: {data.shape[0]} rows successfully loaded.")
    print(f"Data saved to {file_path}")

@task
def delete_file(extract_id,transform_id,ti = None):
    import os
    file_path = [ti.xcom_pull(key='transform_file_path',task_ids=transform_id), 
                 ti.xcom_pull(key='extract_file_path',task_ids=extract_id)]
    for i in file_path:
        print(f"Deleting file: {i}")
        try:
            os.remove(i)
            print(f"File {i} deleted successfully.")
        except OSError as e:
            print(f"Error deleting file {i}: {e}")
   


