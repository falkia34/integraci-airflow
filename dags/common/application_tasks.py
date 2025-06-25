from airflow.decorators import task
from datetime import datetime
import json
import polars as pl
import requests
import os
from airflow.models import XCom

@task
def extract_data_from_db(hook,target_dir=None,query_stmt = None, ti=None):
    print(f'target_dir: {target_dir}')
    """
    Extract data from a database and save it to a file.
    Args:
        hook: Database connection hook.
        target_dir: Directory to save the extracted data.
        query_stmt: SQL query to extract data.
        ti: Task instance for XCom communication.
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = f"{target_dir}/extract_db_data_{timestamp}.parquet"

    df = pl.read_database(query_stmt, connection=hook.get_conn())
    print(f"Data from DB: {df}")
    df.write_parquet(file_path, compression = None)
    ti.xcom_push(key = 'extract_num_rows', value = df.height) 
    print(f"Data extracted and saved to {file_path}")
    return file_path


@task
def data_split_worker(extract_path,target_dir, num_workers,ti=None):
    """
    Split the DataFrame into chunks for parallel processing.
    Args:
        extract_id: Task ID of the data extraction task.
        target_dir: Directory to save the split data.
        num_workers: Number of workers to split the data into.
        ti: Task instance for XCom communication.
    """

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    num_rows = ti.xcom_pull(task_ids='extract_data_from_db', key="extract_num_rows")
    df = pl.scan_parquet(extract_path)
    # Split the DataFrame into chunks
    chunk_size = num_rows // num_workers
    
    all_file_paths = [extract_path]
    
    all_chunks_path = []

    for i in range(num_workers):
        file_path = f"{target_dir}/data_split_worker_{i}_{timestamp}.parquet"
        start = i * chunk_size
        end = (i + 1) * chunk_size if i != num_workers - 1 else num_rows
        chunk = df.slice(start, end - start)
        chunk.sink_parquet(file_path, compression = 'uncompressed')
        all_chunks_path.append(file_path)
        all_file_paths.append(file_path)
    
    ti.xcom_push(key="all_file_paths", value=all_file_paths)
    print(f"Data split into {num_workers} chunks and saved to {target_dir}")

    return all_chunks_path

@task
def extract_data_from_api(target_dir=None, 
                          chunk = None, 
                          api_url="https://gitlab.com/api/v4/projects/divistant.com%2Fdemo%2Finternal-demo%2Fapplications%2F{application}%2F{service_type}%2F{service_name}/languages", 
                          ti=None):
    '''
    Extract data from API and save it to a file.
    Args:
        target_dir: Directory to save the extracted data.
        chunk: Chunk number for the current worker.
        api_url: API URL to fetch data from.
        headers: Headers for the API request.
        params: Parameters for the API request.
        ti: Task instance for XCom communication.
    '''
    num_chunk = ti.map_index
    print(f"Chunk number: {num_chunk}")
    token = os.getenv("GITLAB_TOKEN")
    print(f"Token: {token}")
    headers = {"PRIVATE-TOKEN":token}

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = f"{target_dir}/extract_api_data_{num_chunk}_{timestamp}.parquet"
    chunk_path = chunk
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
                    "script_languages": json.dumps(data)
                }
                data_api.append(new_data)
                    
        else:
            print(f"Failed to retrieve data for {application}/{service_name}/{service_type}. Status code: {response.status_code}")
    
    df = pl.DataFrame(data_api)
    print(f"Data from API: {df}")   
    df.write_parquet(file_path, compression = None)
    # all_file_paths = ti.xcom_pull(key="all_file_paths", map_index=-1)
        # print('all_file_path= ',all_file_paths)
        # all_file_paths.append(file_path)
    # ti.xcom_push(key="all_file_paths", value=all_file_paths, map_index=-1)
    print(f"Data extracted from API and saved to {file_path}")
    return file_path
    



@task
def upsert_with_mogrify(hook,files_path = None, target_table = None , ti=None):
    """
    Perform upsert operation using mogrify for efficient batch insertion.

    Args:
        hook: Database connection hook.
        num_workers: Number of workers used for data extraction.
        target_table: Target table for upsert operation.
        ti: Task instance for XCom communication.

    Steps:
        1. Collect all file paths from XCom for the extracted API data.
        2. Read the data from the files into a single DataFrame.
        3. Convert the DataFrame into a list of dictionaries for insertion.
        4. Use mogrify to prepare a batch SQL query for upsert.
        5. Execute the query and commit the transaction.
    """
    # Read the data from the files into a single DataFrame
    df = pl.read_parquet(files_path)
    data = df.to_dicts()
    conn = hook.get_conn()

    print(f"Data to be inserted: {data}")


    # Prepare one large SQL string
    with conn.cursor() as cur:
        values_str = ",".join(
            cur.mogrify("(%(application)s, %(service_name)s, %(service_type)s, %(script_languages)s )", row).decode("utf-8")
            for row in data
        )

        query = f"""
            INSERT INTO {target_table} ( 
                application,
                service_name,
                service_type,
                script_languages
            )
            VALUES {values_str}
            ON CONFLICT (application, service_name, service_type) DO UPDATE
            SET script_languages = EXCLUDED.script_languages;
        """
        cur.execute(query)
    conn.commit()
    

@task
def delete_files(ti=None):
    data_dir = os.path.join(os.getcwd(), "data")
    
    # delete all parquet files in the data directory
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)
                os.remove(file_path)
                print(f"Deleted file: {file_path}")
