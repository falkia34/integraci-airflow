from airflow.decorators import task
import pandas as pd
from datetime import datetime

@task
def extract(size,extract_dir):
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
    return extract_path

@task
def transform(extract_file_path, transform_dir):
    data = pd.read_csv(extract_file_path)
    # Transforming data
    data['name_upper'] = data['name'].str.upper()
    # loading data to file system
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{transform_dir}/transformed_data_{timestamp}.csv"
    data.to_csv(file_path, index=False)
    print(f"Transforming data: {data.shape[0]} rows successfully transformed.")
    return file_path

@task
def load(source_path, target_dir):
    # read file_path
    data = pd.read_csv(source_path)
    # loading data to file system
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{target_dir}/loaded_data_{timestamp}.csv"
    data.to_csv(file_path, index=False)
    print(f"Loading data: {data.shape[0]} rows successfully loaded.")
    print(f"Data saved to {file_path}")
    return file_path

@task
def delete_file(file_path):
    import os
    for i in file_path:
        print(f"Deleting file: {i}")
        try:
            os.remove(i)
            print(f"File {i} deleted successfully.")
        except OSError as e:
            print(f"Error deleting file {i}: {e}")
   


