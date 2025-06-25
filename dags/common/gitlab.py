from airflow.decorators import task
from airflow.models import TaskInstance
from datetime import datetime
import json
import polars as pl
import requests
import os


@task(task_id='extract_lang_from_gitlab_api', task_display_name='Extract Languages from GitLab API')
def extract_lang_from_gitlab_api(
    file_path: str,
    extract_dir: str,
    api_url: str = "https://gitlab.com/api/v4/projects/divistant.com%2Fdemo%2Finternal-demo%2Fapplications%2F{application}%2F{service_type}%2F{service_name}/languages",
    ti: TaskInstance = None,
):
    """
    Extract languages data from GitLab API.

    Args:
        file_path: Path to the file containing the data to be processed.
        extract_dir: Directory to save the extracted data.
        api_url: API URL template to fetch data from.
        ti: Task instance for XCom communication.
    """

    token = os.getenv("GITLAB_TOKEN")
    headers = {"PRIVATE-TOKEN": token}

    chunk_num = ti.map_index
    print(f"Chunk number: {chunk_num}")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if chunk_num is -1:
        result_path = f"{extract_dir}/gitlab_api_data_{timestamp}.parquet"
    else:
        result_path = f"{extract_dir}/gitlab_api_data_{chunk_num}_{timestamp}.parquet"
    print(f'Result path: {result_path}')

    df = pl.scan_parquet(file_path)
    df = df.collect()

    data_api = []

    for row in df.iter_rows(named=True):
        application = row["application"]
        service_name = row["service_name"]
        service_type = row["service_type"]

        url = api_url.format(
            application=application,
            service_type=service_type,
            service_name=service_name,
        )
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            print(
                f"Data for {application}/{service_name}/{service_type}: {data}"
            )

            if data != {}:
                new_data = {
                    "application": application,
                    "application_id": row["application_id"],
                    "service_name": service_name,
                    "service_type": service_type,
                    "service_type_id": row["service_type_id"],
                    "languages": json.dumps(data, separators=(',', ':')).lower()
                }
                data_api.append(new_data)
        else:
            print(
                f"Failed to retrieve data for {application}/{service_name}/{service_type}. Status code: {response.status_code}"
            )

    df = pl.DataFrame(data_api)
    print(f"Data from GitLab API: {df}")
    df.write_parquet(result_path, compression='uncompressed')
    print(f"Data extracted from GitLab API and saved to {result_path}")

    return result_path
