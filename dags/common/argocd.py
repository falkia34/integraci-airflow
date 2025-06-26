from airflow.decorators import task
from airflow.models import TaskInstance
from datetime import datetime
import json
import polars as pl
import requests
import os
import uuid


@task(task_id='extract_status_from_argocd_api', task_display_name='Extract Deployment Status from ArgoCD API')
def extract_status_from_argocd_api(
    file_path: str,
    envs_path: str,
    extract_dir: str,
    api_url: str = "https://gitops-temp.divistant.net/api/v1/applications/{service_type}-{service_name}-{env}-{cluster}",
    ti: TaskInstance = None,
):
    """
    Extract deployment status data from ArgoCD API.

    Args:
        file_path: Path to the file containing the data to be processed.
        envs_path: Path to the file containing the environments.
        extract_dir: Directory to save the extracted data.
        api_url: API URL template to fetch data from.
        ti: Task instance for XCom communication.
    """

    token = os.getenv("ARGOCD_TOKEN")
    headers = {"Authorization": f"Bearer {token}"}

    chunk_num = ti.map_index
    print(f"Chunk number: {chunk_num}")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if chunk_num is -1:
        result_path = f"{extract_dir}/argocd_api_data_{timestamp}.parquet"
    else:
        result_path = f"{extract_dir}/argocd_api_data_{chunk_num}_{timestamp}.parquet"
    print(f'Result path: {result_path}')

    df_envs = pl.scan_parquet(envs_path)
    environments = df_envs.collect().get_column("environment").to_list()

    df = pl.scan_parquet(file_path)
    df = df.collect()

    data_api = []
    cluster = "drc"

    for row in df.iter_rows(named=True):
        service_name = row["service_name"]
        service_type = row["service_type"]

        statuses = {}

        for env in environments:
            url = api_url.format(
                service_type=service_type,
                service_name=service_name,
                env=env,
                cluster=cluster,
            )
            response = requests.get(url, headers=headers)
            timestamp = datetime.now()

            if response.status_code == 200:
                data = response.json()
                data = data.get('status', {}).get(
                    'health', {}).get('status', 'Unknown')
                print(
                    f"Data for {service_name}/{service_type}/{env}/{cluster}: {data}"
                )

                if data == "Healthy":
                    data = "healthy"
                elif data == "Suspended" or data == "Progressing" or data == "Missing" or data == "Degraded":
                    data = "degraded"
                else:
                    data = "unknown"

                statuses[env] = data
            else:
                print(
                    f"Failed to retrieve data for {service_name}/{service_type}/{env}/{cluster}. Status code: {response.status_code}"
                )
                statuses[env] = "unknown"

        new_data = {
            "id": str(uuid.uuid4()),
            "application_id": row["application_id"],
            "service_id": row["service_id"],
            "service_name": service_name,
            "service_type": service_type,
            "service_type_id": row["service_type_id"],
            "status": json.dumps(statuses, separators=(',', ':')).lower(),
            "date_created": timestamp.isoformat(),
        }
        data_api.append(new_data)

    df = pl.DataFrame(data_api)
    print(f"Data from ArgoCD API: {df}")
    df.write_parquet(result_path, compression='uncompressed')
    print(f"Data extracted from ArgoCD API and saved to {result_path}")

    return result_path
