from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from common.tasks import extract, transform, load, delete_file
from airflow.models import Variable

size = int(Variable.get("size"))  # Default size is 5 MB
target_dir = Variable.get("target_dir")
extract_dir = Variable.get("extract_dir")
transform_dir = Variable.get("transform_dir")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="migrate_app",
    default_args=default_args,
    description="Contoh modular DAG pakai TaskFlow API",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["aditya", "modular"],
) as dag:

    data = extract(size,extract_dir)
    transformed = transform(data, transform_dir)
    loaded = load(transformed, target_dir)
    delete_file([transformed, data])
