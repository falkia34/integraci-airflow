from airflow.decorators import dag
from airflow.utils.dates import days_ago
from datetime import timedelta
from common.tasks import extract, transform, load, delete_file
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="migrate_app",
    default_args=default_args,
    description="Contoh modular DAG pakai TaskFlow API",
    schedule_interval="@daily",
    start_date=days_ago(1),
    max_active_runs=1, # Memastikan hanya satu run aktif agar tidak ada tumpang tindih
    catchup=False, # Tidak melakukan catchup 
    tags=["aditya", "modular"],
)
def migrate_app():
    size = int(Variable.get("size",default_var=5))  # Default size is 5 MB
    target_dir = Variable.get("target_dir")
    extract_dir = Variable.get("extract_dir")
    transform_dir = Variable.get("transform_dir")
    tables = Variable.get("tables", deserialize_json=True, default_var=[{}])

    for i in tables:
        extract_id = f'extract_override_{i}'
        transform_id = f'transform_override_{i}'
        load_id = f'load_override_{i}'

        # Override task_id for each table
        data = extract.override(task_id = extract_id )(size, extract_dir)
        transformed = transform.override(task_id = transform_id)(extract_id,transform_dir)
        loaded = load.override(task_id = load_id)(transform_id,target_dir)
        deleted = delete_file.override(task_id = f'delete_override_{i}')(extract_id,transform_id)

        data >> transformed >> loaded >> deleted

migrate_app_dag = migrate_app()
