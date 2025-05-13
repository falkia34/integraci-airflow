from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from datetime import timedelta
from common.application_tasks import *
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="migrate_gitlab_to_directus",
    default_args=default_args,
    description="Migrasi data dari Gitlab ke Directus",
    schedule_interval="@daily",
    start_date=days_ago(1),
    max_active_runs=1, # Memastikan hanya satu run aktif agar tidak ada tumpang tindih
    catchup=False, # Tidak melakukan catchup 
    tags=["aditya", "modular"],
)
def migrate_app(): 
    extract_dir = Variable.get("extract_dir",default_var = '/home/aditya/coding-aditya/aditya_airflow/data/extract')
    transform_dir = Variable.get("transform_dir",'/home/aditya/coding-aditya/aditya_airflow/data/transform')
    directus_conn_hook = PostgresHook(postgres_conn_id="directus")
    num_workers = int(Variable.get("num_workers", default_var=3 )) # Mengambil jumlah worker dari Airflow Variable, default 3


    query_stmt = """SELECT
                        s.service_name,
                        a.application,
                        st.service_type
                    from 
                        public.cicd_application_service s
                    full JOIN
                        public.cicd_application a
                    on
                        s.application = a.id
                    full JOIN
                        public.cicd_application_servicetype st
                    on
                        s.service_type = st.id
                    where
                        s.service_name is not null
                        and a.application is not null
                        and st.service_type is not null
                    ORDER BY
                        2,3;
                """

    extract = extract_data_from_db(hook=directus_conn_hook, target_dir=extract_dir, query_stmt=query_stmt)
    data_splited = data_split_worker.override(task_id = 'data_split_worker')(extract_id="extract_data_from_db",target_dir= transform_dir, num_workers=num_workers)
    extracted_workers = []
    for i in range(num_workers):
        extracted_worker = extract_data_from_api.override(task_id = f'extract_data_from_api_{i}')(target_dir=extract_dir, chunk = i)
        extracted_workers.append(extracted_worker)

    


    # Chain the tasks
    
    for i in range(num_workers):
        chain(extract,data_splited, extracted_workers[i])

migrate_app_dag = migrate_app()
