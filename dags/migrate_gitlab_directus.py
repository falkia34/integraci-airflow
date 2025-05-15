from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from datetime import timedelta
from common.application_tasks import *
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook  
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="migrate_gitlab_to_directus_v2",
    default_args=default_args,
    description="Migrasi data dari Gitlab ke Directus",
    schedule="0 0 * * * *", # Setiap hari jam 00:00
    start_date=datetime.now() - timedelta(days=1),
    max_active_runs=1, # Memastikan hanya satu run aktif agar tidak ada tumpang tindih
    catchup=False, # Tidak melakukan catchup 
    tags=["aditya", "modular"],
)
def migrate_app(): 
    """
    DAG untuk memigrasi data dari Gitlab ke Directus
    """
    
    extract_dir = Variable.get("extract_dir",default =  f'{os.getcwd()}/data/extract')
    transform_dir = Variable.get("transform_dir",default= f'{os.getcwd()}/data/transform')
    directus_conn_hook = PostgresHook(postgres_conn_id="directus")
    num_workers = int(Variable.get("num_workers", default=3 )) # Mengambil jumlah worker dari Airflow Variable, default 3
    target_table = Variable.get("target_table", default = 'dev.cicd_app_service_languages_wh')
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
    query_stmt = Variable.get("query_stmt", default = query_stmt)

    # extract = extract_data_from_db(hook=directus_conn_hook, target_dir=extract_dir, query_stmt=query_stmt)
    # data_splited = data_split_worker.override(task_id = 'data_split_worker')(extract_id="extract_data_from_db",target_dir= transform_dir, num_workers=num_workers)
    # extracted_workers = []
    # for i in range(num_workers):
    #     extracted_worker = extract_data_from_api.override(task_id = f'extract_data_from_api_{i}')(target_dir=extract_dir, chunk = i)
    #     extracted_workers.append(extracted_worker)

    # upserted = upsert_with_mogrify.override(task_id = 'upsert_with_mogrify')(hook=directus_conn_hook, num_workers=num_workers, target_table=target_table)
    # deleted = delete_files.override(task_id = 'delete_from_table')()


    extract = extract_data_from_db(hook=directus_conn_hook, target_dir=extract_dir, query_stmt=query_stmt)
    data_splited = data_split_worker.override(task_id = 'data_split_worker')(extract_path=extract,target_dir= transform_dir, num_workers=num_workers)
    extracted_workers = extract_data_from_api.partial( target_dir=extract_dir).expand(chunk = data_splited)
    upserted = upsert_with_mogrify(hook=directus_conn_hook, files_path = extracted_workers, target_table=target_table)
    upserted >> delete_files()

    # for i in range(num_workers):
    #     chain(extract,data_splited, extracted_workers[i], upserted,deleted)

migrate_app_dag = migrate_app()
