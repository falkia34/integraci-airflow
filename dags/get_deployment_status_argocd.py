from airflow.decorators import dag
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dags.common.file import create_dirs, delete_dirs
from dags.common.database import extract_data_from_db, update_data_on_db
from dags.common.argocd import extract_status_from_argocd_api
from dags.common.dataframe import split_dataframe
import os


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="get_deployment_status_argocd",
    dag_display_name="Get Deployment Status from ArgoCD",
    default_args=default_args,
    description="Ambil status deployment dari ArgoCD dan simpan ke Directus",
    schedule="0 * * * *",  # Setiap jam
    start_date=datetime.now() - timedelta(days=1),
    max_active_runs=1,  # Memastikan hanya satu run aktif agar tidak ada tumpang tindih
    catchup=False,
    tags=["integraci", "argocd", "directus", "status"],
)
def get_deployment_status_argocd():
    """
    DAG untuk mengambil status deployment dari ArgoCD dan menyimpannya ke Directus.
    """

    # Mengambil jumlah worker dari Airflow Variable, default 3
    num_workers = int(Variable.get("num_workers", default=3))
    dag_id = "{{ dag.dag_id }}"
    extract_dir = str(Variable.get(
        "extract_dir", default=f'{os.getcwd()}/data/extract'
    )) + "/" + dag_id
    transform_dir = str(Variable.get(
        "transform_dir", default=f'{os.getcwd()}/data/transform'
    )) + "/" + dag_id

    directus_conn_hook = PostgresHook(postgres_conn_id="directus")
    extract_vars_query = """
        SELECT
            a.application,
            a.id AS application_id,
            s.id AS service_id,
            s.service_name,
            st.service_type,
            st.id AS service_type_id
        FROM 
            public.cicd_application_service s
        FULL JOIN
            public.cicd_application a
        ON
            s.application = a.id
        FULL JOIN
            public.cicd_application_servicetype st
        ON
            s.service_type = st.id
        WHERE
            s.service_name is not null
            and a.application is not null
            and st.service_type is not null
        ORDER BY
            1, 3;
    """
    extract_envs_query = """
        SELECT
            environment
        FROM
            public.cicd_environment;
    """
    update_deployment_status_query = """
        UPDATE public.cicd_application_service
        SET status = %(status)s
        WHERE application = %(application_id)s
        AND service_name = %(service_name)s
        AND service_type = %(service_type_id)s;
    """
    insert_deployment_status_query = """
        INSERT INTO cicd_application_service_status (id, service_id, status, date_created)
        VALUES (%(id)s, %(service_id)s, %(status)s, %(date_created)s);
    """

    created_dirs = create_dirs(
        dirs=[extract_dir, transform_dir],
    )
    extracted_envs_path = extract_data_from_db(
        db_hook=directus_conn_hook,
        extract_dir=extract_dir,
        query=extract_envs_query,
    )
    extracted_vars_path = extract_data_from_db.override(
        task_id="extract_vars_from_db",
    )(
        db_hook=directus_conn_hook,
        extract_dir=extract_dir,
        query=extract_vars_query,
    )
    splitted_vars_path = split_dataframe(
        file_path=extracted_vars_path,  # type: ignore
        transform_dir=transform_dir,
        num_workers=num_workers,
        extract_task_id="extract_vars_from_db",
    )
    extracted_statuses_path = extract_status_from_argocd_api.partial(
        extract_dir=extract_dir,
        envs_path=extracted_envs_path,
    ).expand(
        file_path=splitted_vars_path,
    )
    updated_data = update_data_on_db(
        db_hook=directus_conn_hook,
        file_path=extracted_statuses_path,  # type: ignore
        query=update_deployment_status_query,
    )
    inserted_data = update_data_on_db(
        db_hook=directus_conn_hook,
        file_path=extracted_statuses_path,  # type: ignore
        query=insert_deployment_status_query,
    )

    created_dirs >> extracted_envs_path >> extracted_vars_path
    updated_data >> inserted_data >> delete_dirs(
        dirs=[extract_dir, transform_dir],
    )


get_deployment_status_argocd_dag = get_deployment_status_argocd()
