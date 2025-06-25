from airflow.decorators import dag
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dags.common.file import create_dirs, delete_dirs
from dags.common.database import extract_data_from_db, update_data_on_db
from dags.common.gitlab import extract_lang_from_gitlab_api
from dags.common.dataframe import split_dataframe
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="get_languages_gitlab",
    dag_display_name="Get Languages from Gitlab",
    default_args=default_args,
    description="Ambil data bahasa dari Gitlab dan simpan ke Directus",
    schedule="0 0 * * * *",  # Setiap hari jam 00:00
    start_date=datetime.now() - timedelta(days=1),
    max_active_runs=1,  # Memastikan hanya satu run aktif agar tidak ada tumpang tindih
    catchup=False,
    tags=["integraci", "gitlab", "directus", "languages"],
)
def get_languages_gitlab():
    """
    DAG untuk mengambil data bahasa dari Gitlab dan menyimpannya ke Directus.
    """

    # Mengambil jumlah worker dari Airflow Variable, default 3
    num_workers = int(Variable.get("num_workers", default=3))
    extract_dir = Variable.get(
        "extract_dir", default=f'{os.getcwd()}/data/extract')
    transform_dir = Variable.get(
        "transform_dir", default=f'{os.getcwd()}/data/transform')

    directus_conn_hook = PostgresHook(postgres_conn_id="directus")
    extract_vars_query = """
        SELECT
            a.application,
            a.id AS application_id,
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
    update_languages_query = """
        UPDATE public.cicd_application_service
        SET languages = %(languages)s
        WHERE application = %(application_id)s
        AND service_name = %(service_name)s
        AND service_type = %(service_type_id)s;
    """

    created_dirs = create_dirs(
        dirs=[extract_dir, transform_dir],
    )
    extracted_vars_path = extract_data_from_db(
        db_hook=directus_conn_hook,
        extract_dir=extract_dir,
        query=extract_vars_query,
    )
    splitted_vars_path = split_dataframe(
        file_path=extracted_vars_path,  # type: ignore
        transform_dir=transform_dir,
        num_workers=num_workers,
    )
    extracted_languages_path = extract_lang_from_gitlab_api.partial(extract_dir=extract_dir).expand(
        file_path=splitted_vars_path,
    )
    updated_data = update_data_on_db(
        db_hook=directus_conn_hook,
        file_path=extracted_languages_path,  # type: ignore
        query=update_languages_query,
    )

    created_dirs >> extracted_vars_path
    updated_data >> delete_dirs(
        dirs=[extract_dir, transform_dir],
    )


get_languages_gitlab_dag = get_languages_gitlab()
