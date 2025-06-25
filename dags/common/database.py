from airflow.decorators import task
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import polars as pl


@task(task_id='extract_data_from_db', task_display_name='Extract Data from DB')
def extract_data_from_db(
    db_hook: PostgresHook,
    extract_dir: str,
    query: str,
    ti: TaskInstance = None,
):
    """
    Extract data from a database and save it to a file.

    Args:
        hook: Database connection hook.
        extract_dir: Directory to save the extracted data.
        query: SQL query to extract data.
        ti: Task instance for XCom communication.
    """

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    result_path = f"{extract_dir}/db_data_{timestamp}.parquet"
    print(f'Result path: {result_path}')

    df = pl.read_database(query, connection=db_hook.get_conn())
    print(f"Data from DB: {df}")

    df.write_parquet(result_path, compression='uncompressed')
    ti.xcom_push(key='extracted_rows_count', value=df.height)
    print(f"Data extracted and saved to {result_path}")

    return result_path


@task(task_id='update_data_on_db', task_display_name='Update Data on DB')
def update_data_on_db(
    db_hook: PostgresHook,
    file_path: str,
    query: str,
    ti=None,
):
    """
    Perform upsert operation using mogrify for efficient batch insertion.

    Args:
        db_hook: Database connection hook.
        num_workers: Number of workers used for data extraction.
        target_table: Target table for upsert operation.
        ti: Task instance for XCom communication.

    """

    # Read the data from the files into a single DataFrame
    df = pl.read_parquet(file_path)
    data = df.to_dicts()
    print(f"Data to be inserted: {data}")

    conn = db_hook.get_conn()

    # Prepare one large SQL string
    with conn.cursor() as cursor:
        for datum in data:
            update_query = cursor.mogrify(query, datum).decode("utf-8")
            print(f"Executing query: {update_query}")
            cursor.execute(update_query)

    conn.commit()
