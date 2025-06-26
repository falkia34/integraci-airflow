from airflow.decorators import task
from airflow.models import TaskInstance
from datetime import datetime
import polars as pl


@task(task_id='split_dataframe', task_display_name='Split DataFrame for Parallel Processing')
def split_dataframe(
    file_path: str,
    transform_dir: str,
    num_workers: int,
    extract_task_id: str = 'extract_data_from_db',
    ti: TaskInstance = None,
):
    """
    Split the DataFrame into chunks for parallel processing.

    Args:
        file_path: Path to the file containing the data to be split.
        transform_dir: Directory to save the split data.
        num_workers: Number of workers to split the data into.
        ti: Task instance for XCom communication.
    """

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    rows_count = ti.xcom_pull(
        task_ids=extract_task_id,
        key="extracted_rows_count",
    )
    df = pl.scan_parquet(file_path)
    # Split the DataFrame into chunks
    chunk_size = rows_count // num_workers

    all_chunk_paths: list[str] = []

    for i in range(num_workers):
        result_path = f"{transform_dir}/split_data_{i}_{timestamp}.parquet"
        start = i * chunk_size
        end = (i + 1) * chunk_size if i != num_workers - 1 else rows_count
        chunk = df.slice(start, end - start)
        print(f"Data of the #{i+1} chunk: {chunk.collect()}")
        chunk.sink_parquet(result_path, compression='uncompressed')
        all_chunk_paths.append(result_path)

    print(
        f"Data split into {num_workers} chunks and saved to {transform_dir}"
    )

    return all_chunk_paths
