from airflow.decorators import task
from airflow.models import TaskInstance
import os


@task(task_id='create_dirs', task_display_name='Create Directories')
def create_dirs(
    dirs: list[str],
    ti: TaskInstance = None,
):
    """
    Create directories if they do not exist.

    Args:
        dirs: List of directories to create.
        ti: Task instance for XCom communication.
    """

    for directory in dirs:
        os.makedirs(directory, exist_ok=True)
        print(f"Directory created: {directory}")


@task(task_id='delete_dirs', task_display_name='Delete Directories')
def delete_dirs(
    dirs: list[str],
    ti: TaskInstance = None,
):
    """
    Delete directories and all of their contents.

    Args:
        dirs: List of directories to delete.
        ti: Task instance for XCom communication.
    """

    for directory in dirs:
        if os.path.exists(directory):
            for root, dirs, files in os.walk(directory, topdown=False):
                for file in files:
                    file_path = os.path.join(root, file)
                    os.remove(file_path)
                    print(f"Deleted file: {file_path}")
                for dir in dirs:
                    dir_path = os.path.join(root, dir)
                    os.rmdir(dir_path)
                    print(f"Deleted directory: {dir_path}")
            os.rmdir(directory)
            print(f"Deleted directory: {directory}")
        else:
            print(f"Directory does not exist: {directory}")
