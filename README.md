Mengatur PostgreSQL sebagai pengganti SQLite agar dapat mengaktifkan fitur LocalExecutor yang bertujuan untuk mengeksekusi task secara paralel. Hal-hal yang perlu: 

## Quick Start
### Prerequisite
    - Python 3.10.xx
    - PostgreSQL
### Installation Steps
1. Buat User dan Grant Previlages di PostgreSQL
    ```sql
    CREATE USER airflow WITH PASSWORD 'your_password';
    -- Berikan privilege minimum yang diperlukan untuk Airflow
    CREATE DATABASE airflow;
    GRANT CONNECT ON DATABASE airflow TO airflow;
    \c airflow
    GRANT ALL PRIVILEGES ON SCHEMA public TO airflow;
    ```
2. Buat Database untuk menyimpan metadata Airflow
    ```sql
    -- Buat database airflow_metadata untuk menyimpan metadata Airflow
    CREATE DATABASE airflow_metadata;
    GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO airflow;
    ```
3. Isikan semua variables di `.env`
    ```bash 
    export AIRFLOW_HOME= #"<path to your airflow home>"
    export PYTHONPATH= #"<directory where you have arranged your modules>"
    export AIRFLOW__CORE__FERNET_KEY= # "<for encryption in variables>"
    export GITLAB_TOKEN= # "<your gitlab token>"
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN= # "<connection string for your database>"
    export AIRFLOW__CORE__EXECUTOR= # "<executor type> [LocalExecutor, CeleryExecutor, SequentialExecutor]>"
    export AIRFLOW__CORE__DAGS_FOLDER= # "<path to your dags folder>"```
4. Lalu jalankan file `execute_file.sh`
    ```bash
    bash execute_file.sh
    ```



Ada beberapa variable yang bisa didefinisikan di Airflow UI agar script berjalan sesuai dengan yang diinginkan seperti (Opstional but recommended)

```
extract_dir = 
transform_dir = 
num_workers = 
target_table = 
query_stmt = 
```

Dag yang digunakan adalah `migrate_gitlab_directus.py`