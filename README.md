mengatur PostgreSQL sebagai pengganti SQLite agar dapat mengaktifkan fitur LocalExecutor yang bertujuan untuk mengeksekusi task secara paralel. Hal-hal yang perlu: 

```
# airflow.cfg
sql_alchemy_conn = postgresql+psycopg2://username:password@localhost:5432/airflow
executor = LocalExecutor
```

Agar bisa melakukan import custom modules perlu ada penyesuaian pada variabel `PYTHONPATH`. Pastikan direktori tempat modul berada sudah ditambahkan ke dalam `PYTHONPATH`. Contoh:

```bash
export PYTHONPATH=$PYTHONPATH:/path-to-airflow_home/
```
Lalu restart Airflow Webserver 
```
airflow standalone
```

Ada beberapa variable yang bisa didefinisikan di Airflow UI agar script berjalan sesuai dengan yang diinginkan seperti 

```
extract_dir = 
transform_dir = 
num_workers = 
target_table = 
query_stmt = 
```

Dag yang digunakan adalah `migrate_gitlab_directus.py`