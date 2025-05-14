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

