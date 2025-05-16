# Pastikan menggunakan Python versi 3.10.
# Pastikan file .env telah terisi dengan konfigurasi yang diperlukan sebelum menjalankan kode ini.

python3.10 -m venv env310
source enn310/bin/activate
source .env
echo "AIRFLOW_HOME ${AIRFLOW_HOME}"
echo "PYTHONPATH ${PYTHONPATH}"
echo "AIRFLOW_CORE_FERNET_KEY ${AIRFLOW_CORE_FERNET_KEY}"
echo "GITLAB_TOKEN ${GITLAB_TOKEN}"
echo "AIRFLOW_DATABASE_SQLALCHEMY_CONN ${AIRFLOW_DATABASE_SQLALCHEMY_CONN}"
echo "AIRFLOW_CORE_EXECUTOR ${AIRFLOW_CORE_EXECUTOR}"
echo "AIRFLOW__CORE__DAGS_FOLDER ${AIRFLOW__CORE__DAGS_FOLDER}"
pip install -r requirements.txt
airflow standalone
