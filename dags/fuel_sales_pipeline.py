from airflow import DAG
from datetime import datetime
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from os.path import exists
from operators.dowload_file_operator import DownloadFileOperator

TARGET_DIR = "/opt/raizen/data/"
TARGET_FILE = "vendas-combustiveis-m3.xls"
SOURCE_FILE = "https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls"

def _verify_file_dowloaded():
  file_exists = exists(TARGET_DIR + TARGET_FILE)
  if file_exists:
    return 'file_already_dowloaded'

  return 'needs_download_file'

with DAG("fuel_sales_pipeline",
  start_date=datetime(2022, 6, 22),
  schedule_interval='@daily',
  catchup=False 
) as dag:

  verify_file_dowloaded = BranchPythonOperator(
    task_id='verify_file_dowloaded',
    python_callable=_verify_file_dowloaded
  )

  download_file = DownloadFileOperator(
    task_id='download_file',
    source_file=SOURCE_FILE,
    target_dir=TARGET_DIR,
    target_file=TARGET_FILE,
    dag=dag)

  needs_download_file = DummyOperator(task_id='needs_download_file')
  file_already_dowloaded = DummyOperator(task_id='file_already_dowloaded')

verify_file_dowloaded >> needs_download_file >> download_file 
verify_file_dowloaded >> file_already_dowloaded