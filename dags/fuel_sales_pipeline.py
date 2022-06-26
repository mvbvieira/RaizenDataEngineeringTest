from airflow import DAG
from datetime import datetime
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from os.path import exists
from operators.dowload_file_operator import DownloadFileOperator
from operators.convert_xlsx_operator import ConvertXlsxOperator
from scripts.convert_to_parquet import convert_to_parquet


TARGET_DIR = "/opt/raizen/data/xls/"
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

  convert_file = ConvertXlsxOperator(
    task_id='convert_to_xlsx',
    source_file=SOURCE_FILE,
    target_dir=TARGET_DIR,
    trigger_rule='one_success',
    dag=dag)

  convert_to_parquet_oil_derivative = PythonOperator(
    task_id='convert_to_parquet_oil_derivative',
    python_callable=convert_to_parquet,
    op_kwargs={'sheet_name': 'DPCache_m3', 'table_name': 'oil_derivative'},
    dag=dag)

  convert_to_parquet_diesel = PythonOperator(
    task_id='convert_to_parquet_diesel',
    python_callable=convert_to_parquet,
    op_kwargs={'sheet_name': 'DPCache_m3_2', 'table_name': 'diesel'},
    dag=dag)

  needs_download_file = DummyOperator(task_id='needs_download_file')
  file_already_dowloaded = DummyOperator(task_id='file_already_dowloaded')

verify_file_dowloaded >> needs_download_file >> download_file >> convert_file
verify_file_dowloaded >> file_already_dowloaded >> convert_file

convert_file >> convert_to_parquet_oil_derivative
convert_file >> convert_to_parquet_diesel