import sys
sys.path.append('airflow')

from airflow.models import DAG
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from os.path import join
from pathlib import Path

query = "datascience"
with DAG(
  dag_id='ExtractTwitter',
  start_date=days_ago(6),
  schedule_interval='0 0 * * *'
) as dag:
  base_folder = join(
    str(Path(__file__).parents[2]),
    'datalake',
    '{stage}',
    'twitter_datascience',
    '{partition}'
  )
  partition_folder_extract = 'extract_date={{ data_interval_start.strftime("%Y-%m-%d") }}'

  twitter_operator = TwitterOperator(
    task_id='twitter_datascience',
    file_path=join(
      base_folder.format(stage='Bronze', partition=partition_folder_extract),
      'datascience_{{ ds_nodash }}.json'
    ),
    start_time='{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}',
    end_time='{{ data_interval_end.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}',
    query=query
  )

  twitter_transform = SparkSubmitOperator(
    task_id='transform_twitter_datascience',
    application='/home/adauto_junior/cursos/alura/airflow-transformacao-dados-spark/src/scripts/transformation.py',
    name='twitter_transformation',
    application_args=[
      '--src', 
      base_folder.format(stage='Bronze', partition=partition_folder_extract), 
      '--dst',
      base_folder.format(stage='Silver', partition=''),
      '--process-date',
      '{{ ds }}'
    ]
  )

twitter_operator >> twitter_transform