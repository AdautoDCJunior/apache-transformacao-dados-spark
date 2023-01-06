import sys
sys.path.append('airflow')

from airflow.models import DAG
from operators.twitter_operator import TwitterOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from os.path import join

query = "datascience"
with DAG(
  dag_id='ExtractTwitter',
  start_date=days_ago(6),
  schedule_interval='0 0 * * *'
) as dag:
    to = TwitterOperator(
      task_id='twitter_datascience',
      file_path=join(
        'datalake',
        'twitter_datascience',
        f'extract_date={{{{ ds }}}}',
        f'datascience_{{{{ ds_nodash }}}}.json'
      ),
      start_time='{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}',
      end_time='{{ data_interval_end.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}',
      query=query
    )