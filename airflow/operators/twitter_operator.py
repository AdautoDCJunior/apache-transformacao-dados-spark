import sys
sys.path.append('airflow')

from airflow.models import DAG, TaskInstance, BaseOperator
from hook.twitter_hook import TwitterHook
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path
import json

class TwitterOperator(BaseOperator):
  template_fields = ['file_path', 'start_time', 'end_time', 'query']

  def __init__(self, file_path, start_time, end_time, query, **kwargs):
    self.file_path = file_path
    self.start_time = start_time
    self.end_time = end_time
    self.query = query
    super().__init__(**kwargs)

  def create_parent_folder(self):
    Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)

  def execute(self, context):
    start_time = self.start_time
    end_time = self.end_time
    query = self.query

    self.create_parent_folder()
    with open(self.file_path, 'w') as output_file:
      for pg in TwitterHook(start_time, end_time, query).run():
        json.dump(pg, output_file, ensure_ascii=False)
        output_file.write('\n')

if __name__ == '__main__':
  timestamp_format = '%Y-%m-%dT%H:%M:%S.00Z'

  start_time = (datetime.now() + timedelta(-1)).date().strftime(timestamp_format)
  end_time = datetime.now().strftime(timestamp_format)
  query = "datascience"

  with DAG(dag_id='TwitterTest', start_date=datetime.now()) as dag:
    to = TwitterOperator(
      task_id='test_run',
      file_path=join(
        'datalake',
        'twitter_datascience',
        f'extract_date={datetime.now().date()}',
        f'datascience_{datetime.now().date().strftime("%Y%m%d")}.json'
      ),
      start_time=start_time,
      end_time=end_time,
      query=query
    )
    ti = TaskInstance(task=to)
    to.execute(ti.task_id)