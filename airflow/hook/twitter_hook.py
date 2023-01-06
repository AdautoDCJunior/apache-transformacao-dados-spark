from os.path import join
from pathlib import Path
from airflow.providers.http.hooks.http import HttpHook
from requests import Request
from datetime import datetime, timedelta
import json

class TwitterHook(HttpHook):
  def __init__(self, start_time, end_time, query, conn_id=None) -> None:
    self.start_time = start_time
    self.end_time = end_time
    self.query = query
    self.conn_id = conn_id or 'twitter_default'
    super().__init__(http_conn_id=self.conn_id)

  def create_url(self) -> str:
    start_time = self.start_time
    end_time = self.end_time
    query = self.query

    fields = json.load(open(join(Path(__file__).parents[0], 'fields.json')))

    tweet_fields = f'tweet.fields={",".join(fields["tweet_fields"])}'
    user_fields = f'expansions=author_id&user.fields={",".join(fields["user_fields"])}'

    url_raw = f'{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}'

    return url_raw

  def connect_to_endpoint(self, url, session):
    request = Request("GET", url)
    prep = session.prepare_request(request)
    self.log.info(f'URL: {url}')

    return self.run_and_check(session, prep, {})

  def paginate(self, url_raw, session):
    list_json_response = []

    response = self.connect_to_endpoint(url_raw, session)
    json_response = response.json()
    list_json_response.append(json_response)

    i = 1
    while 'next_token' in json_response.get('meta', {}) and i < 10:
      next_token = json_response['meta']['next_token']
      url = f'{url_raw}&next_token={next_token}'
      response = self.connect_to_endpoint(url, session)
      json_response = response.json()
      list_json_response.append(json_response)

      i += 1

    return list_json_response

  def run(self):
    session = self.get_conn()
    url_raw = self.create_url()

    return self.paginate(url_raw, session)

if __name__ == '__main__':
  timestamp_format = '%Y-%m-%dT%H:%M:%S.00Z'

  start_time = (datetime.now() + timedelta(-1)).date().strftime(timestamp_format)
  end_time = datetime.now().strftime(timestamp_format)
  query = "datascience"

  for pg in TwitterHook(start_time, end_time, query).run():
    print(json.dumps(pg, indent=4, sort_keys=True))