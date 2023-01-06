from pyspark.sql import SparkSession, DataFrame, functions as f
from os.path import join
import argparse

def get_tweets_data(df: DataFrame) -> DataFrame:
  return df.select(
    f.explode('data').alias('tweets')
  ).select(
    'tweets.author_id',
    'tweets.conversation_id',
    'tweets.created_at',
    'tweets.id',
    'tweets.public_metrics.*',
    'tweets.text'
  )

def get_users_data(df: DataFrame) -> DataFrame:
  return df.select(
    'includes.users'
  ).select(
    f.explode('users').alias('users')
  ).select(
    'users.*'
  ).distinct()

def export_json(df: DataFrame, dst: str) -> None:
  df.coalesce(1).write.json(
  path=dst,
  mode='overwrite'
)

def twitter_transformation(
  spark: SparkSession,
  src: str,
  dst: str,
  process_date
) -> None:
  df = spark.read.json(src)
  tweet_df = get_tweets_data(df)
  user_df = get_users_data(df)

  table_dst = join(dst, '{table_name}', f'process_date={process_date}')
  
  export_json(tweet_df, table_dst.format(table_name='tweet'))
  export_json(user_df, table_dst.format(table_name='user'))

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description='Spark Twitter Transformation'
  )

  parser.add_argument('--src', required=True)
  parser.add_argument('--dst', required=True)
  parser.add_argument('--process-date', required=True)

  args = parser.parse_args()

  spark = SparkSession\
    .builder\
    .appName("twitter_transformation")\
    .getOrCreate()

  twitter_transformation(spark, args.src, args.dst, args.process_date)