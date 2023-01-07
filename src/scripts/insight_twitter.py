from pyspark.sql import SparkSession, DataFrame, functions as f
from os.path import join
import argparse

def get_tweet_conversations(df: DataFrame) -> DataFrame:
  return df.alias('tweet').groupBy(
    f.to_date('created_at').alias('created_date')
  ).agg(
    f.count_distinct('author_id').alias('n_tweet'),
    f.sum('like_count').alias('n_like'),
    f.sum('quote_count').alias('n_quote'),
    f.sum('reply_count').alias('n_reply'),
    f.sum('retweet_count').alias('n_retweet')
  ).withColumn(
    'weekday',
    f.date_format('created_date', 'E')
  ).orderBy('created_date')

def export_json(df: DataFrame, dst: str) -> None:
  df.coalesce(1).write.json(
    path=dst,
    mode='overwrite'
  )

def twitter_insight(
  spark: SparkSession,
  src: str,
  dst: str,
  process_date: str
) -> None:
  tweet_df = spark.read.json(join(src, 'tweet'))
  tweet_conversation_df = get_tweet_conversations(tweet_df)
  export_json(tweet_conversation_df, join(dst, f'process_date={process_date}'))

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description='Spark Twitter Transformation Silver'
  )

  parser.add_argument('--src', required=True)
  parser.add_argument('--dst', required=True)
  parser.add_argument('--process-date', required=True)

  args = parser.parse_args()

  spark = SparkSession\
    .builder\
    .appName("twitter_transformation")\
    .getOrCreate()

  twitter_insight(spark, args.src, args.dst, args.process_date)