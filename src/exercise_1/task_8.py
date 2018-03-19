import pyspark
from pyspark import sql, SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from utils import get_tweets

def a(tweets):
    pass


if __name__ == "__main__":
    task = '8'
    tweets = get_tweets(task, True)

    spark = SparkSession.builder.master("").enableHiveSupport().getOrCreate()
    tweetdf = spark.createDataFrame(tweets)
    new_tweetdf = tweetdf \
        .withColumn('utc_time', tweetdf._1) \
        .withColumn('country_name', tweetdf._2) \
        .withColumn('country_code', tweetdf._3) \
        .withColumn('place_type', tweetdf._4) \
        .withColumn('place_name', tweetdf._5) \
        .withColumn('language', tweetdf._6) \
        .withColumn('user_screen_name', tweetdf._7) \
        .withColumn('username', tweetdf._8) \
        .withColumn('timezone_offset', tweetdf._9) \
        .withColumn('number_of_friends', tweetdf._10) \
        .withColumn('tweet_text', tweetdf._11) \
        .withColumn('latitude', tweetdf._12.cast(FloatType())) \
        .withColumn('longitude', tweetdf._13.cast(FloatType())) \