from pyspark.sql import Row
from pyspark.sql import SQLContext
from constants import *
from utils import get_tweets_and_context


def a(tweets, sc):
    sc.sql("SELECT count(*) FROM tweets").show()


def b(tweets, sc):
    sc.sql("SELECT count(DISTINCT username) FROM tweets").show()


def c(tweets, sc):
    sc.sql("SELECT count(DISTINCT country_name) FROM tweets").show()


def d(tweets, sc):
    sc.sql("SELECT count(DISTINCT place_name) FROM tweets").show()


def e(tweets, sc):
    sc.sql("SELECT count(DISTINCT language_) FROM tweets").show()


def f(tweets, sc):
    sc.sql("SELECT MIN(latitude), MIN(longitude) FROM tweets").show()


def g(tweets, sc):
    sc.sql("SELECT MAX(latitude), MAX(longitude) FROM tweets").show()

if __name__ == "__main__":
    task = '8'
    tweets, sc = get_tweets_and_context(task, False)

    sc_sql = SQLContext(sc)

    cols = tweets.map(lambda t: Row(
        utc_time=float(t[COLUMNS.index('utc_time')]),
        country_name=t[COLUMNS.index('country_name')],
        country_code=t[COLUMNS.index('country_code')],
        place_type=t[COLUMNS.index('place_type')],
        place_name=t[COLUMNS.index('place_name')],
        language_=t[COLUMNS.index('language')],
        username=t[COLUMNS.index('username')],
        user_screen_name=t[COLUMNS.index('user_screen_name')],
        timezome_offset=float(t[COLUMNS.index('timezone_offset')]),
        number_of_friends=int(t[COLUMNS.index('number_of_friends')]),
        tweet_text=t[COLUMNS.index('tweet_text')],
        latitude=float(t[COLUMNS.index('latitude')]),
        longitude=float(t[COLUMNS.index('longitude')])
    ))

    df = sc_sql.createDataFrame(cols)
    df.registerTempTable("tweets")

    a(tweets, sc_sql)
    b(tweets, sc_sql)
    c(tweets, sc_sql)
    d(tweets, sc_sql)
    e(tweets, sc_sql)
    f(tweets, sc_sql)
    g(tweets, sc_sql)