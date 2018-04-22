from utils import get_tweets, result_file, write_results
from constants import *
from datetime import datetime as dt


def time_zone_tweets(tweets):
    tuples = tweets.map(lambda t: (t[COLUMNS.index('country_name')],
                                   float(t[COLUMNS.index('utc_time')]) / 1000 +
                                   float(t[COLUMNS.index('timezone_offset')])))\
        .map(lambda t: ((t[0], dt.fromtimestamp(t[1]).strftime("%H")), 1))\
        .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: (rdd1+rdd2)))\
        .map(lambda t: (t[0][0], (t[0][1], t[1])))\
        .reduceByKey(lambda x, y: x if x[1] > y[1] else y)\
        .map(lambda t: "%s\t%s\t%s" % (t[0], t[1][0], t[1][1]))\
        .collect()

    return tuples


if __name__ == "__main__":
    task = '4'
    tweets = get_tweets(task, False)

    result_file = open(result_file(task), "w")

    results = time_zone_tweets(tweets)

    write_results(result_file, results, cols=['country_name', 'start_time', "number_tweets_hour"])
