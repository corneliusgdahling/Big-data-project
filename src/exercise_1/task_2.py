from utils import get_tweets, result_file, write_results
from constants import *


def total_tweets(tweets):
    total_tweets_by_country = tweets.map(lambda x: (x[COLUMNS.index('country_name')], 1)) \
        .filter(lambda c: len(c[0].strip()) > 0)\
        .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: (rdd1 + rdd2))) \
        .sortByKey() \
        .sortBy(lambda t: t[1], False) \
        .map(lambda x: "%s\t%s" % (x[0], x[1])).collect()

    return total_tweets_by_country


if __name__ == "__main__":
    task = '2'
    tweets = get_tweets(task, False)

    result_file = open(result_file(task), "w")

    results = total_tweets(tweets)

    write_results(result_file, results)
