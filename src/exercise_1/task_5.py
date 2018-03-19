from utils import get_tweets, result_file, write_results
from constants import *


def tweets_place(tweets):
    tuples = \
        tweets.filter(lambda t: t[COLUMNS.index('country_code')] == 'US' and t[COLUMNS.index('place_type')] == 'city')\
        .map(lambda t: (t[COLUMNS.index('place_name')], 1))\
        .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: rdd1 + rdd2))\
        .sortByKey()\
        .sortBy(lambda t: t[1], False)\
        .map(lambda t: '%s\t%s' % (t[0], t[1]))\
        .collect()

    return tuples


if __name__ == "__main__":
    task = '5'
    tweets = get_tweets(task, False)

    result_file = open(result_file(task), "w")

    results = tweets_place(tweets)

    write_results(result_file, results, cols=['place_name', 'num_tweets'])
