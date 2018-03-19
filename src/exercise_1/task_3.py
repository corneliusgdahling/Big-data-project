from utils import get_tweets, result_file, write_results
from constants import *


def centroids(tweets):
    tuples = tweets.map(lambda t:
                        (t[COLUMNS.index('country_name')],
                         1,
                         float(t[COLUMNS.index('latitude')]),
                         float(t[COLUMNS.index('longitude')])))\
        .keyBy(lambda x: x[0])\
        .aggregateByKey(
        (0, 0.0, 0.0),
        (lambda x, y: (x[0] + y[1], x[1] + y[2], x[2] + y[3])),
        (lambda rdd1, rdd2: (rdd1[0] + rdd2[0], rdd1[1] + rdd2[1], rdd1[2] + rdd2[2]))) \
        .filter(lambda t: t[1][0] > 10)\
        .map(lambda t: "%s\t%s\t%s" % (t[0], t[1][1] / t[1][0], t[1][2] / t[1][0]))\
        .collect()

    return tuples


if __name__ == "__main__":
    task = '3'
    tweets = get_tweets(task, False)

    cols = ['country_name', "latitude", "longitude"]

    result_file = open(result_file(task), "w")

    results = centroids(tweets)

    write_results(result_file, results, cols)
