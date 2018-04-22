from utils import get_tweets, result_file
from constants import *


def frequency_words(tweets):
    tuples = tweets.filter(lambda t: t[COLUMNS.index('country_code')] == 'US')\
        .flatMap(lambda t: t[COLUMNS.index('tweet_text')].lower().split())\
        .filter(lambda w: len(w) >= 2 and w not in STOPWORDS)\
        .map(lambda w: (w, 1))\
        .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: rdd1 + rdd2)) \
        .takeOrdered(10, key=lambda x: -x[1])\

    return tuples


if __name__ == "__main__":
    task = '6'
    tweets = get_tweets(task, False)

    result_file = open(result_file(task), "w")

    results = frequency_words(tweets)

    result_file.write('%s\t%s\n' % ('word', 'num_tweets'))

    for i in range(len(results)):
        if i != len(results) - 1:
            result_file.write('%s\t%s\n' % (results[i][0], results[i][1]))
        else:
            result_file.write('%s\t%s' % (results[i][0], results[i][1]))