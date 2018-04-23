from constants import *
from utils_2 import *
import time


def classify(train_file, in_file, out_file):
    tweets, sc = get_tweets_and_context('1', train_file, True) # Remember to fix this to False
    f = open(in_file, 'r')
    for line in f.readlines():
        words = line.strip().split()
    words_filtered = [w for w in words if w not in STOPWORDS]
    num_tweets = tweets.count()
    # num_places = tweets.map(lambda t: t[COLUMNS.index('place_name')]).distinct().count()
    place_tuple = tweets.map(lambda t: (t[COLUMNS.index('place_name')],
                                        t[COLUMNS.index('tweet_text')].lower().strip()))
    # num_tweets_by_place = place_tuple.countByKey()
    num_tweets_by_place_tuple = place_tuple.aggregateByKey(0, (lambda c, v: c + 1), (lambda rdd1, rdd2: rdd1 + rdd2))
    # place_tuple.mapValues(lambda l: l.split(" ")).groupByKey().mapValues(list)
    printy = place_tuple.take(10)
    result = ""
    for word in words_filtered:
        temp_result = place_tuple.aggregateByKey(0, (lambda c, v: c + 1 if word in v else c + 0), (lambda rdd1, rdd2: rdd1 + rdd2))
        if result:
            result = result.union(temp_result).reduceByKey(lambda x, y: x + y)
        else:
            result = temp_result

    resultados = result.filter(lambda t: t[1] > 5).take(20)
    print(resultados)

    place_tuple = place_tuple.join(num_tweets_by_place_tuple).map(lambda t: (t[0], t[1][1])).distinct()
    x = place_tuple.collect()
    sum = 0
    for val in x:
        sum += val[1]

    print(printy)
    print(num_tweets)
    print(x)
    print('Sum of num place by key is:', sum, sum == num_tweets)


if __name__ == "__main__":
    train_file = '../data/geotweets.tsv'
    in_file = '../data/input.txt'
    out_file = '../results_phase_2/output1.tsv'
    start_time = time.time()
    classify(train_file, in_file, out_file)
    print("Total time:", time.time()-start_time)