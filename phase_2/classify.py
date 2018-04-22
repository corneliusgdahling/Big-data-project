from constants import *
from utils_2 import *
import time


def classify(train_file, in_file, out_file):
    tweets, sc = get_tweets_and_context('1', True) # Remember to fix this to False
    tweets.persist()
    num_tweets = tweets.count()
    # num_places = tweets.map(lambda t: t[COLUMNS.index('place_name')]).distinct().count()
    place_tuple = tweets.map(lambda t: (t[COLUMNS.index('place_name')],
                                        t[COLUMNS.index('tweet_text')].lower().strip()))
    place_tuple.persist()
    num_place_by_key = place_tuple.countByKey()
    place_tuple = place_tuple.groupByKey()
    place_tuple = place_tuple.mapValues(lambda l: l.split(" "))
    place_tuple = place_tuple.mapValues(list)
    printy = place_tuple.take(10)

    print(printy)
    print(num_tweets)
    print(num_place_by_key)
    print('Sum of num place by key is:', sum(num_place_by_key.values()), sum(num_place_by_key.values()) == num_tweets)


if __name__ == "__main__":
    train_file = '../data/geotweets.tsv'
    in_file = ""
    out_file = '../results_phase_2/output1.tsv'
    start_time = time.time()
    classify(train_file, in_file, out_file)
    # pprint.pprint(x)
    print("Total time: ", time.time()-start_time)