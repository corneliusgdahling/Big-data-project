from pyspark import SparkContext, SparkConf
import time


def get_tweets(task, train_file, sample=True):
    start_time = time.time()
    conf = SparkConf().setAppName(task)
    sc = SparkContext(conf=conf)
    tweets = sc.textFile(train_file)
    tweets_split = tweets.map(lambda x: x.split('\t'))
    print("Init time:", time.time() - start_time)
    return tweets_split.sample(False, 0.1, 5) if sample else tweets_split


def get_tweets_and_context(task, train_file, sample=True):
    start_time = time.time()
    conf = SparkConf().setAppName(task)
    sc = SparkContext(conf=conf)
    tweets = sc.textFile(train_file)
    tweets_split = tweets.map(lambda x: x.split('\t'))
    print("Init time:", time.time() - start_time)
    return (tweets_split.sample(False, 0.1, 5), sc) if sample else (tweets_split, sc)


def get_stopwords():
    words = []
    f = open('../data/stopwords.txt', 'r')
    for word in f:
        words.append(word.strip("\n"))
    f.close()
    return words


def result_file(task):
    return '../results_phase_1/result' + "_" + task + '.tsv'


def write_results(result_file, results, cols):
    for i, c in enumerate(cols):
        if i != len(cols) - 1:
            result_file.write(c + "\t")
        else:
            result_file.write(c + "\n")
    for i, r in enumerate(results):
        if i != len(results) - 1:
            result_file.write(r + "\n")
        else:
            result_file.write(r)
