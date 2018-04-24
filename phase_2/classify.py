from constants import *
from utils_2 import *
import time


def classify(train_file, in_file, out_file):

    # Get tweets from training file
    tweets, sc = get_tweets_and_context('1', train_file, False)

    # Read tweets from input file and create list of words
    f = open(in_file, 'r')
    for line in f.readlines():
        words = line.strip().split()

    # Remove stopwords from input words
    words_filtered = [w for w in words if w not in STOPWORDS]

    # Gets total number of tweets. Value rather than RDD is returned because it is a constant
    num_tweets = tweets.count()

    # Create tuple with only place name and tweet text. Tweet text is set to lowercase
    place_tuple = tweets.map(lambda t: (t[COLUMNS.index('place_name')],
                                        t[COLUMNS.index('tweet_text')].lower().strip()))

    # Sum up the number of tweets by place (aggregate by key - where place is the key)
    num_tweets_by_place_tuple = place_tuple.aggregateByKey(0, (lambda x, _: x + 1), (lambda rdd1, rdd2: rdd1 + rdd2))

    # Loop through the words in tweet text and multiply the number of occurrences for each word by place
    result = ""
    for word in words_filtered:

        # Sum the number of occurrences of the word for each key
        temp_result = place_tuple.aggregateByKey(0, (lambda x, y: x + 1 if word in y else x + 0),
                                                 (lambda rdd1, rdd2: rdd1 + rdd2))

        # If result has value, multiply number of occurence from this word with result, else make calc above result
        if result:
            result = result.union(temp_result).reduceByKey(lambda x, y: x * y)
        else:
            result = temp_result

    # Join result and number of tweets by place and map them to create a single tuple (k, v, v) for each place
    place_tuple = place_tuple.join(num_tweets_by_place_tuple).map(lambda t: (t[0], t[1][1])).join(result).distinct()\
        .map(lambda t: (t[0], t[1][0], t[1][1]))

    # Finish bayes formula by mapping values to result values
    bayes_formula = place_tuple.map(lambda t: (t[0], t[1]*t[2] / float(t[1]**len(words_filtered) * num_tweets)))

    # Take max value from formula (highest probability)
    max = bayes_formula.map(lambda t: t[1]).max()

    # If all have probability 0, stop
    if max == 0.0:
        return

    # Check if there are multiple places with highest probability
    best = bayes_formula.filter(lambda t: t[1] == max).collect()

    out = open(out_file, 'w')

    # Write places with highest probability
    for i in range(len(best)):
        out.write(best[0][0] + "\t")

    # Write highest probability
    out.write(str(best[0][1]))

if __name__ == "__main__":
    train_file = '../data/geotweets.tsv'
    in_file = '../data/input.txt'
    out_file = '../results_phase_2/output1.tsv'
    start_time = time.time()
    classify(train_file, in_file, out_file)
    print("Total time:", time.time()-start_time)