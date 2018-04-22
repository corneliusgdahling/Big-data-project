from utils import get_tweets, result_file, write_results
from constants import *


def frequency_words_part_two(tweets):
    top_5 = tweets.filter(
        lambda t: t[COLUMNS.index('country_code')] == 'US' and t[COLUMNS.index('place_type')] == 'city') \
        .map(lambda t: (t[COLUMNS.index('place_name')], 1)) \
        .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: rdd1 + rdd2)) \
        .sortByKey() \
        .sortBy(lambda t: t[1], False) \
        .map(lambda t: (t[0], t[1])) \
        .top(5, key=lambda t: t[1])

    top_5_cities = [x[0] for x in top_5]

    final_result = []

    for city in top_5_cities:
        top_10_words = tweets.filter(lambda t: t[COLUMNS.index('country_code')] == 'US' and
                                               t[COLUMNS.index('place_name')] == city) \
            .flatMap(lambda t: t[COLUMNS.index('tweet_text')].lower().strip().split(' ')) \
            .filter(lambda w: len(w) > 2 and w not in STOPWORDS) \
            .map(lambda w: (w, 1)) \
            .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: rdd1 + rdd2)) \
            .sortBy(lambda (w, c): c, False) \
            .takeOrdered(10, key=lambda x: -x[1])

        tuple_formatted = city
        for w in top_10_words:
            tuple_formatted += '\t%s\t%s' % (w[0], w[1])

        final_result.append(tuple_formatted)
    return final_result


if __name__ == "__main__":
    task = '7'
    tweets = get_tweets(task, False)

    result_file = open(result_file(task), "w")

    results = frequency_words_part_two(tweets)

    write_results(result_file, results, cols=['place_name',
                                              'most_frequent_word',
                                              'frequency',
                                              '2._most_frequent_word',
                                              'frequency',
                                              '3._most_frequent_word',
                                              'frequency',
                                              '4._most_frequent_word',
                                              'frequency',
                                              '5._most_frequent_word',
                                              'frequency',
                                              '6._most_frequent_word',
                                              'frequency',
                                              '7._most_frequent_word',
                                              'frequency',
                                              '8._most_frequent_word',
                                              'frequency',
                                              '9._most_frequent_word',
                                              'frequency',
                                              '10._most_frequent_word',
                                              'frequency'])
