from utils import get_tweets, result_file, write_results
from constants import *


# 2715066
def a(tweets):
    return "Tweet count\t" + str(tweets.count())


# 499822
def b(tweets):
    num_distinct_users = tweets.map(lambda t: t[COLUMNS.index('username')]).distinct().count()
    return "Number of users\t" + str(num_distinct_users)


# 70
def c(tweets):
    num_distinct_countries = tweets.map(lambda t: t[COLUMNS.index('country_name')]).distinct().count()
    return "Number of countries\t" + str(num_distinct_countries)


#23121
def d(tweets):
    num_distinct_places = tweets.map(lambda t: t[COLUMNS.index('place_name')]).distinct().count()
    return "Number of places\t" + str(num_distinct_places)


# 46
def e(tweets):
    num_distinct_languages = tweets.map(lambda t: t[COLUMNS.index('language')]).distinct().count()
    return "Number of languages\t" + str(num_distinct_languages)


# -54.87555556
def f(tweets):
    min_latitude = tweets.map(lambda t: float(t[COLUMNS.index('latitude')])).min()
    return "Min latitude\t" + str(min_latitude)


# -159.83019441
def g(tweets):
    min_longitude = tweets.map(lambda t: float(t[COLUMNS.index('longitude')])).min()
    return "Min longitude\t" + str(min_longitude)


# 69.83186826
def h(tweets):
    max_latitude = tweets.map(lambda t: float(t[COLUMNS.index('latitude')])).max()
    return "Max latitude\t" + str(max_latitude)


# 153.03508445
def i(tweets):
    max_longitude = tweets.map(lambda t: float(t[COLUMNS.index('longitude')])).max()
    return "Max longitude\t" + str(max_longitude)


# 87.2014098368
def j(tweets):
    num_chars = float(tweets.map(lambda t: len(t[COLUMNS.index('tweet_text')])).sum())
    avg_length = num_chars / tweets.count()
    return "Tweet avg num chars\t" + str(avg_length)


# 11.9472119646
def k(tweets):
    num_words = float(tweets.map(lambda t: len(t[COLUMNS.index('tweet_text')].split())).sum())
    avg_length = num_words / tweets.count()
    return "Tweet avg num words\t" + str(avg_length)


if __name__ == "__main__":
    task = '1'
    tweets = get_tweets(task, False)

    result_file = open(result_file(task), "w")

    a = a(tweets)
    b = b(tweets)
    c = c(tweets)
    d = d(tweets)
    e = e(tweets)
    f = f(tweets)
    g = g(tweets)
    h = h(tweets)
    i = i(tweets)
    j = j(tweets)
    k = k(tweets)

    results = [a, b, c, d, e, f, g, h, i, j, k]

    write_results(result_file, results)
