import asyncio
import datetime
from functools import partial
from itertools import groupby, chain
import textwrap
import threading
import os

from dateutil.relativedelta import relativedelta
from twython import Twython


app_key = os.environ.get('app_key')
app_secret = os.environ.get('app_secret')

twitter = Twython(app_key, app_secret)
loop = asyncio.get_event_loop()

current_day = datetime.datetime.utcnow().strftime("%Y-%m-%d")
keywords = textwrap.dedent("""\
  #atom.io
""")
search_query = " OR ".join(k for k in keywords.splitlines())


def chunks(l, n):
  """ Yield successive n-sized chunks from l.
  """
  for i in range(0, len(l), n):
    yield l[i:i + n]


def get_tweets():
  # http://stackoverflow.com/questions/7400656/twitter-search-atom-api-exclude-retweets
  return twitter.search(q=search_query + " +exclude:retweets", result_type='mixed', since=current_day, lang="en",
                        include_entities=False,
                        count=100)


def acceptable_tweet(tweet):
  ret_val = True

  if ret_val:
    if tweet['retweet_count'] >= 20 or tweet['favorite_count'] >= 20:
      ret_val = False

  if ret_val:
    if tweet['user']['statuses_count'] < 50:
      ret_val = False

  if ret_val:
    followers_count = tweet['user']['followers_count']
    following_count = tweet['user']['friends_count']

    if following_count >= 10:
      if followers_count > 500:
        ret_val = (following_count / followers_count) >= .65
    else:
      ret_val = False

  return ret_val


past_week = (datetime.datetime.utcnow() - relativedelta(weeks=1)).strftime("%Y-%m-%d")


def user_search(acceptable_users):
  print("{0}: starting".format(threading.current_thread()))
  ret_val = twitter.search(q=acceptable_users, result_type='recent',
                        since=past_week, lang="en",
                        include_entities=False, count=100)
  from time import sleep
  # sleep(5)
  print("{0}: finishing".format(threading.current_thread()))
  return ret_val

@asyncio.coroutine
def acceptable_tweets(tweets):
  ret_val = [tweet for tweet in tweets if acceptable_tweet(tweet)]
  usernames = sorted({tweet['user']['screen_name'] for tweet in ret_val})

  chunked_names = (list(chunks(usernames, 10)))


  @asyncio.coroutine
  def each_chunk(chunked_names):
    acceptable_users = " OR ".join(["from:" + un for un in chunked_names])

    recent_user_tweets = yield from loop.run_in_executor(
      None,
      partial(user_search, acceptable_users)
    )

    name_key = lambda tweet: tweet['user']['screen_name']

    recent_user_tweets = sorted(recent_user_tweets['statuses'], key=name_key)

    user_grouped_tweets = {key: len(list(value)) for key, value in (groupby(recent_user_tweets, name_key))}
    user_grouped_tweets = {key: value for key, value in user_grouped_tweets.items() if value >= 2}

    new_val = [tweet for tweet in ret_val if tweet['user']['screen_name'] in user_grouped_tweets]
    return new_val

  sem = asyncio.Semaphore(5)

  new_ret_val = []
  for ch in chunked_names:
    with (yield from sem):
      results = yield from each_chunk(ch)

      new_ret_val.append(results)

  return new_ret_val


@asyncio.coroutine
def main():
  print('starting')

  python_tweets = yield from loop.run_in_executor(None, get_tweets)

  python_tweets = python_tweets['statuses']

  print("recieved %s tweets" % len(python_tweets))

  python_tweets = yield from acceptable_tweets(python_tweets)

  python_tweets = chain.from_iterable(python_tweets)
  for tweet in python_tweets:
    urls = tweet['user']['entities']['url']['urls'][0]['expanded_url'] if 'url' in tweet['user']['entities'] else None
    print("User: {0} ---- {1} ---- {2} ---- {3}".format(tweet['user']['screen_name'], tweet['text'],tweet['user'][
      'description'],urls))


    # for tweet in python_tweets['statuses']:
    # pprint(tweet)


@asyncio.coroutine
def output():
  print('searching')


if __name__ == "__main__":
  print(threading.current_thread().ident)

  f = asyncio.wait([main(), output()])
  loop.run_until_complete(f)
