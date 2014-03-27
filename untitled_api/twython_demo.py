import asyncio
import datetime
from itertools import groupby
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
  #firebase
  #atom.io
  #python
""")
search_query = " OR ".join(k for k in keywords.splitlines())


def get_tweets():
  return twitter.search(q=search_query, result_type='mixed', since=current_day, lang="en", include_entities=False)


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


@asyncio.coroutine
def acceptable_tweets(tweets):
  ret_val = [tweet for tweet in tweets if acceptable_tweet(tweet)]
  usernames = [tweet['user']['screen_name'] for tweet in ret_val]
  acceptable_users = " OR ".join(["from:" + un for un in usernames])
  past_week = (datetime.datetime.utcnow() - relativedelta(weeks=1)).strftime("%Y-%m-%d")

  recent_user_tweets = yield from loop.run_in_executor(
    None,
    lambda: twitter.search(q=acceptable_users, result_type='recent',
                           since=past_week, lang="en",
                           include_entities=False, count=100)
  )

  print(acceptable_users)
  print(past_week)
  print("recieved %s tweets" % len(recent_user_tweets['statuses']))

  name_key = lambda tweet: tweet['user']['screen_name']

  recent_user_tweets = sorted(recent_user_tweets['statuses'], key=name_key)

  user_grouped_tweets = {key: len(list(value)) for key, value in (groupby(recent_user_tweets, name_key))}
  user_grouped_tweets = {key: value for key, value in user_grouped_tweets.items() if value >= 2}

  ret_val = [tweet for tweet in ret_val if tweet['user']['screen_name'] in user_grouped_tweets]
  return ret_val


@asyncio.coroutine
def main():
  print('starting')

  python_tweets = yield from loop.run_in_executor(None, get_tweets)

  python_tweets = python_tweets['statuses']

  print("recieved %s tweets" % len(python_tweets))

  python_tweets = yield from acceptable_tweets(python_tweets)

  for tweet in python_tweets:
    print("User: {0} ---- {1}".format(tweet['user']['screen_name'], tweet['text']))


    # for tweet in python_tweets['statuses']:
    # pprint(tweet)


@asyncio.coroutine
def output():
  print('searching')


if __name__ == "__main__":
  print(threading.current_thread().ident)

  f = asyncio.wait([main(), output()])
  loop.run_until_complete(f)
