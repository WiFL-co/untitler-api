import asyncio
import datetime
from functools import partial
from itertools import groupby, chain
import textwrap
import threading
import os

from dateutil.relativedelta import relativedelta
import gspread

from twython import Twython


app_key = os.environ.get('app_key')
app_secret = os.environ.get('app_secret')

google_username = os.environ.get('google_username')
google_password = os.environ.get('google_password')
google_spreadsheet = os.environ.get('google_spreadsheet')

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

  python_tweets = list(chain.from_iterable(python_tweets))

  gc = gspread.login(google_username, google_password)
  wks = gc.open_by_url(google_spreadsheet)
  new_worksheet_len = len(wks.worksheets()) + 1
  new_ws_time = datetime.datetime.now().strftime("%I:%M %p on %B %d, %Y")

  new_worksheet_name = "Sheet{0} {1}: {2}".format(new_worksheet_len, new_ws_time, ", ".join(k for k in
                                                                                            keywords.splitlines(
                                                                                            )))[:50]

  worksheet = wks.add_worksheet(title=new_worksheet_name, rows="100", cols="20")

  cols = ['User', 'Tweet', 'Bio', 'Website']

  for i, c in enumerate(cols, start=1):
    worksheet.update_cell(1, i, c)

  sheet_range = "A2:{0}{1}".format(chr(len(cols) - 1 + ord("A")), len(python_tweets) +1)
  cell_ranges = worksheet.range(sheet_range)

  for i, tweet in enumerate(python_tweets):
    username = tweet['user']['screen_name']
    tweet_text = tweet['text']
    bio = tweet['user']['description']
    urls = tweet['user']['entities']['url']['urls'][0]['expanded_url'] if 'url' in tweet['user']['entities'] else None
    print("User: {0} ---- {1} ---- {2} ---- {3}".format(username, tweet_text, bio, urls))
    cell_ranges[i * 4 + 0].value = username
    cell_ranges[i * 4 + 1].value = tweet_text
    cell_ranges[i * 4 + 2].value = bio
    cell_ranges[i * 4 + 3].value = urls

  worksheet.update_cells(cell_ranges)

@asyncio.coroutine
def output():
  print('searching')


if __name__ == "__main__":
  print(threading.current_thread().ident)

  f = asyncio.wait([main(), output()])
  loop.run_until_complete(f)
