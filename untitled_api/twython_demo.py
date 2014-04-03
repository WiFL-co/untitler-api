import aiohttp
import asyncio
import datetime
from functools import partial
from itertools import groupby, chain
import json
import textwrap
import threading
import os

from dateutil.relativedelta import relativedelta
import gspread

from twython import Twython
from untitled_api.libs.text_utils.parsers.text_parser import get_canonical_name_from_keywords


app_key = os.environ.get('app_key')
app_secret = os.environ.get('app_secret')

google_username = os.environ.get('google_username')
google_password = os.environ.get('google_password')
google_spreadsheet = os.environ.get('google_spreadsheet')
persona_spreadsheet = os.environ.get('persona_spreadsheet')

twitter = Twython(app_key, app_secret)

sem = asyncio.Semaphore(5)
loop = asyncio.get_event_loop()

current_day = (datetime.datetime.utcnow() - relativedelta(days=1)).strftime("%Y-%m-%d")
keywords = textwrap.dedent("""\
  postgres
  mongodb
  back-end
""")
search_query = " OR ".join(k for k in keywords.splitlines())


def chunks(l, n):
  """ Yield successive n-sized chunks from l.
  """
  for i in range(0, len(l), n):
    yield l[i:i + n]


def get_tweets(query):
  # http://stackoverflow.com/questions/7400656/twitter-search-atom-api-exclude-retweets
  return twitter.search(q=query + ' +exclude:retweets -"rt" -"mt"', result_type='recent', since=current_day,
                        lang="en",
                        include_entities=False,
                        count=100)


def get_trending_keywords_from_tweets(source_handle):
  return twitter.search(q="from:" + source_handle, result_type='mixed', since=current_day,
                        lang="en",
                        include_entities=False,
                        count=100)


def acceptable_tweet(tweet):
  ret_val = True

  if ret_val:
    if tweet['in_reply_to_user_id']:
      ret_val = False

  if ret_val:
    if tweet['retweet_count'] >= 20 or tweet['favorite_count'] >= 20:
      ret_val = False

  if ret_val:
    if tweet['user']['statuses_count'] < 50:
      ret_val = False

  if ret_val:
    followers_count = tweet['user']['followers_count']
    following_count = tweet['user']['friends_count']

    if following_count >= 30 and followers_count >= 30:
      if followers_count > 500:
        ret_val = (following_count / followers_count) >= .65
    else:
      ret_val = False

  return ret_val


past_week = (datetime.datetime.utcnow() - relativedelta(weeks=1)).strftime("%Y-%m-%d")


def user_search(acceptable_users):
  print("{0}: starting".format(threading.current_thread()))
  ret_val = twitter.search(q=acceptable_users + ' +exclude:retweets -"rt" -"mt"', result_type='recent',
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
  def each_chunk(chunked_names, sem):
    acceptable_users = " OR ".join(["from:" + un for un in chunked_names])

    with (yield from sem):
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

  alchemy_url = 'http://access.alchemyapi.com/calls/text/TextGetRankedNamedEntities'
  alchemy_key = os.environ.get('alchemy_key')

  @asyncio.coroutine
  def is_proper_entity(person, sem):
    with (yield from sem):
      params = {"outputMode": "json", "apikey": alchemy_key, "text": person['user']['name']}
      alchemy_response = yield from aiohttp.request('GET', alchemy_url, params=params)
      result = yield from alchemy_response.read_and_close(decode=True)

    try:
      if result['entities'][0]['type'].lower() == 'person':
        return person
      else:
        return None
    except:
      return None


  chunks_done, pending = yield from asyncio.wait([each_chunk(ch, sem) for ch in chunked_names])

  new_ret_val = list(chain.from_iterable(ch.result() for ch in chunks_done))

  people_done, pending = yield from asyncio.wait([is_proper_entity(ch, sem) for ch in new_ret_val])

  new_ret_val = list(filter(None, map(lambda p: p.result(), people_done)))

  return new_ret_val


def get_persona_keywords(persona_wks):
  client_persona = persona_wks.get_worksheet(0).col_values(1)
  ta_persona = persona_wks.get_worksheet(0).col_values(2)

  del client_persona[0], ta_persona[0]
  return {p: p for p in set(client_persona + ta_persona)}


def get_keywords_to_use(persona_keywords, tweets):
  ret_val = []

  for tweet in tweets:
    found_keywords = get_canonical_name_from_keywords(tweet['text'], persona_keywords)
    for fk in found_keywords:
      ret_val.append(fk.keyword_id)

  return list(set(ret_val))


def get_keyword_sources(persona_wks):
  keyword_sources = persona_wks.get_worksheet(0).col_values(3)

  del keyword_sources[0]
  return keyword_sources


@asyncio.coroutine
def main():
  print('starting')

  gc = gspread.login(google_username, google_password)

  if search_query:
    keywords_to_use = keywords.splitlines()
    keywords_to_use_str = search_query
  else:

    persona_wks = gc.open_by_url(persona_spreadsheet)

    persona_keywords = get_persona_keywords(persona_wks)

    keyword_sources = get_keyword_sources(persona_wks)

    keyword_tweets, pending = (
      yield from
      asyncio.wait([
        loop.run_in_executor(None, partial(get_trending_keywords_from_tweets, ks)) for ks in keyword_sources
      ])
    )

    keyword_tweets = list(chain.from_iterable(ch.result()['statuses'] for ch in keyword_tweets))

    keywords_to_use = get_keywords_to_use(persona_keywords, keyword_tweets)

    keywords_to_use_str = " OR ".join(keywords_to_use)

  python_tweets = yield from loop.run_in_executor(None, partial(get_tweets, keywords_to_use_str))

  python_tweets = python_tweets['statuses']

  print("recieved %s tweets" % len(python_tweets))

  python_tweets = yield from acceptable_tweets(python_tweets)

  wks = gc.open_by_url(google_spreadsheet)
  new_worksheet_len = len(wks.worksheets()) + 1
  new_ws_time = datetime.datetime.now().strftime("%I:%M %p on %B %d, %Y")

  new_worksheet_name = "Sheet{0} {1}: {2}".format(new_worksheet_len, new_ws_time, ", ".join(keywords_to_use))[:50]

  worksheet = wks.add_worksheet(title=new_worksheet_name, rows="100", cols="20")

  cols = ['Profile', 'Name', 'Action they recently took', 'Suggested ideas for engagement',
          "Why they're in target audience", 'Followers',
          'Following',
          'Website']

  i = 0
  for i, c in enumerate(cols, start=1):
    worksheet.update_cell(1, i, c)

  worksheet.update_cell(1, i + 1, "Notes")
  worksheet.update_cell(1, i + 2, ", ".join(keywords_to_use))

  col_length = len(cols)
  sheet_range = "A2:{0}{1}".format(chr(col_length - 1 + ord("A")), len(python_tweets) + 1)
  cell_ranges = worksheet.range(sheet_range)

  for i, tweet in enumerate(python_tweets):
    username = "https://twitter.com/{0}".format(tweet['user']['screen_name'])
    human_name = tweet['user']['name']
    followers_count = tweet['user']['followers_count']
    following_count = tweet['user']['friends_count']
    tweet_link = "{0}/status/{1}".format(username, tweet['id_str'])
    tweet_text = tweet['text']
    bio = tweet['user']['description']
    urls = tweet['user']['entities']['url']['urls'][0]['expanded_url'] if 'url' in tweet['user']['entities'] else None
    print("User: {0} ---- {1} ---- {2} ---- {3}".format(username, tweet_text, bio, urls))
    cell_ranges[i * col_length + 0].value = username
    cell_ranges[i * col_length + 1].value = human_name
    cell_ranges[i * col_length + 2].value = tweet_link
    cell_ranges[i * col_length + 3].value = tweet_text
    cell_ranges[i * col_length + 4].value = bio
    cell_ranges[i * col_length + 5].value = followers_count
    cell_ranges[i * col_length + 6].value = following_count
    cell_ranges[i * col_length + 7].value = urls

  worksheet.update_cells(cell_ranges)


@asyncio.coroutine
def output():
  print('searching')


if __name__ == "__main__":
  print(threading.current_thread().ident)

  f = asyncio.wait([main(), output()])
  loop.run_until_complete(f)
