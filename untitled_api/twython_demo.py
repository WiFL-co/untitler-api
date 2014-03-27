import asyncio
import datetime
import json
import textwrap
import threading
import os

from twython import Twython


app_key = os.environ.get('app_key')
app_secret = os.environ.get('app_secret')

twitter = Twython(app_key, app_secret)
loop = asyncio.get_event_loop()

current_day = datetime.datetime.utcnow().strftime("%Y-%m-%d")
keywords = textwrap.dedent("""\
  #firebase
  #atom.io
""")
search_query = " OR ".join(k for k in keywords.splitlines())


def get_tweets():
  return twitter.search(q=search_query, result_type='mixed', since=current_day, lang="en", include_entities=False)


@asyncio.coroutine
def main():
  print('starting')

  python_tweets = yield from  loop.run_in_executor(None, get_tweets)

  print(json.dumps(python_tweets['statuses']))

  # for tweet in python_tweets['statuses']:
  # pprint(tweet)


@asyncio.coroutine
def output():
  print('searching')


if __name__ == "__main__":
  print(threading.current_thread().ident)

  f = asyncio.wait([main(), output()])
  loop.run_until_complete(f)
