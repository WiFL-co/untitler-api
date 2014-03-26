import asyncio
import textwrap
import logging
import re

import aiohttp
from aiohttp.session import Session
import bs4
import tqdm


genres_to_save = set()
sem = asyncio.Semaphore(2)
session = Session()

year_re = re.compile(r"\d{4}")
stop_words_string = textwrap.dedent("""\
  book
  fav
  read
  series
  fiction
  buy
  own
  kindle
  wishlist
  wish-list
  nook
  library
  maybe
  review
  dnf
  finish
  stars
  star
  signed
  series
  my
  meh
  loved
  amazing
  have
  recommend
  want
  hold
  borrow
  debut
  get
  mine
  hardcover
  best
""")

stop_words = set(w for w in stop_words_string.splitlines())

import unicodedata


def strip_accents(s):
  return ''.join(c for c in unicodedata.normalize('NFD', s)
                 if unicodedata.category(c) != 'Mn')


# http://stackoverflow.com/a/518232/173957
@asyncio.coroutine
def get(*args, **kwargs):
  response = yield from aiohttp.request('GET', *args, compress=True, session=session,
                                        **kwargs)
  # handle non-200 like 404
  return (yield from response.read_and_close(decode=True))


@asyncio.coroutine
def login(*args, **kwargs):
  request = yield from aiohttp.request('GET', "https://www.goodreads.com/user/sign_in",
                                       session=session,
                                       **kwargs)

  page = yield from request.read_and_close(decode=True)

  soup = bs4.BeautifulSoup(page)

  n_hidden = soup.find(attrs={"name": "n"})['value']
  csrf_token = soup.find(attrs={"name": "authenticity_token"})['value']

  import os
  username = os.environ.get("username")
  password = os.environ.get("password")
  login_data = {"n": n_hidden, "authenticity_token": csrf_token, "user[email]": username,
                "user[password]": password}

  request = yield from aiohttp.request('POST', "https://www.goodreads.com/user/sign_in",
                                       session=session,
                                       data=login_data,
                                       **kwargs)
  request.close()

  return page


def only_printable(the_string):
  from string import printable

  new_string = ''.join(char for char in the_string if char in printable)
  return new_string


def get_genres(page):
  import html


  soup = bs4.BeautifulSoup(page)
  genres = soup.select(".shelfStat")
  for g in genres:
    genre, count = g.a.text, int(g.find(class_="greyText").text.replace('books', '').replace(',', '').strip())
    if not (any(x in genre for x in stop_words) or year_re.search(genre)):
      count_ = (strip_accents(html.unescape(only_printable(genre))), count)
      if count_[0]:      genres_to_save.add(count_)

  from pprint import pprint

  pprint(genres_to_save)


@asyncio.coroutine
def print_genre(query):
  yield from login()
  url = query
  # url = 'http://thepiratebay.se/search/{}/0/7/0'.format(query)

  with (yield from sem):
    page = yield from get(url)

  magnet = get_genres(page)


@asyncio.coroutine
def wait_with_progress(coros):
  for f in tqdm.tqdm(asyncio.as_completed(coros), total=len(coros)):
    yield from f


def main():
  genre_pages = ['https://www.goodreads.com/shelf']
  loop = asyncio.get_event_loop()
  f = wait_with_progress([print_genre(d) for d in genre_pages])
  loop.run_until_complete(f)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  main()













# import logging
# import re
# import signal
# import sys
# import asyncio
# import urllib.parse
#
# import aiohttp
#
#
# class Crawler:
#
# def __init__(self, rooturl, loop, maxtasks=100):
# self.rooturl = rooturl
# self.loop = loop
# self.todo = set()
# self.busy = set()
# self.done = {}
# self.tasks = set()
# self.sem = asyncio.Semaphore(maxtasks)
#
#     # session stores cookies between requests and uses connection pool
#     self.session = aiohttp.Session()
#
#   @asyncio.coroutine
#   def run(self):
#     asyncio.Task(self.addurls([(self.rooturl, '')]))  # Set initial work.
#     yield from asyncio.sleep(1)
#     while self.busy:
#       yield from asyncio.sleep(1)
#
#     self.session.close()
#     self.loop.stop()
#
#   @asyncio.coroutine
#   def addurls(self, urls):
#     for url, parenturl in urls:
#       url = urllib.parse.urljoin(parenturl, url)
#       url, frag = urllib.parse.urldefrag(url)
#       if (url.startswith(self.rooturl) and
#               url not in self.busy and
#               url not in self.done and
#               url not in self.todo):
#         self.todo.add(url)
#         yield from self.sem.acquire()
#         task = asyncio.Task(self.process(url))
#         task.add_done_callback(lambda t: self.sem.release())
#         task.add_done_callback(self.tasks.remove)
#         self.tasks.add(task)
#
#   @asyncio.coroutine
#   def process(self, url):
#     print('processing:', url)
#
#     self.todo.remove(url)
#     self.busy.add(url)
#     try:
#       resp = yield from aiohttp.request(
#         'get', url, session=self.session)
#     except Exception as exc:
#       print('...', url, 'has error', repr(str(exc)))
#       self.done[url] = False
#     else:
#       if resp.status == 200 and resp.get_content_type() == 'text/html':
#         data = (yield from resp.read()).decode('utf-8', 'replace')
#         urls = re.findall(r'(?i)href=["\']?([^\s"\'<>]+)', data)
#         asyncio.Task(self.addurls([(u, url) for u in urls]))
#
#       resp.close()
#       self.done[url] = True
#
#     self.busy.remove(url)
#     print(len(self.done), 'completed tasks,', len(self.tasks),
#           'still pending, todo', len(self.todo))
#
#
# def main():
#   loop = asyncio.get_event_loop()
#
#   c = Crawler('https://www.goodreads.com/shelf', loop)
#   asyncio.Task(c.run())
#
#   try:
#     loop.add_signal_handler(signal.SIGINT, loop.stop)
#   except RuntimeError:
#     pass
#   loop.run_forever()
#   print('todo:', len(c.todo))
#   print('busy:', len(c.busy))
#   print('done:', len(c.done), '; ok:', sum(c.done.values()))
#   print('tasks:', len(c.tasks))
#
#
# if __name__ == '__main__':
#   if '--iocp' in sys.argv:
#     from asyncio import events, windows_events
#     sys.argv.remove('--iocp')
#     logging.info('using iocp')
#     el = windows_events.ProactorEventLoop()
#     events.set_event_loop(el)
#
#   main()
