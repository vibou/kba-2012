#!/usr/bin/python

import os
import re
import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver

from config import RedisDB
from tornado.options import define, options

define("port", default=8888, help="run on the given port", type=int)

corpus_dir = './corpus/cleansed'

class Dir(dict):
  """
  A dict that allows for object-like property access syntax.
  """
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class File(dict):
  """
  A dict that allows for object-like property access syntax.
  """
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class HomeHandler(tornado.web.RequestHandler):
  def get(self):

    dirs = []
    for fname in os.listdir(corpus_dir):
      fpath = os.path.join(corpus_dir, fname)
      if not os.path.isdir(fpath): continue
      dir = Dir()
      dir['name'] = fname
      dirs.append(dir)

    self.render("corpus-index.html", title="KBA", dirs=dirs)

class DateHandler(tornado.web.RequestHandler):
  def get(self, slug):
    date_dir = os.path.join(corpus_dir, slug)

    if not os.path.isdir(date_dir):
      raise tornado.web.HTTPError(404)

    files = []
    for fname in os.listdir(date_dir):
      ## ignore other files
      if not fname.endswith('.gpg'): continue
      if not fname.endswith('.xz'): continue

      file = os.path.join(date_dir, fname)

    self.render("date-index.html", title=slug, files=files, date=date)

class Application(tornado.web.Application):
  def __init__(self):
    handlers = [
      (r"/", HomeHandler),
      (r"/date-([^/]+)/", DateHandler),
      (r"/date-([^/]+)//file-([^/]+)/", FileHandler),
    ]

    settings = dict(
      app_title = u"HoTweets",
      template_path = os.path.join(os.path.dirname(__file__), "templates"),
    )

    tornado.web.Application.__init__(self, handlers, **settings)

def main():
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()
