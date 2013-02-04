#!/usr/bin/python

"""
Provide a web interface to browse the distribution
"""

import os
import re
import time
import datetime

import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver

import redis
from config import RedisDB
from tornado.options import define, options

define("port", default=9999, help="run on the given port", type=int)

'''
Transfer the raw data to HTML
Basically the goal is to make sure each paragraph is embedded in <p></p>
'''
def raw2html(raw):
  sentences = raw.split('\n')
  html = ""
  for sent in sentences:
    sent = "<p>" + sent + "</p>\n"
    html += sent

  return html

class DictItem(dict):
  """
  A dict that allows for object-like property access syntax.
  """
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class BaseHandler(tornado.web.RequestHandler):
  @property
  def _exact_match_db(self):
    return self.application._exact_match_db

  @property
  def _wiki_ent_list_db(self):
    return self.application._wiki_ent_list_db

  @property
  def _wiki_ent_dist_db(self):
    return self.application._wiki_ent_dist_db

class HomeHandler(BaseHandler):
  def get(self):
    self.render("home.html", title='KBA Wiki Ent Dist', items=date_items)

class IDFRelHandler(BaseHandler):
  def get(self, ent_id):
    self.render("idf-rel.html")

class IDFRelDistHandler(BaseHandler):
  def get(self, ent_id):


    line = '%d\t%d\n' %(prevSunday.strftime('%Y-%m-%d'), week_cum)
    self.write(line)

class Application(tornado.web.Application):
  def __init__(self):
    handlers = [
      (r"/", HomeHandler),
      (r"/idf/rel", IDFRelHandler),
      (r"/idf/rel/dist", IDFRelDistHandler),
    ]

    settings = dict(
      app_title = u"Wiki Ent Dist",
      template_path = os.path.join(os.path.dirname(__file__), "dist"),
    )

    tornado.web.Application.__init__(self, handlers, **settings)

    # global database connections for all handles
    self._exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB._test_exact_match_db)

    self._wiki_ent_list_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_list_db)

    self._wiki_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_dist_db)

def main():
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()

