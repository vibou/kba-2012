#!/usr/bin/python

"""
Provide a web interface to browse the temporal information of related entities
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

define("port", default=8888, help="run on the given port", type=int)

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
  def _rel_ent_dist_db(self):
    return self.application._rel_ent_dist_db

class HomeHandler(BaseHandler):
  def get(self):
    url = '/ent'
    self.redirect(url)

class EntIndexHandler(BaseHandler):
  def get(self):
    num = self._rel_ent_dist_db.llen(RedisDB.query_ent_list)
    if 0 == num:
      msg = 'no item found'
      self.render("error.html", msg=msg)
      return

    query_ent_list = self._rel_ent_dist_db.lrange(RedisDB.query_ent_list, 0, num)
    query_ent_hash = self._rel_ent_dist_db.hmget(RedisDB.query_ent_hash,
        query_ent_list)
    ret_items = []
    for index in query_ent_list:
      ret_item = DictItem()
      ret_item['id'] = index
      ret_item['ent'] = query_ent_hash[int(index)]
      ret_items.append(ret_item)

    self.render("ent-index.html", title='KBA Query Entities', ret_items=ret_items)

class EntViewHandler(BaseHandler):
  def get(self, ent_id):
    hash_key = 'query-%s' % ent_id
    keys = self._rel_ent_dist_db.hkeys(hash_key)
    if 0 == len(keys):
      msg = 'no data found'
      self.render("error.html", msg=msg)
      return

    ## retrieve the basic information of the relevant entity
    '''
    db_keys = ['id', 'query', 'ent', 'url']
    db_item = self._wiki_ent_list_db.hmget(ent_id, db_keys)

    item = DictItem()
    item['id'] = db_item[0]
    item['query'] = db_item[1]
    item['ent'] = db_item[2]
    item['url'] = db_item[3]
    item['num'] = len(keys)
    '''

    ent = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, ent_id)

    self.render("ent-view.html", ent_id=ent_id, ent=ent)

class EntDistHandler(BaseHandler):
  def get(self, ent_id):
    hash_key = 'query-%s' % ent_id
    keys = self._rel_ent_dist_db.hkeys(hash_key)
    if 0 == len(keys):
      msg = 'no data found'
      self.render("error.html", msg=msg)
      return

    ## sort the keys by date: http://stackoverflow.com/q/2589479
    keys.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))
    db_item = self._rel_ent_dist_db.hmget(hash_key, keys)

    line = 'date\tclose\n'
    self.write(line)

    for idx, val in enumerate(db_item):
      val = db_item[idx]
      line = '%s\t%s\n' %(keys[idx], val)
      self.write(line)

class Application(tornado.web.Application):
  def __init__(self):
    handlers = [
      (r"/", HomeHandler),
      (r"/ent", EntIndexHandler),
      (r"/ent/(\d+)", EntViewHandler),
      (r"/ent/dist/(\d+)", EntDistHandler),
    ]

    settings = dict(
      app_title = u"KBA Retrieval Browser",
      template_path = os.path.join(os.path.dirname(__file__), "templates"),
    )

    tornado.web.Application.__init__(self, handlers, **settings)

    # global database connections for all handles
    self._rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.rel_ent_dist_db)

def main():
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()

