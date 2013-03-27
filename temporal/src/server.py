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
    num = self._exact_match_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item_list = self._exact_match_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score']
      the_ret_item = self._exact_match_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['score'] = the_ret_item[4]
      ret_items.append(ret_item)

    self.render("train-ret-index.html", title='KBA Training Results', ret_items=ret_items)

class EntViewHandler(BaseHandler):
  def get(self, ent_id):
    keys = self._rel_ent_dist_db.hkeys(ent_id)
    if 0 == len(keys):
      msg = 'no data found'
      self.render("error.html", msg=msg)
      return

    ## retrieve the basic information of the relevant entity
    db_keys = ['id', 'query', 'ent', 'url']
    db_item = self._wiki_ent_list_db.hmget(ent_id, db_keys)

    item = DictItem()
    item['id'] = db_item[0]
    item['query'] = db_item[1]
    item['ent'] = db_item[2]
    item['url'] = db_item[3]
    item['num'] = len(keys)

    ## retrieve the URL list of the relevant document
    list_name = '%s-list' %(ent_id)
    num = self._rel_ent_dist_db.llen(list_name)
    ret_item_list = self._rel_ent_dist_db.lrange(list_name, 0, num)
    ret_items = []
    for store_item in ret_item_list:
      list = store_item.split(':')
      stream_id = list[0]
      ret_url = list[1]
      rel = list[2]
      list = stream_id.split('-')
      epoch = list[0]
      time = datetime.datetime.utcfromtimestamp(float(epoch))
      date = '%d-%.2d-%.2d' %(time.year, time.month, time.day)

      ret_item = DictItem()
      ret_item['date'] = date
      ret_item['stream_id'] = stream_id
      ret_item['url'] = ret_url
      ret_item['rel'] = rel
      ret_items.append(ret_item)
    ## sort it by date
    ## thanks to http://stackoverflow.com/q/2589479
    ret_items.sort(key=lambda x: datetime.datetime.strptime(x['date'], '%Y-%m-%d'))

    self.render("rel-ent-view.html", ent_id=ent_id, ent_item=item,
        ret_items=ret_items)

class EntDistHandler(BaseHandler):
  def get(self, ent_id):
    keys = self._rel_ent_dist_db.hkeys(ent_id)
    if 0 == len(keys):
      msg = 'no data found'
      self.render("error.html", msg=msg)
      return
    ## sort the keys by date: http://stackoverflow.com/q/2589479
    keys.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
    db_item = self._rel_ent_dist_db.hmget(ent_id, keys)

    line = 'date\tclose\n'
    self.write(line)

    cumu = 0
    for idx, val in enumerate(db_item):
      cumu += int(db_item[idx])
      line = '%s\t%s\n' %(keys[idx], cumu)
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

