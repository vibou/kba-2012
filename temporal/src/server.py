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

  @property
  def _test_db(self):
    return self.application._test_db

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

    ## retrieve the list of revisions
    rel_ent_hash_key = 'query-rel-ent-%s' % ent_id
    rel_ent_keys = self._rel_ent_dist_db.hkeys(rel_ent_hash_key)

    ## sort the keys by date: http://stackoverflow.com/q/2589479
    rel_ent_keys.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))
    db_item = self._rel_ent_dist_db.hmget(rel_ent_hash_key, rel_ent_keys)

    date_list = []

    for idx, rel_ent_str in enumerate(db_item):
      item = DictItem()
      item['date'] = rel_ent_keys[idx]
      item['num'] = len(rel_ent_str.split('='))
      date_list.append(item)

    ent = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, ent_id)
    self.render("ent-view.html", ent_id=ent_id, ent=ent, date_list=date_list)

class EntRevHandler(BaseHandler):
  def get(self, ent_id, date):
    ## retrieve the list of revisions
    rel_ent_hash_key = 'query-rel-ent-%s' % ent_id
    if not self._rel_ent_dist_db.hexists(rel_ent_hash_key, date):
      msg = 'no data found'
      self.render("error.html", msg=msg)
      return

    rel_ent_str = self._rel_ent_dist_db.hget(rel_ent_hash_key, date)
    rel_ent_list = rel_ent_str.split('=')

    ent = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, ent_id)
    self.render("ent-rev.html", ent_id=ent_id, ent=ent, date=date,
        rel_ent_list=rel_ent_list)

class EntDistHandler(BaseHandler):
  def get(self, ent_id):
    hash_key = 'query-%s' % ent_id
    keys = self._rel_ent_dist_db.hkeys(hash_key)
    if 0 == len(keys):
      msg = 'no dist data found'
      self.render("error.html", msg=msg)
      return

    rel_num_hash_key = 'query-rel-ent-num-%s' % ent_id
    rel_num_keys = self._test_db.hkeys(rel_num_hash_key)
    if 0 == len(rel_num_keys):
      msg = 'no rel_num data found'
      self.render("error.html", msg=msg)
      return

    if set(keys) != set(rel_num_keys):
      msg = 'dist and rel_num data are not consistent'
      self.render("error.html", msg=msg)
      return

    eval_hash_key = 'query-opt-C-F-%s' % ent_id
    #eval_hash_key = 'query-opt-CR-F-%s' % ent_id
    eval_keys = self._rel_ent_dist_db.hkeys(eval_hash_key)
    if 0 == len(eval_keys):
      msg = 'no eval_dist data found'
      self.render("error.html", msg=msg)
      return

    if set(keys) != set(eval_keys):
      msg = 'dist and eval_dist data are not consistent'
      self.render("error.html", msg=msg)
      return

    cr_eval_hash_key = 'query-opt-CR-F-%s' % ent_id
    cr_eval_keys = self._rel_ent_dist_db.hkeys(cr_eval_hash_key)
    if 0 == len(cr_eval_keys):
      msg = 'no cr_eval_dist data found'
      self.render("error.html", msg=msg)
      return

    if set(keys) != set(cr_eval_keys):
      msg = 'dist and cr_eval_dist data are not consistent'
      self.render("error.html", msg=msg)
      return

    ## sort the keys by date: http://stackoverflow.com/q/2589479
    keys.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))

    # retrieve from DB
    db_item = self._rel_ent_dist_db.hmget(hash_key, keys)
    rel_num_db_item = self._test_db.hmget(rel_num_hash_key, keys)
    eval_db_item = self._rel_ent_dist_db.hmget(eval_hash_key, keys)
    cr_eval_db_item = self._rel_ent_dist_db.hmget(cr_eval_hash_key, keys)

    line = 'date\tclose\n'
    self.write(line)

    for idx, dist_val in enumerate(db_item):
      dist_val = db_item[idx]
      rel_num_val = rel_num_db_item[idx]
      eval_val = eval_db_item[idx]
      cr_eval_val = cr_eval_db_item[idx]
      line = '%s\t%s\t%s\t%s\t%s\n' %(keys[idx], dist_val, rel_num_val,
          eval_val, cr_eval_val)
      self.write(line)

class Application(tornado.web.Application):
  def __init__(self):
    handlers = [
      (r"/", HomeHandler),
      (r"/ent", EntIndexHandler),
      (r"/ent/(\d+)", EntViewHandler),
      (r"/ent/dist/(\d+)", EntDistHandler),
      (r"/ent/(\d+)/(\d+-\d+)", EntRevHandler),
    ]

    settings = dict(
      app_title = u"KBA Retrieval Browser",
      template_path = os.path.join(os.path.dirname(__file__), "templates"),
    )

    tornado.web.Application.__init__(self, handlers, **settings)

    # global database connections for all handles
    self._rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.rel_ent_dist_db)

    self._test_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.test_db)

def main():
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()

