#!/usr/bin/python

"""
Provide a web interface to browse the retrieval
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

from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from cStringIO import StringIO
from kba_thrift.ttypes import StreamItem, StreamTime, ContentItem

define("port", default=7777, help="run on the given port", type=int)

corpus_dir = './corpus/cleansed'

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
  def _eval_db(self):
    return self.application._eval_db
  @property
  def _wiki_ent_list_db(self):
    return self.application._wiki_ent_list_db

class HomeHandler(BaseHandler):
  def get(self):
    url = '/browse'
    self.redirect(url)

class BrowseHandler(BaseHandler):
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

    self.render("ret-index.html", title='KBA', ret_items=ret_items)

class RetHandler(BaseHandler):
  def get(self, ret_id):
    ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score', 'stream_data']
    the_ret_item = self._exact_match_db.hmget(ret_id, ret_item_keys)

    if not the_ret_item[5]:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item = DictItem()
    ret_item['id'] = the_ret_item[0]
    ret_item['query'] = the_ret_item[1]
    ret_item['file'] = the_ret_item[2]
    ret_item['stream_id'] = the_ret_item[3]
    ret_item['score'] = the_ret_item[4]
    ret_item['stream_data'] = the_ret_item[5]

    self.render("ret-item.html", title='ret_item', ret_item=ret_item)

class EvalHandler(BaseHandler):
  def get(self):
    num = self._eval_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item_list = self._eval_db.lrange(RedisDB.ret_item_list, 0, num)
    ## remove duplicates in the list
    ## thanks to:
    ## http://docs.python.org/faq/programming.html#how-do-you-remove-duplicates-from-a-list
    ret_item_list = list(set(ret_item_list))
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score', 'judge1', 'judge2']
      the_ret_item = self._eval_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['score'] = the_ret_item[4]
      ret_item['judge1'] = the_ret_item[5]
      ret_item['judge2'] = the_ret_item[6]
      ret_items.append(ret_item)

    self.render("eval-index.html", title='KBA Qrels', ret_items=ret_items)

class EvalItemHandler(BaseHandler):
  def get(self, ret_id):
    ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score',
        'stream_data', 'judge1', 'judge2']
    the_ret_item = self._eval_db.hmget(ret_id, ret_item_keys)

    if not the_ret_item[5]:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item = DictItem()
    ret_item['id'] = the_ret_item[0]
    ret_item['query'] = the_ret_item[1]
    ret_item['file'] = the_ret_item[2]
    ret_item['stream_id'] = the_ret_item[3]
    ret_item['score'] = the_ret_item[4]
    ret_item['stream_data'] = the_ret_item[5]
    ret_item['judge1'] = the_ret_item[6]
    ret_item['judge2'] = the_ret_item[7]

    self.render("eval-item.html", title='ret_item', ret_item=ret_item)

class WikiEntListHandler(BaseHandler):
  def get(self):
    num = self._wiki_ent_list_db.llen(RedisDB.wiki_ent_list)
    if 0 == num:
      msg = 'no wiki_ent found'
      self.render("error.html", msg=msg)
      return

    ent_item_list = self._wiki_ent_list_db.lrange(RedisDB.wiki_ent_list, 0, num)
    ## remove duplicates in the list
    ## thanks to:
    ## http://docs.python.org/faq/programming.html#how-do-you-remove-duplicates-from-a-list
    #ent_item_list = list(set(ent_item_list))
    ent_items = []
    for ent_id in ent_item_list:
      keys = ['id', 'query', 'ent', 'url']
      db_item = self._wiki_ent_list_db.hmget(ent_id, keys)

      item = DictItem()
      item['id'] = db_item[0]
      item['query'] = db_item[1]
      item['ent'] = db_item[2]
      item['url'] = db_item[3]
      ent_items.append(item)

    self.render("wiki-ent-list.html", title='KBA Wiki Ent List', ent_items=ent_items)

class Application(tornado.web.Application):
  def __init__(self):
    handlers = [
      (r"/", HomeHandler),
      (r"/browse", BrowseHandler),
      (r"/eval", EvalHandler),
      (r"/ret/(\d+)", RetHandler),
      (r"/eval/(\d+)", EvalItemHandler),
      (r"/wiki-ent-list", WikiEntListHandler),
    ]

    settings = dict(
      app_title = u"KBA Retrieval Browser",
      template_path = os.path.join(os.path.dirname(__file__), "templates"),
    )

    tornado.web.Application.__init__(self, handlers, **settings)

    # global database connections for all handles
    self._exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.exact_match_db)
    self._eval_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.eval_db)
    self._wiki_ent_list_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.wiki_ent_list_db)

def main():
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()
