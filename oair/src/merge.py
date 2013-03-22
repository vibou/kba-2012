#!/usr/bin/python

"""
Merge the results of previous URL-name based exact match with entity surface
name based fuzzy match
"""

import os
import re
import time
import datetime

import redis
from config import RedisDB
from tornado.options import define, options

from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from cStringIO import StringIO
from kba_thrift.ttypes import StreamItem, StreamTime, ContentItem


class DictItem(dict):
  """
  A dict that allows for object-like property access syntax.
  """
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class Merger():
    # global database connections for all handles
    self._exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.fuzzy_match_db)

    self._test_exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.test_exact_match_db)

    self._filtered_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        #db=RedisDB.filtered_train_db)
        db=RedisDB.filtered_test_db)

  def get_fuzzy_result(self):
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


  def get_exact_results(self):
    num = self._test_exact_match_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item_list = self._test_exact_match_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score']
      the_ret_item = self._test_exact_match_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['score'] = the_ret_item[4]
      ret_items.append(ret_item)

def main():

if __name__ == "__main__":
  main()

