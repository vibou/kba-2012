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
  _exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.test_exact_match_db)

  _fuzzy_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.fuzzy_match_db)

  _merge_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.oair_test_db)

  _ret_items = []

  def get_exact_results(self):
    num = self._exact_match_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      print 'no exact match results found'
      return

    print 'Loading %d exact match results' % num

    ret_item_list = self._exact_match_db.lrange(RedisDB.ret_item_list, 0, num)
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'stream_data', 'score']
      the_ret_item = self._exact_match_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['stream_data'] = the_ret_item[4]
      ret_item['score'] = the_ret_item[5]
      self._ret_items.append(ret_item)

  def get_fuzzy_result(self):
    num = self._fuzzy_match_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      print'no fuzzy match results found'
      return

    print 'Loading %d fuzzy match results' % num

    ret_item_list = self._fuzzy_match_db.lrange(RedisDB.ret_item_list, 0, num)
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'stream_data', 'score']
      the_ret_item = self._fuzzy_match_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      # assign new ID
      ret_item['id'] = str(100000 + int(the_ret_item[0]))
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['stream_data'] = the_ret_item[4]
      ret_item['score'] = the_ret_item[5]
      self._ret_items.append(ret_item)

  def save_to_db(self):
    num = len(self._ret_items)

    print 'Saving %d merged results' % num

    for ret_item in self._ret_items:
      id = ret_item['id']
      print '%s / %d ' %( id, num )
      self._merge_db.rpush(RedisDB.ret_item_list, id)
      self._merge_db.hmset(id, ret_item)

    print 'Done.'

  def save_to_file(self):
    num = len(self._ret_items)

    file = 'oair.txt'
    f = open(file, 'w')
    print 'Saving %d merged results to %s' % (num, file)

    self._ret_items.sort(key=lambda x: x['query'])
    for ret_item in self._ret_items:
      id = ret_item['id']
      query = ret_item['query']
      stream_id = ret_item['stream_id']
      score = ret_item['score']
      f.write('%s %s %s\n' %(query, stream_id, score))

    f.close()
    print 'Done.'


def main():
  merger = Merger()
  merger.get_exact_results()
  merger.get_fuzzy_result()
  #merger.save_to_db()
  merger.save_to_file()

if __name__ == "__main__":
  main()

