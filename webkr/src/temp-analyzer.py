#!/usr/bin/python
'''
Apply different temporal analytical methods over the data the mine the latent
relations between topic entity and its related entities

'''

import re
import os
import sys
import traceback
import gzip
import json
import time
import datetime
from collections import defaultdict
from cStringIO import StringIO

import redis
from config import RedisDB

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

# http://stackoverflow.com/a/1060330/219617
def daterange(start_date, end_date):
  for n in range(int ((end_date - start_date).days)):
    yield start_date + timedelta(n)

class DictItem(dict):
  """
  A dict that allows for object-like property access syntax.
  """
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class WikiMatch():
  '''
  Apply exact matching
  '''

  _doc_item_list = defaultdict(list)

  _ent2id_hash = {}   # related entity to ID hash
  _e2d_hash = {}
  _d2e_hash = {}

  _doc_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.train_doc_db)
      #db=RedisDB.test_doc_db)

  _ent_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.ent_db)

  _train_edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.train_edmap_db)

  _test_edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.test_edmap_db)

  def temp_correl(self, query_id):
    '''
    process the streaming item one by one
    '''
    # get the topic entity
    query = self._ent_db.hget(RedisDB.query_ent_hash, query_id)

    # collect all related entities throught all revisions
    key = 'ent-list-%s' % query_id
    db_item = self._test_edmap_db.hmget(key, eid_keys)
    log('%d entities' % len(db_item))

    ent_list = []
    for idx, ent in enumerate(db_item):
      item = DictItem()
      item['eid'] = eid
      item['ent'] = ent
      ent_list.append(item)

    # clear the hashes
    self._ent2id_hash.clear()
    self._e2d_hash.clear()
    self._d2e_hash.clear()

  def date_range(self):
    '''
    Date range for whole data (both training and testing data)
    '''
    start_date = date(2011, 10, 07)
    end_date = date(2012, 5, 2)
    return daterange(start_date, end_date)

  def est_doc_dist_query(self, query_id):
    '''
    estimate the temporal distribution of topic entity
    '''
    doc_dist_hash = {}

    # padding 0 for all avaiable dates
    for day in self.date_range():
      d_date = '%d-%.2d-%.2d' %(d_time.year, d_time.month, d_time.day)
      doc_dist_hash[d_date] = 0

    # training data
    key = 'd2e-map-%s' % query_id
    did_keys = self._edmap_db.hkeys(key)
    for did in did_keys:
      epoch = float(did.split('-')[0])
      d_time = datetime.datetime.utcfromtimestamp(epoch)
      d_date = '%d-%.2d-%.2d' %(d_time.year, d_time.month, d_time.day)
      if d_date not in doc_dist_hash:
        doc_dist_hash[d_date] = 1
      else:
        doc_dist_hash[d_date] += 1

    # testing data
    key = 'd2e-map-%s' % query_id
    did_keys = self._edmap_db.hkeys(key)
    for did in did_keys:
      epoch = float(did.split('-')[0])
      d_time = datetime.datetime.utcfromtimestamp(epoch)
      d_date = '%d-%.2d-%.2d' %(d_time.year, d_time.month, d_time.day)
      if d_date not in doc_dist_hash:
        doc_dist_hash[d_date] = 1
      else:
        doc_dist_hash[d_date] += 1

    return doc_dist_hash

  def est_doc_dist_ent(self, query_id, ent_id):
    '''
    estimate the temporal distribution of related entity
    '''
    key = 'e2d-map-%s' % query_id
    if not self._test_edmap_db.hexists(key, ent_id):
      msg = 'Invalid ent_id: %s' % ent_id
      log(msg)
      return

    doc_dist_hash = {}
    # padding 0 for all avaiable dates
    for day in self.date_range():
      d_date = '%d-%.2d-%.2d' %(d_time.year, d_time.month, d_time.day)
      doc_dist_hash[d_date] = 0

    # training data
    e2d_str = self._train_edmap_db.hget(key, ent_id)
    e2d = json.loads(e2d_str)
    for did in e2d:
      epoch = float(did.split('-')[0])
      d_time = datetime.datetime.utcfromtimestamp(epoch)
      d_date = '%d-%.2d-%.2d' %(d_time.year, d_time.month, d_time.day)
      if d_date not in doc_dist_hash:
        doc_dist_hash[d_date] = 1
      else:
        doc_dist_hash[d_date] += 1

    # testing data
    e2d_str = self._test_edmap_db.hget(key, ent_id)
    e2d = json.loads(e2d_str)
    for did in e2d:
      epoch = float(did.split('-')[0])
      d_time = datetime.datetime.utcfromtimestamp(epoch)
      d_date = '%d-%.2d-%.2d' %(d_time.year, d_time.month, d_time.day)
      if d_date not in doc_dist_hash:
        doc_dist_hash[d_date] = 1
      else:
        doc_dist_hash[d_date] += 1

    return doc_dist_hash

def main():
  analyzer = TempAnalyzer()

  query_id_list = range(0, 29, 1)
  for query_id in query_id_list:
    log('Query %d' % query_id)
    analyzer.temp_correl(query_id)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

