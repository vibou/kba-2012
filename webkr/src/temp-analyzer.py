#!/usr/bin/python
'''
Apply different temporal analytical methods over the data the mine the latent
relations between topic entity and its related entities

'''
## use float division instead of integer division
from __future__ import division

import re
import os
import sys
import gzip
import json
import time
import math
import datetime
import traceback
from collections import defaultdict
from cStringIO import StringIO
import numpy as np

import redis
from config import RedisDB

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

# http://stackoverflow.com/a/1060330/219617
def daterange(start_date, end_date):
  for n in range(int ((end_date - start_date).days)):
    yield start_date + datetime.timedelta(n)

class DictItem(dict):
  """
  A dict that allows for object-like property access syntax.
  """
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class TempAnalyzer():
  '''
  Apply exact matching
  '''

  _doc_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.train_doc_db)
      #db=RedisDB.test_doc_db)

  _ent_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.ent_db)

  _train_edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.train_edmap_db)

  _test_edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.test_edmap_db)

  _qrels_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.qrels_db)

  _temp_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.temp_db)

  _total_doc_num = 36546 + 63564

  def idf(self, query_id):
    '''
    Estimate the IDF of topic entity and its related entities
    '''
    # collect all related entities throught all revisions
    key = 'e2d-map-%s' % query_id
    eid_keys = self._test_edmap_db.hkeys(key)
    eid_keys.sort(key=lambda x: int(x))
    key = 'ent-list-%s' % query_id
    db_item = self._test_edmap_db.hmget(key, eid_keys)
    log('%d entities' % len(db_item))

    idf_key = 'idf-%s' % query_id

    ent_list = []
    for idx, ent in enumerate(db_item):
      ent_id = eid_keys[idx]
      doc_list = self.ent_doc_list(query_id, ent_id)
      doc_num = len(doc_list)
      p_occ = doc_num / self._total_doc_num
      log_idf = - math.log(p_occ)
      #print 'IDF [%s / %s (%s)] %f' %(query_id, ent_id, ent, log_idf)
      # write to DB
      self._temp_db.hset(idf_key, ent_id, log_idf)

  def ent_doc_list(self, query_id, ent_id):
    '''
    Get all the documents which mention the related entity
    '''
    key = 'e2d-map-%s' % query_id
    if not self._test_edmap_db.hexists(key, ent_id):
      msg = 'Invalid ent_id: %s' % ent_id
      log(msg)
      return

    doc_hash = {}

    # training data
    if self._train_edmap_db.hexists(key, ent_id):
      e2d_str = self._train_edmap_db.hget(key, ent_id)
      e2d = json.loads(e2d_str)
      for did in e2d:
        doc_hash[did] = 1

    # testing data
    e2d_str = self._test_edmap_db.hget(key, ent_id)
    e2d = json.loads(e2d_str)
    for did in e2d:
      doc_hash[did] = 1

    return doc_hash

  def cor_rel(self, query_id):
    '''
    Estimate the corrleation between the temporal distribution of topic entity
    and its related entities
    '''
    # get the topic entity
    query = self._ent_db.hget(RedisDB.query_ent_hash, query_id)

    # collect all related entities throught all revisions
    key = 'e2d-map-%s' % query_id
    eid_keys = self._test_edmap_db.hkeys(key)
    eid_keys.sort(key=lambda x: int(x))
    key = 'ent-list-%s' % query_id
    db_item = self._test_edmap_db.hmget(key, eid_keys)
    log('%d entities' % len(db_item))

    ent_list = []
    for idx, ent in enumerate(db_item):
      eid = eid_keys[idx]
      item = DictItem()
      item['eid'] = eid
      item['ent'] = ent
      ent_list.append(item)

    # estimate the temporal distribution for topic entity and its related
    # entities
    topic_dist = self.est_doc_dist_query(query_id)
    topic_norm_list = self.normalize_dist(topic_dist)

    correl_key = 'correl-%s' % query_id
    for ent in ent_list:
      ent_id = ent['eid']
      ent_str = ent['ent']
      ent_dist = self.est_doc_dist_ent(query_id, ent_id)

      if len(topic_dist) != len(ent_dist):
        log('dist length mismatch: %s - %s [%s] (%d : %d)' %(query_id, ent_id,
          ent_str, len(topic_dist), len(ent_dist)))
        continue

      ent_norm_list = self.normalize_dist(ent_dist)
      correl_array = np.correlate(topic_norm_list, ent_norm_list)
      if 1 != len(correl_array):
        log('Invalid correlation array: %s - %s [%s]' %(query_id, ent_id,
          ent_str))
      correl = correl_array[0]

      # write to DB
      self._temp_db.hset(correl_key, ent_id, correl)

      #print 'Correlation [ %s - %s (%s) ]: %f' % (query_id, ent_id, ent_str,
          #correl)

  def normalize_dist(self, dist_hash):
    '''
    Apply normalization over the temporal distribution, generate the list of
    float values
    '''
    sum = 0
    norm_list = []
    for d_time in dist_hash:
      val = dist_hash[d_time]
      norm_list.append(val)
      sum = sum + val

    for idx, val in enumerate(norm_list):
      val = val / sum
      norm_list[idx] = val

    return norm_list

  def date_range(self):
    '''
    Date range for whole data (both training and testing data)
    '''
    start_date = datetime.date(2011, 10, 07)
    end_date = datetime.date(2012, 5, 3)
    return daterange(start_date, end_date)

  def est_doc_dist_query(self, query_id):
    '''
    estimate the temporal distribution of topic entity
    '''
    doc_dist_hash = {}

    # padding 0 for all available dates
    for d_time in self.date_range():
      d_date = '%d-%.2d-%.2d' %(d_time.year, d_time.month, d_time.day)
      doc_dist_hash[d_date] = 0

    # training data
    key = 'd2e-map-%s' % query_id
    did_keys = self._train_edmap_db.hkeys(key)
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
    did_keys = self._test_edmap_db.hkeys(key)
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
    # padding 0 for all available dates
    for d_time in self.date_range():
      d_date = '%d-%.2d-%.2d' %(d_time.year, d_time.month, d_time.day)
      doc_dist_hash[d_date] = 0

    # training data
    if self._train_edmap_db.hexists(key, ent_id):
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
    #analyzer.cor_rel(query_id)
    analyzer.idf(query_id)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

