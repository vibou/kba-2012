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

  _edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.train_edmap_db)
      #db=RedisDB.test_edmap_db)

  def process_data(self, query_id):
    '''
    process the streaming item one by one
    '''
    # first, get the current query
    query = self._ent_db.hget(RedisDB.query_ent_hash, query_id)

    # collect all related entities throught all revisions
    hash_key = 'query-rel-ent-%s' % query_id
    dt_list = self._ent_db.hkeys(hash_key)
    dt_list.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))

    for dt in dt_list:
      rel_ent_str = self._ent_db.hget(hash_key, dt)
      rel_ent_list = rel_ent_str.split('=')

      for ent in rel_ent_list:
        if ent in self._ent2id_hash:
          continue
        else:
          id = len(self._ent2id_hash.keys())
          self._ent2id_hash[ent] = id

    # now we collected all the related entities, each of which has a unique ID
    # for each document, we will then build the map between it and each of the
    # related entities

    print '%d entities in total' % len(self._ent2id_hash.keys())
    for doc_item in self._doc_item_list[query]:
      stream_id = doc_item['stream_id']
      stream_data = doc_item['stream_data']
      self.build_map(stream_id, stream_data)

    # clear the hashes
    self._ent2id_hash.clear()
    self._e2d_hash.clear()
    self._d2e_hash.clear()

def main():
  analyzer = TempAnalyzer()

  query_id_list = range(0, 29, 1)
  for query_id in query_id_list:
    print 'Query %d' % query_id
    analyzer.temp_correl(query_id)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

