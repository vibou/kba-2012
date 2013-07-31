#!/usr/bin/python
'''

Generate the E-D and D-E map between documents and related entities, which
serves as cache (index) to accelerate the processing of processing on tuning
the performance

gen-ed-map.py <query_id>
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

QUERY_ENT_MATCH_SCORE = 100
WIKI_ENT_MATCH_SCORE = 1

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
      #db=RedisDB.train_doc_db)
      db=RedisDB.test_doc_db)

  _rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.ent_db)

  _edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      #db=RedisDB.train_edmap_db)
      db=RedisDB.test_edmap_db)

  def format_query(self, query):
    '''
    format the original query
    '''
    # remove parentheses
    parentheses_regex = re.compile( '\(.*\)' )
    query = parentheses_regex.sub( '', query)

    ## replace non word character to space
    non_word_regex = re.compile( '(\W+|\_+)' )
    query = non_word_regex.sub( ' ', query)

    ## compress multiple spaces
    space_regex = re.compile ( '\s+' )
    query = space_regex.sub( ' ', query)

    ## remove leading space
    space_regex = re.compile ( '^\s' )
    query = space_regex.sub( '', query)

    ## remove trailing space
    space_regex = re.compile ( '\s$' )
    query = space_regex.sub( '', query)

    return query.lower()

  def sanitize(self, str):
    '''
    sanitize the streaming item
    '''
    ## replace non word character to space
    non_word_regex = re.compile( '(\W+|\_+)' )
    str = non_word_regex.sub( ' ', str)

    ## compress multiple spaces
    space_regex = re.compile ( '\s+' )
    str = space_regex.sub( ' ', str)

    return str.lower()

  def build_map(self, did, doc):
    '''
    Calculate the score of a document w.r.t. the given query
    '''
    self._d2e_hash[did] = {}

    for ent in self._ent2id_hash:
      eid = self._ent2id_hash[ent]
      ent_str = ' %s ' % self.format_query(ent)

      if re.search(ent_str, doc, re.I | re.M):
        ## change to count match once to count the total number of matches
        match_list = re.findall(ent_str, doc, re.I | re.M)
        score = len(match_list)

        # update the map
        if eid not in self._e2d_hash:
          self._e2d_hash[eid] = {}
        self._e2d_hash[eid][did] = score
        self._d2e_hash[did][eid] = score

  def process_data(self, query_id):
    '''
    process the streaming item one by one
    '''
    # first, get the current query
    org_query = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, query_id)

    hash_key = 'query-rel-ent-%s' % query_id
    dt_list = self._rel_ent_dist_db.hkeys(hash_key)
    dt_list.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))

    # for each revision (i.e. a list of related entities), collect all the
    # related entities
    for dt in dt_list:
      rel_ent_str = self._rel_ent_dist_db.hget(hash_key, dt)
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
    for doc_item in self._doc_item_list[org_query]:
      stream_id = doc_item['stream_id']
      stream_data = doc_item['stream_data']
      self.build_map(stream_id, stream_data)

    print 'Saving to DB'

    # save the entity list
    key = 'ent-list-%s' % query_id
    for ent in self._ent2id_hash:
      eid = self._ent2id_hash[ent]
      self._edmap_db.hset(key, eid, ent)

    # save the E2D map
    key = 'e2d-map-%s' % query_id
    for eid in self._e2d_hash:
      str = json.dumps(self._e2d_hash[eid])
      self._edmap_db.hset(key, eid, str)

    # save the D2E map
    key = 'd2e-map-%s' % query_id
    for did in self._d2e_hash:
      str = json.dumps(self._d2e_hash[did])
      self._edmap_db.hset(key, did, str)

    # clear the hashes
    self._ent2id_hash.clear()
    self._e2d_hash.clear()
    self._d2e_hash.clear()

  def load_documents(self):
    '''
    Load all the documents which has exact match with the query entity
    '''
    num = self._doc_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      print 'no doc_item found'
      return

    print 'Loading %d documents from DB' % num
    doc_item_list = self._doc_db.lrange(RedisDB.ret_item_list, 0, num)
    for ret_id in doc_item_list:

      doc_item_keys = ['id', 'query', 'file', 'stream_id', 'stream_data']
      db_item = self._doc_db.hmget(ret_id, doc_item_keys)

      if 'None' == db_item[1]:
        print 'Invalid record: %s' % ret_id
        continue

      doc_item = {}
      doc_item['id'] = db_item[0]
      doc_item['query'] = db_item[1]
      doc_item['file'] = db_item[2]
      doc_item['stream_id'] = db_item[3]
      doc_item['stream_data'] = self.sanitize(db_item[4])

      query = db_item[1]
      self._doc_item_list[query].append(doc_item)

def main():
  match = WikiMatch()
  match.load_documents()

  query_id_list = range(0, 29, 1)
  for query_id in query_id_list:
    print 'Query %d' % query_id
    match.process_data(query_id)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

