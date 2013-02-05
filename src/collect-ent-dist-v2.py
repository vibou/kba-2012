#!/usr/bin/python
'''
Collect the distribution informaton of related entities for each query entity

In this version only the documents which have exact match with the query
entities will be processed.

There would be roughly 40,000 documents

collect-ent-dist.py <thrift_dir>
'''

import re
import os
import sys
import traceback
import gzip
import json
import time
import copy
import hashlib
import subprocess
from collections import defaultdict
from cStringIO import StringIO

import redis
from config import RedisDB

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

class EntDistCollector():
  '''
  Apply exact matching
  '''
  _query_hash = {}
  _org_query_hash = {}
  _query2id_hash = {}
  _wiki_ent_hash = {}

  _exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      #db=RedisDB.exact_match_db)
      db=RedisDB.test_exact_match_db)

  _wiki_ent_list_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_list_db)

  _wiki_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_dist_db)

  def parse_query(self, query_file):
    '''
    parse the query
    '''
    queries = json.load(open(query_file))
    query_list = queries['topic_names']

    ## format the query list
    for index, item in enumerate(query_list):
      self._org_query_hash[index] = item
      self._query2id_hash[item] = index
      item = self.format_query(item)
      self._query_hash[index] = item

    ## dump the query list
    for index, item in enumerate(query_list):
      print '%d\t%s' % (index, item)

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

  def load_wiki_ent(self):
    num = self._wiki_ent_list_db.llen(RedisDB.wiki_ent_list)
    if 0 == num:
      print 'no wiki_ent found'
      return

    ent_item_list = self._wiki_ent_list_db.lrange(RedisDB.wiki_ent_list, 0, num)
    ent_items = []

    for ent_id in ent_item_list:
      keys = ['id', 'query', 'ent', 'url']
      db_item = self._wiki_ent_list_db.hmget(ent_id, keys)
      query = db_item[1]
      ent = db_item[2]
      ent = self.format_query(ent)
      self._wiki_ent_hash[ent_id] = ent

  def process_stream_item(self, ret_id, query, stream_id, stream_data):
    '''
    process the streaming item: applying exact match for each of the related
    entities
    '''
    doc = self.sanitize(stream_data)
    doc_len = len(doc.split(' '))

    for id in self._wiki_ent_hash:
      try:
        ent = self._wiki_ent_hash[id]
        ent_str = ' %s ' % ent
        if re.search(ent_str, doc, re.I | re.M):
          ## change to count match once to count the total number of matches
          match_list = re.findall(ent_str, doc, re.I | re.M)
          match_num = len(match_list)

          list_name = 'doc-list-%s' % id
          val = '%s:%d:%s' %(stream_id, doc_len, match_num)
          self._wiki_ent_dist_db.lpush(list_name, val)
          print '%s %s-%s : %s' %(ret_id, id, ent, val)
      except:
        # Catch any unicode errors while printing to console
        # and just ignore them to avoid breaking application.
        print "Exception in process_stream_item()"
        print '-'*60
        traceback.print_exc(file=sys.stdout)
        print '-'*60
        exit(-1)

  def process_stream_item_query(self, ret_id, query, stream_id, stream_data):
    '''
    process the streaming item: applying exact match for each of the query
    entities
    '''
    if query in self._query2id_hash:
      qid = self._query2id_hash[query]
    else:
      print 'Invalid query: [%s].\nNO qid found.' %query
      return

    doc = self.sanitize(stream_data)
    doc_len = len(doc.split(' '))

    try:
      query_str = ' %s ' % self.format_query(query)
      if re.search(query_str, doc, re.I | re.M):
        ## change to count match once to count the total number of matches
        match_list = re.findall(query_str, doc, re.I | re.M)
        match_num = len(match_list)

        list_name = 'query-doc-list-%s' % qid
        val = '%s:%d:%s' %(stream_id, doc_len, match_num)
        self._wiki_ent_dist_db.lpush(list_name, val)
        print '%s %s-%s : %s' %(ret_id, qid, query, val)
    except:
      # Catch any unicode errors while printing to console
      # and just ignore them to avoid breaking application.
      print "Exception in process_stream_item_query()"
      print '-'*60
      traceback.print_exc(file=sys.stdout)
      print '-'*60
      exit(-1)

  def parse_data(self):
    '''
    Parse all the documents which has exact match with the query entity
    '''

    num = self._exact_match_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      print 'no ret_item found'
      return

    ret_item_list = self._exact_match_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'stream_data']
      the_ret_item = self._exact_match_db.hmget(ret_id, ret_item_keys)

      ret_item = {}
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['stream_data'] = the_ret_item[4]

      ## process data
      self.process_stream_item_query(ret_id, ret_item['query'], ret_item['stream_id'],
          ret_item['stream_data'])
      #self.process_stream_item(ret_id, ret_item['query'], ret_item['stream_id'],
          #ret_item['stream_data'])

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('query')
  args = parser.parse_args()

  collector = EntDistCollector()
  collector.parse_query(args.query)
  collector.load_wiki_ent()
  collector.parse_data()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

