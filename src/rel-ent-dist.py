#!/usr/bin/python
'''
Generate the distribution of relevant entities in the documents which
mentioned the query entities

wiki-dist-analyze.py <query>
'''

import re
import os
import sys
import time
import datetime

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

QUERY_ENT_MATCH_SCORE = 100
WIKI_ENT_MATCH_SCORE = 1

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

class WikiMatch():
  '''
  Apply exact matching
  '''
  _query_hash = {}
  _org_query_hash = {}
  _query2id_hash = {}
  _wiki_ent_hash = None

  #_ret_url_prefix = 'train'
  _ret_url_prefix = 'wiki'

  # documents which have match with the query entity
  _exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      #db=RedisDB.exact_match_db)
      #db=RedisDB.test_exact_match_db)
      db=RedisDB.fuzzy_match_db)

  # the relevant entities of query entities
  _wiki_ent_list_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_list_db)

  # the distribution of all the relevant entities over time
  _rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.rel_ent_dist_db)

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
    #for index, item in enumerate(query_list):
      #print '%d\t%s' % (index, item)

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

    ## initialize the dictionary with list as elements
    ## see http://stackoverflow.com/questions/960733/python-creating-a-dictionary-of-lists
    self._wiki_ent_hash = defaultdict(list)
    for ent_id in ent_item_list:
      keys = ['id', 'query', 'ent', 'url']
      db_item = self._wiki_ent_list_db.hmget(ent_id, keys)
      id = db_item[0]
      query = db_item[1]
      ent = db_item[2]
      ent = self.format_query(ent)
      ent_str = '%s:%s' %(id, ent)
      self._wiki_ent_hash[query].append(ent_str)

  def parse_query_doc(self, qid, ret_id, did, doc):
    '''
    estimate the occurrences of relevant entities of one query in the document
    '''
    query = self._query_hash[qid]
    org_query = self._org_query_hash[qid]

    list = did.split('-')
    epoch = list[0]
    time = datetime.datetime.utcfromtimestamp(float(epoch))
    date = '%d-%.2d-%.2d' %(time.year, time.month, time.day)

    if org_query in self._wiki_ent_hash:
      for ent_str in self._wiki_ent_hash[org_query]:
        ent_list = ent_str.split(':')
        ent_id = ent_list[0]
        ent = ent_list[1]
        if re.search(ent, doc, re.I | re.M):
          ## change to count match once to count the total number of matches
          match_list = re.findall(ent, doc, re.I | re.M)
          matched_num = len(match_list)

          ## save the URL of the document in the page
          ret_url = '/%s/ret/%s' %(self._ret_url_prefix, ret_id)
          list_name = '%s-list' %(ent_id)
          store_item = '%s:%s' %(did, ret_url)
          self._rel_ent_dist_db.rpush(list_name, store_item)

          ## increse the matched number
          if self._rel_ent_dist_db.hexists(ent_id, date):
            self._rel_ent_dist_db.hincrby(ent_id, date, matched_num)
          else:
            self._rel_ent_dist_db.hset(ent_id, date, matched_num)
    else:
      print 'I can not find the query [%s] in self._wiki_ent_hash' %org_query

  def process_stream_item(self, org_query, ret_id, stream_id, stream_data):
    '''
    process the streaming item: applying exact match for each of the query
    entity
    '''

    if org_query in self._query2id_hash:
      qid = self._query2id_hash[org_query]
    else:
      print 'Invalid query: [%s].\nNO qid found.' %query
      return

    # for debug purpose, we only consider one query only
    #if not 0 == qid:
      #return

    new_stream_data = self.sanitize(stream_data)
    query = self._query_hash[qid]

    try:
      self.parse_query_doc(qid, ret_id, stream_id, new_stream_data)

    except:
      # Catch any unicode errors while printing to console
      # and just ignore them to avoid breaking application.
      print "Exception in process_stream_item()"
      print '-'*60
      traceback.print_exc(file=sys.stdout)
      print '-'*60
      #pass
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
      self.process_stream_item(ret_item['query'], ret_item['id'],
          ret_item['stream_id'], ret_item['stream_data'])
      print '%s / %d' %(ret_id, num)

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('query')
  args = parser.parse_args()

  match = WikiMatch()
  match.parse_query(args.query)
  match.load_wiki_ent()
  match.parse_data()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

