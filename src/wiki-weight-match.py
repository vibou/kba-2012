#!/usr/bin/python
'''
using the query entity and the entities on its associated wikipedia page
(which consist of the set of related entities) to filtering the streaming
documents

the weight of each related entity is pre-calculated and will be applied to the
relevance score of each document

In this version, we only consider the documents which has already been
extracted and has exact match with the query entity

wiki-weight-match.py <query> <weight_list>
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
from cStringIO import StringIO

import redis
from config import RedisDB

QUERY_ENT_MATCH_SCORE = 100
WIKI_ENT_MATCH_SCORE = 10

class DictItem(dict):
  '''
  A dict that allows for object-like property access syntax.
  '''
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

class WikiWeightMatch():
  '''
  Apply exact matching
  '''
  _query_hash = {}
  _org_query_hash = {}
  _query2id_hash = {}
  _wiki_ent_hash = {}
  _weight_hash = {}

  _exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      #db=RedisDB.exact_match_db)
      db=RedisDB.test_exact_match_db)
      #db=RedisDB.fuzzy_match_db)
      #db=RedisDB.filtered_test_db)

  _wiki_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_match_db)

  _wiki_ent_list_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_list_db)

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

  def load_weight_list(self, weight_list):
    try:
      with open(weight_list) as f:
        for line in f.readlines():
          values = line.strip().split('\t')
          if not 4 == len(values):
            print 'Invalid record: %s!' % line.strip()
            continue
          point = DictItem()
          point.id = values[0]
          point.weight = float(values[1])
          point.ent = values[2]
          point.query = values[3]
          self._weight_hash[point.id] = point
    except IOError as e:
      print '-' * 60
      traceback.print_exec(file = sys.stdout)
      print '-' * 60
      exit(-1)

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

      if not query in self._wiki_ent_hash:
        self._wiki_ent_hash[query] = {}
      self._wiki_ent_hash[query][ent_id] = ent

  def calc_score(self, qid, doc):
    '''
    Calculate the score of a document w.r.t. the given query
    '''
    query = self._query_hash[qid]
    org_query = self._org_query_hash[qid]

    score = 0
    ## first, calculate it with the query entity
    #score = score + QUERY_ENT_MATCH_SCORE

    ## then, calculate the score using related entities
    if org_query in self._wiki_ent_hash:
      for ent_id in self._wiki_ent_hash[org_query]:

        ent = self._wiki_ent_hash[org_query][ent_id]
        ent_str = ' %s ' %(ent)
        if not ent_id in self._weight_hash:
          continue
        weight = self._weight_hash[ent_id].weight

        if re.search(ent_str, doc, re.I | re.M):
          ## change to count match once to count the total number of matches
          match_list = re.findall(ent_str, doc, re.I | re.M)
          score = score + WIKI_ENT_MATCH_SCORE * len(match_list) * weight
    else:
      print 'I can not find the query [%s] in self._wiki_ent_hash' %org_query

    return score

  def process_stream_item(self, org_query, fname, stream_id, stream_data):
    '''
    process the streaming item: applying exact match for each of the query
    entity
    '''

    if org_query in self._query2id_hash:
      qid = self._query2id_hash[org_query]
    else:
      print 'Invalid query: [%s].\nNO qid found.' %query
      return

    new_stream_data = self.sanitize(stream_data)
    query = self._query_hash[qid]

    try:
      score = self.calc_score(qid, new_stream_data)

      #id = self._wiki_match_db.llen(RedisDB.ret_item_list)
      id = 0
      id = id + 1
      #self._wiki_match_db.rpush(RedisDB.ret_item_list, id)

      ## create a hash record
      ret_item = {'id' : id}
      ret_item['query'] = org_query
      ret_item['file'] = fname
      ret_item['stream_id'] = stream_id
      ret_item['stream_data'] = stream_data
      ret_item['score'] = score
      #self._wiki_match_db.hmset(id, ret_item)

      ## verbose output
      print 'Match: %d - %s - %s - %d' %(id, org_query, stream_id, score)
      #print '%s %s %d' %(org_query, stream_id, score)
    except:
      # Catch any unicode errors while printing to console
      # and just ignore them to avoid breaking application.
      print "Exception in process_stream_item()"
      print '-'*60
      traceback.print_exc(file = sys.stdout)
      print '-'*60
      pass

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
      self.process_stream_item(ret_item['query'], ret_item['file'],
          ret_item['stream_id'], ret_item['stream_data'])

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('query')
  parser.add_argument('weight_list')
  args = parser.parse_args()

  match = WikiWeightMatch()
  match.parse_query(args.query)
  match.load_weight_list(args.weight_list)
  match.load_wiki_ent()
  match.parse_data()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

