#!/usr/bin/python
'''
process the documents by removing the unimportant parts and extracing the main
body

Some heuristics are involved in the process.

proc-doc.py <query>
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

QUERY_ENT_MATCH_SCORE = 100
WIKI_ENT_MATCH_SCORE = 1

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

class ProcDoc():
  '''
  Apply exact matching
  '''

  # references to the databases
  _source_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      #db=RedisDB.exact_match_db)
      #db=RedisDB.test_exact_match_db)
      db=RedisDB.fuzzy_match_db)

  _filtered_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.filtered_test_db)

  # the pre-defined threshold of the valid paragraphs
  _VALID_PARAG_LEN_ = 30
  _VALID_PERIOD_NUM_ = 1

  def sanitize(self, str):
    '''
    sanitize the streaming item
    '''
    ## replace non word character to space
    non_word_regex = re.compile( '(\W+|\_+)' )
    str = non_word_regex.sub( ' ', str)

    ## compress multiple spaces
    space_regex = re.compile ( '\s+' )
    str = space_regex.sub( ' ', str )

    return str.lower()

  def remove_noise(self, doc):
    '''
    Remove the noise parts in the document, which include headers, sidebar,
    footers, etc.

    The heuristics applied here are:
    * those noise items are relative short, e.g. less than 20 terms
    * no period (.) is in the items
    '''

    new_doc = ''
    items = doc.split('\n')
    new_items = []
    for item in items:
      # first, estimate the length of this item by terms
      # and remove those which are shorter than the threshold
      sanitized = self.sanitize(item)
      num_terms = len(sanitized.split(' '))
      if num_terms < self._VALID_PARAG_LEN_:
        continue

      # then, count how many periods (.) in the item
      # and remove those with less than the threshold
      if re.search('\.', item, re.I | re.M):
        match_list = re.findall('\.', item, re.I | re.M)
        num_match = len(match_list)
        if num_match < self._VALID_PERIOD_NUM_:
          continue

      # now the item is qualified to be kept
      new_items.append(item)

    new_doc = '\n'.join(new_items)
    return new_doc

  def process_stream_item(self, org_query, fname, stream_id, stream_data):
    '''
    process the streaming item: applying exact match for each of the query
    entity
    '''

    try:
      new_stream_data = self.remove_noise(stream_data)

      return new_stream_data
    except:
      # Catch any unicode errors while printing to console
      # and just ignore them to avoid breaking application.
      print "Exception in process_stream_item()"
      print '-'*60
      traceback.print_exc(file=sys.stdout)
      print '-'*60

  def parse_data(self):
    '''
    Parse all the documents which has exact match with the query entity
    '''

    num = self._source_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      print 'no ret_item found'
      return

    ret_item_list = self._source_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'stream_data', 'score']
      the_ret_item = self._source_db.hmget(ret_id, ret_item_keys)

      ret_item = {}
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['stream_data'] = the_ret_item[4]
      ret_item['score'] = the_ret_item[5]

      ## process data
      filtered_doc = self.process_stream_item(ret_item['query'], ret_item['file'],
          ret_item['stream_id'], ret_item['stream_data'])
      print 'Process %s' %(ret_id)

      ## save the document to database then
      id = self._filtered_db.llen(RedisDB.ret_item_list)
      self._filtered_db.rpush(RedisDB.ret_item_list, id)

      ret_item['stream_data'] = filtered_doc
      self._filtered_db.hmset(id, ret_item)

  def test_parse_data(self):
    '''
    Do testing of parse_data()
    '''

    ret_id = 10900
    ret_item_keys = ['id', 'query', 'file', 'stream_id', 'stream_data']
    the_ret_item = self._source_db.hmget(ret_id, ret_item_keys)

    ret_item = {}
    ret_item['id'] = the_ret_item[0]
    ret_item['query'] = the_ret_item[1]
    ret_item['file'] = the_ret_item[2]
    ret_item['stream_id'] = the_ret_item[3]
    ret_item['stream_data'] = the_ret_item[4]

    ## process data
    filtered_doc = self.process_stream_item(ret_item['query'], ret_item['file'],
        ret_item['stream_id'], ret_item['stream_data'])

    ## verbose output
    print "Filtered document:"
    print '-'*60
    print filtered_doc
    print '-'*60

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  args = parser.parse_args()

  proc = ProcDoc()
  #proc.test_parse_data()
  proc.parse_data()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

