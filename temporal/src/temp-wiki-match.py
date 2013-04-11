#!/usr/bin/python
'''
using the query entity and the entities on its associated wikipedia page
(which consist of the set of related entities) to filtering the streaming
documents

Basically the score is calculated based on the occurrences of the related
entities in the document

This program will iterate over all the related entity list from all the
available revisions, estimate the scores for each of them.

temp-wiki-match.py <query_id> <save_dir>
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

  _doc_item_list = []

  _exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      #db=RedisDB.oair_doc_train_db)
      db=RedisDB.oair_doc_test_db)

  _rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.rel_ent_dist_db)

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

  def calc_score(self, query, doc, rel_ent_list):
    '''
    Calculate the score of a document w.r.t. the given query
    '''

    score = 0

    ## first, calculate it with the query entity
    ## use the query entity as the regex to apply exact match
    #query_str = ' %s ' % query
    #if re.search(query_str, doc, re.I | re.M):
    score = score + QUERY_ENT_MATCH_SCORE

    for ent in rel_ent_list:
      ent = self.format_query(ent)
      ent_str = ' %s ' % ent
      if re.search(ent_str, doc, re.I | re.M):
        ## change to count match once to count the total number of matches
        match_list = re.findall(ent_str, doc, re.I | re.M)
        score = score + WIKI_ENT_MATCH_SCORE * len(match_list)

    return score

  def process_data(self, query_id, save_dir):
    '''
    process the streaming item one by one
    '''

    # first, get the current query
    org_query = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, query_id)
    query = self.format_query(org_query)

    hash_key = 'query-rel-ent-%s' % query_id
    dt_list = self._rel_ent_dist_db.hkeys(hash_key)
    dt_list.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))

    # for each revision (i.e. a list of related entities), estimate relevance
    # score for each of the document
    for dt in dt_list:
      rel_ent_str = self._rel_ent_dist_db.hget(hash_key, dt)
      rel_ent_list = rel_ent_str.split('=')

      ret_item_list = []
      for doc_item in self._doc_item_list:
        stream_data = doc_item['stream_data']
        ret_item = {}
        ret_item['query'] = org_query
        ret_item['stream_id'] = doc_item['stream_id']

        try:
          score = self.calc_score(query, stream_data, rel_ent_list)
          ret_item['score'] = score
          ret_item_list.append(ret_item)

        except:
          # Catch any unicode errors while printing to console
          # and just ignore them to avoid breaking application.
          print "Exception in process_stream_item()"
          print '-'*60
          traceback.print_exc(file=sys.stdout)
          print '-'*60
          pass

      # now save the file for this revision
      save_file = os.path.join(save_dir, query_id, dt)
      print 'Saving %s' % save_file
      try:
        with open(save_file, 'w') as f:
          for ret_item in ret_item_list:
            f.write('%s %s %s\n' %(ret_item['query'], ret_item['stream_id'],
              ret_item['score']))
      except IOError as e:
        print 'Failed to save file: %s' % save_file

  def load_data(self, query_id):
    '''
    Load all the documents which has exact match with the query entity
    '''
    num = self._exact_match_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      print 'no doc_item found'
      return

    # first, get the current query
    query = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, query_id)

    print 'Loading documents for query: %s' % query

    doc_item_list = self._exact_match_db.lrange(RedisDB.ret_item_list, 0, num)
    for ret_id in doc_item_list:
      doc_item_keys = ['id', 'query', 'file', 'stream_id', 'stream_data']
      db_item = self._exact_match_db.hmget(ret_id, doc_item_keys)

      doc_item = {}
      doc_item['id'] = db_item[0]
      doc_item['query'] = db_item[1]
      doc_item['file'] = db_item[2]
      doc_item['stream_id'] = db_item[3]
      doc_item['stream_data'] = self.sanitize(db_item[4])

      # for one process, we only handle the documents for the current query only
      if doc_item['query'] != query:
        continue

      self._doc_item_list.append(doc_item)

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('query_id')
  parser.add_argument('save_dir')
  args = parser.parse_args()

  match = WikiMatch()
  match.load_data(args.query_id)
  match.process_data(args.query_id, args.save_dir)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

