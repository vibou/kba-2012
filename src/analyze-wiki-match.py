#!/usr/bin/python
'''
Conduct analyses over the wiki-matched documents

In this version, we only consider the documents which has already been
extracted and has exact match with the query entity

analyze-wiki-match.py  <query> <annotation>
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
import csv
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

  _annotation = dict()

  _exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      #db=RedisDB.exact_match_db)
      db=RedisDB.test_exact_match_db)

  _wiki_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_match_db)

  _wiki_ent_list_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_list_db)

  _analyze_wiki_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.analyze_wiki_match_db)

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

  def load_annotation (self, path_to_annotation_file, include_relevant, include_neutral):
    '''
    Loads the annotation file into a dict

    path_to_annotation_file: string filesystem path to the annotation file
    include_relevant: true to include docs marked relevant and central
    '''
    annotation_file = csv.reader(open(path_to_annotation_file, 'r'), delimiter='\t')

    for row in annotation_file:
      ## Skip comments
      if row[0][0] == "#":
        continue

      stream_id = row[2]
      urlname = row[3]
      rating = int(row[5])

      if include_neutral:
        thresh = 0
      elif include_relevant:
        thresh = 1
      else:
        thresh = 2

      ## Add the stream_id and urlname to a hashed dictionary
      ## 0 means that its not central 1 means that it is central

      if (stream_id, urlname) in self._annotation:
        ## 2 means the annotators gave it a yes for centrality
        if rating < thresh:
          self._annotation[(stream_id, urlname)] = False
      else:
        self._annotation[(stream_id, urlname)] = rating >= thresh


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
      query = db_item[1]
      ent = db_item[2]
      ent = self.format_query(ent)
      self._wiki_ent_hash[query].append(ent)

  def calc_score(self, qid, doc):
    '''
    Calculate the score of a document w.r.t. the given query
    '''
    query = self._query_hash[qid]
    org_query = self._org_query_hash[qid]

    score = 0
    ## first, calculate it with the query entity
    ## use the query entity as the regex to apply exact match
    if re.search(query, doc, re.I | re.M):
      score = score + QUERY_ENT_MATCH_SCORE

    if org_query in self._wiki_ent_hash:
      for ent in self._wiki_ent_hash[org_query]:
        if re.search(ent, doc, re.I | re.M):
          ## change to count match once to count the total number of matches
          match_list = re.findall(ent, doc, re.I | re.M)
          score = score + WIKI_ENT_MATCH_SCORE * len(match_list)
    else:
      print 'I can not find the query [%s] in self._wiki_ent_hash' %org_query

    return score

  '''
  Transfer the raw data to HTML
  Basically the goal is to make sure each paragraph is embedded in <p></p>
  '''
  def raw2html(self, qid, doc):
    '''
    Calculate the score of a document w.r.t. the given query
    '''
    query = self._query_hash[qid]
    org_query = self._org_query_hash[qid]

    #doc = self.sanitize(doc)

    score = 0
    ## first, calculate it with the query entity
    ## use the query entity as the regex to apply exact match
    match = re.search(query, doc, re.I | re.M)
    if match:
      score = score + QUERY_ENT_MATCH_SCORE
      query_span = "<span class=\"qent\">" + query + "</span>"
      query_regex = ur'%s' %( query )
      replacer = re.compile(query_regex, re.I | re.M)
      doc = replacer.sub(query_span, doc.decode('unicode-escape'))

    if org_query in self._wiki_ent_hash:
      for ent in self._wiki_ent_hash[org_query]:
        if re.search(ent, doc, re.I | re.M):
          ## change to count match once to count the total number of matches
          match_list = re.findall(ent, doc, re.I | re.M)
          score = score + WIKI_ENT_MATCH_SCORE * len(match_list)

          ent_span = "<span class=\"rent\">" + ent + "</span>"
          ent_regex = ur'%s' %( ent )
          replacer = re.compile(ent_regex, re.I | re.M)
          doc = replacer.sub(ent_span, doc)

    else:
      print 'I can not find the query [%s] in self._wiki_ent_hash' %org_query

    return doc

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

      if score < 100:
        return

      id = self._wiki_match_db.llen(RedisDB.ret_item_list)

      ## create a hash record
      ret_item = {'id' : id}
      ret_item['query'] = org_query
      ret_item['file'] = fname
      ret_item['stream_id'] = stream_id
      ret_item['stream_data'] = self.raw2html(qid, stream_data)
      ret_item['score'] = score

      in_annotation_set = (stream_id, org_query) in self._annotation
      ## In the annotation set and relevant
      if in_annotation_set and self._annotation[(stream_id, org_query)]:
        ## mark it as relevant
        ret_item['rel'] = 'Yes'
      else:
        ## mark it as irrelevant
        ret_item['rel'] = 'No'

      self._wiki_match_db.hmset(id, ret_item)

      #id = id + 1
      self._wiki_match_db.rpush(RedisDB.ret_item_list, id)

      ## verbose output
      print 'Match: %d - %s - %s - %d - %s' %(id, org_query, stream_id, score,
          ret_item['rel'])
    except:
      # Catch any unicode errors while printing to console
      # and just ignore them to avoid breaking application.
      print "Exception in process_stream_item()"
      print '-'*60
      traceback.print_exc(file=sys.stdout)
      print '-'*60
      sys.exit(-1)

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
  parser.add_argument('annotation')
  args = parser.parse_args()

  match = WikiMatch()
  match.parse_query(args.query)
  match.load_annotation(args.annotation, False, False)
  match.load_wiki_ent()
  match.parse_data()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

