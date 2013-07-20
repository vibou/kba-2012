#!/usr/bin/python
'''
Estimate the distribution of related entities of query entities. For each
related entity, it will only be considered when it occured in one of the
relevant documents of the query entity.

est-rel-ent-dist.py <dump>
'''

import re
import os
import sys
import traceback
import gzip
import json
import time
import urllib2
import datetime

# see: https://github.com/dcramer/py-wikimarkup
from wikimarkup import parse, registerInternalLinkHook

import redis
from config import RedisDB

## the current query
g_cur_idx = 0
g_dist_hash = {}
g_timestamp = ''
g_rev_hash = {}
g_doc_list = {}

g_rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
    db=RedisDB.rel_ent_dist_db)

g_exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
    db=RedisDB.train_doc_db)
    #db=RedisDB.test_doc_db)

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

def parse_json(json_file):
  try:
    with open(json_file) as f:
      print 'Loading %s' % (json_file)
      dump_json = json.load(f)

  except IOError as e:
    print 'Failed to open file: %s' % json_file

  global g_cur_idx
  global g_dist_hash
  global g_timestamp
  global g_rel_ent_dist_db

  sorted_index = dump_json.keys()
  sorted_index.sort(key=lambda x: int(x))

  for index in sorted_index:
    g_cur_idx = index
    g_dist_hash[g_cur_idx] = {}
    g_rev_hash[g_cur_idx] = {}

    query = dump_json[index]['query']
    g_rel_ent_dist_db.rpush(RedisDB.query_ent_list, index)
    g_rel_ent_dist_db.hset(RedisDB.query_ent_hash, index, query)
    print 'Query: %s %s' % (index, query)
    #continue

    for rev_id in dump_json[index]['revisions']:
      revid = dump_json[index]['revisions'][rev_id]['revid']
      g_timestamp = dump_json[index]['revisions'][rev_id]['timestamp']
      text = dump_json[index]['revisions'][rev_id]['text']
      #print '%s %s %s' % (query, revid, timestamp)

      g_rev_hash[g_cur_idx][g_timestamp] = revid

      try:
        html = parse(text)
      # catch all other exceptions in the parse process,
      # print out the traceback, and move on without interruption
      except:
        print "Exception in parse_json()"
        print '-' * 60
        traceback.print_exc(file=sys.stdout)
        print '-' * 60
        pass

    sorted_ts = g_dist_hash[index].keys()
    # sort the timestamp in chronic order
    sorted_ts.sort(key=lambda x: datetime.datetime.strptime(x,
      '%Y-%m-%dT%H:%M:%SZ'))

    # for each month, save the number of related entities
    # as well as the related entities
    last_revid = 0
    last_ts = ''
    last_dt_str = datetime.datetime.strptime(sorted_ts[0],
        '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m')

    rel_ent_hash = {}
    irrel_ent_hash = {}
    for ts in sorted_ts:
      revid = g_rev_hash[index][ts]

      dt_str = datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m')
      if dt_str != last_dt_str:
        # save the related entities for last month
        ent_list = []
        for ent in g_dist_hash[index][last_ts]:
          # check the cache first
          if ent in rel_ent_hash:
            ent_list.append(ent)
            continue

          if ent in irrel_ent_hash:
            continue

          # update the cache accordingly
          if in_rel_doc(query, ent):
            ent_list.append(ent)
            rel_ent_hash[ent] = 1
          else:
            irrel_ent_hash[ent] = 1

        # save the number of related entities for last month
        hash_key = 'query-rel-ent-num-%s' % index
        ent_num = len(ent_list)
        g_rel_ent_dist_db.hset(hash_key, last_dt_str, ent_num)
        print '%s %s %s %s %d' % (index, query, last_dt_str, last_revid, ent_num)

      last_revid = revid
      last_dt_str = dt_str
      last_ts = ts
      #print '%s %s %d' %(query, ts, rel_ent_num)

    #return

def in_rel_doc(query, ent):
  global g_doc_list

  matched = False

  try:
    ent = format_query(ent)
    ent_str = ' %s ' % ent
    for doc_item in g_doc_list[query]:
      doc = doc_item['stream_data']
      if re.search(ent_str, doc, re.I | re.M):
        matched = True
        break
  except:
    # Catch any unicode errors while printing to console
    # and just ignore them to avoid breaking application.
    print "Exception in process_stream_item()"
    print '-'*60
    traceback.print_exc(file=sys.stdout)
    print '-'*60
    pass

  return matched

def format_query(query):
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

def load_data():
  '''
  Load all the documents which has exact match with the query entity
  '''
  global g_exact_match_db
  global g_doc_list

  num = g_exact_match_db.llen(RedisDB.ret_item_list)
  if 0 == num:
    print 'no doc_item found'
    return

  print 'Loading %d documents' % num
  doc_item_list = g_exact_match_db.lrange(RedisDB.ret_item_list, 0, num)
  for ret_id in doc_item_list:
    doc_item_keys = ['id', 'query', 'file', 'stream_id', 'stream_data']
    db_item = g_exact_match_db.hmget(ret_id, doc_item_keys)

    doc_item = {}
    doc_item['id'] = db_item[0]
    doc_item['query'] = db_item[1]
    doc_item['file'] = db_item[2]
    doc_item['stream_id'] = db_item[3]
    doc_item['stream_data'] = sanitize(db_item[4])

    query = db_item[1]
    if query in g_doc_list:
      g_doc_list[query].append(doc_item)
    else:
      g_doc_list[query] = []
      g_doc_list[query].append(doc_item)

def sanitize(str):
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

def wikipediaLinkHook(parser_env, namespace, body):
  # namespace is going to be 'Wikipedia'
  (article, pipe, text) = body.partition('|')
  href = article.strip().capitalize().replace(' ', '_')
  text = (text or article).strip()

  wiki_url_base = 'http://en.wikipedia.org/wiki/'
  url = wiki_url_base + href

  # ignore long entities
  if href.__len__() > 50:
    return ''

  ent = href

  global g_cur_idx
  global g_dist_hash
  global g_timestamp

  if g_timestamp not in g_dist_hash[g_cur_idx]:
    g_dist_hash[g_cur_idx][g_timestamp] = {}

  if ent not in g_dist_hash[g_cur_idx][g_timestamp]:
    g_dist_hash[g_cur_idx][g_timestamp][ent] = 1

  return '<a href="http://en.wikipedia.org/wiki/%s">%s</a>' % (href, text)

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('dump_file')
  args = parser.parse_args()

  load_data()
  registerInternalLinkHook(None, wikipediaLinkHook)
  parse_json(args.dump_file)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

