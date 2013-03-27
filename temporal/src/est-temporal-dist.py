#!/usr/bin/python
'''
For a given entity, this script will parse the dump of all revisions, and
generate the temporal distrubiton of related entities

est-temporal-dist.py <dump>
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

g_rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
    db=RedisDB.rel_ent_dist_db)

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

    query = dump_json[index]['query']
    g_rel_ent_dist_db.hset(RedisDB.query_ent_hash, index, query)
    print 'Query: %s %s' % (index, query)
    continue

    for rev_id in dump_json[index]['revisions']:
      revid = dump_json[index]['revisions'][rev_id]['revid']
      g_timestamp = dump_json[index]['revisions'][rev_id]['timestamp']
      text = dump_json[index]['revisions'][rev_id]['text']
      #print '%s %s %s' % (query, revid, timestamp)

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


    g_rel_ent_dist_db.rpush(RedisDB.query_ent_list, index)

    sorted_ts = g_dist_hash[index].keys()
    # sort the timestamp in chronic order
    sorted_ts.sort(key=lambda x: datetime.datetime.strptime(x,
      '%Y-%m-%dT%H:%M:%SZ'))

    # for each month, save the number of related entities
    last_num = 0
    last_dt_str = datetime.datetime.strptime(sorted_ts[0],
        '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m')
    for ts in sorted_ts:
      dt_str = datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m')
      if dt_str != last_dt_str:
        hash_key = 'query-%s' % index
        g_rel_ent_dist_db.hset(hash_key, dt_str, last_num)
        print '%s %s %s %d' % (index, query, dt_str, last_num)

      last_num = len(g_dist_hash[index][ts].keys())
      last_dt_str = dt_str
      #print '%s %s %d' %(query, ts, rel_ent_num)

    #return

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

  registerInternalLinkHook(None, wikipediaLinkHook)
  parse_json(args.dump_file)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'
