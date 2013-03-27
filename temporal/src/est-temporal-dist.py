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
from wikimarkup import parse, registerInternalLinkHook

import redis
from config import RedisDB

## the current query
g_current_query = ''
g_wiki_ent_list_db = None

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

def parse_json(json_file):
  try:
    with open(json_file) as f:
      dump_json = json.loads(f)

      for index in dump_json:
        query = dump_json[index]['query']
        for rev_id in dump_json[index]['revisions']:
          revid = dump_json[index]['revid']
          timestamp = dump_json[index]['timestamp']
          print '%s %s %s' % (query, revid, timestamp)

  except IOError as e:
    print 'Failed to open file: %s' % json_file
    continue

def parse_wiki():
  global g_current_query
  g_current_query = query
  html = parse(text)

def wikipediaLinkHook(parser_env, namespace, body):
  # namespace is going to be 'Wikipedia'
  (article, pipe, text) = body.partition('|')
  href = article.strip().capitalize().replace(' ', '_')
  text = (text or article).strip()
  global g_current_query
  #print '%s : %s' %(g_current_query, href)
  ## insert the records into the database

  wiki_url_base = 'http://en.wikipedia.org/wiki/'
  url = wiki_url_base + href

  if href.__len__() > 50:
    return ''

  global g_wiki_ent_list_db
  uniq_id = '%s-%s' %(g_current_query, href)
  if False == g_wiki_ent_list_db.sismember(RedisDB.wiki_ent_set, uniq_id):
    g_wiki_ent_list_db.sadd(RedisDB.wiki_ent_set, uniq_id)

    id = g_wiki_ent_list_db.llen(RedisDB.wiki_ent_list)
    id = id + 1
    g_wiki_ent_list_db.rpush(RedisDB.wiki_ent_list, id)

    ent_item = {'id' : id}
    ent_item['query'] = g_current_query
    ent_item['ent'] = href
    ent_item['url'] = url
    g_wiki_ent_list_db.hmset(id, ent_item)

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
