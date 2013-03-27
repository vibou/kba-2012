#!/usr/bin/python
'''
Parse the WikiPedia page of KBA track query entities, extract the entities from the
internal links in the page

extract-wiki-ent.py <query>
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

class EntWikiExtractor():
  '''
  Apply exact matching
  '''
  _query_hash = {}
  ## the URL template to dump the content in JSON
  WIKI_API_URL = 'http://en.wikipedia.org/w/api.php?'\
                  'action=query&prop=revisions&rvprop=content'\
                  '&redirects=true&format=json&titles='

  global g_wiki_ent_list_db
  g_wiki_ent_list_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_list_db)

  def parse_query(self, query_file):
    '''
    parse the query in json format
    '''
    queries = json.load(open(query_file))
    query_list = queries['topic_names']

    ## format the query list
    for index, item in enumerate(query_list):
      self._query_hash[index] = item

  def parse_wiki(self):
    for index in self._query_hash:
      query = self._query_hash[index]
      content = self.retrieve(query)
      if content:
        doc = json.load(content)
        ## get the content of the markup
        ## thanks to http://goo.gl/wDPha
        text = doc['query']['pages'].itervalues().next()['revisions'][0]['*']
        global g_current_query
        g_current_query = query
        html = parse(text)
        print 'Query processed: %s' %query
        ## wait for 1 second to avoid unnecessary banning from WikiPedia server
        time.sleep(1)
      else:
        print 'Skipping query %s' %query

  def retrieve(self, query):
    ## disguise myself as Firefox
    headers = {
      'User-Agent': 'Mozilla/5.0 (Windows) Gecko/20080201 Firefox/2.0.0.12',
      'Accept': 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9',
      'Accept-Language': 'en-US,en;q=0.5',
      'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
      'Connection': 'keep-alive'
    }
    ## construct the WikiPedia page URL
    url = self.WIKI_API_URL + query
    ## retrieve the WikiPedia page
    try:
      req = urllib2.Request(url, headers=headers)
      response = urllib2.urlopen(req)
      return response
    except  urllib2.HTTPError as error:
      print "Exception in retrieve(). Error code: %d" %error.code
      print '-'*60
      traceback.print_exc(file=sys.stdout)
      print '-'*60
      return None
    except:
      # Catch any unicode errors while printing to console
      # and just ignore them to avoid breaking application.
      print "Exception in retrieve()"
      print '-'*60
      traceback.print_exc(file=sys.stdout)
      print '-'*60
      return None

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
  parser.add_argument('query')
  args = parser.parse_args()

  registerInternalLinkHook(None, wikipediaLinkHook)
  extractor = EntWikiExtractor()
  extractor.parse_query(args.query)
  extractor.parse_wiki()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'
