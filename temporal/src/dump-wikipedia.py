#!/usr/bin/python
'''
Dump all the revisions of a given entity list from Wikipedia

dump-wikipedia.py <query>
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

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

class DumpWikipedia():
  '''
  Dump the reivisions of entities from Wikipedia
  '''
  _query_hash = {}
  _wiki_json_hash = {}

  '''
  The URL template to dump the content in JSON.
  All the parameters of Wikipedia API can be found at

    http://www.mediawiki.org/wiki/API%3aProperties#Revisions%3a_Example
  '''
  WIKI_API_URL = 'http://en.wikipedia.org/w/api.php?'\
                  'action=query&prop=revisions&rvprop=content|timestamp|ids'\
                  '&redirects=true&format=json&titles='

  def parse_query(self, query_file):
    '''
    parse the query in json format
    '''
    queries = json.load(open(query_file))
    query_list = queries['topic_names']

    ## format the query list
    for index, item in enumerate(query_list):
      self._query_hash[index] = item

  def dump_wiki(self):
    for index in self._query_hash:
      query = self._query_hash[index]
      content = self.retrieve(query)
      if not content:
        print 'Skipping query %s' %query
        continue

      doc = json.load(content)

      ## get the content of the markup
      ## thanks to http://goo.gl/wDPha
      text = doc['query']['pages'].itervalues().next()['revisions'][0]['*']

      # save the processed json dump to hash
      self._wiki_json_hash[index] = doc

      print 'Query processed: %s' %query
      ## wait for 10 seconds to avoid unnecessary banning from Wikipedia server
      time.sleep(10)

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

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('query')
  args = parser.parse_args()

  registerInternalLinkHook(None, wikipediaLinkHook)
  dump = DumpWikipedia()
  dump.parse_query(args.query)
  dump.dump_wiki()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'
