#!/usr/bin/python
'''
Prase the WikiPedia page of KBA track query entities, extract the entities from the
internal links in the page

extract-wiki-ent.py <query> <output>
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

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

class EntWikiExtractor():
  '''
  Apply exact matching
  '''
  _query_hash = {}
  WIKI_API_URL = 'http://en.wikipedia.org/w/api.php?'\
                  'action=query&prop=revisions&rvprop=content'\
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

  def parse_wiki(self, output):
    for index in self._query_hash:
      query = self._query_hash[index]
      content = self.retrieve(query)
      if content:
        doc = json.load(content)
        ## get the content of the markup
        ## thanks to http://goo.gl/wDPha
        text = doc['query']['pages'].itervalues().next()['revisions'][0]['*']
        html = parse(text)
        print 'Query processed: %s' %query
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

    from wikimarkup import parse, registerInternalLinkHook

def wikipediaLinkHook(parser_env, namespace, body):
  # namespace is going to be 'Wikipedia'
  (article, pipe, text) = body.partition('|')
  href = article.strip().capitalize().replace(' ', '_')
  text = (text or article).strip()
  print 'Entity: %s' %href
  return '<a href="http://en.wikipedia.org/wiki/%s">%s</a>' % (href, text)

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('query')
  parser.add_argument('output')
  args = parser.parse_args()

  registerInternalLinkHook(None, wikipediaLinkHook)
  extractor = EntWikiExtractor()
  extractor.parse_query(args.query)
  extractor.parse_wiki(args.output)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'
