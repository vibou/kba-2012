#!/usr/bin/python
'''
Dump all the revisions of a given entity list from Wikipedia

dump-wikipedia.py <query> <json_file>
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
                  '&rvlimit=100&redirects=true&format=json&titles='

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
    '''
    Iteratively dump all the revision of a given wikipedia entity
    '''
    for index in self._query_hash:
      query = self._query_hash[index]

      ## construct the WikiPedia page URL
      init_url = self.WIKI_API_URL + query
      url = init_url

      dict_dump = {}
      dict_dump['query'] = query
      dict_dump['revisions'] = {}

      last_rev_id = 1
      while 0 != last_rev_id:
        # we now move on to retrieve the next batch starting fro the last
        # revision id we have collected
        if 1 != last_rev_id:
          url = init_url + '&rvstartid=' + str(last_rev_id)

        content = self.retrieve(url)
        if not content:
          break

        doc = json.load(content)
        revisions = doc['query']['pages'].itervalues().next()['revisions']

        for rev in revisions:
          rev_hash = {}
          # check whether the content field exists
          if '*' not in rev:
            continue
          rev_hash['timestamp'] = rev['timestamp']
          rev_hash['text'] = rev['*']
          rev_hash['revid'] = rev['revid']
          last_rev_id = rev['parentid']
          # append the current revision to the hash
          so_far = len(dict_dump['revisions'].keys())
          dict_dump['revisions'][so_far] = rev_hash
          print '%s %d %d %s' %(query, so_far, rev_hash['revid'],
              rev_hash['timestamp'])

        ## wait for 1 second to avoid unnecessary banning from Wikipedia server
        time.sleep(1)

      # save the processed json dump to hash
      self._wiki_json_hash[index] = dict_dump

      print '-' * 60
      print 'Query processed: %s' %query
      print '-' * 60
      ## for debug purpose only
      #return
      ## wait for 10 seconds to avoid unnecessary banning from Wikipedia server
      time.sleep(10)

  def save_json(self, json_file):
    '''
    Serialize the json data into file
    '''
    try:
      with open(json_file, 'w') as f:
        json_str = json.dumps(self._wiki_json_hash)
        f.write(json_str)
    except IOError as e:
      print 'Can not save file: %s' % json_file

    print 'File %s saved.' % json_file

  def retrieve(self, url):
    ## disguise myself as Firefox
    headers = {
      'User-Agent': 'Mozilla/5.0 (Windows) Gecko/20080201 Firefox/2.0.0.12',
      'Accept': 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9',
      'Accept-Language': 'en-US,en;q=0.5',
      'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
      'Connection': 'keep-alive'
    }

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
  parser.add_argument('json_file')
  args = parser.parse_args()

  dump = DumpWikipedia()
  dump.parse_query(args.query)
  dump.dump_wiki()
  dump.save_json(args.json_file)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'
