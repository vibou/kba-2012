#!/usr/bin/python
'''
collect the distribution informaton of related entities for each query entity

collect-ent-dist.py <query> <thrift_dir>
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

try:
  from thrift import Thrift
  from thrift.transport import TTransport
  from thrift.protocol import TBinaryProtocol
except:
  ## If we are running in condor, then we might have to tell it
  ## where to find the python thrift library.  This path is from
  ## building thrift from source and not installing it.  It can be
  ## downloaded from:
  ## http://pypi.python.org/packages/source/t/thrift/thrift-0.8.0.tar.gz
  ## and built using
  ##    python setup.py build
  sys.path.append('/path/to/thrift')

  from thrift import Thrift
  from thrift.transport import TTransport
  from thrift.protocol import TBinaryProtocol

from kba_thrift.ttypes import StreamItem, StreamTime, ContentItem

import redis
from config import RedisDB

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

class EntDistCollector():
  '''
  Apply exact matching
  '''
  _query_hash = {}
  _org_query_hash = {}
  _query2id_hash = {}
  _wiki_ent_hash = {}

  _exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      #db=RedisDB.exact_match_db)
      db=RedisDB.test_exact_match_db)

  _wiki_ent_list_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_list_db)

  _wiki_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_dist_db)

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
    for index, item in enumerate(query_list):
      print '%d\t%s' % (index, item)

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

  def load_wiki_ent(self):
    num = self._wiki_ent_list_db.llen(RedisDB.wiki_ent_list)
    if 0 == num:
      print 'no wiki_ent found'
      return

    ent_item_list = self._wiki_ent_list_db.lrange(RedisDB.wiki_ent_list, 0, num)
    ent_items = []

    for ent_id in ent_item_list:
      keys = ['id', 'query', 'ent', 'url']
      db_item = self._wiki_ent_list_db.hmget(ent_id, keys)
      query = db_item[1]
      ent = db_item[2]
      ent = self.format_query(ent)
      self._wiki_ent_hash[id] = ent

  def process_stream_item(self, fname, stream_id, stream_data):
    '''
    process the streaming item: applying exact match for each of the query
    entity
    '''
    doc = self.sanitize(stream_data)
    doc_len = len(doc.split(' '))

    for id in self._wiki_ent_hash:
      try:
        ent = self_wiki_ent_hash[id]
        ent_str = ' %s ' % ent
        if re.search(ent_str, doc, re.I | re.M):
          ## change to count match once to count the total number of matches
          match_list = re.findall(ent_str, doc, re.I | re.M)
          match_num = len(match_list)

          list_name = 'doc-list-%s' % id
          val = '%s:%d:%s' %(stream_id, doc_len, match_num)
          self._wiki_ent_dist_db.lpush(list_name, val)
      except:
        # Catch any unicode errors while printing to console
        # and just ignore them to avoid breaking application.
        print "Exception in process_stream_item()"
        print '-'*60
        traceback.print_exc(file=sys.stdout)
        print '-'*60
        pass

  def parse_thift_data(self, thrift_dir):
    '''
    Parse the thift data in a given directory, apply exact matching over
    the streaming documents
    '''
    for fname in os.listdir(thrift_dir):
      ## ignore other files, e.g. stats.json
      if fname.endswith('.gpg'): continue
      if fname.endswith('.xz'): continue

      ## verbose output
      print 'Process %s' % fname

      ### reverse the steps from above:
      ## load the encrypted data
      fpath = os.path.join(thrift_dir, fname)
      thrift_data = open(fpath).read()

      assert len(thrift_data) > 0, "failed to load: %s" % fpath

      ## wrap it in a file obj, thrift transport, and thrift protocol
      transport = StringIO(thrift_data)
      transport.seek(0)
      transport = TTransport.TBufferedTransport(transport)
      protocol = TBinaryProtocol.TBinaryProtocol(transport)

      ## iterate over all thrift items
      while 1:
        stream_item = StreamItem()
        try:
          stream_item.read(protocol)
        except EOFError:
          break

        ## process data
        self.process_stream_item(fname, stream_item.stream_id, stream_item.body.cleansed)
        ## suppress the verbose output
        #print '%s' % stream_item.doc_id

      ## close that transport
      transport.close()

      # free memory
      thrift_data = None

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('query')
  parser.add_argument('thrift_dir')
  args = parser.parse_args()

  collector = EntDistCollector()
  collector.parse_query(args.query)
  collector.load_wiki_ent()
  collector.parse_thift_data(args.thrift_dir)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

