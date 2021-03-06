#!/usr/bin/python
'''
apply fuzzy matching of query entities to the streaming documents

fuzzy-match.py <query> <thrift_dir>
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

class FuzzyMatch():
  '''
  Apply exact matching
  '''
  _query_hash = {}
  _org_query_hash = {}
  _alias_query_hash = {}

  _fuzzy_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.fuzzy_match_db)

  def parse_query(self, query_file):
    '''
    parse the query
    '''
    queries = json.load(open(query_file))
    query_list = queries['topic_names']

    ## format the query list
    for index, item in enumerate(query_list):
      self._org_query_hash[index] = item
      item = self.format_query(item)
      self._query_hash[index] = item

    ## dump the query list
    for index, item in enumerate(query_list):
      print '%d\t%s' % (index, item)

  def parse_alias_list(self, alias_dir):
    '''
    parse the alisa file
    '''
    for query in os.listdir(alias_dir):
      alias_file = os.path.join(alias_dir, query)
      try:
        with open(alias_file) as f:
          for line in [line.strip() for line in f]:
            # encode in Unicode
            line = unicode(line, errors='ignore')
            alias = self.format_query(line)
            if not query in self._alias_query_hash:
              self._alias_query_hash[query] = {}
            self._alias_query_hash[query][alias] = 1
            print 'Query: %s - Alias: [%s]' %(query, alias)
      except IOError as e:
        print 'Can not open file: %s' % alias_file
        print 'Will exit.'
        sys.exit(0)

  def format_query(self, query):
    '''
    format the original query
    '''
    # remove parentheses
    parentheses_regex = re.compile( '\(.*\)' )
    query = parentheses_regex.sub( '', query )

    ## replace non word character to space
    non_word_regex = re.compile( '(\W+|\_+)' )
    query = non_word_regex.sub( ' ', query )

    ## compress multiple spaces to single one
    space_regex = re.compile ( '\s+' )
    query = space_regex.sub( ' ', query )

    space_regex = re.compile ( '^\s+' )
    query = space_regex.sub( '', query )

    space_regex = re.compile ( '\s+$' )
    query = space_regex.sub( '', query )

    return query.lower()

  def sanitize(self, str):
    '''
    sanitize the streaming item
    '''
    ## replace non word character to space
    non_word_regex = re.compile( '(\W+|\_+)' )
    str = non_word_regex.sub( ' ', str )

    ## compress multiple spaces
    space_regex = re.compile ( '\s+' )
    str = space_regex.sub( ' ', str )

    return str.lower()

  def fuzzy_match(self, index, doc):
    '''
    Conduct fuzzy match between the query and document
    The value returned is boolean
    '''
    org_query = self._org_query_hash[index]
    query = self._query_hash[index]
    doc = unicode(doc, errors='ignore')

    matched = False
    query_str = ' %s ' %(query)
    if re.search(query_str, doc, re.I | re.M):
      #matched = True
      return matched

    if not org_query in self._alias_query_hash:
      return matched

    for alias in self._alias_query_hash[org_query]:
      if alias == query:
        continue

      alias_str = ' %s ' %(alias)
      if re.search(alias_str, doc, re.I | re.M):
        matched = True
        break

    return matched

  def process_stream_item(self, fname, stream_id, stream_data):
    '''
    process the streaming item: applying exact match for each of the query
    entity
    '''
    new_stream_data = self.sanitize(stream_data)

    for index in self._query_hash:
      query = self._query_hash[index]
      try:
        ## use the query entity as the regex to apply exact match
        #if re.search(query, new_stream_data, re.I | re.M):
        if True == self.fuzzy_match(index, new_stream_data):
          id = self._fuzzy_match_db.llen(RedisDB.ret_item_list)
          self._fuzzy_match_db.rpush(RedisDB.ret_item_list, id)

          ## create a hash record
          ret_item = {'id' : id}
          ret_item['query'] = self._org_query_hash[index]
          ret_item['file'] = fname
          ret_item['stream_id'] = stream_id
          ret_item['stream_data'] = stream_data
          ret_item['score'] = 1000
          self._fuzzy_match_db.hmset(id, ret_item)

          ## verbose output
          print 'Match: %d - %s - %s' %(id, query, stream_id)
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

  match = FuzzyMatch()
  match.parse_query(args.query)
  match.parse_alias_list('entity-surface-forms/')
  match.parse_thift_data(args.thrift_dir)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

