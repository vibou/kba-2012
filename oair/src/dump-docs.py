#!/usr/bin/python
'''
Dump the documents from trift document set

fuzzy-match.py <list> <thrift_dir>
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
from temp_config import RedisDB

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

class FuzzyMatch():
  '''
  Apply exact matching
  '''

  _doc_hash = {}
  #_write_to_db = False
  _write_to_db = True

  _oair_doc_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.oair_doc_train_db)
      #db=RedisDB.oair_doc_test_db)

  def parse_doc_list(self, doc_list_file):
    '''
    parse the doc_list
    '''
    try:
      with open(doc_list_file) as f:
        for line in [line.strip() for line in f]:
          values = line.split('\t')
          did = values[0]
          query = values[1]
          if did not in self._doc_hash:
            self._doc_hash[did] = {}
          # save in hash
          self._doc_hash[did][query] = 1

    except IOError as e:
      print 'Can not open file: %s' % alias_file
      print 'Will exit.'
      sys.exit(0)

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

  def process_stream_item(self, fname, stream_id, stream_data):
    '''
    process the streaming item: applying exact match for each of the query
    entity
    '''
    new_stream_data = self.sanitize(stream_data)

    try:
      ## use the query entity as the regex to apply exact match
      if stream_id in self._doc_hash:
        for query in self._doc_hash[stream_id]:
          id = self._oair_doc_db.llen(RedisDB.ret_item_list)
          if self._write_to_db:
            self._oair_doc_db.rpush(RedisDB.ret_item_list, id)

          ## create a hash record
          ret_item = {'id' : id}
          ret_item['query'] = query
          ret_item['file'] = fname
          ret_item['stream_id'] = stream_id
          ret_item['stream_data'] = stream_data
          ret_item['score'] = 1000
          if self._write_to_db:
            self._oair_doc_db.hmset(id, ret_item)

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
  parser.add_argument('doc_list')
  parser.add_argument('thrift_dir')
  args = parser.parse_args()

  match = FuzzyMatch()
  match.parse_doc_list(args.doc_list)
  match.parse_thift_data(args.thrift_dir)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

