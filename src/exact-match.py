#!/usr/bin/python
'''
apply exact matching of query entities to the streaming documents
'''

import re
import os
import sys
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

query_list = None

def log(m, newline='\n'):
  sys.stderr.write(m + newline)
  sys.stderr.flush()

def parse_query(query_file):
  queries = json.load(open(query_file))
  global query_list
  query_list = queries['topic_names']

  ## format the query list
  for index, item in enumerate(query_list):
    item = format_query(item)
    query_list[index] = item

  ## dump the query list
  for index, item in enumerate(query_list):
    print '%d\t%s' % (index, item)

def format_query(query):
  # remove parentheses
  parentheses_regex = re.compile( '\(.*\)' )
  query = parentheses_regex.sub( '', query)

  ## replace non word character to space
  non_word_regex = re.compile( '(\W+|\_+)' )
  query = non_word_regex.sub( ' ', query)

  ## compress multiple spaces
  space_regex = re.compile ( '\s+' )
  query = space_regex.sub( ' ', query)
  return query

def trim(str):
  ## replace non word character to space
  non_word_regex = re.compile( '(\W+|\_+)' )
  str = non_word_regex.sub( ' ', str)

  ## compress multiple spaces
  space_regex = re.compile ( '\s+' )
  str = space_regex.sub( ' ', str)
  return query


def parse_thift_data(thrift_dir, date_hour):
  '''
  Parse the thift data, apply exact matching over the streaming documents
  '''
  for fname in os.listdir(os.path.join(thrift_dir, date_hour)):
    ## ignore other files, e.g. stats.json
    if not fname.endswith('.xz.gpg'): continue

    ### reverse the steps from above:
    ## load the encrypted data
    fpath = os.path.join(thrift_dir, date_hour, fname)
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

      ## dump data
      print stream_item.doc_id
      print stream_item.abs_url
      print stream_item.original_url
      print stream_item.body.cleansed
      print '\n\n'

      ## ensure that json source_metadata can still be decoded
      source_metadata = json.loads(stream_item.source_metadata)
      if stream_item.source == 'news':
        assert 'language' in source_metadata

      ## close that transport
      transport.close()
    # free memory
    thrift_data = None

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser(usage='apply exact match')
  parser.add_argument('query_file')
  parser.add_argument('thrift_dir')

  args = parser.parse_args()

  parse_query(args.query_file)
