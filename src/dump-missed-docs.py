#!/usr/bin/python
'''
Extract the documents missed by our method

exact-match.py <thrift_dir> <annotation> <res>
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
import csv

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

class DumpMissedDocs():
  '''
  Dump the missed documents which are not retrieved by our exact match methods
  '''
  _annotation = dict()
  _missed_docs = dict()
  _retrieved_docs = dict()

  _missed_docs_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.missed_docs_db)

  def parse_annotation (self, path_to_annotation_file, include_relevant, include_neutral):
    '''
    Loads the annotation file into a dict

    path_to_annotation_file: string filesystem path to the annotation file
    include_relevant: true to include docs marked relevant and central
    '''
    annotation_file = csv.reader(open(path_to_annotation_file, 'r'), delimiter='\t')

    for row in annotation_file:
      ## Skip comments
      if row[0][0] == "#":
        continue

      stream_id = row[2]
      urlname = row[3]
      rating = int(row[5])

      # just skipt the stream item which are in 2011
      timestamp = int(stream_id.split('-')[0])
      if timestamp <= 1325375999:
        continue

      if include_neutral:
        thresh = 0
      elif include_relevant:
        thresh = 1
      else:
        thresh = 2

      ## here we kept all the ratings as what they are in the annotation list
      if (stream_id, urlname) in self._annotation:
        ## old rating which is greater than newer one will be overwritten
        if rating < self._annotation[(stream_id, urlname)]:
          self._annotation[(stream_id, urlname)] = rating
      else:
        self._annotation[(stream_id, urlname)] = rating

    for (stream_id, urlname) in self._annotation:
      rating = self._annotation[(stream_id, urlname)]
      if rating > 0:
        if(stream_id, urlname) in self._retrieved_docs:
          continue
        else:
          if stream_id in self._missed_docs:
            self._missed_docs[stream_id][urlname] = rating
          else:
            self._missed_docs[stream_id] = dict()
            self._missed_docs[stream_id][urlname] = rating

  def parse_res (self, path_to_res_file):
    '''
    Loads the result file
    '''
    res_file = csv.reader(open(path_to_res_file, 'r'), delimiter=' ')

    for row in res_file:
      ## Skip comments
      if row[0][0] == "#":
        continue

      urlname = row[0]
      stream_id = row[1]
      score = int(row[2])
      self._retrieved_docs[(stream_id, urlname)] = 1

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
      #print 'Process %s' % fname

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
        stream_id = stream_item.stream_id
        if stream_id in self._missed_docs:
          for urlname in self._missed_docs[stream_id]:
            id = self._missed_docs_db.llen(RedisDB.ret_item_list)
            ret_item = {'id' : id}
            ret_item['file'] = fname
            ret_item['query'] = urlname
            ret_item['rating'] = self._missed_docs[stream_id][urlname]
            ret_item['stream_id'] = stream_id
            ret_item['stream_data'] = stream_item.body.cleansed

            self._missed_docs_db.hmset(id, ret_item)
            self._missed_docs_db.rpush(RedisDB.ret_item_list, id)
            print 'Missed %s %s\n\n\n' %(urlname, stream_id)

        ## suppress the verbose output
        #print '%s' % stream_item.doc_id

      ## close that transport
      transport.close()

      # free memory
      thrift_data = None

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('thrift_dir')
  parser.add_argument('annotation')
  parser.add_argument('res')
  args = parser.parse_args()

  match = DumpMissedDocs()
  match.parse_res(args.res)
  match.parse_annotation(args.annotation, True, False)
  match.parse_thift_data(args.thrift_dir)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

