#!/usr/bin/python
'''
Import some data into DB

import2db.py

'''
## use float division instead of integer division
from __future__ import division

END_OF_2012 = 1325375999

QREL_FILE = 'eval/after.txt'
TRAIN_RET_DIR = 'ret/train/'
TEST_RET_DIR = 'ret/test/'
g_cutoff_step = 1

import os
import csv
import gzip
import json
import argparse
import datetime
from collections import defaultdict

import redis
from config import RedisDB

QRELS_DB = redis.Redis(host=RedisDB.host, port=RedisDB.port,
  db=RedisDB.qrels_db)

def load_annotation (path_to_annotation_file, include_relevant, is_training):
  '''
  Loads the annotation file into a dict

  path_to_annotation_file: string filesystem path to the annotation file
  include_relevant: true to include docs marked relevant and central
  '''
  annotation_file = csv.reader(open(path_to_annotation_file, 'r'), delimiter='\t')
  print 'Loading %s' % path_to_annotation_file

  thresh = 0
  if include_relevant:
    thresh = 1
  else:
    thresh = 2

  annotation = dict()
  for row in annotation_file:
    ## Skip comments
    if row[0][0] == "#":
      continue

    stream_id = row[2]
    timestamp = int(stream_id.split('-')[0])
    urlname = row[3]
    rating = int(row[5])

    # skip if it is training data
    if (not is_training) and (timestamp <= END_OF_2012):
      continue

    if (is_training) and (timestamp > END_OF_2012):
      continue

    ## Add the stream_id and urlname to a hashed dictionary
    ## 0 means that its not central 1 means that it is central

    if urlname not in annotation:
      annotation[urlname] = {}

    if stream_id in annotation[urlname]:
      ## 2 means the annotators gave it a yes for centrality
      if rating < thresh:
        annotation[urlname][stream_id] = False
    else:
      annotation[urlname][stream_id] = rating >= thresh

  return annotation

def import_qrels():
  ## Load in the annotation data, including both training and testing data
  c_annotation = load_annotation(QREL_FILE, False, False)
  rc_annotation = load_annotation(QREL_FILE, True, False)

  query_keys = c_annotation.keys()
  query_keys.sort()
  for idx, query in enumerate(query_keys):
    #key = 'training-c-%d' % idx
    key = 'testing-c-%d' % idx
    str = json.dumps(c_annotation[query])
    QRELS_DB.hset(key, idx, str)

  query_keys = rc_annotation.keys()
  query_keys.sort()
  for idx, query in enumerate(query_keys):
    #key = 'training-rc-%d' % idx
    key = 'testing-rc-%d' % idx
    str = json.dumps(rc_annotation[query])
    QRELS_DB.hset(key, idx, str)

def main():
  parser = argparse.ArgumentParser(description=__doc__, usage=__doc__)
  #parser.add_argument('qrels_file')
  args = parser.parse_args()

  import_qrels()

if __name__ == '__main__':
  try:
      main()
  except KeyboardInterrupt:
      print '\nGoodbye!'

