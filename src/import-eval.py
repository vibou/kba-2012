#!/usr/bin/python
'''
import evaluation result to DB
'''

import re
import os
import sys
import traceback
import time
import fileinput
import datetime

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

class ImportEval():
  '''
  Apply exact matching
  '''
  _eval_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.eval_db)
  _item_list = []

  def parse_eval(self, eval_file):
    '''
    parse the judgment file
    '''
    try:
      with open(eval_file) as f: pass
    except IOError as e:
      print 'File %s does not exist.' %eval_file
      exit(-1)

    num_ret = 0

    for line in fileinput.input([eval_file]):
      ## skip comments
      if re.match(r'\#', line): continue

      line = line.replace('\n', '')
      item_list = line.split('\t')
      if(int(item_list[5]) > 0 and int(item_list[6]) > 0):
        num_ret = num_ret + 1

        item = {}
        item['id'] = num_ret
        item['stream_id'] = item_list[2]
        item['query'] = item_list[3]
        item['score'] = item_list[4]
        item['judge1'] = item_list[5]
        item['judge2'] = item_list[6]
        self._item_list.append(item)

    print '%d records in total' %num_ret

  def process_stream_item(self, fname, item, stream_data):
    '''
    process the streaming item: applying exact match for each of the query
    entity
    '''
    try:
      id = item['id']
      self._eval_db.rpush(RedisDB.ret_item_list, id)

      ## create a hash record
      ret_item = {'id' : id}
      ret_item['query'] = item['query']
      ret_item['file'] = fname
      ret_item['stream_id'] = item['stream_id']
      ret_item['stream_data'] = stream_data
      ret_item['score'] = item['score']
      ret_item['judge1'] = item['judge1']
      ret_item['judge2'] = item['judge2']
      self._eval_db.hmset(id, ret_item)
      self._eval_db.rpush(RedisDB.ret_item_list, id)
    except:
      # Catch any unicode errors while printing to console
      # and just ignore them to avoid breaking application.
      print "Exception in process_stream_item()"
      print '-'*60
      traceback.print_exc(file=sys.stdout)
      print '-'*60
      pass

  def parse_thift_data(self, corpus_dir):
    '''
    Parse the thift data, find the files in which the stream_items are
    '''
    for item in sorted(self._item_list, key=lambda item:item['id']):
      ## skip the existing items
      ret_item_keys = ['id']
      if self._eval_db.hexists(item['id'], ret_item_keys):
        print 'Skipping %d' %item['id']

      target_id = item['stream_id']
      list = target_id.split('-')
      epoch = list[0]

      time = datetime.datetime.utcfromtimestamp(float(epoch))
      date = '%d-%.2d-%.2d-%.2d' %(time.year, time.month, time.day, time.hour)
      date_dir = os.path.join(corpus_dir, date)

      if not os.path.isdir(date_dir):
        print 'directory %s can no be opened' %date_dir

      found = False
      for fname in os.listdir(date_dir):
        ## ignore other files, e.g. stats.json
        if fname.endswith('.gpg'): continue
        if fname.endswith('.xz'): continue

        ### reverse the steps from above:
        ## load the encrypted data
        fpath = os.path.join(date_dir, fname)
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

          if stream_item.stream_id == target_id:
            self.process_stream_item(fname, item, stream_item.body.cleansed)
            found = True
            break

        ## close that transport
        transport.close()

        # free memory
        thrift_data = None

        if found:
          print 'Item %d processed' %item['id']
          break

      if not found:
        print 'Item %d can not be found in any file' %item['id']

def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('query')
  parser.add_argument('corpus_dir')
  args = parser.parse_args()

  object = ImportEval()
  object.parse_eval(args.query)
  object.parse_thift_data(args.corpus_dir)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'

