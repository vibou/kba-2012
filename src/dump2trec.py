#!/usr/bin/python

"""
Dump the date from thrift to trec format
"""

import os
import re
import time
import datetime

from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from cStringIO import StringIO
from kba_thrift.ttypes import StreamItem, StreamTime, ContentItem

class Doc(dict):
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class Dump2Trec():

  _corpus_dir = ''
  _date = ''
  _save_dir = ''
  _save_file = ''

  def ProcessDir(self, corpus, date, save):
    self._corpus_dir = corpus
    self._date = date
    self._save_dir = save
    self._save_file = os.path.join(self._save_dir, self._date)
    self.ScanDateDir()

  """
  Scane a dir with specific date and time
  """
  def ScanDateDir(self):
    date_dir = os.path.join(self._corpus_dir, self._date)

    if not os.path.isdir(date_dir):
      msg = 'directory %s can not be opened' %date_dir
      print 'Error: %s' % (msg)
      return

    print 'Processing %s' %( self._date )

    files = []
    for fname in os.listdir(date_dir):
      ## ignore other files
      if fname.endswith('.gpg'): continue
      if fname.endswith('.xz'): continue

      fpath = os.path.join(date_dir, fname)
      self.ProcessThriftFile(fpath)
      #return

  """
  Process one thrift file
  """
  def ProcessThriftFile(self, fpath):
    thrift_data = open(fpath).read()

    if not len(thrift_data) > 0:
      msg = 'failed to load: %s' % fpath
      print 'Error: %s' % (msg)
      return

    #print 'Processing %s' %( fpath )

    ## wrap it in a file obj, thrift transport, and thrift protocol
    transport = StringIO(thrift_data)
    transport.seek(0)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    docs = []
    ## iterate over all thrift items
    while 1:
      stream_item = StreamItem()
      try:
        stream_item.read(protocol)
        doc = Doc()
        doc.id = stream_item.stream_id
        doc.epoch = stream_item.stream_time.epoch_ticks
        doc.time = datetime.datetime.utcfromtimestamp(doc.epoch).ctime()
        doc.title = stream_item.title.cleansed
        doc.body = stream_item.body.cleansed
        doc.anchor = stream_item.anchor.cleansed

        self.SaveDoc(doc)
      except EOFError:
        break

    ## close that transport
    transport.close()

    # free memory
    thrift_data = None

  """
  Save one streaming document
  """
  def SaveDoc(self, doc):
    # prepare for the document data in TREC format
    doc_str = '<DOC>\n'

    str = '<DOCNO> %s </DOCNO>\n' %( doc.id )
    doc_str += str

    doc_str += '<TEXT>\n'
    str = '%s\n' %( doc.body )
    doc_str += str

    doc_str += '</TEXT>\n'
    doc_str += '</DOC>\n'

    if os.path.isfile(self._save_file):
      # append to the file
      open(self._save_file, 'a').write(doc_str)
    else:
      # create the file and write the data then
      open(self._save_file, 'wb').write(doc_str)


def main():
  import argparse
  parser = argparse.ArgumentParser(usage=__doc__)
  parser.add_argument('corpus_dir')
  parser.add_argument('date')
  parser.add_argument('save_dir')
  args = parser.parse_args()

  dump = Dump2Trec()
  dump.ProcessDir(args.corpus_dir, args.date, args.save_dir)

if __name__ == "__main__":
  main()

