#!/usr/bin/python

"""
Provide a web interface to browser the corpus
"""

import os
import re
import time
import datetime

import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver

import redis
from config import RedisDB
from tornado.options import define, options

from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from cStringIO import StringIO
from kba_thrift.ttypes import StreamItem, StreamTime, ContentItem

define("port", default=8888, help="run on the given port", type=int)

#corpus_dir = './corpus/cleansed'
#corpus_dir = './uncompressed/training'
corpus_dir = './corpus/testing'

class Dir(dict):
  """
  A dict that allows for object-like property access syntax.
  """
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class File(dict):
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class Doc(dict):
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class HomeHandler(tornado.web.RequestHandler):
  def get(self):

    self.render("index.html", title="KBA")

class BrowseHandler(tornado.web.RequestHandler):
  def get(self):

    dirs = []
    for fname in os.listdir(corpus_dir):
      fpath = os.path.join(corpus_dir, fname)
      if not os.path.isdir(fpath): continue
      dir = Dir()
      dir['name'] = fname
      dirs.append(dir)

    self.render("corpus-index.html", title="KBA", dirs=dirs)


class DateHandler(tornado.web.RequestHandler):
  def get(self, date):
    date_dir = os.path.join(corpus_dir, date)

    if not os.path.isdir(date_dir):
      msg = 'directory %s can not be opened' %date_dir
      #raise tornado.web.HTTPError(404, log_message=msg)
      self.render("error.html", msg=msg)
      return

    files = []
    for fname in os.listdir(date_dir):
      ## ignore other files
      if fname.endswith('.gpg'): continue
      if fname.endswith('.xz'): continue

      file = File()
      file['name'] = fname
      files.append(file)

    self.render("date-index.html", title=date, files=files, date=date)

class FileHandler(tornado.web.RequestHandler):
  def get(self, date, file):

    ## load the thrift data
    fpath = os.path.join(corpus_dir, date, file)
    thrift_data = open(fpath).read()

    if not len(thrift_data) > 0:
      msg = 'failed to load: %s' % fpath
      #raise tornado.web.HTTPError(404, log_message=msg)
      self.render("error.html", msg=msg)
      return

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
        docs.append(doc)
      except EOFError:
        break

    ## close that transport
    transport.close()

    # free memory
    thrift_data = None

    self.render("file-index.html", title=file, date=date, file=file, docs=docs)

class DocHandler(tornado.web.RequestHandler):
  def get(self, date, file, epoch, doc_id):
    date_dir = os.path.join(corpus_dir, date)
    target_id = '%s-%s' %(epoch, doc_id)

    if not os.path.isdir(date_dir):
      msg = 'directory %s can not be opened' %date_dir
      #raise tornado.web.HTTPError(404, log_message=msg)
      self.render("error.html", msg=msg)
      return

    doc = Doc()
    doc['title'] = 'Null'
    doc['body'] = 'Null'
    doc['anchor'] = 'Null'
    doc['date'] = date
    doc['file'] = file
    doc['time'] = datetime.datetime.utcfromtimestamp(float(epoch)).ctime()
    doc['id'] = target_id

    fpath = os.path.join(date_dir, file)
    thrift_data = open(fpath).read()

    if not len(thrift_data) > 0:
      msg = 'failed to load: %s' % fpath
      #raise tornado.web.HTTPError(404, log_message=msg)
      self.render("error.html", msg=msg)
      return

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
        if stream_item.stream_id == target_id:
          found = True
          doc['title'] = stream_item.title.cleansed
          doc['body'] = stream_item.body.cleansed
          doc['anchor'] = stream_item.anchor.cleansed
          break
      except EOFError:
        break

    self.render("doc.html", title=doc_id, doc=doc)

class SearchHandler(tornado.web.RequestHandler):
  def get(self, epoch, id):
    time = datetime.datetime.utcfromtimestamp(float(epoch))
    date = '%d-%.2d-%.2d-%.2d' %(time.year, time.month, time.day, time.hour)
    date_dir = os.path.join(corpus_dir, date)

    target_id = '%s-%s' %(epoch, id)

    if not os.path.isdir(date_dir):
      msg = 'directory %s can not be opened' %date_dir
      #raise tornado.web.HTTPError(404, log_message=msg)
      self.set_status(404)
      self.render("error.html", msg=msg)
      return

    doc = Doc()
    doc['title'] = 'Null'
    doc['body'] = 'Null'
    doc['anchor'] = 'Null'
    doc['date'] = date
    doc['file'] = 'Null'
    doc['time'] = datetime.datetime.utcfromtimestamp(float(epoch)).ctime()
    doc['id'] = target_id
    #self.write('searching')
    #self.flush()

    for fname in os.listdir(date_dir):
      ## ignore other files
      if fname.endswith('.gpg'): continue
      if fname.endswith('.xz'): continue

      fpath = os.path.join(date_dir, fname)
      thrift_data = open(fpath).read()

      if not len(thrift_data) > 0:
        msg = 'failed to load: %s' % fpath
        #raise tornado.web.HTTPError(404, log_message=msg)
        self.render("error.html", msg=msg)
        return

      ## wrap it in a file obj, thrift transport, and thrift protocol
      transport = StringIO(thrift_data)
      transport.seek(0)
      transport = TTransport.TBufferedTransport(transport)
      protocol = TBinaryProtocol.TBinaryProtocol(transport)

      found = False

      ## iterate over all thrift items
      while 1:
        stream_item = StreamItem()
        try:
          stream_item.read(protocol)
          if stream_item.stream_id == target_id:
            found = True
            doc['title'] = stream_item.title.cleansed
            doc['body'] = stream_item.body.cleansed
            doc['anchor'] = stream_item.anchor.cleansed
            doc['file'] = fname
            break
        except EOFError:
          break

      if found: break

    self.render("doc.html", title=target_id, doc=doc)

  def post(self):
    id = self.get_argument('id')
    url = '/doc/%s' % id
    self.redirect(url, permanent=True)

class Application(tornado.web.Application):
  def __init__(self):
    handlers = [
      (r"/", HomeHandler),
      (r"/browse", BrowseHandler),
      (r"/date-([^/]+)", DateHandler),
      (r"/date-([^/]+)/file-([^/]+)", FileHandler),
      (r"/date-([^/]+)/file-([^/]+)/doc-(\d+)-(\w+)", DocHandler),
      (r"/search", SearchHandler),
      (r"/doc/(\d+)-(\w+)", SearchHandler),
    ]

    settings = dict(
      app_title = u"KBA Corpus Browser",
      template_path = os.path.join(os.path.dirname(__file__), "templates"),
    )

    tornado.web.Application.__init__(self, handlers, **settings)

def main():
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()
