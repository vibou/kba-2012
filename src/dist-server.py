#!/usr/bin/python

"""
Provide a web interface to browse the distribution
"""
# force division to be floating point
# http://stackoverflow.com/a/1267892
from __future__ import division

import os
import re
import time
import datetime
import math

import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver

import redis
import csv
from config import RedisDB
from tornado.options import define, options


define("port", default=9999, help="run on the given port", type=int)

'''
Chunk the list into bins with the same size
http://stackoverflow.com/a/312464
'''
def chunks(l, n):
  '''
  Yield successive n-sized chunks from l.
  '''
  for i in xrange(0, len(l), n):
    yield l[i:i+n]

'''
Transfer the raw data to HTML
Basically the goal is to make sure each paragraph is embedded in <p></p>
'''
def raw2html(raw):
  sentences = raw.split('\n')
  html = ""
  for sent in sentences:
    sent = "<p>" + sent + "</p>\n"
    html += sent

  return html

class DictItem(dict):
  '''
  A dict that allows for object-like property access syntax.
  '''
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class BaseHandler(tornado.web.RequestHandler):
  @property
  def _exact_match_db(self):
    return self.application._exact_match_db

  @property
  def _wiki_ent_list_db(self):
    return self.application._wiki_ent_list_db

  @property
  def _wiki_ent_dist_db(self):
    return self.application._wiki_ent_dist_db

  @property
  def _annotation(self):
    return self.application._annotation

  @property
  def _rel_num(self):
    return self.application._rel_num

  @property
  def _doc_num_all(self):
    return 39138

class HomeHandler(BaseHandler):
  def get(self):
    self.render('home.html', title='KBA Wiki Ent Dist')

class IDFMIHandler(BaseHandler):
  def get(self):
    self.render('idf-mi.html')

class IDFMIDistHandler(BaseHandler):
  def get(self):
    #                    p(occ|rel)
    # weight(ent) = log ------------
    #                     p(occ)

    ent_num = self._wiki_ent_list_db.llen(RedisDB.wiki_ent_list)
    ent_list = self._wiki_ent_list_db.lrange(RedisDB.wiki_ent_list, 0, ent_num)

    so_far = 0
    point_list = []
    for ent_id in ent_list:
      so_far = so_far + 1
      #if so_far > 100:
        #break

      keys = ['id', 'query', 'ent', 'url']
      db_item = self._wiki_ent_list_db.hmget(ent_id, keys)

      query = db_item[1]
      ent = db_item[2]
      if not query in self._annotation:
        continue

      list_name = 'doc-list-%s' % ent_id
      list_len = self._wiki_ent_dist_db.llen(list_name)
      if not list_len > 0:
        continue

      item_list = self._wiki_ent_dist_db.lrange(list_name, 0, list_len)
      occ_num = list_len

      rel_num = len(self._annotation[query].keys())
      occ_rel_num = 0
      all_doc_num = self._doc_num_all

      for item in item_list:
        items = item.split(':')
        stream_id = items[0]
        num = items[2]

        # check whether the document is relevant
        if stream_id in self._annotation[query]:
          occ_rel_num = occ_rel_num + 1

      p_occ_rel = occ_rel_num / rel_num
      p_occ = occ_num / all_doc_num
      # boundary check
      if not p_occ_rel > 0:
        continue

      log_idf = math.log(p_occ)
      w_ent = math.log(p_occ_rel / p_occ)

      #line = '%6.3f\t%6.3f\t%s\t%s\t%s\n' %(log_idf, w_ent, ent_id, ent, query)
      line = '%6.3f\t%6.3f\n' %(log_idf, w_ent)
      self.write(line)

      point = DictItem()
      point['occ_num'] = occ_num
      point['log_idf'] = log_idf
      point['w_ent'] = w_ent
      point_list.append(point)

    ## sort the point list occ_num
    point_list.sort(key=lambda x: x['w_ent'])
    ## group them into bins, and select the median value of the bin to output
    for bin in list(chunks(point_list, 10)):
      median = int(len(bin)/2)
      med_point = bin[median]
      #line = '%6.3f\t%6.3f\n' %(med_point['log_idf'], med_point['w_ent'])
      #self.write(line)

class IDFMIQueryDistHandler(BaseHandler):
  def get(self):
    #                    p(occ|rel)
    # weight(ent) = log ------------
    #                     p(occ)

    query_list = range(0, 29)
    point_list = []
    for qid in query_list:
      list_name = 'query-doc-list-%s' % qid
      list_len = self._wiki_ent_dist_db.llen(list_name)
      if not list_len > 0:
        continue

      item_list = self._wiki_ent_dist_db.lrange(list_name, 0, list_len)
      occ_num = list_len

      query = self._wiki_ent_list_db.hget(RedisDB.query_ent_list, qid)
      rel_num = len(self._annotation[query].keys())
      occ_rel_num = 0
      all_doc_num = self._doc_num_all

      for item in item_list:
        items = item.split(':')
        stream_id = items[0]
        num = items[2]

        # check whether the document is relevant
        if stream_id in self._annotation[query]:
          occ_rel_num = occ_rel_num + 1

      p_occ_rel = occ_rel_num / rel_num
      p_occ = occ_num / all_doc_num
      # boundary check
      if not p_occ_rel > 0:
        continue

      log_idf = math.log(p_occ)
      w_ent = math.log(p_occ_rel / p_occ)

      #line = '%6.3f\t%6.3f\t%s\t%d\n' %(log_idf, w_ent, query, qid)
      line = '%6.3f\t%6.3f\n' %(log_idf, w_ent)
      self.write(line)

class IDFRelHandler(BaseHandler):
  def get(self):
    self.render('idf-rel.html')

class IDFRelDistHandler(BaseHandler):
  def get(self):
    # weight(ent) = p(rel|occ)

    ent_num = self._wiki_ent_list_db.llen(RedisDB.wiki_ent_list)
    ent_list = self._wiki_ent_list_db.lrange(RedisDB.wiki_ent_list, 0, ent_num)

    so_far = 0
    point_list = []
    for ent_id in ent_list:
      so_far = so_far + 1
      #if so_far > 100:
        #break

      keys = ['id', 'query', 'ent', 'url']
      db_item = self._wiki_ent_list_db.hmget(ent_id, keys)

      query = db_item[1]
      if not query in self._annotation:
        continue

      list_name = 'doc-list-%s' % ent_id
      list_len = self._wiki_ent_dist_db.llen(list_name)
      if not list_len > 0:
        continue

      item_list = self._wiki_ent_dist_db.lrange(list_name, 0, list_len)
      occ_num = list_len

      rel_num = len(self._annotation[query].keys())
      occ_rel_num = 0
      all_doc_num = self._doc_num_all

      for item in item_list:
        items = item.split(':')
        stream_id = items[0]
        num = items[2]

        # check whether the document is relevant
        if stream_id in self._annotation[query]:
          occ_rel_num = occ_rel_num + 1

      p_rel_occ = occ_rel_num / occ_num
      p_occ = occ_num / all_doc_num
      # boundary check
      if not p_rel_occ > 0:
        continue

      log_idf = math.log(p_occ)
      w_ent = math.log(p_rel_occ)

      line = '%6.3f\t%6.3f\n' %(log_idf, w_ent)
      self.write(line)

      point = DictItem()
      point['occ_num'] = occ_num
      point['log_idf'] = log_idf
      point['w_ent'] = w_ent
      point_list.append(point)

    ## sort the point list occ_num
    point_list.sort(key=lambda x: x['w_ent'])
    ## group them into bins, and select the median value of the bin to output
    for bin in list(chunks(point_list, 10)):
      median = int(len(bin)/2)
      med_point = bin[median]
      #line = '%6.3f\t%6.3f\n' %(med_point['log_idf'], med_point['w_ent'])
      #self.write(line)

class IDFRelQueryDistHandler(BaseHandler):
  def get(self):
    # weight(ent) = p(rel|occ)

    query_list = range(0, 29)
    point_list = []
    for qid in query_list:
      list_name = 'query-doc-list-%d' % qid
      list_len = self._wiki_ent_dist_db.llen(list_name)
      if not list_len > 0:
        continue

      item_list = self._wiki_ent_dist_db.lrange(list_name, 0, list_len)
      occ_num = list_len

      query = self._wiki_ent_list_db.hget(RedisDB.query_ent_list, qid)
      rel_num = len(self._annotation[query].keys())
      occ_rel_num = 0
      all_doc_num = self._doc_num_all

      for item in item_list:
        items = item.split(':')
        stream_id = items[0]
        num = items[2]

        # check whether the document is relevant
        if stream_id in self._annotation[query]:
          occ_rel_num = occ_rel_num + 1

      p_rel_occ = occ_rel_num / occ_num
      p_occ = occ_num / all_doc_num
      # boundary check
      if not p_rel_occ > 0:
        continue

      log_idf = math.log(p_occ)
      w_ent = math.log(p_rel_occ)

      line = '%6.3f\t%6.3f\n' %(log_idf, w_ent)
      self.write(line)

class DocLenRelHandler(BaseHandler):
  def get(self):
    self.render('doc-len-rel.html')

class DocLenRelDistHandler(BaseHandler):
  def get(self):
    doc_num = self._exact_match_db.llen(RedisDB.ret_item_list)
    doc_id_list = self._exact_match_db.lrange(RedisDB.ret_item_list, 0, doc_num)

    so_far = 0
    doc_list = []
    for doc_id in doc_id_list:
      so_far = so_far + 1
      #if so_far > 1000:
        #break

      keys = ['id', 'query', 'stream_id', 'len', 'rel']
      db_item = self._exact_match_db.hmget(doc_id, keys)

      doc = DictItem()
      doc['len'] = int(db_item[3])
      doc['rel'] = int(db_item[4])
      doc_list.append(doc)

    ## sort doc_list by document length
    doc_list.sort(key=lambda x: x['len'])
    for bin in list(chunks(doc_list, 100)):
      num_rel = 0
      for doc in bin:
        if doc['rel'] > 0:
          num_rel = num_rel + 1

      median = int(len(bin)/2)
      doc_len_med = bin[median]['len']
      p_rel = num_rel / self._rel_num

      line = '%d\t%6.3f\n' %(doc_len_med, p_rel)
      self.write(line)

class Application(tornado.web.Application):
  def __init__(self):
    handlers = [
      (r'/', HomeHandler),
      (r'/idf/mi', IDFMIHandler),
      (r'/idf/mi/dist', IDFMIDistHandler),
      (r'/idf/mi/query/dist', IDFMIQueryDistHandler),
      (r'/idf/rel', IDFRelHandler),
      (r'/idf/rel/dist', IDFRelDistHandler),
      (r'/idf/rel/query/dist', IDFRelQueryDistHandler),
      (r'/doc/len/rel', DocLenRelHandler),
      (r'/doc/len/rel/dist', DocLenRelDistHandler),
    ]

    settings = dict(
      app_title = u'Wiki Ent Dist',
      template_path = os.path.join(os.path.dirname(__file__), "dist"),
    )

    tornado.web.Application.__init__(self, handlers, **settings)

    # global database connections for all handles
    self._exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      #db=RedisDB.train_exact_match_db)
      db=RedisDB.test_exact_match_db)

    self._wiki_ent_list_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.wiki_ent_list_db)

    self._wiki_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      #db=RedisDB.train_wiki_ent_dist_db)
      db=RedisDB.wiki_ent_dist_db)

    self._annotation = self.load_annotation('eval/qrels/all.txt', True, False)

    self._rel_num = 0
    for query in self._annotation:
      num = len(self._annotation[query])
      self._rel_num = self._rel_num + num

  def load_annotation (self, path, include_relevant, include_neutral):
    '''
    Loads the annotation file into a dict

    path: string filesystem path to the annotation file
    include_relevant: true to include docs marked relevant and central
    '''
    annotation_file = csv.reader(open(path, 'r'), delimiter='\t')
    annotation = {}
    print 'Loading %s' % path

    # this is the epoch time for the last second of 2011, i.e. Dec 31 2011
    # 23:59:59 GMT+0000
    epoch_thred = 1325375999

    for row in annotation_file:
      ## Skip comments
      if row[0][0] == "#":
        continue

      stream_id = row[2]
      epoch = int(stream_id.split('-')[0])
      if not epoch > epoch_thred:
      #if epoch > epoch_thred:
        continue

      urlname = row[3]
      rating = int(row[5])

      ## 0 means that its not central 1 means that it is central
      ## 2 means the annotators gave it a yes for centrality
      if include_neutral:
        thresh = 0
      elif include_relevant:
        thresh = 1
      else:
        thresh = 2

      if rating < thresh:
        continue

      ## Add the stream_id and urlname to a hashed dictionary
      if not urlname in annotation:
        annotation[urlname] = {}

      if not stream_id in annotation[urlname]:
        annotation[urlname][stream_id] = rating
      else:
        if rating < annotation[urlname][stream_id]:
          annotation[urlname][stream_id] = rating
    print 'Done.'
    return annotation

def main():
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()

