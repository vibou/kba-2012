#!/usr/bin/python

"""
Provide a web interface to browse the temporal information of related entities
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

define("port", default=8888, help="run on the given port", type=int)

class DictItem(dict):
  """
  A dict that allows for object-like property access syntax.
  """
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

class BaseHandler(tornado.web.RequestHandler):
  @property
  def _rel_ent_dist_db(self):
    return self.application._rel_ent_dist_db

  @property
  def _doc_db(self):
    return self.application._doc_db

  @property
  def _test_db(self):
    return self.application._test_db

class HomeHandler(BaseHandler):
  def get(self):
    url = '/ent'
    self.redirect(url)

class EntIndexHandler(BaseHandler):
  def get(self):
    num = self._rel_ent_dist_db.llen(RedisDB.query_ent_list)
    if 0 == num:
      msg = 'no item found'
      self.render("error.html", msg=msg)
      return

    query_ent_list = self._rel_ent_dist_db.lrange(RedisDB.query_ent_list, 0, num)
    query_ent_hash = self._rel_ent_dist_db.hmget(RedisDB.query_ent_hash,
        query_ent_list)
    ret_items = []
    for index in query_ent_list:
      ret_item = DictItem()
      ret_item['id'] = index
      ret_item['ent'] = query_ent_hash[int(index)]
      ret_items.append(ret_item)

    self.render("ent-index.html", title='KBA Query Entities', ret_items=ret_items)

class EntViewHandler(BaseHandler):
  def get(self, ent_id):
    hash_key = 'query-%s' % ent_id
    keys = self._rel_ent_dist_db.hkeys(hash_key)
    if 0 == len(keys):
      msg = 'no data found'
      self.render("error.html", msg=msg)
      return

    ## retrieve the list of revisions
    rel_ent_hash_key = 'query-rel-ent-%s' % ent_id
    rel_ent_keys = self._rel_ent_dist_db.hkeys(rel_ent_hash_key)

    ## sort the keys by date: http://stackoverflow.com/q/2589479
    rel_ent_keys.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))
    db_item = self._rel_ent_dist_db.hmget(rel_ent_hash_key, rel_ent_keys)

    date_list = []

    for idx, rel_ent_str in enumerate(db_item):
      item = DictItem()
      item['date'] = rel_ent_keys[idx]
      item['num'] = len(rel_ent_str.split('='))
      date_list.append(item)

    ent = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, ent_id)
    self.render("ent-view.html", ent_id=ent_id, ent=ent, date_list=date_list)

class EntRevHandler(BaseHandler):
  def get(self, ent_id, date):
    ## check whether this revision exists or not
    rel_ent_hash_key = 'query-rel-ent-%s' % ent_id
    if not self._rel_ent_dist_db.hexists(rel_ent_hash_key, date):
      msg = 'no data found'
      self.render("error.html", msg=msg)
      return

    rel_ent_str = self._rel_ent_dist_db.hget(rel_ent_hash_key, date)
    rel_ent_list = rel_ent_str.split('=')

    ent = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, ent_id)
    self.render("ent-rev.html", ent_id=ent_id, ent=ent, date=date,
        rel_ent_list=rel_ent_list)

class EntDistHandler(BaseHandler):
  def get(self, ent_id):
    hash_key = 'query-%s' % ent_id
    keys = self._rel_ent_dist_db.hkeys(hash_key)
    if 0 == len(keys):
      msg = 'no dist data found'
      self.render("error.html", msg=msg)
      return

    rel_num_hash_key = 'query-rel-ent-num-%s' % ent_id
    rel_num_keys = self._test_db.hkeys(rel_num_hash_key)
    if 0 == len(rel_num_keys):
      msg = 'no rel_num data found'
      self.render("error.html", msg=msg)
      return

    if set(keys) != set(rel_num_keys):
      msg = 'dist and rel_num data are not consistent'
      self.render("error.html", msg=msg)
      return

    eval_hash_key = 'query-opt-C-F-%s' % ent_id
    #eval_hash_key = 'query-opt-CR-F-%s' % ent_id
    eval_keys = self._rel_ent_dist_db.hkeys(eval_hash_key)
    if 0 == len(eval_keys):
      msg = 'no eval_dist data found'
      self.render("error.html", msg=msg)
      return

    if set(keys) != set(eval_keys):
      msg = 'dist and eval_dist data are not consistent'
      self.render("error.html", msg=msg)
      return

    cr_eval_hash_key = 'query-opt-CR-F-%s' % ent_id
    cr_eval_keys = self._rel_ent_dist_db.hkeys(cr_eval_hash_key)
    if 0 == len(cr_eval_keys):
      msg = 'no cr_eval_dist data found'
      self.render("error.html", msg=msg)
      return

    if set(keys) != set(cr_eval_keys):
      msg = 'dist and cr_eval_dist data are not consistent'
      self.render("error.html", msg=msg)
      return

    ## sort the keys by date: http://stackoverflow.com/q/2589479
    keys.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))

    # retrieve from DB
    db_item = self._rel_ent_dist_db.hmget(hash_key, keys)
    rel_num_db_item = self._test_db.hmget(rel_num_hash_key, keys)
    eval_db_item = self._rel_ent_dist_db.hmget(eval_hash_key, keys)
    cr_eval_db_item = self._rel_ent_dist_db.hmget(cr_eval_hash_key, keys)

    line = 'date\tclose\n'
    self.write(line)

    for idx, dist_val in enumerate(db_item):
      dist_val = db_item[idx]
      rel_num_val = rel_num_db_item[idx]
      eval_val = eval_db_item[idx]
      cr_eval_val = cr_eval_db_item[idx]
      line = '%s\t%s\t%s\t%s\t%s\n' %(keys[idx], dist_val, rel_num_val,
          eval_val, cr_eval_val)
      self.write(line)

class DocListHandler(BaseHandler):
  def get(self, ent_id):
    num = self._doc_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no doc item found'
      self.render("error.html", msg=msg)
      return

    # get the query entity
    ent = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, ent_id)

    ret_item_list = self._doc_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []

    # we only select the documents associated with the query
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'stream_id']
      db_item = self._doc_db.hmget(ret_id, ret_item_keys)

      if ent != db_item[1]:
        continue

      ret_item = DictItem()
      ret_item['id'] = db_item[0]
      ret_item['query'] = db_item[1]
      ret_item['stream_id'] = db_item[2]
      ret_items.append(ret_item)

    self.render("doc-list.html", ent=ent, ent_id=ent_id, ret_items=ret_items)

class DocViewHandler(BaseHandler):
  def get(self, ent_id, doc_id):
    # get the query entity
    ent = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, ent_id)

    if not self._doc_db.exists(doc_id):
      msg = 'Invalid document ID %s' % doc_id
      self.render("error.html", msg=msg)
      return

    ret_item_keys = ['id', 'query', 'stream_id']
    db_item = self._doc_db.hmget(doc_id, ret_item_keys)

    # make sure the document ID is associated with the query
    if ent != db_item[1]:
      msg = ' Invalid document ID for entity %s' % ent
      self.render("error.html", msg=msg)
      return

    stream_id = db_item[2]

    # now, list all the available revisions from Wikipedia page of the query entity
    ## retrieve the list of revisions
    rev_hash_key = 'query-rel-ent-%s' % ent_id
    rev_keys = self._rel_ent_dist_db.hkeys(rev_hash_key)

    ## sort the keys by date: http://stackoverflow.com/q/2589479
    rev_keys.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))
    db_item = self._rel_ent_dist_db.hmget(rev_hash_key, rev_keys)

    date_list = []
    for idx, rel_ent_str in enumerate(db_item):
      item = DictItem()
      item['date'] = rev_keys[idx]
      item['num'] = len(rel_ent_str.split('='))
      date_list.append(item)

    self.render("doc-view.html", ent_id=ent_id, ent=ent, doc_id=doc_id,
        stream_id=stream_id, date_list=date_list)

class DocRevViewHandler(BaseHandler):
  def get(self, query_id, doc_id, date):
    # retrieve query from DB
    org_query = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, query_id)

    # retrieve document from DB
    if not self._doc_db.exists(doc_id):
      msg = 'Invalid document ID %s' % doc_id
      self.render("error.html", msg=msg)
      return

    ret_item_keys = ['id', 'query', 'stream_id', 'stream_data']
    db_item = self._doc_db.hmget(doc_id, ret_item_keys)

    # make sure the document ID is associated with the query
    if org_query != db_item[1]:
      msg = ' Invalid document ID for query %s' % org_query
      self.render("error.html", msg=msg)
      return

    stream_id = db_item[2]
    doc = db_item[3]

    ## retrieve the related entities for this revision
    rel_ent_hash_key = 'query-rel-ent-%s' % query_id
    if not self._rel_ent_dist_db.hexists(rel_ent_hash_key, date):
      msg = 'no revision data found for %s' %date
      self.render("error.html", msg=msg)
      return

    rel_ent_str = self._rel_ent_dist_db.hget(rel_ent_hash_key, date)
    rel_ent_list = rel_ent_str.split('=')

    '''
    Transfer the raw data to HTML
    Basically the goal is to make sure each paragraph is embedded in <p></p>
    '''
    ## first, calculate it with the query entity
    ## use the query entity as the regex to apply exact match
    query = self.format_query(org_query)
    query_str = '[\s\W]%s[\s\W]' % query
    match = re.search(query, doc, re.I | re.M)
    doc = unicode(doc, errors='ignore')
    if match:
      query_span = '<span class=\"qent\"> ' + query + ' </span>'
      query_regex = ur'[\s\W]%s[\s\W]' %( query )
      replacer = re.compile(query_regex, re.I | re.M)
      doc = replacer.sub(query_span, doc)

    ent_match_sum = 0
    ent_uniq_match_sum = 0
    for ent in rel_ent_list:
      ent_str = '[\s\W]%s[\s\W]' % ent
      if re.search(ent_str, doc, re.I | re.M):
        ## change to count match once to count the total number of matches
        match_list = re.findall(ent_str, doc, re.I | re.M)
        ent_match_sum += len(match_list)
        ent_uniq_match_sum += 1

        ent_span = '<span class=\"rent\"> ' + ent + ' </span>'
        ent_regex = ur'[\s\W]%s[\s\W]' %( ent )
        replacer = re.compile(ent_regex, re.I | re.M)
        doc = replacer.sub(ent_span, doc)

    doc = self.raw2html(doc)
    self.render("doc-rev-view.html", query_id=query_id, query=org_query, doc_id=doc_id,
        stream_id=stream_id, date=date, doc=doc,
        ent_match_sum = ent_match_sum, ent_uniq_match_sum=ent_uniq_match_sum)

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

    ## remove leading space
    space_regex = re.compile ( '^\s' )
    query = space_regex.sub( '', query)

    ## remove trailing space
    space_regex = re.compile ( '\s$' )
    query = space_regex.sub( '', query)

    return query.lower()

  '''
  Transfer the raw data to HTML
  Basically the goal is to make sure each paragraph is embedded in <p></p>
  '''
  def raw2html(self, raw):
    sentences = raw.split('\n')
    html = ""
    for sent in sentences:
      sent = "<p>" + sent + "</p>\n"
      html += sent

    return html

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

class Application(tornado.web.Application):
  def __init__(self):
    handlers = [
      (r"/", HomeHandler),
      (r"/ent", EntIndexHandler),
      (r"/ent/(\d+)", EntViewHandler),
      (r"/ent/dist/(\d+)", EntDistHandler),
      (r"/ent/(\d+)/(\d+-\d+)", EntRevHandler),
      (r"/doc/(\d+)", DocListHandler),
      (r"/doc/(\d+)/(\d+)", DocViewHandler),
      (r"/doc/(\d+)/(\d+)/(\d+-\d+)", DocRevViewHandler),
    ]

    settings = dict(
      app_title = u"KBA Retrieval Browser",
      template_path = os.path.join(os.path.dirname(__file__), "templates"),
    )

    tornado.web.Application.__init__(self, handlers, **settings)

    # global database connections for all handles
    self._rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.rel_ent_dist_db)

    self._doc_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.oair_doc_test_db)

    self._test_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.test_db)

def main():
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()

