#!/usr/bin/python

"""
Provide a web interface to browse the retrieval
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

define("port", default=7777, help="run on the given port", type=int)

corpus_dir = './corpus/cleansed'

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
  def _exact_match_db(self):
    return self.application._exact_match_db
  @property
  def _eval_db(self):
    return self.application._eval_db
  @property
  def _wiki_ent_list_db(self):
    return self.application._wiki_ent_list_db

  @property
  def _test_exact_match_db(self):
    return self.application._test_exact_match_db

  @property
  def _wiki_match_db(self):
    return self.application._wiki_match_db

  @property
  def _new_wiki_match_db(self):
    return self.application._new_wiki_match_db

  @property
  def _missed_docs_db(self):
    return self.application._missed_docs_db

  @property
  def _rel_ent_dist_db(self):
    return self.application._rel_ent_dist_db

  @property
  def _filtered_db(self):
    return self.application._filtered_db

class HomeHandler(BaseHandler):
  def get(self):
    url = '/wiki'
    self.redirect(url)

class TrainIndexHandler(BaseHandler):
  def get(self):
    num = self._exact_match_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item_list = self._exact_match_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score']
      the_ret_item = self._exact_match_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['score'] = the_ret_item[4]
      ret_items.append(ret_item)

    self.render("train-ret-index.html", title='KBA Training Results', ret_items=ret_items)

class TestIndexHandler(BaseHandler):
  def get(self):
    num = self._test_exact_match_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item_list = self._test_exact_match_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score']
      the_ret_item = self._test_exact_match_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['score'] = the_ret_item[4]
      ret_items.append(ret_item)

    self.render("test-ret-index.html", title='KBA Testing Results', ret_items=ret_items)

class FilteredIndexHandler(BaseHandler):
  def get(self):
    num = self._filtered_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item_list = self._filtered_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score']
      the_ret_item = self._filtered_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['score'] = the_ret_item[4]
      ret_items.append(ret_item)

    self.render("filter-ret-index.html", title='KBA Filtering Results', ret_items=ret_items)

class TrainRetHandler(BaseHandler):
  def get(self, ret_id):
    ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score', 'stream_data']
    the_ret_item = self._exact_match_db.hmget(ret_id, ret_item_keys)

    if not the_ret_item[5]:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item = DictItem()
    ret_item['id'] = the_ret_item[0]
    ret_item['query'] = the_ret_item[1]
    ret_item['file'] = the_ret_item[2]
    ret_item['stream_id'] = the_ret_item[3]
    ret_item['score'] = the_ret_item[4]
    ret_item['stream_data'] = raw2html(the_ret_item[5])

    list = the_ret_item[3].split('-')
    epoch = list[0]
    ret_item['time'] = datetime.datetime.utcfromtimestamp(float(epoch)).ctime()

    self.render("ret-item.html", title='ret_item', ret_item=ret_item)

class TestRetHandler(BaseHandler):
  def get(self, ret_id):
    ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score', 'stream_data']
    the_ret_item = self._test_exact_match_db.hmget(ret_id, ret_item_keys)

    if not the_ret_item[5]:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item = DictItem()
    ret_item['id'] = the_ret_item[0]
    ret_item['query'] = the_ret_item[1]
    ret_item['file'] = the_ret_item[2]
    ret_item['stream_id'] = the_ret_item[3]
    ret_item['score'] = the_ret_item[4]
    ret_item['stream_data'] = raw2html(the_ret_item[5])

    list = the_ret_item[3].split('-')
    epoch = list[0]
    ret_item['time'] = datetime.datetime.utcfromtimestamp(float(epoch)).ctime()

    self.render("ret-item.html", title='ret_item', ret_item=ret_item)

class FilteredRetHandler(BaseHandler):
  def get(self, ret_id):
    ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score', 'stream_data']
    the_ret_item = self._filtered_db.hmget(ret_id, ret_item_keys)

    if not the_ret_item[5]:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item = DictItem()
    ret_item['id'] = the_ret_item[0]
    ret_item['query'] = the_ret_item[1]
    ret_item['file'] = the_ret_item[2]
    ret_item['stream_id'] = the_ret_item[3]
    ret_item['score'] = the_ret_item[4]
    ret_item['stream_data'] = raw2html(the_ret_item[5])

    list = the_ret_item[3].split('-')
    epoch = list[0]
    ret_item['time'] = datetime.datetime.utcfromtimestamp(float(epoch)).ctime()

    self.render("ret-item.html", title='ret_item', ret_item=ret_item)

class WikiIndexHandler(BaseHandler):
  def get(self):
    num = self._wiki_match_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item_list = self._wiki_match_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score', 'rel']
      the_ret_item = self._wiki_match_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]

      file_list = the_ret_item[2].split('.')
      ret_item['file'] = file_list[0]

      ret_item['stream_id'] = the_ret_item[3]
      ret_item['score'] = the_ret_item[4]
      ret_item['rel'] = the_ret_item[5]
      ret_items.append(ret_item)

    self.render("wiki-ret-index.html", title='KBA Testing Results', ret_items=ret_items)

class WikiRetHandler(BaseHandler):
  def get(self, ret_id):
    ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score', 'rel', 'stream_data']
    the_ret_item = self._wiki_match_db.hmget(ret_id, ret_item_keys)

    if not the_ret_item[5]:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item = DictItem()
    ret_item['id'] = the_ret_item[0]
    ret_item['query'] = the_ret_item[1]
    ret_item['file'] = the_ret_item[2]
    ret_item['stream_id'] = the_ret_item[3]
    ret_item['score'] = the_ret_item[4]
    ret_item['rel'] = the_ret_item[5]
    ret_item['stream_data'] = raw2html(the_ret_item[6])

    list = the_ret_item[3].split('-')
    epoch = list[0]
    ret_item['time'] = datetime.datetime.utcfromtimestamp(float(epoch)).ctime()

    self.render("wiki-ret-item.html", title='ret_item', ret_item=ret_item)

class NewWikiIndexHandler(BaseHandler):
  def get(self):
    num = self._new_wiki_match_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item_list = self._new_wiki_match_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score', 'rel']
      the_ret_item = self._new_wiki_match_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['score'] = the_ret_item[4]
      ret_item['rel'] = the_ret_item[5]
      ret_items.append(ret_item)

    self.render("new-wiki-ret-index.html", title='KBA Testing Results', ret_items=ret_items)

class NewWikiRetHandler(BaseHandler):
  def get(self, ret_id):
    url = '/wiki/ret/%s' % ret_id
    self.redirect(url, permanent=True)

    '''
    ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score', 'rel', 'stream_data']
    the_ret_item = self._new_wiki_match_db.hmget(ret_id, ret_item_keys)

    if not the_ret_item[5]:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item = DictItem()
    ret_item['id'] = the_ret_item[0]
    ret_item['query'] = the_ret_item[1]
    ret_item['file'] = the_ret_item[2]
    ret_item['stream_id'] = the_ret_item[3]
    ret_item['score'] = the_ret_item[4]
    ret_item['rel'] = the_ret_item[5]
    ret_item['stream_data'] = self.raw2html(the_ret_item[6])

    list = the_ret_item[3].split('-')
    epoch = list[0]
    ret_item['time'] = datetime.datetime.utcfromtimestamp(float(epoch)).ctime()

    self.render("wiki-ret-item.html", title='ret_item', ret_item=ret_item)
    '''

class EvalHandler(BaseHandler):
  def get(self):
    num = self._eval_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item_list = self._eval_db.lrange(RedisDB.ret_item_list, 0, num)
    ## remove duplicates in the list
    ## thanks to:
    ## http://docs.python.org/faq/programming.html#how-do-you-remove-duplicates-from-a-list
    ret_item_list = list(set(ret_item_list))
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score', 'judge1', 'judge2']
      the_ret_item = self._eval_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['file'] = the_ret_item[2]
      ret_item['stream_id'] = the_ret_item[3]
      ret_item['score'] = the_ret_item[4]
      ret_item['judge1'] = the_ret_item[5]
      ret_item['judge2'] = the_ret_item[6]
      ret_items.append(ret_item)

    self.render("eval-index.html", title='KBA Qrels', ret_items=ret_items)

class EvalItemHandler(BaseHandler):
  def get(self, ret_id):
    ret_item_keys = ['id', 'query', 'file', 'stream_id', 'score',
        'stream_data', 'judge1', 'judge2']
    the_ret_item = self._eval_db.hmget(ret_id, ret_item_keys)

    if not the_ret_item[5]:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item = DictItem()
    ret_item['id'] = the_ret_item[0]
    ret_item['query'] = the_ret_item[1]
    ret_item['file'] = the_ret_item[2]
    ret_item['stream_id'] = the_ret_item[3]
    ret_item['score'] = the_ret_item[4]
    ret_item['stream_data'] = raw2html(the_ret_item[5])
    ret_item['judge1'] = the_ret_item[6]
    ret_item['judge2'] = the_ret_item[7]

    self.render("eval-item.html", title='ret_item', ret_item=ret_item)

class WikiEntListHandler(BaseHandler):
  def get(self):
    num = self._wiki_ent_list_db.llen(RedisDB.wiki_ent_list)
    if 0 == num:
      msg = 'no wiki_ent found'
      self.render("error.html", msg=msg)
      return

    ent_item_list = self._wiki_ent_list_db.lrange(RedisDB.wiki_ent_list, 0, num)
    ## remove duplicates in the list
    ## thanks to:
    ## http://docs.python.org/faq/programming.html#how-do-you-remove-duplicates-from-a-list
    #ent_item_list = list(set(ent_item_list))
    ent_items = []
    for ent_id in ent_item_list:
      keys = ['id', 'query', 'ent', 'url']
      db_item = self._wiki_ent_list_db.hmget(ent_id, keys)

      keys = self._rel_ent_dist_db.hkeys(ent_id)
      item = DictItem()
      item['id'] = db_item[0]
      item['query'] = db_item[1]
      item['ent'] = db_item[2]
      item['url'] = db_item[3]
      item['num'] = len(keys)
      ent_items.append(item)

    self.render("wiki-ent-list.html", title='KBA Wiki Ent List', ent_items=ent_items)

class RelEntViewHandler(BaseHandler):
  def get(self, ent_id):
    keys = self._rel_ent_dist_db.hkeys(ent_id)
    if 0 == len(keys):
      msg = 'no data found'
      self.render("error.html", msg=msg)
      return

    ## retrieve the basic information of the relevant entity
    db_keys = ['id', 'query', 'ent', 'url']
    db_item = self._wiki_ent_list_db.hmget(ent_id, db_keys)

    item = DictItem()
    item['id'] = db_item[0]
    item['query'] = db_item[1]
    item['ent'] = db_item[2]
    item['url'] = db_item[3]
    item['num'] = len(keys)

    ## retrieve the URL list of the relevant document
    list_name = '%s-list' %(ent_id)
    num = self._rel_ent_dist_db.llen(list_name)
    ret_item_list = self._rel_ent_dist_db.lrange(list_name, 0, num)
    ret_items = []
    for store_item in ret_item_list:
      list = store_item.split(':')
      stream_id = list[0]
      ret_url = list[1]
      rel = list[2]
      list = stream_id.split('-')
      epoch = list[0]
      time = datetime.datetime.utcfromtimestamp(float(epoch))
      date = '%d-%.2d-%.2d' %(time.year, time.month, time.day)

      ret_item = DictItem()
      ret_item['date'] = date
      ret_item['stream_id'] = stream_id
      ret_item['url'] = ret_url
      ret_item['rel'] = rel
      ret_items.append(ret_item)
    ## sort it by date
    ## thanks to http://stackoverflow.com/q/2589479
    ret_items.sort(key=lambda x: datetime.datetime.strptime(x['date'], '%Y-%m-%d'))

    self.render("rel-ent-view.html", ent_id=ent_id, ent_item=item,
        ret_items=ret_items)

class RelEntDistHandler(BaseHandler):
  def get(self, ent_id):
    keys = self._rel_ent_dist_db.hkeys(ent_id)
    if 0 == len(keys):
      msg = 'no data found'
      self.render("error.html", msg=msg)
      return
    ## sort the keys by date: http://stackoverflow.com/q/2589479
    keys.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
    db_item = self._rel_ent_dist_db.hmget(ent_id, keys)

    line = 'date\tclose\n'
    self.write(line)

    cumu = 0
    for idx, val in enumerate(db_item):
      cumu += int(db_item[idx])
      line = '%s\t%s\n' %(keys[idx], cumu)
      self.write(line)

class MissedIndexHandler(BaseHandler):
  def get(self):
    num = self._missed_docs_db.llen(RedisDB.ret_item_list)
    if 0 == num:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item_list = self._missed_docs_db.lrange(RedisDB.ret_item_list, 0, num)
    ret_items = []
    for ret_id in ret_item_list:
      ret_item_keys = ['id', 'query', 'stream_id', 'rating']
      the_ret_item = self._missed_docs_db.hmget(ret_id, ret_item_keys)

      ret_item = DictItem()
      ret_item['id'] = the_ret_item[0]
      ret_item['query'] = the_ret_item[1]
      ret_item['stream_id'] = the_ret_item[2]
      ret_item['rating'] = the_ret_item[3]
      ret_items.append(ret_item)

    self.render("missed-index.html", title='KBA Missed Results', ret_items=ret_items)

class MissedRetHandler(BaseHandler):
  def get(self, ret_id):
    ret_item_keys = ['id', 'query', 'file', 'stream_id', 'rating', 'stream_data']
    the_ret_item = self._missed_docs_db.hmget(ret_id, ret_item_keys)

    if not the_ret_item[5]:
      msg = 'no ret_item found'
      self.render("error.html", msg=msg)
      return

    ret_item = DictItem()
    ret_item['id'] = the_ret_item[0]
    ret_item['query'] = the_ret_item[1]
    ret_item['file'] = the_ret_item[2]
    ret_item['stream_id'] = the_ret_item[3]
    ret_item['rating'] = the_ret_item[4]
    ret_item['stream_data'] = raw2html(the_ret_item[5])

    list = the_ret_item[3].split('-')
    epoch = list[0]
    ret_item['time'] = datetime.datetime.utcfromtimestamp(float(epoch)).ctime()

    self.render("missed-item.html", title='ret_item', ret_item=ret_item)

class Application(tornado.web.Application):
  def __init__(self):
    handlers = [
      (r"/", HomeHandler),
      (r"/train", TrainIndexHandler),
      (r"/test", TestIndexHandler),
      (r"/filter", FilteredIndexHandler),
      (r"/wiki", WikiIndexHandler),
      (r"/new-wiki", NewWikiIndexHandler),
      (r"/eval", EvalHandler),
      (r"/missed", MissedIndexHandler),
      (r"/train/ret/(\d+)", TrainRetHandler),
      (r"/test/ret/(\d+)", TestRetHandler),
      (r"/filter/ret/(\d+)", FilteredRetHandler),
      (r"/wiki/ret/(\d+)", WikiRetHandler),
      (r"/new-wiki/ret/(\d+)", NewWikiRetHandler),
      (r"/eval/(\d+)", EvalItemHandler),
      (r"/missed/ret/(\d+)", MissedRetHandler),
      (r"/wiki-ent-list", WikiEntListHandler),
      (r"/rel-ent/(\d+)", RelEntViewHandler),
      (r"/rel-ent/dist/(\d+)", RelEntDistHandler),
    ]

    settings = dict(
      app_title = u"KBA Retrieval Browser",
      template_path = os.path.join(os.path.dirname(__file__), "templates"),
    )

    tornado.web.Application.__init__(self, handlers, **settings)

    # global database connections for all handles
    self._exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        #db=RedisDB.train_exact_match_db)
        db=RedisDB.fuzzy_match_db)

    self._eval_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.eval_db)
    self._wiki_ent_list_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.wiki_ent_list_db)

    self._test_exact_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        #db=RedisDB.test_exact_match_db)
        #db=RedisDB.fuzzy_match_db)
        db=RedisDB.oair_test_db)

    self._wiki_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.wiki_match_db)

    self._new_wiki_match_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.new_wiki_match_db)

    self._missed_docs_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.missed_docs_db)

    self._rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        db=RedisDB.rel_ent_dist_db)

    self._filtered_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
        #db=RedisDB.filtered_train_db)
        db=RedisDB.filtered_test_db)

def main():
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()

