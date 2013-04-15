#!/usr/bin/python

"""
Provide a web interface to browse the temporal information of related entities
"""

import os
import re
import json
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

  @property
  def _edmap_db(self):
    return self.application._edmap_db

  @property
  def _qrels_db(self):
    return self.application._qrels_db

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

class TuneHandler(BaseHandler):
  '''
  Tune the performance by adding or removing any related entities. The
  performance will be reported by F1 at diffrent levels of cutoffs.
  '''
  def get(self, query_id):
    hash_key = 'query-%s' % query_id
    keys = self._rel_ent_dist_db.hkeys(hash_key)
    if 0 == len(keys):
      msg = 'no data found'
      self.render("error.html", msg=msg)
      return

    query = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, query_id)

    ## retrieve the list of related entities
    ## here we only retrieve the entities which have occurred in the relevant
    ## documents, i.e. effective entities
    key = 'e2d-map-%s' % query_id
    eid_keys = self._edmap_db.hkeys(key)
    eid_keys.sort(key=lambda x: int(x))
    map_db_item = self._edmap_db.hmget(key, eid_keys)

    key = 'ent-list-%s' % query_id
    db_item = self._edmap_db.hmget(key, eid_keys)

    ent_list = []
    for idx, ent in enumerate(db_item):
      eid = eid_keys[idx]
      map_str = map_db_item[idx]
      map_json = json.loads(map_str)
      item = DictItem()
      item['eid'] = eid
      item['ent'] = ent
      item['doc_num'] = len(map_json.keys())
      ent_list.append(item)

    self.render("tune-view.html", query_id=query_id, query=query, ent_list=ent_list)

class TunePerfHandler(BaseHandler):
  '''
  Tune the performance by changing the related entities (either adding,
  removing). The performance is reported by F1 (or SU) at each cufoff level
  '''
  def get(self, query_id, ent_str):
    # load the qrels
    key = 'testing-c'
    str = self._qrels_db.hget(key, query_id)
    c_qrels = json.loads(str)

    key = 'testing-rc'
    str = self._qrels_db.hget(key, query_id)
    rc_qrels = json.loads(str)

    # generate the document list
    scored_doc_list = {}
    key = 'e2d-map-%s' % query_id
    eid_keys = self._edmap_db.hkeys(key)

    # get the ent list specified by the parameters from passed-in URL
    if ' ' != ent_str:
      ent_list = ent_str.split(' ')
      new_ent_list = list(set(eid_keys) & set(ent_list))
      eid_keys = new_ent_list

    if 0 == len(eid_keys):
      # in case there is no valid entity, just return two points
      line = '0\t0.0\t0.0\n'
      self.write(line)
      line = '100\t0.0\t0.0\n'
      self.write(line)
      return

    eid_keys.sort(key=lambda x: int(x))
    e2d_list = self._edmap_db.hmget(key, eid_keys)
    for e2d_str in e2d_list:
      e2d = json.loads(e2d_str)
      for did in e2d:
        score = e2d[did]
        if did not in scored_doc_list:
          scored_doc_list[did] = 0
        scored_doc_list[did] += score

    # applying filtering over the scored document on different cutoffs
    c_CM = score_confusion_matrix(scored_doc_list, c_qrels)
    rc_CM = score_confusion_matrix(scored_doc_list, rc_qrels)
    c_scores = performance_metrics(c_CM)
    rc_scores = performance_metrics(rc_CM)

    for cutoff in sorted(c_scores.keys()):
      c_f1 = c_scores[cutoff]['F']
      rc_f1 = rc_scores[cutoff]['F']
      line = '%d\t%6.3f\t%6.3f\n' %(cutoff, c_f1, rc_f1)
      self.write(line)

class GreedyPerfHandler(BaseHandler):
  '''
  Get the related entity list selected by the greedy algorithm
  '''
  def get(self, query_id):
    key = 'greedy-ent-list-c'
    #key = 'greedy-ent-list-rc'
    sel_eid_str = self._edmap_db.hget(key, query_id)
    sel_eid = json.loads(sel_eid_str)
    eid_keys = sel_eid.keys()

    if 0 == len(eid_keys):
      line = 'N/A'
      self.write(line)
      return

    eid_keys.sort(key=lambda x: int(x))
    for eid in eid_keys:
      line = '%s\t1\n' % eid
      self.write(line)

def precision(TP, FP):
    '''
    Calculates the precision given the number of true positives (TP) and
    false-positives (FP)
    '''
    if (TP+FP) > 0:
        return float(TP) / (TP + FP)
    else:
        return 0.0

def recall(TP, FN):
    '''
    Calculates the recall given the number of true positives (TP) and
    false-negatives (FN)
    '''
    if (TP+FN) > 0:
        return float(TP) / (TP + FN)
    else:
        return 0.0

def fscore(precision, recall):
    '''
    Calculates the F-score given the precision and recall
    '''
    if precision + recall > 0:
        return float(2 * precision * recall) / (precision + recall)
    else:
        return 0.0

def scaled_utility(TP, FP, FN, MinNU = -0.5):
    '''
    Scaled Utility from http://trec.nist.gov/pubs/trec11/papers/OVER.FILTERING.pdf

    MinNU is an optional tunable parameter
    '''
    if (TP + FN) > 0:
        T11U = float(2 * TP - FP)
        MaxU = float(2 * (TP + FN))
        T11NU = float(T11U) / MaxU
        return (max(T11NU, MinNU) - MinNU) / (1 - MinNU)
    else:
        return 0.0

def performance_metrics (CM, debug=False):
    '''
    Computes the performance metrics (precision, recall, F-score, scaled utility)

    CM: dict containing the confusion matrix calculated from score_confusion_matrix()
    '''
    ## Compute the performance statistics
    Scores = dict()

    for cutoff in CM:
        Scores[cutoff] = dict()
        ## Precision
        Scores[cutoff]['P'] = precision(CM[cutoff]['TP'], CM[cutoff]['FP'])

        ## Recall
        Scores[cutoff]['R'] = recall(CM[cutoff]['TP'], CM[cutoff]['FN'])

        ## F-Score
        Scores[cutoff]['F'] = fscore(Scores[cutoff]['P'], Scores[cutoff]['R'])

        ## Scaled Utility from http://trec.nist.gov/pubs/trec11/papers/OVER.FILTERING.pdf
        Scores[cutoff]['SU'] = scaled_utility(CM[cutoff]['TP'],
            CM[cutoff]['FP'], CM[cutoff]['FN'])
    return Scores

def score_confusion_matrix (scored_doc_list, annotation, debug=False):
    '''
    This function generates the confusion matrix (number of true/false positives
    and true/false negatives.

    path_to_run_file: str, a filesystem link to the run submission
    annotation: dict, containing the annotation data
    unannotated_is_TN: boolean, true to count unannotated as negatives
    include_training: boolean, true to include training documents

    returns a confusion matrix dictionary for each urlname
    '''
    # default: false
    unannotated_is_TN = False

    END_OF_2012 = 1325375999

    ## Create a dictionary containing the confusion matrix (CM)
    cutoffs = range(0, 100, 1)
    CM = dict()

    ## count the total number of assertions per entity
    num_assertions = {'total': 0,
                      'in_TTR': 0,
                      'in_ETR': 0,
                      'in_annotation_set': 0}
    for cutoff in cutoffs:
        CM[cutoff] = dict(TP=0, FP=0, FN=0, TN=0)

    ## Iterate through every row of the run
    for did in scored_doc_list:
        score = scored_doc_list[did]
        timestamp = int(did.split('-')[0])

        ## keep track of total number of assertions per entity
        num_assertions['total'] += 1
        if timestamp <= END_OF_2012:
            num_assertions['in_TTR'] += 1
        else:
            num_assertions['in_ETR'] += 1

        in_annotation_set = did in annotation

        if in_annotation_set:
            num_assertions['in_annotation_set'] += 1

        ## In the annotation set and relevant
        if in_annotation_set and annotation[did]:
            for cutoff in cutoffs:
                if score > cutoff:
                    ## If above the cutoff: true-positive
                    CM[cutoff]['TP'] += 1

        ## In the annotation set and non-relevant
        elif in_annotation_set and not annotation[did]:
            for cutoff in cutoffs:
                if score > cutoff:
                    ## Above the cutoff: false-positive
                    CM[cutoff]['FP'] += 1
                else:
                    ## Below the cutoff: true-negative
                    CM[cutoff]['TN'] += 1
        ## Not in the annotation set so its a negative (if flag is true)
        elif unannotated_is_TN:
            for cutoff in cutoffs:
                if score > cutoff:
                    ## Above the cutoff: false-positive
                    CM[cutoff]['FP'] += 1
                else:
                    ## Below the cutoff: true-negative
                    CM[cutoff]['TN'] += 1

    ## Correct FN for things in the annotation set that are NOT in the run
    ## First, calculate number of true things in the annotation set
    annotation_positives = 0
    for did in annotation:
        annotation_positives += annotation[did]

    for cutoff in CM:
        ## Then subtract the number of TP at each cutoffs
        ## (since FN+TP==True things in annotation set)
        CM[cutoff]['FN'] = annotation_positives - CM[cutoff]['TP']

    if debug:
        print 'showing assertion counts:'
        print json.dumps(num_assertions, indent=4, sort_keys=True)

    return CM

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
      (r"/tune/(\d+)", TuneHandler),
      (r"/tune/(\d+)/([\d\+]+)", TunePerfHandler),
      (r"/tune/(\d+)/greedy", GreedyPerfHandler),
      (r"/static/(.*)", tornado.web.StaticFileHandler, {"path": "/local/data/xliu/www"}),
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

    self._edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.test_edmap_db)

    self._qrels_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.qrels_db)

def main():
  tornado.options.parse_command_line()
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()

