#!/usr/bin/python
'''
To use propagation network to estimate the relevance score of documents

propagation-network.py <query_id>

'''
## use float division instead of integer division
from __future__ import division

import os
import csv
import gzip
import json
import argparse
import datetime
import operator
from collections import defaultdict

import redis
from config import RedisDB

def getMedian(numericValues):
    '''
    Returns the median from a list

    numericValues: list of numbers
    '''
    theValues = sorted(numericValues)
    if len(theValues) % 2 == 1:
        return theValues[(len(theValues)+1)//2-1]
    else:
        lower = theValues[len(theValues)//2-1]
        upper = theValues[len(theValues)//2]
        return (float(lower + upper)) / 2

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

class PropagationNetwork():
  def __init__(self):
    self._train_edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.train_edmap_db)

    self._test_edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.test_edmap_db)

    self._qrels_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.qrels_db)

    self._rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.rel_ent_dist_db)

    self._ret_list = {}
    self._cutoff_list = {}

  def estimate_score(self, query_id):
    '''
    Generate the optimized related entity subset using greedy algorithm
    '''
    train_qrels_c_key = 'training-c'
    #test_qrels_c_key = 'testing-c'

    #train_qrels_rc_key = 'training-rc'
    #test_qrels_rc_key = 'testing-rc'

    # load the qrels of training data
    str = self._qrels_db.hget(train_qrels_c_key, query_id)
    train_qrels = json.loads(str)

    # get all the available entity candidates
    key = 'e2d-map-%s' % query_id
    all_eid = self._train_edmap_db.hkeys(key)
    all_eid.sort(key=lambda x: int(x))

    key = 'ent-list-%s' % query_id
    db_item = self._train_edmap_db.hmget(key, all_eid)

    ent_hash = {}
    for idx, ent in enumerate(db_item):
      eid = all_eid[idx]
      ent_hash[eid] = ent

    # get the optimal cutoff for the training data
    (cutoff, f1_score) = self.max_perf(query_id, all_eid, train_qrels)

    # get a list of relevant documents with their scores
    scored_train_doc_list = self.get_doc_list(query_id, ent_hash.keys())
    rel_doc_list = {}
    for did in scored_train_doc_list:
      if did not in train_qrels:
        continue
      if not train_qrels[did]:
        continue

      # now what we have are relevant documents
      score = scored_train_doc_list[did]
      rel_doc_list[did] = score

    # use the relevant document list to get d2e map
    key = 'd2e-map-%s' % query_id
    d2e_list = self._train_edmap_db.hmget(key, rel_doc_list.keys())

    # generate the scored entity list
    scored_ent_list = {}
    for d2e_str in d2e_list:
      d2e = json.loads(d2e_str)
      for eid in d2e:
        score = d2e[eid]
        if eid not in scored_ent_list:
          scored_ent_list[eid] = 0
        scored_ent_list[eid] += score

    # use the scored entity list to score documents in the testing data
    key = 'e2d-map-%s' % query_id
    eid_list = scored_ent_list.keys()
    eid_list.sort(key=lambda x: int(x))
    e2d_list = self._test_edmap_db.hmget(key, eid_list)

    # generate the scored document list
    scored_test_doc_list = {}
    #for e2d_str in e2d_list:
    for idx, e2d_str in enumerate(e2d_list):
      if None == e2d_str:
        continue
      e2d = json.loads(e2d_str)
      eid = eid_list[idx]
      ent_score = scored_ent_list[eid]
      for did in e2d:
        score = e2d[did] * ent_score

        if did not in scored_test_doc_list:
          scored_test_doc_list[did] = 0
        scored_test_doc_list[did] += score

    # apply normalization to the score to [0, 1]
    # get the maximal score: http://stackoverflow.com/a/268285
    max_score = max(scored_test_doc_list.iteritems(), key=operator.itemgetter(1))[1]
    max_score = max_score / 1000
    for did in scored_test_doc_list:
      score = scored_test_doc_list[did]
      score = round(score / max_score)
      scored_test_doc_list[did] = score

    self._ret_list[query_id] = scored_test_doc_list
    #self._cutoff_list[query_id] = cutoff
    self._cutoff_list[query_id] = 0

    return f1_score

  def save_run_file(self, save_file):
    '''
    Save the filtered document list into run file
    '''
    print 'Saving %s' %save_file
    try:
      with open(save_file, 'w') as f:
        query_id_list = self._ret_list.keys()
        query_id_list.sort(key=lambda x: int(x))

        for query_id in query_id_list:
          query = self._rel_ent_dist_db.hget(RedisDB.query_ent_hash, query_id)
          scored_doc_list = self._ret_list[query_id]
          cutoff = self._cutoff_list[query_id]
          for did in scored_doc_list:
            score = scored_doc_list[did]
            if score > cutoff:
              f.write('udel_fang UDInfo_OPT_ENT %s %s %d\n'
                  #%(did, query, 1000))
                  %(did, query, score))
            #else:
              #print 'Skipping %s %s %d %d' %(did, query, score, cutoff)
    except IOError as e:
      print 'Failed to save file: %s' % save_file

  def max_perf(self, query_id, eid_list, qrels):
    '''
    Get the maximum performance given an entity list
    '''
    scored_doc_list = self.get_doc_list(query_id, eid_list)

    # applying filtering over the scored document on different cutoffs
    CM = score_confusion_matrix(scored_doc_list, qrels)
    scores = performance_metrics(CM)
    (cutoff, max) = self.max_score(scores, 'F')
    return (cutoff, max)

  def get_doc_list(self, query_id, eid_list):
    '''
    Get the scored doc list for a given related entity list from training data
    '''
    key = 'e2d-map-%s' % query_id
    eid_list.sort(key=lambda x: int(x))
    e2d_list = self._train_edmap_db.hmget(key, eid_list)

    # generate the document list
    scored_doc_list = {}
    for e2d_str in e2d_list:
      e2d = json.loads(e2d_str)
      for did in e2d:
        score = e2d[did]
        if did not in scored_doc_list:
          scored_doc_list[did] = 0
        scored_doc_list[did] += score

    return scored_doc_list

  def max_score(self, scores, measure):
    '''
    Get the maximum scores from a score list with different cutoffs
    Valid measures include 'P', 'R', 'F' and 'SU'
    '''
    measure_list = {'P':1, 'R':1, 'F':1, 'SU':1}
    if measure not in measure_list:
      print 'Invalid measure: %s' % measure
      return 0.0

    max = 0.0
    max_cf = 0
    for cutoff in sorted(scores.keys()):
      score = scores[cutoff][measure]
      if score > max:
        max = score
        max_cf = cutoff

    return (max_cf, max)

def main():
  parser = argparse.ArgumentParser(description=__doc__, usage=__doc__)
  parser.add_argument(
    '--use-micro-averaging', default=False, action='store_true', dest='micro_is_true',
      help='compute scores for each mention and then average regardless of entity.'\
        'Default is macro averaging')
  parser.add_argument(
    '--include-relevant', default=False, action='store_true', dest='include_relevant',
    help='in addition to documents rated central, also include those rated relevant')
  parser.add_argument(
    '--debug', default=False, action='store_true', dest='debug',
    help='print out debugging diagnostics')
  args = parser.parse_args()

  pn = PropagationNetwork()

  # run over all the queries
  query_id_list = range(0, 29, 1)
  score_list = []

  for query_id in query_id_list:
    score = pn.estimate_score(str(query_id))
    print 'Query %d - %.3f' %(query_id, score)
    score_list.append(score)
    #if len(score_list) > 5:
      #break

  if len(score_list):
    avg = reduce(lambda x, y: x+y, score_list) / len(score_list)
    print 'Average: %6.3f' % avg

  pn.save_run_file('runs/test/prop-net')

if __name__ == '__main__':
  try:
      main()
  except KeyboardInterrupt:
      print '\nGoodbye!'

