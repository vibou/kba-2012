#!/usr/bin/python
'''
Get the optimized evaluation performance of results from generating the optimal
subset of related entities from the whole entity candidate set of Wikipedia

query-opt-ent.py <query_id>

'''
## use float division instead of integer division
from __future__ import division

import os
import csv
import gzip
import json
import argparse
import datetime
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

class TuneQueryOptEnt():
  def __init__(self):
    self._edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.edmap_db)

    self._qrels_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.qrels_db)

  def greedy_tune(self, query_id, qrels_key):
    '''
    Generate the optimized related entity subset using greedy algorithm
    '''
    qrels_key_hash = {'testing-c':1, 'testing-rc': 1}
    if qrels_key not in qrels_key_hash:
      print 'Invalid qrels_key: %s' % qrels_key

    # load the qrels
    str = self._qrels_db.hget(key, query_id)
    qrels = json.loads(str)

    # get all the available entity candidates
    key = 'e2d-map-%s' % query_id
    all_eid = self._edmap_db.hkeys(key)

    # TODO select the subset of entities incrementally in a greedy way
    sel_eid = None
    left_eid = all_eid.copy()
    g_max_score = 0.0

    while len(left_eid.keys()) > 0:
      sel_num = len(sel_eid.keys())
      left_num = len(left_eid.keys())
      print '%d selected, %d left' %(sel_num, left_num)

      # the max scores for this round
      max_eid = None
      max_score = 0.0

      # iterate over all the left entities, and try to add one to see
      # whether it can lead to better prformance
      for eid in left_eid:
        cur_sel_eid = sel_eid.copy()
        cur_sel_eid[eid] = 1
        (cutoff, score) = self.max_perf(sel_eid.keys(), qrels)

        if score > max_score:
          max_eid = cur_sel_eid.copy()
          max_score = score

      # if we can keep increasing the max performance in this round, keep
      # going
      if max_score > g_max_score:
        g_max_score = max_score
        sel_eid = max_eid.copy()

        # remove the selected entity in this round
        for eid in left_eid:
          if eid in sel_eid:
            del left_eid[eid]

      # otherwise, we have reached the local optimum, which means convergence.
      # Thus, we stop here
      else:
        break

    (cutoff, score) = self.max_perf(sel_eid.keys(), sel_eid)
    sel_eid.sort(key=lambda x: int(x))
    key = 'ent-list-%s' % query_id
    db_item = self._edmap_db.hmget(key, sel_eid)

    ent_list = []
    for idx, ent in enumerate(db_item):
      eid = eid_keys[idx]
      ent_list.append(ent)

    print 'Selected entity list [%d] :\n%s' % (len(sel_eid.keys()),
        ' '.join(ent_list))
    print '-' * 60
    print 'Cutoff: %d' % cutoff
    print 'Max F: %6.3f' % score

  def max_perf(self, eid_list, qrels):
    '''
    Get the maximum performance given an entity list
    '''
    eid_list.sort(key=lambda x: int(x))
    e2d_list = self._edmap_db.hmget(key, eid_list)

    # generate the document list
    scored_doc_list = {}
    for e2d_str in e2d_list:
      e2d = json.loads(e2d_str)
      for did in e2d:
        score = e2d[did]
        if did not in scored_doc_list:
          scored_doc_list[did] = 0
        scored_doc_list[did] += score

    # applying filtering over the scored document on different cutoffs
    CM = score_confusion_matrix(scored_doc_list, qrels)
    scores = performance_metrics(CM)
    (cutoff, max) = self.max_score(scores, 'F')
    return (cutoff, max)

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

    return (curoff, max)

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
  parser.add_argument('query_id')
  args = parser.parse_args()

  tuner = TuneQueryOptEnt()

  qrels_c_key = 'testing-c'
  tuner.greedy_tune(args.query_id, qrels_c_key)

  #qrels_rc_key = 'testing-rc'
  #tuner.greedy_tune(args.query_id, qrels_rc_key)

if __name__ == '__main__':
  try:
      main()
  except KeyboardInterrupt:
      print '\nGoodbye!'

