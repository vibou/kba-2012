#!/usr/bin/python
'''
Apply the optimized entity set from training data to testing data

train-test-greedy-ent.py

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

class TuneQueryOptEnt():
  def __init__(self, opt, t_opt):
    greedy_dict = {'train': RedisDB.train_greedy_db, 'test': RedisDB.test_greedy_db}

    self._train_greedy_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=greedy_dict[t_opt])

    self._test_edmap_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.test_edmap_db)

    self._qrels_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.qrels_db)

    self._rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
      db=RedisDB.ent_db)

    self._ret_list = {}
    self._cutoff_list = {}

    self._opt = opt
    self._t_opt = t_opt

    self._score_mult = 50

  def greedy_tune(self, query_id, opt):
    '''
    Generate the optimized related entity subset using greedy algorithm
    '''
    opt_dict = {'c': 1, 'rc':1}
    if opt not in opt_dict:
      print 'Invalid opt: %s' % opt
      return

    # load the optimized entity list from DB
    greedy_ent_list_dict = {'c':'greedy-ent-list-c', 'rc':'greedy-ent-list-rc'}
    key = greedy_ent_list_dict[opt]
    str = self._train_greedy_db.hget(key, query_id)
    sel_eid = json.loads(str)

    # load the optimized cutoff from DB
    greedy_cutoff_dict = {'c':'greedy-cutoff-c', 'rc':'greedy-cutoff-rc'}
    key = greedy_cutoff_dict[opt]
    cutoff = int(self._train_greedy_db.hget(key, query_id))

    scored_doc_list = self.get_doc_list(query_id, sel_eid)
    self._ret_list[query_id] = scored_doc_list
    self._cutoff_list[query_id] = cutoff

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
            if score <= cutoff * self._score_mult + 100:
              continue
            # clamp score above 1000 to 1000
            if score > 1000:
              score = 1000
            f.write('udel_fang UDInfo_OPT_ENT %s %s %d\n'
                %(did, query, score))
                  #%(did, query, 1000))
            #else:
              #print 'Skipping %s %s %d %d' %(did, query, score, cutoff)
    except IOError as e:
      print 'Failed to save file: %s' % save_file

  def get_doc_list(self, query_id, eid_dict):
    '''
    Get the scored doc list for a given related entity list
    '''
    scored_doc_list = {}
    e_num = len(eid_dict)

    key = 'e2d-map-%s' % query_id
    for eid  in eid_dict:
      # the linear kernel for entity weighting
      e_weight = e_num - int(eid_dict[eid]) + 1
      e2d_str = self._test_edmap_db.hget(key, eid)

      if None == e2d_str:
        continue
      e2d = json.loads(e2d_str)
      for did in e2d:
        score = e2d[did] * self._score_mult
        #score = e2d[did] * self._score_mult * e_weight
        if did not in scored_doc_list:
          scored_doc_list[did] = 100
        scored_doc_list[did] += score

    return scored_doc_list

def main():
  parser = argparse.ArgumentParser(description=__doc__, usage=__doc__)
  parser.add_argument(
    '--debug', default=False, action='store_true', dest='debug',
    help='print out debugging diagnostics')
  args = parser.parse_args()

  # run over all the queries
  query_id_list = range(0, 29, 1)

  #opt = 'c'
  opt = 'rc'

  t_opt = 'train'
  #t_opt = 'test'

  tuner = TuneQueryOptEnt(opt, t_opt)
  for query_id in query_id_list:
    print 'Query %d' % query_id
    tuner.greedy_tune(str(query_id), opt)

  path_base = 'runs/uniform/'
  #path_base = 'runs/linear/'
  path_apd_dict = {'train': 'tune', 'test': 'test'}
  file_dict = {'c': 'c-opt_greedy', 'rc': 'rc-opt_greedy'}
  file_path = '%s%s/%s' %(path_base, path_apd_dict[t_opt], file_dict[opt])

  tuner.save_run_file(file_path)

if __name__ == '__main__':
  try:
      main()
  except KeyboardInterrupt:
      print '\nGoodbye!'

