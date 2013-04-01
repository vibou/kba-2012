#!/usr/bin/python
'''
Get the optimal evaluation performance of results from testing data by tuning
the optimal cutoff from the results of training data.

This script is based on the official KBA evaluation script

query-opt-eval.py <query_id>

'''
## use float division instead of integer division
from __future__ import division

END_OF_2012 = 1325375999

import os
import csv
import gzip
import json
import argparse
import datetime
from collections import defaultdict

import redis
from config import RedisDB

g_qrel_file = 'eval/after.txt'
g_train_ret_dir = 'ret/train/'
g_test_ret_dir = 'ret/test/'
g_cutoff_step = 1

g_rel_ent_dist_db = redis.Redis(host=RedisDB.host, port=RedisDB.port,
    db=RedisDB.rel_ent_dist_db)

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
        Scores[cutoff]['SU'] = scaled_utility(CM[cutoff]['TP'], CM[cutoff]['FP']
            CM[cutoff]['FN'])
    return Scores

def score_confusion_matrix (path_to_run_file, annotation, debug):
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

    ## Open the run file
    if path_to_run_file.endswith('.gz'):
        run_file = gzip.open(path_to_run_file, 'r')
    else:
        run_file = open(path_to_run_file, 'r')

    ## Create a dictionary containing the confusion matrix (CM)
    cutoffs = range(100, 200, 1)
    CM = dict()

    ## count the total number of assertions per entity
    num_assertions = {}

    query = ''

    ## Iterate through every row of the run
    for onerow in run_file:
        ## Skip Comments
        if onerow.startswith('#'):
            continue

        row = onerow.split()
        urlname = row[0]
        stream_id = row[1]
        timestamp = int(stream_id.split('-')[0])
        score = int(row[2])
        query = urlname

        if urlname not in num_assertions:
            num_assertions[urlname] = {'total': 0,
                                       'in_TTR': 0,
                                       'in_ETR': 0,
                                       'in_annotation_set': 0}

        ## keep track of total number of assertions per entity
        num_assertions[urlname]['total'] += 1
        if timestamp <= END_OF_2012:
            num_assertions[urlname]['in_TTR'] += 1
        else:
            num_assertions[urlname]['in_ETR'] += 1

        for cutoff in cutoffs:
          CM[cutoff] = dict(TP=0, FP=0, FN=0, TN=0)

        in_annotation_set = (stream_id, urlname) in annotation

        if in_annotation_set:
            num_assertions[urlname]['in_annotation_set'] += 1

        ## In the annotation set and relevant
        if in_annotation_set and annotation[(stream_id, urlname)]:
            for cutoff in cutoffs:
                if score > cutoff:
                    ## If above the cutoff: true-positive
                    CM[cutoff]['TP'] += 1

        ## In the annotation set and non-relevant
        elif in_annotation_set and not annotation[(stream_id, urlname)]:
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
    annotation_positives = defaultdict(int)
    for key in annotation:
        stream_id = key[0]
        urlname = key[1]
        annotation_positives[urlname] += annotation[(stream_id,urlname)]

    for cutoff in CM:
        ## Then subtract the number of TP at each cutoffs
        ## (since FN+TP==True things in annotation set)
        CM[cutoff]['FN'] = annotation_positives[query]
          - CM[cutoff]['TP']

    if debug:
        print 'showing assertion counts:'
        print json.dumps(num_assertions, indent=4, sort_keys=True)

    return CM

def load_annotation (path_to_annotation_file, include_relevant, is_training):
    '''
    Loads the annotation file into a dict

    path_to_annotation_file: string filesystem path to the annotation file
    include_relevant: true to include docs marked relevant and central
    '''
    annotation_file = csv.reader(open(path_to_annotation_file, 'r'), delimiter='\t')

    thresh = 0
    if include_relevant:
        thresh = 1
    else:
        thresh = 2

    annotation = dict()
    for row in annotation_file:
        ## Skip comments
        if row[0][0] == "#":
            continue

        stream_id = row[2]
        timestamp = int(stream_id.split('-')[0])
        urlname = row[3]
        rating = int(row[5])

        # skip if it is training data
        if (not is_training) and (timestamp <= END_OF_2012):
            continue

        ## Add the stream_id and urlname to a hashed dictionary
        ## 0 means that its not central 1 means that it is central

        if (stream_id, urlname) in annotation:
            ## 2 means the annotators gave it a yes for centrality
            if rating < thresh:
                annotation[(stream_id, urlname)] = False
        else:
            annotation[(stream_id, urlname)] = rating >= thresh

    return annotation

if __name__ == '__main__':
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

    global g_qrel_file
    ## Load in the annotation data, including both training and testing data
    train_annotation = load_annotation(g_qrel_file, args.include_relevant,
        True)
    test_annotation = load_annotation(g_qrel_file, args.include_relevant,
        False)

    global g_train_ret_dir
    global g_test_ret_dir
    train_dir = os.path.join(g_train_ret_dir, args.query_id)
    test_dir = os.path.join(g_test_ret_dir, args.query_id)

    global g_rel_ent_dist_db
    query = g_rel_ent_dist_db.hget(RedisDB.query_ent_hash, args.query_id)

    opt_F_hash = []
    ts_list = os.listdir(train_dir)
    ts_list.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))
    for ts in ts_list:
        train_run_file = os.path.join(train_dir, ts)
        test_run_file = os.path.join(test_dir, ts)

        ## Generate the confusion matrix for both train and test runs
        train_CM = score_confusion_matrix(train_run_file, train_annotation,
             debug=args.debug)
        test_CM = score_confusion_matrix(test_run_file, test_annotation,
             debug=args.debug)

        ## Generate performance metrics for both train and test runs
        train_scores = performance_metrics(train_CM)
        test_scores = performance_metrics(test_CM)

        ## get the optimal cutoff for the training data
        opt_F = 0
        opt_cutoff = 0
        cut_off_list = train_scores.keys()
        cut_off_list.sort()
        for cutoff in cut_off_list:
          F = train_scores[cutoff]['F']
          if F > opt_F:
            opt_F = F
            opt_cutoff = cutoff

        opt_F = test_scores[opt_cutoff]
        opt_F_hash[ts] = opt_F
        print '%s %f' % (ts, opt_F)

    # write back to DB
    print 'Writing back to DB'
    hash_key = 'query-opt-F'
    for ts in ts_list:
        opt_F = opt_F_hash[ts]
        g_rel_ent_dist_db.hset(hash_key, ts, str(opt_F))
