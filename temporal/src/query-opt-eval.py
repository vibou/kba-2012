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
from collections import defaultdict

import redis
from config import RedisDB

g_qrel_file = 'eval/after.txt'
g_train_ret_dir = 'ret/train/'
g_test_ret_dir = 'ret/test/'

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

def write_performance_metrics (path_to_write_csv, CM, Scores):
    '''
    Writes a CSV file with the performance metrics at each cutoff

    path_to_write_csv: string with CSV file destination
    CM: dict, Confusion matrix generated from score_confusion_matrix()
    Scores: dict containing the score metrics computed using performance_metrics()
    '''
    writer = csv.writer(open(path_to_write_csv, 'wb'), delimiter=',')
    ## Write a header
    writer.writerow(['urlname','cutoff', 'TP', 'FP', 'FN', 'TN', 'P', 'R', 'F', 'SU'])

    ## Write the metrics for each cutoff and urlname to a new line
    for urlname in sorted(CM):
        for cutoff in sorted(CM[urlname], reverse=True):
            writer.writerow([urlname, cutoff,
                             CM[urlname][cutoff]['TP'], CM[urlname][cutoff]['FP'],
                             CM[urlname][cutoff]['FN'], CM[urlname][cutoff]['TN'],
                             Scores[urlname][cutoff]['P'], Scores[urlname][cutoff]['R'],
                             Scores[urlname][cutoff]['F'], Scores[urlname][cutoff]['SU']])

def full_run_metrics(CM, Scores, micro=False):
    '''
    Computes the metrics for the whole run over all the entities

    CM: dict, the confusion matrix for each urlname defined below
    Scores: dict, the scores for each urlname
    macro, bool, false=average over mentions, true=average over all urlnames

    returns (CM_total, Scores_average) the average of the scores and the summed
    confusion matrix
    '''

    flipped_CM = defaultdict(dict)
    for key, val in CM.items():
        for subkey, subval in val.items():
            flipped_CM[subkey][key] = subval

    CM_total = dict()

    for cutoff in flipped_CM:
        CM_total[cutoff] = dict(TP=0, FP=0, FN=0, TN=0)
        for urlname in flipped_CM[cutoff]:
            for key in CM[urlname][cutoff]:
                CM_total[cutoff][key] += CM[urlname][cutoff][key]

    flipped_Scores = defaultdict(dict)
    for key, val in Scores.items():
        for subkey, subval in val.items():
            flipped_Scores[subkey][key] = subval

    Scores_average = dict()
    ## Do macro averaging
    if not micro:
        for cutoff in flipped_Scores:
            Scores_average[cutoff] = dict(P=0.0, R=0.0, F=0.0, SU=0.0)
            ## Sum over urlnames for each cutoff
            for urlname in flipped_Scores[cutoff]:
                for metric in flipped_Scores[cutoff][urlname]:
                    Scores_average[cutoff][metric] += Scores[urlname][cutoff][metric]
        ## Divide by the number of urlnames to get the average metrics
        for cutoff in Scores_average:
            for metric in Scores_average[cutoff]:
                Scores_average[cutoff][metric] = Scores_average[cutoff][metric] / len(Scores)
    ## Do micro averaging
    else:
        tempCM = dict(average=CM_total)
        tempScores = performance_metrics(tempCM)
        Scores_average = tempScores['average']
    return (CM_total,Scores_average)

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

    for urlname in CM:
        Scores[urlname] = dict()
        for cutoff in CM[urlname]:
            Scores[urlname][cutoff] = dict()
            ## Precision
            Scores[urlname][cutoff]['P'] = precision(CM[urlname][cutoff]['TP'],
                                            CM[urlname][cutoff]['FP'])
            ## Recall
            Scores[urlname][cutoff]['R'] = recall(CM[urlname][cutoff]['TP'],
                                            CM[urlname][cutoff]['FN'])
            ## F-Score
            Scores[urlname][cutoff]['F'] = fscore(Scores[urlname][cutoff]['P'],
                                            Scores[urlname][cutoff]['R'])
            ## Scaled Utility from http://trec.nist.gov/pubs/trec11/papers/OVER.FILTERING.pdf
            Scores[urlname][cutoff]['SU'] = scaled_utility(CM[urlname][cutoff]['TP'],
                                                  CM[urlname][cutoff]['FP'],
                                                  CM[urlname][cutoff]['FN'])
    return Scores

def score_confusion_matrix (path_to_run_file, annotation, cutoff_step, unannotated_is_TN, include_training,debug):
    '''
    This function generates the confusion matrix (number of true/false positives
    and true/false negatives.

    path_to_run_file: str, a filesystem link to the run submission
    annotation: dict, containing the annotation data
    cutoff_step: int, increment between cutoffs
    unannotated_is_TN: boolean, true to count unannotated as negatives
    include_training: boolean, true to include training documents

    returns a confusion matrix dictionary for each urlname
    '''

    ## Open the run file
    if path_to_run_file.endswith('.gz'):
        run_file = gzip.open(path_to_run_file, 'r')
    else:
        run_file = open(path_to_run_file, 'r')

    ## Create a dictionary containing the confusion matrix (CM)
    cutoffs = range(0, 999, cutoff_step)
    CM = dict()

    ## count the total number of assertions per entity
    num_assertions = {}

    ## Iterate through every row of the run
    for onerow in run_file:
        ## Skip Comments
        if onerow.startswith('#'):
            continue

        row = onerow.split()
        stream_id = row[2]
        timestamp = int(stream_id.split('-')[0])
        urlname = row[3]
        score = int(row[4])

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

        ## If the entity has been seen yet create a confusion matrix for it
        if not urlname in CM:
            CM[urlname] = dict()
            for cutoff in cutoffs:
                CM[urlname][cutoff] = dict(TP=0, FP=0, FN=0, TN=0)

        if (not include_training) and (timestamp <= END_OF_2012):
            continue

        in_annotation_set = (stream_id, urlname) in annotation

        if in_annotation_set:
            num_assertions[urlname]['in_annotation_set'] += 1


        ## In the annotation set and relevant
        if in_annotation_set and annotation[(stream_id, urlname)]:
            for cutoff in cutoffs:
                if score > cutoff:
                    ## If above the cutoff: true-positive
                    CM[urlname][cutoff]['TP'] += 1

        ## In the annotation set and non-relevant
        elif in_annotation_set and not annotation[(stream_id, urlname)]:
            for cutoff in cutoffs:
                if score > cutoff:
                    ## Above the cutoff: false-positive
                    CM[urlname][cutoff]['FP'] += 1
                else:
                    ## Below the cutoff: true-negative
                    CM[urlname][cutoff]['TN'] += 1
        ## Not in the annotation set so its a negative (if flag is true)
        elif unannotated_is_TN:
            for cutoff in cutoffs:
                if score > cutoff:
                    ## Above the cutoff: false-positive
                    CM[urlname][cutoff]['FP'] += 1
                else:
                    ## Below the cutoff: true-negative
                    CM[urlname][cutoff]['TN'] += 1

    ## Correct FN for things in the annotation set that are NOT in the run
    ## First, calculate number of true things in the annotation set
    annotation_positives = defaultdict(int)
    for key in annotation:
        stream_id = key[0]
        timestamp = int(stream_id.split('-')[0])

        if (not include_training) and (timestamp <= 1325375999):
            continue

        urlname = key[1]
        annotation_positives[urlname] += annotation[(stream_id,urlname)]

    for urlname in CM:
        for cutoff in CM[urlname]:
            ## Then subtract the number of TP at each cutoffs
            ## (since FN+TP==True things in annotation set)
            CM[urlname][cutoff]['FN'] = annotation_positives[urlname] - CM[urlname][cutoff]['TP']

    if debug:
        print 'showing assertion counts:'
        print json.dumps(num_assertions, indent=4, sort_keys=True)

    return CM

def load_annotation (path_to_annotation_file, include_relevant):
    '''
    Loads the annotation file into a dict

    path_to_annotation_file: string filesystem path to the annotation file
    include_relevant: true to include docs marked relevant and central
    '''
    annotation_file = csv.reader(open(path_to_annotation_file, 'r'), delimiter='\t')

    annotation = dict()
    for row in annotation_file:
       ## Skip comments
       if row[0][0] == "#":
           continue

       stream_id = row[2]
       urlname = row[3]
       rating = int(row[5])

       if include_neutral:
           thresh = 0
       elif include_relevant:
           thresh = 1
       else:
           thresh = 2

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
        '--include-training', default=False, action='store_true', dest='include_training',
        help='includes documents from before the ETR period')
    parser.add_argument(
        '--debug', default=False, action='store_true', dest='debug',
        help='print out debugging diagnostics')
    parser.add_argument('query_id')
    args = parser.parse_args()

    global g_qrel_file
    ## Load in the annotation data, including both training and testing data
    annotation = load_annotation(g_qrel_file, args.include_relevant)

    global g_train_ret_dir
    global g_test_ret_dir
    train_dir = os.path.join(g_train_ret_dir, args.query_id)
    test_dir = os.path.join(g_test_ret_dir, args.query_id)

    for ts in os.listdir(train_dir):
      train_ret_file = os.path.join(train_dir, ts)
      test_ret_file = os.path.join(test_dir, ts)

        ## Generate the confusion matrix for a run
        CM = score_confusion_matrix(
            os.path.join(args.run_dir, run_file),
            annotation, args.cutoff_step, args.unan_is_true, args.include_training,
            debug=args.debug)

        ## Generate performance metrics for a run
        Scores = performance_metrics(CM)

        ## Generate the average metrics
        (CM['average'], Scores['average']) = full_run_metrics(CM, Scores, args.micro_is_true)

        ## split into team name and create stats file
        team_name, run_name = run_file_name.split('-')

        ## Store the top F and SU for each run for each team
        teamscores[run_file_name] = defaultdict(dict)
        for urlname in Scores:
            teamscores[run_file_name][urlname]['F'] = max([Scores[urlname][cutoff]['F']
              for cutoff in Scores[urlname]])
            teamscores[run_file_name][urlname]['SU'] = max([Scores[urlname][cutoff]['SU']
              for cutoff in Scores[urlname]])

        ## Print the top F-Score
        print ' Best F-Score: %.3f' % teamscores[run_file_name]['average']['F']

        ## Output the key performance statistics
        output_filepath = os.path.join(args.run_dir, run_file_name
            + str(args.cutoff_step) + '.csv')
        write_performance_metrics(output_filepath, CM, Scores)
        print ' wrote metrics table to %s' % output_filepath

    ## When folder is finished running output a high level summary of the scores to overview.csv
    write_team_summary('overview.csv', teamscores)
