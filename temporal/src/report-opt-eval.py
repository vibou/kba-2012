#!/usr/bin/python
'''
Get the upper bound of our sytem  on the testing data for each of the query, and
report the average of the optimal values

report-opt-eval.py

'''
## use float division instead of integer division
from __future__ import division

END_OF_2012 = 1325375999

QREL_FILE = 'eval/after.txt'
TRAIN_RET_DIR = 'ret/train/'
TEST_RET_DIR = 'ret/test/'
g_cutoff_step = 1

import os
import csv
import gzip
import json
import argparse
import datetime
from collections import defaultdict

import redis
from config import RedisDB

REL_ENT_DIST_DB = redis.Redis(host=RedisDB.host, port=RedisDB.port,
    db=RedisDB.rel_ent_dist_db)

def main():
    # first, get a list of all available queries
    num = REL_ENT_DIST_DB.llen(RedisDB.query_ent_list)
    if 0 == num:
      print 'no query found'
      return

    query_ent_list = REL_ENT_DIST_DB.lrange(RedisDB.query_ent_list, 0, num)
    db_item = REL_ENT_DIST_DB.hmget(RedisDB.query_ent_hash,
        query_ent_list)

    best_c_f1_list = []
    best_cr_f1_list = []

    for idx, query in enumerate(db_item):
      query_id = query_ent_list[idx]

      # retrieve from  DB
      eval_hash_key = 'query-opt-C-F-%s' % query_id
      eval_keys = REL_ENT_DIST_DB.hkeys(eval_hash_key)

      cr_eval_hash_key = 'query-opt-CR-F-%s' % query_id
      cr_eval_keys = REL_ENT_DIST_DB.hkeys(cr_eval_hash_key)

      eval_keys.sort(key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))
      eval_db_item = REL_ENT_DIST_DB.hmget(eval_hash_key, eval_keys)
      cr_eval_db_item = REL_ENT_DIST_DB.hmget(cr_eval_hash_key, eval_keys)

      best_c_f1 = 0.0
      for c_f1 in eval_db_item:
        if float(c_f1) > best_c_f1:
          best_c_f1 = float(c_f1)

      best_cr_f1 = 0.0
      for cr_f1 in cr_eval_db_item:
        if float(cr_f1) > best_cr_f1:
          best_cr_f1 = float(cr_f1)

      print '%30s\t%6.3f\t%6.3f' %(query, best_c_f1, best_cr_f1)
      best_c_f1_list.append(best_c_f1)
      best_cr_f1_list.append(best_cr_f1)

    # get the average
    # thanks to http://stackoverflow.com/a/9039992/219617
    avg_c_f1 = reduce(lambda x, y: x+y, best_c_f1_list) / float(len(best_c_f1_list))
    avg_cr_f1 = reduce(lambda x, y: x+y, best_cr_f1_list) / float(len(best_cr_f1_list))
    print '%30s\t%6.3f\t%6.3f' %('avg', avg_c_f1, avg_cr_f1)

if __name__ == '__main__':
  try:
      main()
  except KeyboardInterrupt:
      print '\nGoodbye!'

