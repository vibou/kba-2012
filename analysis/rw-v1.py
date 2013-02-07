'''
Apply linear regression to the query entity list, and estimate the weight for
each related entity based on the distance from the linear predictor function

Usage rw-v1.py <query_ent_list> <related_ent_list> <ent_weight>

'''

# http://glowingpython.blogspot.com/2012/03/linear-regression-with-numpy.html

import re
import os
import sys
import traceback
import math

from numpy import arange,array,ones,asarray #,random,linalg
from pylab import plot,show
from scipy import stats
import argparse

class DictItem(dict):
  '''
  A dict that allows for object-like property access syntax.
  '''
  def __getattr__(self, name):
    try:
      return self[name]
    except KeyError:
      raise AttributeError(name)

def load_query_ent(query_ent_list):
  x = []
  y = []

  try:
    with open(query_ent_list) as f:
      for line in f.readlines():
        values = line.strip().split('\t')
        x.append(float(values[0]))
        y.append(float(values[1]))

    # apply linear regression
    slope, intercept, r_value, p_value, std_err = stats.linregress(x,y)

    print 'r value', r_value
    print  'p_value', p_value
    print 'standard deviation', std_err
    return slope, intercept
  except IOError as e:
    print '-' * 60
    traceback.print_exec(file = sys.stdout)
    print '-' * 60
    exit(-1)

def load_related_ent(related_ent_list):
  ent_list = []

  try:
    with open(input_file) as f:
      for line in f.readlines():
        values = line.strip().split('\t')

        point = DictItem()
        point.x = float(values[0])
        point.y = float(values[1])
        point.id = int(values[2])
        point.name = values[3]
        point.query = values[4]
        ent_list.append(point)

    return ent_list
  except IOError as e:
    print '-' * 60
    traceback.print_exec(file = sys.stdout)
    print '-' * 60
    exit(-1)

def apply_reweighting(slope, intercept, x_list, y_list, ent_weight_file):
  if not len(x_list) == len(y_list):
    print 'x_list and y_list must have the same size!'
    return

  for index, x in enumerate(x_list):
    y = y_list[index]
    expected = slope * x + intercept
    delta = math.fabs(expected - y)

def main():
  parser = argparse.ArgumentParser(usage = __doc__)
  parser.add_argument('query_ent_list')
  parser.add_argument('related_ent_list')
  parser.add_argument('ent_weight')
  args = parser.parse_args()

  slope, intercept, load_query_ent(args.query_ent_list)
  x, y = load_related_ent(args.related_ent_list)
  apply_reweighting(slope, intercept, x, y, args.ent_weight)

if '__main__' == __name__:
  main()

