'''
Apply linear regression to the input list and plot the results

Usage lr.py <input>
'''

# http://glowingpython.blogspot.com/2012/03/linear-regression-with-numpy.html

from numpy import arange,array,ones,asarray #,random,linalg
from pylab import plot,show
from scipy import stats
import argparse

# esempio rizzardi per approssimare il trend
#xi = arange(0,9)
# linearly generated sequence
#y = [19, 20, 20.5, 21.5, 22, 23, 23, 25.5, 24]

xi = []
y = []
parser = argparse.ArgumentParser(usage=__doc__)
parser.add_argument('input')
args = parser.parse_args()
input_file = args.input

try:
  with open(input_file) as f:
    for line in f.readlines():
      values = line.strip().split('\t')
      xi.append(float(values[0]))
      y.append(float(values[1]))

  slope, intercept, r_value, p_value, std_err = stats.linregress(xi,y)

  print 'r value', r_value
  print  'p_value', p_value
  print 'standard deviation', std_err

  line = slope * asarray(xi) + intercept
  plot(xi, line, 'r-', asarray(xi), y, 'o')
  show()
except IOError as e:
  print 'Oh dear.'
