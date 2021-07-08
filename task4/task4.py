#!/usr/bin/env python

import sys
from csv import reader


parking = sc.textFile(sys.argv[1], 1)
parking = parking.mapPartitions(lambda x: reader(x))

def state(state):
    if state == 'NY':
        return ('NY', 1)
    else:
        return ('Other', 1)

violations_state = parking.map(lambda x: state(x[16]))
total_violations_state = violations_state.reduceByKey(lambda x, y: x + y)

#output
output = total_violations_state.map(lambda x: x[0] + '\t' + str(x[1]))
output.saveAsTextFile('task4.out')