#!/usr/bin/env python

import sys
from csv import reader


parking = sc.textFile(sys.argv[1], 1)
parking = parking.mapPartitions(lambda x: reader(x))

plateid_state = parking.map(lambda x: ((x[14], x[16]), 1))
total_plateid_state = plateid_state.reduceByKey(lambda x, y: x + y)

# get the top 20 vehicles with the greatest number of violations
sorted_list = total_plateid_state.sortBy(lambda x: (-x[1], x[0][0]))
greatest = sc.parallelize(sorted_list.take(20))
greatest = greatest.map(lambda x: (x[0][0], x[0][1], x[1]))

# output
output = greatest.map(lambda x: x[0] + ', ' + x[1] + '\t' + str(x[2]))
output.saveAsTextFile('task6.out')