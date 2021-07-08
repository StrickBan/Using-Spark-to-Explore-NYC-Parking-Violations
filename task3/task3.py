#!/usr/bin/env python

import sys
from csv import reader

lines = sc.textFile(sys.argv[1], 1)
parking = lines.mapPartitions(lambda x: reader(x))

# get total value
sum = parking.map(lambda x: (x[2], float(x[12])))
total_sum = sum.reduceByKey(lambda x, y: x + y)

# get total count
count  = parking.map(lambda x: (x[2], 1))
total_count = count.reduceByKey(lambda x, y: x + y)

#calculate average
count_and_sum = total_sum.join(total_count)
average = count_and_sum.map(lambda x: (x[0], x[1][0], float((x[1][0] / x[1][1]))))

#output
output = average.map(lambda x: x[0] + '\t' + "%.2f" %x[1] + ', ' + "%.2f" %x[2])
output.saveAsTextFile("task3.out")