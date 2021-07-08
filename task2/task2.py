#!/usr/bin/env python

import sys
from csv import reader

parking = sc.textFile(sys.argv[1], 1)
parking = parking.mapPartitions(lambda x: reader(x))
parking = parking.map(lambda x: (x[2], 1))
parking = parking.reduceByKey(lambda x, y: x + y)
output = parking.map(lambda x: x[0] + '\t' + str(x[1]))
output.saveAsTextFile("task2.out")