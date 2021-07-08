#!/usr/bin/env python

import sys
from csv import reader

parking = sc.textFile(sys.argv[1], 1)
open = sc.textFile(sys.argv[2], 1)

parking = parking.mapPartitions(lambda x: reader(x))
open = open.mapPartitions(lambda x: reader(x))

parking = parking.map(lambda x: (x[0], (x[14], x[6], x[2], x[1])))
open = open.map(lambda x: (x[0], ('info', 'info', 'info', 'info')))


parking_minus_open=parking.subtractByKey(open)

output = parking_minus_open.map(lambda x: x[0] + '\t' + x[1][0] + ', ' + x[1][1] + ', ' + x[1][2] + ', ' + x[1][3])

output.saveAsTextFile("task1.out")