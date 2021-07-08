#!/usr/bin/env python

import sys
from csv import reader


parking = sc.textFile(sys.argv[1], 1)
parking = parking.mapPartitions(lambda x: reader(x))

def is_weekend(day):
    weekend_days = [5, 6, 12, 13, 19, 20, 26, 27]
    if day in weekend_days:
        return True
    else:
        return False

#weekday calculations
weekday = parking.map(lambda x: (x[2], (0 if is_weekend(int(x[1][8:])) == True else 1)))
weekday_sum = weekday.reduceByKey(lambda x, y: x + y)
weekday_pair = weekday_sum.map(lambda x: (x[0], float(x[1] / 23)))

#weekend calculations
weekend = parking.map(lambda x: (x[2], (0 if is_weekend(int(x[1][8:])) == False else 1)))
weekend_sum = weekend.reduceByKey(lambda x, y: x + y)
weekend_pair = weekend_sum.map(lambda x: (x[0], float(x[1] / 8)))

weekend_and_weekday = weekend_pair.join(weekday_pair)
output = weekend_and_weekday.map(lambda x: x[0] + '\t' + "%.2f" %x[1][0] + ', ' + "%.2f" %x[1][1])
output.saveAsTextFile('task7.out')