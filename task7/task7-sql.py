#!/usr/bin/env python

import sys
from pyspark.sql.functions import * 
parking = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])

parking.createOrReplaceTempView("parking_df")

result = spark.sql("SELECT violation_code, SUM(weekend)/8 AS weekend_average, SUM(weekday)/23 AS weekday_average FROM (( SELECT violation_code, 0 AS weekend, 1 AS weekday FROM parking_df WHERE DAY(parking_df.issue_date) NOT IN (5,6,12,13,19,20,26,27)) UNION ALL (SELECT violation_code, 1 AS weekend, 0 AS weekday FROM parking_df WHERE DAY(parking_df.issue_date) IN (5,6,12,13,19,20,26,27))) GROUP BY violation_code")

result.select(format_string('%d\t%.2f, %.2f', result.violation_code, result.weekend_average, result.weekday_average)).write.save("task7-sql.out", format="text")