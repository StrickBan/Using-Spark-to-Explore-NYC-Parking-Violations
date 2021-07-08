#!/usr/bin/env python

import sys
from pyspark.sql.functions import * 
parking = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])

parking.createOrReplaceTempView("parking_df")

result = spark.sql("SELECT violation_code, COUNT(violation_code) AS total_count FROM parking_df GROUP BY violation_code")

result.select(format_string('%d\t%d', result.violation_code, result.total_count)).write.save("task2-sql.out", format="text")