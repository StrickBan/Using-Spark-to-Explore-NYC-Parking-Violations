#!/usr/bin/env python


import sys
from pyspark.sql.functions import * 
parking = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])

parking.createOrReplaceTempView("parking_df")

result = spark.sql("SELECT registration_state, COUNT(registration_state) AS total_count FROM (SELECT CASE WHEN parking_df.registration_state LIKE 'NY' THEN 'NY' ELSE 'Other' END AS registration_state FROM parking_df) GROUP BY registration_state")

result.select(format_string('%s\t%d', result.registration_state, result.total_count)).write.save("task4-sql.out", format="text")