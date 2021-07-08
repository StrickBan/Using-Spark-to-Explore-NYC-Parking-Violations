#!/usr/bin/env python

import sys
from pyspark.sql.functions import * 
parking = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])

parking.createOrReplaceTempView("parking_df")

result = spark.sql("SELECT plate_id, registration_state, COUNT(plate_id) AS total_count FROM parking_df GROUP BY plate_id, registration_state ORDER BY total_count DESC LIMIT 1")

result.select(format_string('%s, %s\t%d', result.plate_id, result.registration_state, result.total_count)).write.save("task5-sql.out", format="text")