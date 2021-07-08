#!/usr/bin/env python


import sys
from pyspark.sql.functions import * 
open = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])

open.createOrReplaceTempView("open_df")

result = spark.sql("SELECT license_type, SUM(amount_due) AS total_amount, AVG(amount_due) AS average FROM open_df GROUP BY license_type")

result.select(format_string('%s\t%.2f, %.2f', result.license_type, result.total_amount, result.average)).write.save("task3-sql.out", format="text")