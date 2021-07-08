#!/usr/bin/env python



import sys
from pyspark.sql.functions import * 
parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
open = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])

parking.createOrReplaceTempView("parking_df")
open.createOrReplaceTempView("open_df")

result = spark.sql("SELECT parking_df.summons_number, parking_df.plate_id, parking_df.violation_precinct, parking_df.violation_code, parking_df.issue_date FROM parking_df LEFT JOIN open_df ON parking_df.summons_number = open_df.summons_number WHERE open_df.summons_number is NULL") 

result.select(format_string('%d\t%s, %d, %d, %s', result.summons_number, result.plate_id, result.violation_precinct, result.violation_code, date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out", format="text")