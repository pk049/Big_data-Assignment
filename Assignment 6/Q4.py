from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
            .appName("Q4")\
            .getOrCreate()

# filepath = '/home/sunbeam/Downloads/Fire_Department_Calls_for_Service (1)/Fire_Department_Calls_for_Service.csv'
# # fire_schema = "Call Number Integer ,Unit ID String ,Incident Number Integer ,Call Type String ,Call Date String ,Watch Date String ,Received DtTm timestamp ,Entry DtTm timestamp ,Dispatch DtTm timestamp ,Response DtTm timestamp ,On Scene DtTm timestamp ,Transport DtTm timestamp ,Hospital DtTm timestamp ,Call Final Disposition String ,Available DtTm String ,Address String ,City String ,Zipcode of Incident Integer ,Battalion String ,Station Area String ,Box String ,Original Priority String ,Priority String ,Final Priority Integer ,ALS Unit Boolean ,Call Type Group String ,Number of Alarms Integer ,Unit Type String ,Unit sequence in call dispatch Integer ,Fire Prevention District String ,Supervisor District String ,Neighborhooods - Analysis Boundaries String ,RowID String ,case_location String ,data_as_of String ,data_loaded_at String ,Analysis Neighborhoods Integer"
# format = "csv"
# fire = spark.read\
# 		.format(format)\
#     	.option("header", "true")\
#     	.option("inferSchema", "true")\
#     	.load(filepath)

# fire.write.orc("/home/sunbeam/Desktop/ADVANCESQL/tmp/fire_orc")

# fire.printSchema()

# fire.show()

filepath = '/home/sunbeam/Desktop/ADVANCESQL/tmp/fire_orc/'
# fire_schema = "Call Number Integer ,Unit ID String ,Incident Number Integer ,Call Type String ,Call Date String ,Watch Date String ,Received DtTm timestamp ,Entry DtTm timestamp ,Dispatch DtTm timestamp ,Response DtTm timestamp ,On Scene DtTm timestamp ,Transport DtTm timestamp ,Hospital DtTm timestamp ,Call Final Disposition String ,Available DtTm String ,Address String ,City String ,Zipcode of Incident Integer ,Battalion String ,Station Area String ,Box String ,Original Priority String ,Priority String ,Final Priority Integer ,ALS Unit Boolean ,Call Type Group String ,Number of Alarms Integer ,Unit Type String ,Unit sequence in call dispatch Integer ,Fire Prevention District String ,Supervisor District String ,Neighborhooods - Analysis Boundaries String ,RowID String ,case_location String ,data_as_of String ,data_loaded_at String ,Analysis Neighborhoods Integer"
format = "orc"
fireorc = spark.read\
		.format(format)\
    	.option("header", "true")\
		.option("nullvalue", "Null")\
    	.option("inferSchema", "true")\
    	.orc(filepath)

# fireorc.show()

# 2. Execute following queries on ﬁre dataset.

# 1. How many distinct types of calls were made to the ﬁre department?
# print("Total Calls:",fireorc.select("Call Type").distinct().count())
# Total Calls: 33

# 2. What are distinct types of calls made to the ﬁre department?
# Calls = fireorc.select("Call Type").distinct().limit(33)
# Calls.show(truncate=False)
# +--------------------------------------------+
# |Call Type                                   |
# +--------------------------------------------+
# |Elevator / Escalator Rescue                 |
# |Marine Fire                                 |
# |Aircraft Emergency                          |
# |Confined Space / Structure Collapse         |
# |Administrative                              |
# |Alarms                                      |
# |Odor (Strange / Unknown)                    |
# |Lightning Strike (Investigation)            |
# |Citizen Assist / Service Call               |
# |HazMat                                      |
# |Watercraft in Distress                      |
# |Explosion                                   |
# |Oil Spill                                   |
# |Vehicle Fire                                |
# |Extrication / Entrapped (Machinery, Vehicle)|
# |Other                                       |
# |Outside Fire                                |
# |Traffic Collision                           |
# |Assist Police                               |
# |Gas Leak (Natural and LP Gases)             |
# +--------------------------------------------+

# 3. Find out all responses for delayed times greater than 5 mins?


# 4. What were the most common call types?
# call_types = fireorc.groupBy("Call Type").count().orderBy("count", ascending = False)
# call_types.show(truncate = False)
# +----------------------------------+-------+                                    
# |Call Type                         |count  |
# +----------------------------------+-------+
# |Medical Incident                  |4247943|
# |Alarms                            |720968 |
# |Structure Fire                    |714873 |
# |Traffic Collision                 |259541 |
# |Other                             |110855 |
# |Citizen Assist / Service Call     |96222  |
# |Outside Fire                      |85967  |
# |Water Rescue                      |34061  |
# |Gas Leak (Natural and LP Gases)   |30484  |
# |Vehicle Fire                      |28378  |
# |Electrical Hazard                 |21907  |
# |Structure Fire / Smoke in Building|18894  |
# |Elevator / Escalator Rescue       |17952  |
# |Smoke Investigation (Outside)     |14613  |
# |Odor (Strange / Unknown)          |13673  |
# |Fuel Spill                        |7038   |
# |HazMat                            |4399   |
# |Industrial Accidents              |3333   |
# |Explosion                         |3067   |
# |Train / Rail Incident             |1715   |
# +----------------------------------+-------+

# 5. What zip codes accounted for the most common calls?
# zipcode = fireorc.groupBy("Call Type").count().orderBy(desc("count"))
# zipcodes = fireorc.join(zipcode, "Call Type", "inner")
# zipcodes1 = zipcodes.select("Zipcode of Incident", "Call Type", "count").orderBy(desc("count")).limit(1)
# zipcodes1.show(truncate = False)
# +-------------------+----------------+-------+                                  
# |Zipcode of Incident|Call Type       |count  |
# +-------------------+----------------+-------+
# |94103              |Medical Incident|4247943|
# +-------------------+----------------+-------+
# 6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?
result = fireorc.select("City", "`Neighborhooods - Analysis Boundaries`").where("City = 'SF' AND `Zipcode of Incident` IN (94102, 94103)").distinct()
# result.show(truncate=False)
# +----+------------------------------------+                                     
# |City|Neighborhooods - Analysis Boundaries|
# +----+------------------------------------+
# |SF  |Castro/Upper Market                 |
# |SF  |Western Addition                    |
# |SF  |Financial District/South Beach      |
# |SF  |South of Market                     |
# |SF  |Tenderloin                          |
# |SF  |Mission Bay                         |
# |SF  |Nob Hill                            |
# |SF  |Potrero Hill                        |
# |SF  |Hayes Valley                        |
# |SF  |Mission                             |
# +----+------------------------------------+

# 7. What was the sum of all calls, average, min, and max of the call response times?
result = fireorc.groupBy("`Call Type`")\
						.agg(sum("`Call Type`").alias("sum"),
							 avg("`Call Type`").alias("avg"),
                             min("`Call Type`").alias("min"),
                             max("`Call Type`").alias("max"))
# result.show(truncate=False)

# 8. How many distinct years of data are in the CSV ﬁle?
# 9. What week of the year in 2018 had the most ﬁre calls?
# 10. What neighborhoods in San Francisco had the worst response time in 2018?





spark.stop()







