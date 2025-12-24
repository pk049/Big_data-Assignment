# 1. Find max sal, min sal, avg sal, total sal per dept per job in emp.csv ﬁle.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
            .appName("Q1")\
            .getOrCreate()

filepath = '/home/sunbeam/BigData/data/emp.csv'
emp_schema = "empno int, ename string, job string, mgr int," \
            "hire date, sal double, comm double, deptno int"
emps = spark.read\
            .option("header","false")\
            .schema(emp_schema)\
            .csv(filepath)

emps.printSchema()
emps.show()

## Aggregate Functions
result = emps.groupby("deptno", "job")\
            .agg(
                max("sal").alias("maxsal"),
                min("sal").alias("minsal"),
                avg("sal").alias("avgsal"),
                sum("sal").alias("totalsal")
            )

result.printSchema()
result.show()
