# 5. For each department, rank employees by their salary in descending order. Include employee name, department name, salary, and their rank within the
# department. Handle ties so employees with the same salary get the same rank, with the next rank(s) skipped.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder\
            .appName("Q5")\
            .getOrCreate()

file_emp = '/home/sunbeam/BigData/data/emp.csv'
file_dept = '/home/sunbeam/BigData/data/dept.csv'

emp_schema = StructType([
    StructField("empno", IntegerType(), False),
    StructField("ename", StringType(), True),
    StructField("job", StringType(), True),
    StructField("mgr", IntegerType(), True),
    StructField("hire", DateType(), True),
    StructField("sal", DoubleType(), True),
    StructField("comm", DoubleType(), True),
    StructField("deptno", IntegerType(), True)
])

emps = spark.read\
        .option("header", "false")\
        .schema(emp_schema)\
        .csv(file_emp)

dept_schema = StructType([
    StructField("deptno", IntegerType(), False),
    StructField("dname", StringType(), True),
    StructField("loc", StringType(), True)
])

depts = spark.read\
        .option("header", "false")\
        .schema(dept_schema)\
        .csv(file_dept)

result_join = emps.join(depts, "deptno", "inner")


wnd = Window.partitionBy("deptno").orderBy(desc("sal"))
result = result_join.withColumn("rnk", row_number().over(wnd))



result.select("ename", "dname", "sal", "deptno", "rnk").show()
