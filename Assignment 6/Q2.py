# 2. Find deptwise total sal from emp.csv and dept.csv. Print dname and total sal. Hint: use join()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
            .appName("Q2")\
            .getOrCreate()

file_emp = '/home/sunbeam/BigData/data/emp.csv'
file_dept = '/home/sunbeam/BigData/data/dept.csv'

emp_schema = "empno int, ename string, job string, mgr int, hire date, sal double, comm double, deptno int"

emps = spark.read\
            .schema(emp_schema)\
            .csv(file_emp)

dept_schema = "deptno int, dname string, loc string"

depts = spark.read\
            .schema(dept_schema)\
            .csv(file_dept)

emps.show()
depts.show()

result_join = emps.join(depts, on = "deptno"  , how = "inner")\
                    .groupby("dname")\
                    .sum("sal")\
                    .withColumnRenamed("sum(sal)", "totalsal")\
                    .orderBy(asc("totalsal"))
result_join.show()

spark.stop()




























