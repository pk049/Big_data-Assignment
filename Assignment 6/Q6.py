# 6. Salary Diﬀerence from Department Average For each employee, show how much their salary diﬀers from their department's average salary. Also show
# the department's maximum salary. - Output: ename, dname, sal, dept_avg_sal, sal_diﬀ_from_avg, dept_max_sal

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
            .appName("Q6")\
            .getOrCreate()

file_emp = '/home/sunbeam/BigData/data/emp.csv'
file_dept = '/home/sunbeam/BigData/data/dept.csv'

emp_schema = ("empno int, ename string, job string, mgr int, hire date, sal double, comm double, deptno int")
dept_schema = ("deptno int, dname string, loc string")

emps = spark.read\
            .option("header", "false")\
            .schema(emp_schema)\
            .csv(file_emp)

depts = spark.read\
            .option("header", "false")\
            .schema(dept_schema)\
            .csv(file_dept)

emp_dept = emps.join(depts, "deptno")

emp_dept_avg = emp_dept.groupBy("deptno")\
                .agg(avg("sal").alias("dept_avg_sal"),max("sal").alias("dept_max_sal"))

result_inter = emp_dept.join(emp_dept_avg, "deptno")

result = result_inter.withColumn("sal_diff_from_avg", expr("dept_avg_sal - sal"))

result.show()

result2 = result.selectExpr("ename","dname","sal","dept_avg_sal","sal_diff_from_avg", "dept_max_sal")
result2.show()
