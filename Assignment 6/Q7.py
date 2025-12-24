# 7. For each department, identify:
# The ﬁrst employee hired (earliest hire date)
# The last employee hired (most recent hire date)
# The employee hired immediately after each employee (by hire date)
# Output: deptno, dname, ename, hire, ﬁrst_hire_ﬂag, last_hire_ﬂag, next_hire_employee

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

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

joined = emps.join(depts, "deptno")

joined.show()

wnd = Window.partitionBy("deptno").orderBy("hire")
cols = joined.withColumn("firsthire",first_value("hire").over(wnd))\
                .withColumn("lasthire",last_value("hire").over(wnd.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)))\
                .withColumn("nexthire",lead("hire").over(wnd))
result = cols.select("deptno", "dname", "ename", "hire", "firsthire", "lasthire", "nexthire")

result.show()

# +------+----------+------+----------+----------+----------+----------+
# |deptno|     dname| ename|      hire| firsthire|  lasthire|  nexthire|
# +------+----------+------+----------+----------+----------+----------+
# |    10|ACCOUNTING| CLARK|1981-06-09|1981-06-09|1982-01-23|1981-11-17|
# |    10|ACCOUNTING|  KING|1981-11-17|1981-06-09|1982-01-23|1982-01-23|
# |    10|ACCOUNTING|MILLER|1982-01-23|1981-06-09|1982-01-23|      NULL|
# |    20|  RESEARCH| SMITH|1980-12-17|1980-12-17|1983-01-12|1981-04-02|
# |    20|  RESEARCH| JONES|1981-04-02|1980-12-17|1983-01-12|1981-12-03|
# |    20|  RESEARCH|  FORD|1981-12-03|1980-12-17|1983-01-12|1982-12-09|
# |    20|  RESEARCH| SCOTT|1982-12-09|1980-12-17|1983-01-12|1983-01-12|
# |    20|  RESEARCH| ADAMS|1983-01-12|1980-12-17|1983-01-12|      NULL|
# |    30|     SALES| ALLEN|1981-02-20|1981-02-20|1981-12-03|1981-02-22|
# |    30|     SALES|  WARD|1981-02-22|1981-02-20|1981-12-03|1981-05-01|
# |    30|     SALES| BLAKE|1981-05-01|1981-02-20|1981-12-03|1981-09-08|
# |    30|     SALES|TURNER|1981-09-08|1981-02-20|1981-12-03|1981-09-28|
# |    30|     SALES|MARTIN|1981-09-28|1981-02-20|1981-12-03|1981-12-03|
# |    30|     SALES| JAMES|1981-12-03|1981-02-20|1981-12-03|      NULL|
# +------+----------+------+----------+----------+----------+----------+






