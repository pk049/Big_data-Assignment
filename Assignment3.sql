1. Create transactional emp and dept tables in orc format. Load data from staging table.
CREATE TABLE emp(
empno INT,
ename STRING,
job STRING,
mgr INT,
hire DATE,
sal DECIMAL(9,2),
comm DECIMAL(9,2),
deptno INT
)
STORED AS ORC
TBLPROPERTIES('transactional' = 'true');

CREATE TABLE dept(
deptno INT,
dname STRING,
loc STRING
)
STORED AS ORC
TBLPROPERTIES('transactional' = 'true');

INSERT INTO emp
SELECT * FROM emp_staging;
+------------+------------+------------+----------+-------------+----------+-----------+-------------+
| emp.empno  | emp.ename  |  emp.job   | emp.mgr  |  emp.hire   | emp.sal  | emp.comm  | emp.deptno  |
+------------+------------+------------+----------+-------------+----------+-----------+-------------+
| 7369       | SMITH      | CLERK      | 7902     | 1980-12-17  | 800.00   | NULL      | 20          |
| 7499       | ALLEN      | SALESMAN   | 7698     | 1981-02-20  | 1600.00  | 300.00    | 30          |
| 7521       | WARD       | SALESMAN   | 7698     | 1981-02-22  | 1250.00  | 500.00    | 30          |
| 7566       | JONES      | MANAGER    | 7839     | 1981-04-02  | 2975.00  | NULL      | 20          |
| 7654       | MARTIN     | SALESMAN   | 7698     | 1981-09-28  | 1250.00  | 1400.00   | 30          |
| 7698       | BLAKE      | MANAGER    | 7839     | 1981-05-01  | 2850.00  | NULL      | 30          |
| 7782       | CLARK      | MANAGER    | 7839     | 1981-06-09  | 2450.00  | NULL      | 10          |
| 7788       | SCOTT      | ANALYST    | 7566     | 1982-12-09  | 3000.00  | NULL      | 20          |
| 7839       | KING       | PRESIDENT  | NULL     | 1981-11-17  | 5000.00  | NULL      | 10          |
| 7844       | TURNER     | SALESMAN   | 7698     | 1981-09-08  | 1500.00  | 0.00      | 30          |
+------------+------------+------------+----------+-------------+----------+-----------+-------------

INSERT INTO dept
SELECT * FROM dept_staging;
+--------------+-------------+-----------+
| dept.deptno  | dept.dname  | dept.loc  |
+--------------+-------------+-----------+
| 10           | ACCOUNTING  | NEW YORK  |
| 20           | RESEARCH    | DALLAS    |
| 30           | SALES       | CHICAGO   |
| 40           | OPERATIONS  | BOSTON    |
+--------------+-------------+-----------+


2. Count number of employees per dept per job. Print all combinations of subtotals and grand totals.
select deptno,job,count(*) from emp group by deptno, job with cube;
+---------+------------+------+
| deptno  |    job     | _c2  |
+---------+------------+------+
| NULL    | NULL       | 14   |
| 10      | NULL       | 3    |
| 20      | NULL       | 5    |
| 30      | NULL       | 6    |
| NULL    | ANALYST    | 2    |
| 20      | ANALYST    | 2    |
| NULL    | CLERK      | 4    |
| 10      | CLERK      | 1    |
| 20      | CLERK      | 2    |
| 30      | CLERK      | 1    |
| NULL    | MANAGER    | 3    |
| 10      | MANAGER    | 1    |
| 20      | MANAGER    | 1    |
| 30      | MANAGER    | 1    |
| NULL    | PRESIDENT  | 1    |
| 10      | PRESIDENT  | 1    |
| NULL    | SALESMAN   | 4    |
| 30      | SALESMAN   | 4    |
+---------+------------+------+


3. Display only subtotals of deptwise employees and groupwise employees (count).
select deptno, job, count(*), grouping(deptno,job), grouping(job,deptno) from emp
group by job,deptno with cube;


4. Print number of contacts per district.
select addr.dist,count(*) from contacts group by addr.dist;
+------------+------+
| addr.dist  | _c1  |
+------------+------+
| karad      | 1    |
| pune       | 2    |
+------------+------+

5. Find hottest and coolest month from ncdc data.
CREATE TABLE ncdc_month_staging(
month smallint,
temp smallint,
quality tinyint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES('input.regex' = 
'^.{19}([0-9]{2}).{66}([-\+][0-9]{4})([0-9]).*$');

LOAD DATA LOCAL
INPATH '/home/sunbeam/BigData/data/ncdc/*'
INTO TABLE ncdc_month_staging;

CREATE TABLE ncdc_month(
mn smallint,
temp smallint,
quality tinyint
)
STORED AS ORC
TBLPROPERTIES('transactional' = 'true');

INSERT INTO ncdc_month
SELECT * FROM ncdc_month_staging;

CREATE MATERIALIZED VIEW mv_mncdc as
SELECT mn, avg(temp) avg FROM ncdc_month where quality in (0,1,2,4,5,9)
and temp != 9999 group by mn; 

select * from mv_mncdc;

(select * from mv_mncdc order by avg desc limit 1)
union
(select * from mv_mncdc order by avg asc limit 1);

+---------+---------------------+
| _u1.mn  |       _u1.avg       |
+---------+---------------------+
| 2       | -75.16562866684718  |
| 7       | 160.3485770685968   |
+---------+---------------------+

6. Create a table movie_staging and load data from movies_caret.csv.
   - Hint: TBLPROPERTIES ('skip.header.line.count'='1')
   - Treat `genres` as ARRAY.
   - A. count number of movies for given genre = 'Action'
   - B. count number of genres for movie "Toy Story (1995)"
   
CREATE TABLE movie_staging(
movieId INT,
title STRING,
genres STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '^'
STORED AS TEXTFILE
TBLPROPERTIES('skip.header.line.count' = '1');

LOAD DATA LOCAL
INPATH '/home/sunbeam/BigData/data/movies/movies_caret.csv'
INTO TABLE movie_staging;

CREATE TABLE movie(
movieId INT,
title STRING,
genres ARRAY<STRING>
)
STORED AS ORC;

INSERT INTO movie
SELECT movieId, title, SPLIT(genres, '\\|') FROM movie_staging;


select count(*) totalmovies from movie where ARRAY_CONTAINS(genres, 'Action');
+--------------+
| totalmovies  |
+--------------+
| 1545         |
+--------------+
   
select size(genres) totalgenres from movie where title = "Toy Story (1995)";
+--------------+
| totalgenres  |
+--------------+
| 5            |
+--------------+
