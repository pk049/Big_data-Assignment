CREATE TABLE Customers (
CustomerID INT,
Name VARCHAR(100),
Age INT,
LocationID INT
);

CREATE TABLE Products (
ProductID INT,
ProductName VARCHAR(100),
Category VARCHAR(50),
Price DECIMAL(10, 2)
);

CREATE TABLE Sales (
SaleID INT,
CustomerID INT,
ProductID INT,
SaleDate DATE,
Quantity INT,
TotalAmount DECIMAL(10, 2)
);

CREATE TABLE Locations (
LocationID INT,
City VARCHAR(50),
State VARCHAR(50)
);

A. Retrieve the names of all customers who made a purchase.
select c.name from customers c join sales s on c.customerId = s.customerId join products p on p.productId = s.productId;
+----------------+
|     c.name     |
+----------------+
| John Doe       |
| Bob Johnson    |
| Jane Smith     |
| Alice Brown    |
| Charlie Davis  |
+----------------+

B. List the products and their total sales amounts for a given date range.
select p.productname, sum(s.totalamount) from products p join sales s on p.productid = s.productid group by s.saledate, p.productname;
+----------------+----------+
| p.productname  |   _c1    |
+----------------+----------+
| Bookshelf      | 150.00   |
| Laptop         | 1600.00  |
| Shoes          | 100.00   |
| Smartphone     | 400.00   |
| T-shirt        | 60.00    |
+----------------+----------+

C. Find the total sales amount for each product category.
select sum(s.totalamount), p.category from sales s join products p
on s.productid = p.productid group by p.category;
+----------+--------------+
|   _c0    |  p.category  |
+----------+--------------+
| 60.00    | Clothing     |
| 2000.00  | Electronics  |
| 100.00   | Footwear     |
| 150.00   | Furniture    |
+----------+--------------+

D. Identify the customers who made purchases in a speciﬁc city.
select c.name, l.city from customers c join sales s on s.customerid = c.customerid join locations l on c.locationid = l.locationid;
+----------------+------------+
|     c.name     |   l.city   |
+----------------+------------+
| John Doe       | Pune       |
| Jane Smith     | Mumbai     |
| Bob Johnson    | Pune       |
| Alice Brown    | Bangalore  |
| Charlie Davis  | Mumbai     |
+----------------+------------+

E. Calculate the average age of customers who bought products in the 'Electronics' category.
select p.category, avg(c.age) from products p join sales s on p.productid = s.productid join customers c on c.customerid = s.customerid where p.category = 'Electronics' group by p.category;
+--------------+-------+
|  p.category  |  _c1  |
+--------------+-------+
| Electronics  | 32.5  |
+--------------+-------+

F. List the top 3 products based on total sales amount.
select p.productname, sum(s.totalamount) totalsum from products p join sales s on s.productid = p.productid group by p.productname order by totalsum desc limit 3;
+----------------+-----------+
| p.productname  | totalsum  |
+----------------+-----------+
| Laptop         | 1600.00   |
| Smartphone     | 400.00    |
| Bookshelf      | 150.00    |
+----------------+-----------+

G. Find the total sales amount for each month.
select month(saledate), sum(totalamount) from sales group by month(saledate);
+------+----------+
| _c0  |   _c1    |
+------+----------+
| 1    | 2060.00  |
| 2    | 250.00   |
+------+----------+

H. Identify the products with no sales.
select p.productname, sum(s.totalamount) totalsum from products p join sales s on s.productid = p.productid group by p.productname having totalsum = 0;
+----------------+-----------+
| p.productname  | totalsum  |
+----------------+-----------+
+----------------+-----------+

I. Calculate the total sales amount for each state.
select l.state, sum(s.totalamount) totalsum from sales s join customers c on c.customerid = s.customerid join locations l on 
c.locationid = l.locationid group by l.state;
+--------------+-----------+
|   l.state    | totalsum  |
+--------------+-----------+
| Karnataka    | 100.00    |
| Maharashtra  | 2210.00   |
+--------------+-----------+

J. Retrieve the customer names and their highest purchase amount.
select c.name, max(s.totalamount) from sales s join customers c on c.customerid = s.customerid group by c.name;
+----------------+----------+
|     c.name     |   _c1    |
+----------------+----------+
| Alice Brown    | 100.00   |
| Bob Johnson    | 400.00   |
| Charlie Davis  | 150.00   |
| Jane Smith     | 60.00    |
| John Doe       | 1600.00  |
+----------------+----------+


