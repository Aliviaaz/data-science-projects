1. Download create_db.hql
2. Create EMR cluster
3. Upload create_db.hql to cluster. This part is done locally.
```scp -r -i "key.pem" create_db.hql hadoop@ec2....compute-1.amazonaws.com:~```
Replace ... with actual ec2 address
4. SSH into EMR cluster
```ssh -i 3_key.pem hadoop@ec2-....compute-1.amazonaws.com```
Again, copy the ec2 address from EMR page where it says Primary DNS
5. Run ```hive -f create_db.hql ```. This part and the remaining parts are using the EMR and not done locally.
6. Generate 3 data inputs by running these in hadoop 
    ```cat > /home/hadoop/customers.csv << 'EOF' 

    1001,2001,2024-01-15,John,Smith,CA,90210,USA 

    1002,2002,2024-01-16,Mary,Johnson,TX,75001,USA 

    1003,2003,2024-01-16,David,Williams,NY,10001,USA 

    1004,2004,2024-01-17,Sarah,Brown,FL,33101,USA 

    1005,2005,2024-01-17,Michael,Jones,IL,60601,USA 

    1006,2006,2024-01-18,Jennifer,Garcia,CA,90211,USA 

    1007,2007,2024-01-18,Robert,Miller,TX,75002,USA 

    1008,2008,2024-01-19,Lisa,Davis,NY,10002,USA 

    1009,2009,2024-01-19,William,Rodriguez,FL,33102,USA 

    1010,2010,2024-01-20,Elizabeth,Martinez,IL,60602,USA 

    1011,2011,2024-01-20,James,Hernandez,CA,90212,USA 

    1012,2012,2024-01-21,Patricia,Lopez,TX,75003,USA 

    EOF ```

    ```cat > /home/hadoop/orders.csv << 'EOF' 

    2001,1001,1250.50 

    2002,1002,89.99 

    2003,1003,450.00 

    2004,1004,299.95 

    2005,1005,1899.99 

    2006,1006,34.50 

    2007,1007,567.25 

    2008,1008,899.99 

    2009,1009,78.45 

    2010,1010,345.67 

    2011,1011,1299.99 

    2012,1012,56.78 

    EOF ```

    ```cat > /home/hadoop/products.csv << 'EOF' 

    3001,Apple,iPhone 14 Pro 

    3002,Samsung,Galaxy S23 Ultra 

    3003,Sony,WH-1000XM5 Headphones 

    3004,Dell,XPS 15 Laptop 

    3005,Nike,Air Max 270 

    3006,Adidas,Ultraboost 22 

    3007,Amazon,Echo Dot 5th Gen 

    3008,Google,Pixel 7 Pro 

    3009,Microsoft,Surface Pro 9 

    3010,Bose,QuietComfort 45 

    3011,HP,Envy 13 Laptop 

    3012,Lenovo,ThinkPad X1 Carbon 

    EOF ```

7. Start Hive by running ```hive```
8. Run the following commands. The expected outputs are also shown. 

hive> ```LOAD DATA LOCAL INPATH '/home/hadoop/customers.csv' INTO TABLE customers; ```
hive> ```LOAD DATA LOCAL INPATH '/home/hadoop/orders.csv' INTO TABLE orders; ```
hive> ```LOAD DATA LOCAL INPATH '/home/hadoop/products.csv' INTO TABLE products; ```

hive> ```SELECT COUNT(*) FROM customers; ```
    12 

hive> ```SELECT COUNT(*) FROM orders; ```
    12 

hive> SELECT COUNT(*) FROM products; 
    12 

hive> ```SELECT * FROM customers LIMIT 5; ```
    1001	2001	2024-01-15	John	Smith	CA	90210	USA 
    1002	2002	2024-01-16	Mary	Johnson	TX	75001	USA 
    1003	2003	2024-01-16	David	Williams	NY	10001	USA 
    1004	2004	2024-01-17	Sarah	Brown	FL	33101	USA 
    1005	2005	2024-01-17	Michael	Jones	IL	60601	USA 

hive> ```SELECT * FROM orders LIMIT 5; ```
    2001	1001	1250.50 
    2002	1002	89.99 
    2003	1003	450.00 
    2004	1004	299.95 
    2005	1005	1899.99 

hive> ```SELECT * FROM products LIMIT 5; ```
    3001	Apple	iPhone 14 Pro 
    3002	Samsung	Galaxy S23 Ultra 
    3003	Sony	WH-1000XM5 Headphones 
    3004	Dell	XPS 15 Laptop 
    3005	Nike	Air Max 270 

hive> ```DESCRIBE orders; ```
    order_id            	int                 	Order ID             
    customer_id         	int                 	Customer ID          
    total               	decimal(10,2)       	Order total amount   

hive> ```SELECT customer_id, fname, lname FROM customers LIMIT 5; ```
    1001	John	Smith 
    1002	Mary	Johnson 
    1003	David	Williams 
    1004	Sarah	Brown 
    1005	Michael	Jones 

hive> ```SELECT customer_id, fname, lname FROM customers ORDER BY customer_id DESC LIMIT 5; ```
    1012	Patricia	Lopez 
    1011	James	Hernandez 
    1010	Elizabeth	Martinez 
    1009	William	Rodriguez 
    1008	Lisa	Davis 

hive> ```SELECT * FROM customers WHERE state in ('CA', 'OR', 'WA', 'NV', 'AZ'); ```
    1001	2001	2024-01-15	John	Smith	CA	90210	USA 
    1006	2006	2024-01-18	Jennifer	Garcia	CA	90211	USA 
    1011	2011	2024-01-20	James	Hernandez	CA	90212	USA 

hive> ```SELECT * FROM customers WHERE fname LIKE 'J%'; ```
    1001	2001	2024-01-15	John	Smith	CA	90210	USA 
    1006	2006	2024-01-18	Jennifer	Garcia	CA	90211	USA 
    1011	2011	2024-01-20	James	Hernandez	CA	90212	USA 

hive> ```SELECT o.total, c.fname, c.lname from customers c JOIN orders o ON c.customer_id=o.customer_id; ```
    1250.50	John	Smith 
    89.99	Mary	Johnson 
    450.00	David	Williams 
    299.95	Sarah	Brown 
    1899.99	Michael	Jones 
    34.50	Jennifer	Garcia 
    567.25	Robert	Miller 
    899.99	Lisa	Davis 
    78.45	William	Rodriguez 
    345.67	Elizabeth	Martinez 
    1299.99	James	Hernandez 
    56.78	Patricia	Lopez 

hive> ```SELECT order_id, order_date, lname FROM (SELECT * FROM customers WHERE fname LIKE "J%")jnametable WHERE state in ('CA', 'TX') ORDER BY zipcode, lname; ```
    2001	2024-01-15	Smith 
    2006	2024-01-18	Garcia 
    2011	2024-01-20	Hernandez 

hive> ```SELECT c.customer_id, lname, total FROM customers c JOIN orders o ON(c.customer_id=o.customer_id); ```
    1001	Smith	1250.50 
    1002	Johnson	89.99 
    1003	Williams	450.00 
    1004	Brown	299.95 
    1005	Jones	1899.99 
    1006	Garcia	34.50 
    1007	Miller	567.25 
    1008	Davis	899.99 
    1009	Rodriguez	78.45 
    1010	Martinez	345.67 
    1011	Hernandez	1299.99 
    1012	Lopez	56.78 

hive> SELECT * FROM orders CROSS JOIN products; 

2001	1001	1250.50	3001	Apple	iPhone 14 Pro 

2012	1012	56.78	3001	Apple	iPhone 14 Pro 

2011	1011	1299.99	3001	Apple	iPhone 14 Pro 

2010	1010	345.67	3001	Apple	iPhone 14 Pro 

2009	1009	78.45	3001	Apple	iPhone 14 Pro 

2008	1008	899.99	3001	Apple	iPhone 14 Pro 

2007	1007	567.25	3001	Apple	iPhone 14 Pro 

2006	1006	34.50	3001	Apple	iPhone 14 Pro 

2005	1005	1899.99	3001	Apple	iPhone 14 Pro 

2004	1004	299.95	3001	Apple	iPhone 14 Pro 

2003	1003	450.00	3001	Apple	iPhone 14 Pro 

2002	1002	89.99	3001	Apple	iPhone 14 Pro 

2001	1001	1250.50	3002	Samsung	Galaxy S23 Ultra 

2012	1012	56.78	3002	Samsung	Galaxy S23 Ultra 

2011	1011	1299.99	3002	Samsung	Galaxy S23 Ultra 

2010	1010	345.67	3002	Samsung	Galaxy S23 Ultra 

2009	1009	78.45	3002	Samsung	Galaxy S23 Ultra 

2008	1008	899.99	3002	Samsung	Galaxy S23 Ultra 

2007	1007	567.25	3002	Samsung	Galaxy S23 Ultra 

2006	1006	34.50	3002	Samsung	Galaxy S23 Ultra 

2005	1005	1899.99	3002	Samsung	Galaxy S23 Ultra 

2004	1004	299.95	3002	Samsung	Galaxy S23 Ultra 

2003	1003	450.00	3002	Samsung	Galaxy S23 Ultra 

2002	1002	89.99	3002	Samsung	Galaxy S23 Ultra 

2001	1001	1250.50	3003	Sony	WH-1000XM5 Headphones 

2012	1012	56.78	3003	Sony	WH-1000XM5 Headphones 

2011	1011	1299.99	3003	Sony	WH-1000XM5 Headphones 

2010	1010	345.67	3003	Sony	WH-1000XM5 Headphones 

2009	1009	78.45	3003	Sony	WH-1000XM5 Headphones 

2008	1008	899.99	3003	Sony	WH-1000XM5 Headphones 

2007	1007	567.25	3003	Sony	WH-1000XM5 Headphones 

2006	1006	34.50	3003	Sony	WH-1000XM5 Headphones 

2005	1005	1899.99	3003	Sony	WH-1000XM5 Headphones 

2004	1004	299.95	3003	Sony	WH-1000XM5 Headphones 

2003	1003	450.00	3003	Sony	WH-1000XM5 Headphones 

2002	1002	89.99	3003	Sony	WH-1000XM5 Headphones 

2001	1001	1250.50	3004	Dell	XPS 15 Laptop 

2012	1012	56.78	3004	Dell	XPS 15 Laptop 

2011	1011	1299.99	3004	Dell	XPS 15 Laptop 

2010	1010	345.67	3004	Dell	XPS 15 Laptop 

2009	1009	78.45	3004	Dell	XPS 15 Laptop 

2008	1008	899.99	3004	Dell	XPS 15 Laptop 

2007	1007	567.25	3004	Dell	XPS 15 Laptop 

2006	1006	34.50	3004	Dell	XPS 15 Laptop 

2005	1005	1899.99	3004	Dell	XPS 15 Laptop 

2004	1004	299.95	3004	Dell	XPS 15 Laptop 

2003	1003	450.00	3004	Dell	XPS 15 Laptop 

2002	1002	89.99	3004	Dell	XPS 15 Laptop 

2001	1001	1250.50	3005	Nike	Air Max 270 

2012	1012	56.78	3005	Nike	Air Max 270 

2011	1011	1299.99	3005	Nike	Air Max 270 

2010	1010	345.67	3005	Nike	Air Max 270 

2009	1009	78.45	3005	Nike	Air Max 270 

2008	1008	899.99	3005	Nike	Air Max 270 

2007	1007	567.25	3005	Nike	Air Max 270 

2006	1006	34.50	3005	Nike	Air Max 270 

2005	1005	1899.99	3005	Nike	Air Max 270 

2004	1004	299.95	3005	Nike	Air Max 270 

2003	1003	450.00	3005	Nike	Air Max 270 

2002	1002	89.99	3005	Nike	Air Max 270 

2001	1001	1250.50	3006	Adidas	Ultraboost 22 

2012	1012	56.78	3006	Adidas	Ultraboost 22 

2011	1011	1299.99	3006	Adidas	Ultraboost 22 

2010	1010	345.67	3006	Adidas	Ultraboost 22 

2009	1009	78.45	3006	Adidas	Ultraboost 22 

2008	1008	899.99	3006	Adidas	Ultraboost 22 

2007	1007	567.25	3006	Adidas	Ultraboost 22 

2006	1006	34.50	3006	Adidas	Ultraboost 22 

2005	1005	1899.99	3006	Adidas	Ultraboost 22 

2004	1004	299.95	3006	Adidas	Ultraboost 22 

2003	1003	450.00	3006	Adidas	Ultraboost 22 

2002	1002	89.99	3006	Adidas	Ultraboost 22 

2001	1001	1250.50	3007	Amazon	Echo Dot 5th Gen 

2012	1012	56.78	3007	Amazon	Echo Dot 5th Gen 

2011	1011	1299.99	3007	Amazon	Echo Dot 5th Gen 

2010	1010	345.67	3007	Amazon	Echo Dot 5th Gen 

2009	1009	78.45	3007	Amazon	Echo Dot 5th Gen 

2008	1008	899.99	3007	Amazon	Echo Dot 5th Gen 

2007	1007	567.25	3007	Amazon	Echo Dot 5th Gen 

2006	1006	34.50	3007	Amazon	Echo Dot 5th Gen 

2005	1005	1899.99	3007	Amazon	Echo Dot 5th Gen 

2004	1004	299.95	3007	Amazon	Echo Dot 5th Gen 

2003	1003	450.00	3007	Amazon	Echo Dot 5th Gen 

2002	1002	89.99	3007	Amazon	Echo Dot 5th Gen 

2001	1001	1250.50	3008	Google	Pixel 7 Pro 

2012	1012	56.78	3008	Google	Pixel 7 Pro 

2011	1011	1299.99	3008	Google	Pixel 7 Pro 

2010	1010	345.67	3008	Google	Pixel 7 Pro 

2009	1009	78.45	3008	Google	Pixel 7 Pro 

2008	1008	899.99	3008	Google	Pixel 7 Pro 

2007	1007	567.25	3008	Google	Pixel 7 Pro 

2006	1006	34.50	3008	Google	Pixel 7 Pro 

2005	1005	1899.99	3008	Google	Pixel 7 Pro 

2004	1004	299.95	3008	Google	Pixel 7 Pro 

2003	1003	450.00	3008	Google	Pixel 7 Pro 

2002	1002	89.99	3008	Google	Pixel 7 Pro 

2001	1001	1250.50	3009	Microsoft	Surface Pro 9 

2012	1012	56.78	3009	Microsoft	Surface Pro 9 

2011	1011	1299.99	3009	Microsoft	Surface Pro 9 

2010	1010	345.67	3009	Microsoft	Surface Pro 9 

2009	1009	78.45	3009	Microsoft	Surface Pro 9 

2008	1008	899.99	3009	Microsoft	Surface Pro 9 

2007	1007	567.25	3009	Microsoft	Surface Pro 9 

2006	1006	34.50	3009	Microsoft	Surface Pro 9 

2005	1005	1899.99	3009	Microsoft	Surface Pro 9 

2004	1004	299.95	3009	Microsoft	Surface Pro 9 

2003	1003	450.00	3009	Microsoft	Surface Pro 9 

2002	1002	89.99	3009	Microsoft	Surface Pro 9 

2001	1001	1250.50	3010	Bose	QuietComfort 45 

2012	1012	56.78	3010	Bose	QuietComfort 45 

2011	1011	1299.99	3010	Bose	QuietComfort 45 

2010	1010	345.67	3010	Bose	QuietComfort 45 

2009	1009	78.45	3010	Bose	QuietComfort 45 

2008	1008	899.99	3010	Bose	QuietComfort 45 

2007	1007	567.25	3010	Bose	QuietComfort 45 

2006	1006	34.50	3010	Bose	QuietComfort 45 

2005	1005	1899.99	3010	Bose	QuietComfort 45 

2004	1004	299.95	3010	Bose	QuietComfort 45 

2003	1003	450.00	3010	Bose	QuietComfort 45 

2002	1002	89.99	3010	Bose	QuietComfort 45 

2001	1001	1250.50	3011	HP	Envy 13 Laptop 

2012	1012	56.78	3011	HP	Envy 13 Laptop 

2011	1011	1299.99	3011	HP	Envy 13 Laptop 

2010	1010	345.67	3011	HP	Envy 13 Laptop 

2009	1009	78.45	3011	HP	Envy 13 Laptop 

2008	1008	899.99	3011	HP	Envy 13 Laptop 

2007	1007	567.25	3011	HP	Envy 13 Laptop 

2006	1006	34.50	3011	HP	Envy 13 Laptop 

2005	1005	1899.99	3011	HP	Envy 13 Laptop 

2004	1004	299.95	3011	HP	Envy 13 Laptop 

2003	1003	450.00	3011	HP	Envy 13 Laptop 

2002	1002	89.99	3011	HP	Envy 13 Laptop 

2001	1001	1250.50	3012	Lenovo	ThinkPad X1 Carbon 

2012	1012	56.78	3012	Lenovo	ThinkPad X1 Carbon 

2011	1011	1299.99	3012	Lenovo	ThinkPad X1 Carbon 

2010	1010	345.67	3012	Lenovo	ThinkPad X1 Carbon 

2009	1009	78.45	3012	Lenovo	ThinkPad X1 Carbon 

2008	1008	899.99	3012	Lenovo	ThinkPad X1 Carbon 

2007	1007	567.25	3012	Lenovo	ThinkPad X1 Carbon 

2006	1006	34.50	3012	Lenovo	ThinkPad X1 Carbon 

2005	1005	1899.99	3012	Lenovo	ThinkPad X1 Carbon 

2004	1004	299.95	3012	Lenovo	ThinkPad X1 Carbon 

2003	1003	450.00	3012	Lenovo	ThinkPad X1 Carbon 

2002	1002	89.99	3012	Lenovo	ThinkPad X1 Carbon 

hive> SELECT CONCAT(fname, ' ', lname) AS fullname FROM customers; 

John Smith 

Mary Johnson 

David Williams 

Sarah Brown 

Michael Jones 

Jennifer Garcia 

Robert Miller 

Lisa Davis 

William Rodriguez 

Elizabeth Martinez 

James Hernandez 

Patricia Lopez 

hive> SELECT country, state, count(order_id) FROM customers GROUP BY country, state; 

USA	CA	3 

USA	TX	3 

USA	FL	2 

USA	IL	2 

USA	NY	2 

hive> quit; 