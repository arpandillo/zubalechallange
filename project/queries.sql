--1. Date with max orders (last 30 days of partitions)

SELECT created_date, COUNT(*) AS total_orders
FROM orders
WHERE partition_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY created_date
ORDER BY total_orders DESC
LIMIT 1;


--2. Most demanded product

SELECT p.name, SUM(o.quantity) AS total_quantity
FROM orders o
JOIN products p ON o.product_id = p.id
WHERE o.partition_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY p.name
ORDER BY total_quantity DESC
LIMIT 1;


--3. Top 3 most demanded categories

SELECT p.category, SUM(o.quantity) AS total_quantity
FROM orders o
JOIN products p ON o.product_id = p.id
WHERE o.partition_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY p.category
ORDER BY total_quantity DESC
LIMIT 3;

--Challenge 4 


-- 1. Revenue Trends Over Time

SELECT created_date, SUM(o.quantity * p.price) AS total_revenue
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY created_date
ORDER BY created_date;

--2. Price Sensitivity by Category
SELECT p.category,
       ROUND(CORR(p.price, o.quantity), 2) AS price_quantity_correlation
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.category;

