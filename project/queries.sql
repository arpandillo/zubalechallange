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


