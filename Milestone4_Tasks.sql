## Task 1
SELECT
    country_code AS country,
    COUNT(DISTINCT store_code) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_no_stores DESC;

## Task 2
SELECT
    locality,
    COUNT(*) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    total_no_stores DESC, locality

## Task 3
SELECT SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales, month
FROM
    dim_products
INNER JOIN
orders_table ON dim_products.product_code = orders_table.product_code
INNER JOIN dim_date_times
ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
   month
ORDER BY
    total_sales DESC
LIMIT 6;

## Task 4

SELECT
    COUNT(orders_table.store_code) AS numbers_of_sales,
    COALESCE(SUM(orders_table.product_quantity), 0) AS product_quantity_count,
	CASE
        WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    dim_store_details
LEFT JOIN
    orders_table ON dim_store_details.store_code = orders_table.store_code
GROUP BY
	 CASE
        WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END;

#Task 5
SELECT
	dim_store_details.store_type AS store_type,
    ROUND( cast(SUM(orders_table.product_quantity* dim_products.product_price) as numeric),2) AS total_sales,
	ROUND(COUNT(*) / (CAST((SELECT COUNT(*) FROM orders_table) AS NUMERIC))*100,2) AS percentage_total
FROM
    dim_products
INNER JOIN
    orders_table ON dim_products.product_code = orders_table.product_code	
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
	dim_store_details.store_type

#TASK 6

with rankedData as
(SELECT
	dim_date_times.year AS year,
	dim_date_times.month AS month,
    ROUND( cast(SUM(orders_table.product_quantity* dim_products.product_price) as numeric),2) AS total_sales,
	RANK() OVER (partition by year order by SUM(orders_table.product_quantity* dim_products.product_price) desc) as sale_rank
FROM
    dim_products
INNER JOIN
    orders_table ON dim_products.product_code = orders_table.product_code	
INNER JOIN
    dim_date_times ON orders_table.date_uuid = cast(dim_date_times.date_uuid as text)
GROUP BY
	dim_date_times.year, dim_date_times.month)
select year, month, total_sales
from rankedData 
where sale_rank = 1
order by total_sales desc

#Task 7 - clarification on the question 
SELECT
	COUNT( staff_numbers) AS total_staff_numbers,
	country_code
FROM
    orders_table
INNER JOIN
 dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
	country_code

# Task 8
SELECT
	ROUND( cast(SUM(orders_table.product_quantity* dim_products.product_price) as numeric),2) AS total_sales,
	store_type,
	country_code
FROM
    orders_table
INNER JOIN
 dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN
 dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE country_code ='DE'
GROUP BY
	store_type, country_code
order by total_sales

# Task 9
with baseData as
(SELECT
	year, month, day,
	cast(timestamp as time),
	lead(cast(timestamp as time)) over(order by   year , month, day, timestamp ) as nextSaleTimestamp,
	case when  cast(timestamp as time) > lead(cast(timestamp as time)) over(order by   year , month, day, timestamp ) 
   then cast(timestamp as time) - lead(cast(timestamp as time)) over(order by   year, month, day , timestamp)
   else lead(cast(timestamp as time)) over(order by   year, month, day , timestamp) - cast(timestamp as time) 
 end as diff
FROM
    orders_table
INNER JOIN
 dim_date_times ON orders_table.date_uuid = cast(dim_date_times.date_uuid as text)
)
select year, avg(diff) as actual_time_taken
from baseData 
group by year
order by 1