--SELECT * FROM dim_date_times
--ALTER TABLE dim_date_times
   --ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

--DELETE FROM dim_date_times
--WHERE LENGTH(date_uuid) < 15;
--SELECT * FROM orders_table
--WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);
--SELECT COUNT(store_code)
--FROM orders_table;
-- 435
-- 120123
/* 
-- DONE
-- Assuming dim_customer table
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_customer
    FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
*/
/*
-- Done
-- Assuming dim_product table
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_product
    FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
	*/

/*
-- done
-- Assuming dim_store table
-- had to delete 27973 rows
-- DELETE FROM orders_table
-- WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store
    FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
*/

/*
-- Assuming dim_date table
-- needed to update the orders_table date_uuid to uuid type
-- needed to drop 18 rows thsat weren't of uuid format
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date
    FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
*/

-- drop card_number 4971858637664481 from orders table
-- drop 4222069242355461965, 584541931351
-- DELETE FROM orders_table
-- had to delete 179 rows using below
-- WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);
/*
-- done
-- Assuming dim_date table
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card
    FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
*/
-- product_quantity
--ALTER TABLE dim_store_details
--ADD PRIMARY KEY (store_code);
--SELECT * FROM dim_products
/*
SELECT COUNT(*)
FROM dim_produ
WHERE product_code IS NULL;
*/
/*
SELECT product_code, COUNT(*)
FROM dim_products
GROUP BY product_code
HAVING COUNT(*) > 1 OR product_code IS NULL;

CREATE TABLE temp_table AS
SELECT DISTINCT product_code
FROM dim_products;
*/
--ALTER TABLE dim_products
--ADD PRIMARY KEY (user_uuid);