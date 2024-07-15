INSERT INTO dds.fct_product_sales(product_id,
								  order_id,
								  count, price,
								  total_sum,
								  bonus_payment,
								  bonus_grant) 
WITH temptable AS (SELECT BTRIM((JSON_ARRAY_ELEMENTS(product_payments::JSON)->'product_id')::VARCHAR, '"') AS product_id,
						  BTRIM(order_id::VARCHAR, '"') AS order_id,
						  (JSON_ARRAY_ELEMENTS(product_payments::JSON)->'quantity')::TEXT::INT AS count,
						  (JSON_ARRAY_ELEMENTS(product_payments::JSON)->'price')::TEXT::numeric(19, 5) AS price,
						  (JSON_ARRAY_ELEMENTS(product_payments::JSON)->'product_cost')::TEXT::numeric(19, 5) AS total_sum,
						  (JSON_ARRAY_ELEMENTS(product_payments::JSON)->'bonus_payment')::TEXT::numeric(19, 5) AS bonus_payment,
						  (JSON_ARRAY_ELEMENTS(product_payments::JSON)->'bonus_grant')::TEXT::numeric(19, 5) AS bonus_grant,
						  order_date
				   FROM (SELECT event_value::JSON->'order_id' AS order_id,
				   				event_value::JSON->'product_payments' AS product_payments,
				   				CAST((event_value::JSON->'order_date')::TEXT AS timestamp) AS order_date
				   		 FROM stg.bonussystem_events
				   		 WHERE event_type LIKE 'bonus_transaction') AS transactions),
	  products AS (SELECT product_id, MAX(id) AS id
	  			   FROM dds.dm_products
	  			   GROUP BY product_id)
SELECT dp.id AS product_id,
	   do2.id AS order_id,
	   temptable.count,
	   temptable.price,
	   temptable.total_sum,
	   temptable.bonus_payment,
	   temptable.bonus_grant
FROM temptable
JOIN products dp ON dp.product_id = temptable.product_id
JOIN dds.dm_orders do2 ON do2.order_key = temptable.order_id
JOIN dds.dm_timestamps dt ON dt.id = do2.timestamp_id;