INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status) 
WITH orders AS (SELECT BTRIM(((object_value::JSON->'user')::JSON->'id')::VARCHAR, '"') AS user_id,
					   BTRIM(((object_value::JSON->'restaurant')::JSON->'id')::VARCHAR, '"') AS restaurant_id,
					   CAST((object_value::JSON->'update_ts')::TEXT AS timestamp) AS timestamp_id,
					   object_id AS order_key,
					   BTRIM((object_value::JSON->'final_status')::VARCHAR, '"') AS order_status
				FROM stg.ordersystem_orders),
	 deliveries AS (SELECT  BTRIM((object_value::JSON->'order_id')::VARCHAR, '"') AS order_id,
	 						BTRIM((object_value::JSON->'delivery_id')::VARCHAR, '"') AS delivery_id
	 				FROM stg.deliverysystem_deliveries) 
SELECT du.id AS user_id,
	   dr.id AS restaurant_id,
	   dt.id AS order_ts_id,
	   order_key,
	   order_status,
	   dd.id AS delivery_id,
	   dd.delivery_ts_id,
	   dd.courier_id
FROM orders
JOIN dds.dm_restaurants dr ON dr.restaurant_id = orders.restaurant_id
JOIN dds.dm_users du ON du.user_id = orders.user_id
JOIN dds.dm_timestamps dt ON dt.ts = orders.timestamp_id
JOIN deliveries d ON d.order_id = orders.order_key
JOIN dds.dm_deliveries dd ON dd.delivery_id = d.delivery_id;