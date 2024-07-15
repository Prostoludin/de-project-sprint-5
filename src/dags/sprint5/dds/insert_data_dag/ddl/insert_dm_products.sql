INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to) 
SELECT id AS restaurant_id,
       BTRIM((dish::JSON->'_id')::varchar, '"') AS product_id,
	   BTRIM((dish::JSON->'name')::varchar, '"') AS product_name,
	   ((dish::JSON->'price')::text::numeric) AS product_price,
	   update_ts AS active_from,
	   '2099-12-31 00:00:00.000' AS active_to
FROM (SELECT id,
			 json_array_elements(menu::json) AS dish,
			 update_ts
	  FROM (SELECT id,
	  			   (object_value::JSON -> 'menu')  AS menu,
	  			   update_ts
	  		FROM stg.ordersystem_restaurants
	  		ORDER BY id ASC) AS menues) AS dishes;