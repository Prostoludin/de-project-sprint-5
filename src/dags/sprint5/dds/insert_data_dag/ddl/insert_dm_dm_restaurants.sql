INSERT INTO dds.dm_restaurants(id, restaurant_id, restaurant_name, active_from, active_to)
SELECT id,
	   object_id AS restaurant_id,
	   object_value ::JSON->> 'name' AS restaurant_name,
	   update_ts AS active_from,
	   '2099-12-31 00:00:00.000' AS active_to
FROM stg.ordersystem_restaurants
ORDER BY id ASC
ON CONFLICT (id) DO UPDATE
SET restaurant_id = EXCLUDED.restaurant_id,
	restaurant_name = EXCLUDED.restaurant_name,
	active_from = EXCLUDED.active_from,
	active_to = EXCLUDED.active_to;