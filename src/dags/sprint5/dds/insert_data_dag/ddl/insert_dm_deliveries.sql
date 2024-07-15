INSERT INTO dds.dm_deliveries(delivery_id, delivery_ts_id, courier_id) 
WITH deliveries AS (SELECT object_value,
						   BTRIM((object_value::JSON -> 'delivery_id')::VARCHAR, '"') AS delivery_id,
						   (object_value::JSON -> 'delivery_ts')::TEXT::TIMESTAMP AS delivery_ts,
						   BTRIM((object_value::JSON -> 'courier_id')::VARCHAR, '"') AS courier_id
					FROM stg.deliverysystem_deliveries)
SELECT delivery_id,
	   dt.id AS delivery_ts_id,
	   c.id AS courier_id
FROM deliveries del
JOIN dds.dm_delivery_ts dt ON dt.ts =del.delivery_ts
JOIN dds.dm_couriers c ON c.courier_id =del.courier_id;