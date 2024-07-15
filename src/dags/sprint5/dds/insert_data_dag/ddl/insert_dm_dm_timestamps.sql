INSERT INTO dds.dm_timestamps(ts,
							  "year",
							  "month",
							  "day",
							  "date",
							  "time")
SELECT CAST(object_value::JSON->>'date'AS timestamp) AS ts,
	   date_part('year', (object_value::JSON->>'date')::DATE) AS "year",
	   date_part('month', (object_value::JSON->>'date')::DATE) AS "month",
	   date_part('day', (object_value::JSON->>'date')::DATE) AS "day",
	   date(object_value::JSON->>'date') AS "date",
	   CAST(object_value::JSON->>'date' AS time) AS "time"
FROM stg.ordersystem_orders
WHERE object_value::JSON->>'final_status' IN ('CLOSED', 'CANCELLED')
ORDER BY id ASC
ON CONFLICT (id) DO UPDATE
SET ts = EXCLUDED.ts,
	year = EXCLUDED.year,
	month = EXCLUDED.month,
	day = EXCLUDED.day,
	date = EXCLUDED.date,
	time = EXCLUDED.time;