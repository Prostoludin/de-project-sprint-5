INSERT INTO dds.dm_users(id, user_id, user_name, user_login)
SELECT id,
	   object_id AS user_id,
	   object_value ::JSON->> 'name' AS user_name,
	   object_value ::JSON->> 'login' AS user_login
FROM stg.ordersystem_users
ORDER BY id ASC
ON CONFLICT (id) DO UPDATE
SET user_id = EXCLUDED.user_id,
	user_name = EXCLUDED.user_name,
	user_login = EXCLUDED.user_name;