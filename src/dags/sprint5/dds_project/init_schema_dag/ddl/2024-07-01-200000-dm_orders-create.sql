CREATE TABLE IF NOT EXISTS dds_project.dm_orders (
	id serial4 NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	order_ts_id int4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	delivery_id int4 NOT NULL,
	delivery_ts_id int4 NOT NULL,
	courier_id int4 NOT NULL,
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
	CONSTRAINT dm_orders_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds_project.dm_couriers(id),
	CONSTRAINT dm_orders_delivery__ts_id_fkey FOREIGN KEY (delivery_ts_id) REFERENCES dds_project.dm_delivery_ts(id),
	CONSTRAINT dm_orders_delivery_id_fkey FOREIGN KEY (delivery_id) REFERENCES dds_project.dm_deliveries(id),
	CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds_project.dm_restaurants(id),
	CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (order_ts_id) REFERENCES dds_project.dm_order_ts(id),
	CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds_project.dm_users(id)
);