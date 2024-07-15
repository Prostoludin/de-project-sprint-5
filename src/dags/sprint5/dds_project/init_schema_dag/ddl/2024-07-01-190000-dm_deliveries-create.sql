CREATE TABLE IF NOT EXISTS dds_project.dm_deliveries (
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL,
	delivery_ts_id int4 NOT NULL,
	courier_id int4 NOT NULL,
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds_project.dm_couriers(id),
	CONSTRAINT dm_deliveries_timestamp_id_fkey FOREIGN KEY (delivery_ts_id) REFERENCES dds_project.dm_delivery_ts(id)
);