CREATE TABLE IF NOT EXISTS dds_project.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);