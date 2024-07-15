CREATE TABLE IF NOT EXISTS dds_project.fct_order_delivery (
	id serial4 NOT NULL,
	order_id int4 NOT NULL,
	delivery_id int4 NOT NULL,
	courier_id int4 NOT NULL,
	address varchar NOT NULL,
	rate int4 DEFAULT 5 NOT NULL,
	order_sum numeric(14, 2) DEFAULT 0 NULL,
	order_tips numeric(14, 2) DEFAULT 0 NULL,
	CONSTRAINT fct_order_delivery_order_sum_check CHECK ((order_sum >= (0)::numeric)),
	CONSTRAINT fct_order_delivery_order_tips_check CHECK ((order_tips >= (0)::numeric)),
	CONSTRAINT fct_order_delivery_pkey PRIMARY KEY (id),
	CONSTRAINT fct_order_delivery_rate_check CHECK (((rate > 0) AND (rate <= 5))),
	CONSTRAINT fct_order_delivery_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds_project.dm_couriers(id),
	CONSTRAINT fct_order_delivery_delivery_id_fkey FOREIGN KEY (delivery_id) REFERENCES dds_project.dm_deliveries(id),
	CONSTRAINT fct_order_delivery_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds_project.dm_orders(id)
);