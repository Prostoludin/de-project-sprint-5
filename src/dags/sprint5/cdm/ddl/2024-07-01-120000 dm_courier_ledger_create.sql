CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
id SERIAL NOT NULL PRIMARY KEY,
courier_id VARCHAR(30) NOT NULL,
courier_name VARCHAR(30) NOT NULL,
settlement_year INT NOT NULL CHECK((settlement_year>=2022) AND settlement_year <=2500),
settlement_month INT NOT NULL CHECK((settlement_month>=1) AND settlement_month <=12),
orders_count INT NOT NULL CHECK(orders_count>=0) DEFAULT 0,
orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= (0)::numeric),
rate_avg numeric(2, 1) NOT NULL DEFAULT 0 CHECK ((rate_avg >= (0)::numeric) AND (rate_avg <= (5)::numeric)),
order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= (0)::numeric),
courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= (0)::numeric),
courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= (0)::numeric),
courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= (0)::numeric),
CONSTRAINT dm_settlement_courier_id_check UNIQUE (courier_id, courier_name, settlement_month, settlement_year)
);