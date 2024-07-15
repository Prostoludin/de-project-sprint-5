CREATE TABLE IF NOT EXISTS dds_project.dm_delivery_ts (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_delivery_ts_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_delivery_ts_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_delivery_ts_pkey PRIMARY KEY (id),
	CONSTRAINT dm_delivery_ts_year_check CHECK (((year >= 2022) AND (year < 2500)))
);