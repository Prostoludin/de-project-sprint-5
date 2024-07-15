CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
id serial PRIMARY KEY NOT NULL,
restaurant_id VARCHAR NOT NULL,
restaurant_name VARCHAR NOT NULL,
active_from TIMESTAMP NOT NULL,
active_to TIMESTAMP NOT NULL
);