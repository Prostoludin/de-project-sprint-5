CREATE TABLE IF NOT EXISTS dds.dm_products (
id serial PRIMARY KEY NOT NULL,
restaurant_id INTEGER NOT NULL,
product_id VARCHAR NOT NULL,
product_name VARCHAR NOT NULL,
product_price NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK(product_price >=0),
active_from TIMESTAMP NOT NULL,
active_to TIMESTAMP NOT NULL
);
ALTER TABLE dds.dm_products ADD CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);