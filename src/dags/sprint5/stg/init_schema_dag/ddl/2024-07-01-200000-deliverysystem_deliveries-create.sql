CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
id SERIAL NOT NULL PRIMARY KEY,
object_id varchar(30) NOT NULL,
object_value text NOT NULL,
update_ts timestamp NOT NULL,
CONSTRAINT deliverysystem_deliveries_object_id_uindex UNIQUE (object_id));