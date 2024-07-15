CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
id SERIAL NOT NULL PRIMARY KEY,
object_id varchar(30) NOT NULL,
object_value text NOT NULL,
CONSTRAINT deliverysystem_couriers_object_id_uindex UNIQUE (object_id));