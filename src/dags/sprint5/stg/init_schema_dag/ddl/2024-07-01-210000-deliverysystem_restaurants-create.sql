CREATE TABLE IF NOT EXISTS stg.deliverysystem_restaurants (
id SERIAL NOT NULL PRIMARY KEY,
object_id varchar(30) NOT NULL,
object_value text NOT NULL,
CONSTRAINT deliverysystem_restaurants_object_id_uindex UNIQUE (object_id));