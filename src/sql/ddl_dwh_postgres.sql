CREATE SCHEMA stg AUTHORIZATION pg_database_owner;

DROP TABLE stg.transaction_service;
CREATE TABLE stg.transaction_service (
	object_id varchar NOT NULL,
	object_type varchar NOT NULL,
	sent_dttm timestamp NOT NULL,
	payload text NOT NULL,
	CONSTRAINT transaction_service_pk PRIMARY KEY (object_id)
);