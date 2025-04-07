CREATE DATABASE kafka_error_handler_test;

\c kafka_error_handler_test;

CREATE SCHEMA test;

CREATE TABLE test.raw_message(
    id BIGSERIAL PRIMARY KEY,
    creation_date timestamptz NOT NULL,
    error_text text,
    msg_json jsonb
);