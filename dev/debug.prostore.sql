CREATE DATABASE adg_test;

CREATE TABLE adg_test.env_test (
ID BIGINT,
gos_number varchar,
constraint PK_id PRIMARY KEY(ID)
)
DISTRIBUTED BY (id);

create upload external  table adg_test.env_testEx (
ID BIGINT,
gos_number varchar)
LOCATION 'kafka://p1-adsk-01:2181/env_test' FORMAT 'AVRO';

USE adg_test

BEGIN DELTA
INSERT INTO adg_test.env_test SELECT * FROM adg_test.env_testEx;
COMMIT DELTA

DROP DATABASE adg_test