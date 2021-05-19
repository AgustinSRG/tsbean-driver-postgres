-- Tables for testing the driver

DROP TABLE IF EXISTS "person";
DROP TABLE IF EXISTS "dummy";

CREATE TABLE "person" (
    "id" BIGINT NOT NULL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "surname" VARCHAR(255) NOT NULL,
    "age" INT,
    "has_driver_license" BOOLEAN,
    "preferences" TEXT,
    "birth_date" TIMESTAMPTZ
);

CREATE TABLE "dummy" (
    "id" SERIAL PRIMARY KEY,
    "value1" BIGINT,
    "value2" REAL,
    "value3" VARCHAR(255),
    "data" TEXT
);

