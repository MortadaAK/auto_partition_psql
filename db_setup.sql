
SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Create basic user table
--

CREATE SEQUENCE collections_key_seq
	START WITH 1
	INCREMENT BY 1
	NO MINVALUE
	NO MAXVALUE
	CACHE 1;

CREATE TABLE toys_collections(
    key integer DEFAULT nextval('collections_key_seq') NOT NULL,
    ts timestamptz NOT NULL, -- Check what type of timestamp
    toys_id integer REFERENCES op_toyss(id),
    value numeric
);

ALTER SEQUENCE collections_key_seq OWNED BY toys_collections.key;

-- Partitioning --

CREATE OR REPLACE FUNCTION month_partition()
    RETURNS TRIGGER AS $MONTH$
    DECLARE
        month TEXT;
        month_ts TEXT;
        month_table TEXT;
        new_trigger TEXT;
        new_table TEXT;
        new_insert TEXT;
    BEGIN
        month_ts = date_trunc('month', NEW.ts);
        month := date_part('month', NEW.ts);
        month_table := TG_TABLE_NAME || '_' || month;
        IF NOT EXISTS(SELECT relname FROM pg_class WHERE relname=month_table)
            THEN RAISE NOTICE 'CREATING PARTITION: %', month_table;
            new_trigger := 'trigger_' || month_table;
            new_table := 'CREATE TABLE '
                || month_table
                || '( CHECK ( ts >= '
                || ''''||month_ts||'''::timestamp' 
                || ' AND ts < '
                || ''''||month_ts::timestamp +interval'1 month'||'''::timestamp' 
                || ' ) ) INHERITS ( ' || TG_TABLE_NAME || ' );';
            EXECUTE new_table;
        END IF;
        new_insert := 'INSERT INTO '
            || month_table
            || ' VALUES ('
            || NEW.key ||', '
            ||''''||NEW.ts||'''::timestamp' ||', '
            || NEW.toys_id ||', '
            || NEW.value ||');';
        EXECUTE new_insert;
        RETURN NULL;
    END;
    $MONTH$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION year_partition()
    RETURNS TRIGGER AS $YEAR$
    DECLARE
        year TEXT;
        year_ts TEXT;
        year_table TEXT;
        new_trigger TEXT;
        new_table TEXT;
        new_insert TEXT;
    BEGIN
        year_ts = date_trunc('year', NEW.ts);
        year := date_part('year', NEW.ts);
        year_table := TG_TABLE_NAME || '_' || year;
        IF NOT EXISTS(SELECT relname FROM pg_class WHERE relname=year_table)
            THEN RAISE NOTICE 'CREATING PARTITION: %', year_table;
            new_trigger := 'trigger_' || year_table;
            new_table := 'CREATE TABLE '
                || year_table
                || '( CHECK ( ts >= '
                || ''''||year_ts||'''::timestamp' 
                || ' AND ts < '
                || ''''||year_ts::timestamp +interval'1 year'||'''::timestamp' 
                || ' ) ) INHERITS ( ' || TG_TABLE_NAME || ' );'
                || ' CREATE TRIGGER ' || new_trigger
                || ' BEFORE INSERT '
                || ' ON ' || year_table
                || ' FOR EACH ROW '
                || 'EXECUTE PROCEDURE month_partition();';
            EXECUTE new_table;
        END IF;
        new_insert := 'INSERT INTO '
            || year_table
            || ' VALUES ('
            || NEW.key ||', '
            ||''''||NEW.ts||'''::timestamp' ||', '
            || NEW.toys_id ||', '
            || NEW.value ||');';
        EXECUTE new_insert;
        RETURN NULL;
    END;
    $YEAR$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION toys_partition()
    RETURNS TRIGGER AS $$
    DECLARE
        toys_table TEXT;
        new_trigger TEXT;
        new_table TEXT;
        new_insert TEXT;
    BEGIN
        toys_table := 'toys_' || NEW.toys_id;
        IF NOT EXISTS(SELECT relname FROM pg_class WHERE relname=toys_table)
            THEN RAISE NOTICE 'CREATING PARTITION: %', toys_table;
            new_trigger := 'trigger_' || toys_table;
            new_table := 'CREATE TABLE '
                || toys_table
                || ' ( CHECK ( toys_id = ' 
                || NEW.toys_id
                || ' ) ) INHERITS (toys_collections);'
                || ' CREATE TRIGGER ' || new_trigger
                || ' BEFORE INSERT'
                || ' ON ' || toys_table
                || ' FOR EACH ROW '
                || 'EXECUTE PROCEDURE year_partition();';
            EXECUTE new_table;
        END IF;
        new_insert := 'INSERT INTO '
            || toys_table
            || ' VALUES ('
            || NEW.key ||', '
            || ''''||NEW.ts||'''::timestamp' ||', '
            || NEW.toys_id ||', '
            || NEW.value ||');';
        EXECUTE new_insert;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql VOLATILE;

CREATE TRIGGER insert_to_collections
    BEFORE INSERT
    ON toys_collections
    FOR each ROW
    EXECUTE PROCEDURE toys_partition();








