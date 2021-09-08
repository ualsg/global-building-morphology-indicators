---- //// PUBLIC Schema ////-----
--delete table tbl_create_history if exists
DROP TABLE IF EXISTS {{public_schema}}.tbl_create_history CASCADE;

--create table tbl_create_history
CREATE TABLE {{public_schema}}.tbl_create_history (
    gid serial primary key,
    object_type varchar(20),
    schema_name varchar(50),
    object_identity varchar(200),
    creation_date timestamp without time zone
    );



--delete event trigger before dropping function
DROP EVENT TRIGGER IF EXISTS tbl_create_history_trigger;

--delete table history function
DROP FUNCTION IF EXISTS {{public_schema}}.tbl_create_history_func();

-- create table creation history function
CREATE OR REPLACE FUNCTION {{public_schema}}.tbl_create_history_func()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands  () WHERE command_tag in ('SELECT INTO','CREATE TABLE','CREATE TABLE AS')
    LOOP
        INSERT INTO {{public_schema}}.tbl_create_history (object_type, schema_name, object_identity, creation_date) SELECT obj.object_type, obj.schema_name, obj.object_identity, now();
    END LOOP;
END;
$$;


--ALTER EVENT TRIGGER tbl_create_history_trigger DISABLE;
--DROP EVENT TRIGGER tbl_create_history_trigger;

-- create event trigger and hook it with the tbl_create_history_func function
CREATE EVENT TRIGGER tbl_create_history_trigger ON ddl_command_end
WHEN TAG IN ('SELECT INTO','CREATE TABLE','CREATE TABLE AS')
EXECUTE PROCEDURE {{public_schema}}.tbl_create_history_func();


---- //// GBMI Schema ////-----
--delete table tbl_create_history if exists
DROP TABLE IF EXISTS {{gbmi_schema}}.tbl_create_history CASCADE;

--create table tbl_create_history
CREATE TABLE {{gbmi_schema}}.tbl_create_history (
    gid serial primary key,
    object_type varchar(20),
    schema_name varchar(50),
    object_identity varchar(200),
    creation_date timestamp without time zone
    );



--delete event trigger before dropping function
DROP EVENT TRIGGER IF EXISTS tbl_create_history_trigger;

--delete table history function
DROP FUNCTION IF EXISTS {{gbmi_schema}}.tbl_create_history_func();

-- create table creation history function
CREATE OR REPLACE FUNCTION {{gbmi_schema}}.tbl_create_history_func()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands  () WHERE command_tag in ('SELECT INTO','CREATE TABLE','CREATE TABLE AS')
    LOOP
        INSERT INTO {{gbmi_schema}}.tbl_create_history (object_type, schema_name, object_identity, creation_date) SELECT obj.object_type, obj.schema_name, obj.object_identity, now();
    END LOOP;
END;
$$;


--ALTER EVENT TRIGGER tbl_create_history_trigger DISABLE;
--DROP EVENT TRIGGER tbl_create_history_trigger;

-- create event trigger and hook it with the tbl_create_history_func function
CREATE EVENT TRIGGER tbl_create_history_trigger ON ddl_command_end
WHEN TAG IN ('SELECT INTO','CREATE TABLE','CREATE TABLE AS')
EXECUTE PROCEDURE {{gbmi_schema}}.tbl_create_history_func();



---- //// OSM_QA Schema ////-----
--delete table tbl_create_history if exists
DROP TABLE IF EXISTS {{qa_schema}}.tbl_create_history CASCADE;

--create table tbl_create_history
CREATE TABLE {{qa_schema}}.tbl_create_history (
    gid serial primary key,
    object_type varchar(20),
    schema_name varchar(50),
    object_identity varchar(200),
    creation_date timestamp without time zone
    );



--delete event trigger before dropping function
DROP EVENT TRIGGER IF EXISTS tbl_create_history_trigger;

--delete table history function
DROP FUNCTION IF EXISTS {{qa_schema}}.tbl_create_history_func();

-- create table creation history function
CREATE OR REPLACE FUNCTION {{qa_schema}}.tbl_create_history_func()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands  () WHERE command_tag in ('SELECT INTO','CREATE TABLE','CREATE TABLE AS')
    LOOP
        INSERT INTO {{qa_schema}}.tbl_create_history (object_type, schema_name, object_identity, creation_date) SELECT obj.object_type, obj.schema_name, obj.object_identity, now();
    END LOOP;
END;
$$;


--ALTER EVENT TRIGGER tbl_create_history_trigger DISABLE;
--DROP EVENT TRIGGER tbl_create_history_trigger;

-- create event trigger and hook it with the tbl_create_history_func function
CREATE EVENT TRIGGER tbl_create_history_trigger ON ddl_command_end
WHEN TAG IN ('SELECT INTO','CREATE TABLE','CREATE TABLE AS')
EXECUTE PROCEDURE {{qa_schema}}.tbl_create_history_func();