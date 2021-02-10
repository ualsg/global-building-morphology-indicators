--delete table tbl_create_history if exists
DROP TABLE IF EXISTS tbl_create_history CASCADE;

--create table tbl_create_history
CREATE TABLE tbl_create_history (
    gid serial primary key,
    object_type varchar(20),
    schema_name varchar(50),
    object_identity varchar(200),
    creation_date timestamp without time zone
    );



--delete event trigger before dropping function
DROP EVENT TRIGGER IF EXISTS tbl_create_history_trigger;

--delete table history function
DROP FUNCTION IF EXISTS public.tbl_create_history_func();

-- create table creation history function
CREATE OR REPLACE FUNCTION tbl_create_history_func()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands  () WHERE command_tag in ('SELECT INTO','CREATE TABLE','CREATE TABLE AS')
    LOOP
        INSERT INTO public.tbl_create_history (object_type, schema_name, object_identity, creation_date) SELECT obj.object_type, obj.schema_name, obj.object_identity, now();
    END LOOP;
END;
$$;


--ALTER EVENT TRIGGER tbl_create_history_trigger DISABLE;
--DROP EVENT TRIGGER tbl_create_history_trigger;

-- create event trigger and hook it with the tbl_create_history_func function
CREATE EVENT TRIGGER tbl_create_history_trigger ON ddl_command_end
WHEN TAG IN ('SELECT INTO','CREATE TABLE','CREATE TABLE AS')
EXECUTE PROCEDURE tbl_create_history_func();