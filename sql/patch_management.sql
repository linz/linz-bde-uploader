--------------------------------------------------------------------------------
--
-- $Id$
--
-- linz_bde_loader - LINZ BDE loader for PostgreSQL
--
-- Copyright 2011 Crown copyright (c)
-- Land Information New Zealand and the New Zealand Government.
-- All rights reserved
--
-- This software is released under the terms of the new BSD license. See the 
-- LICENSE file for more information.
--
--------------------------------------------------------------------------------
-- Creates LDS patch versioning management system. This system is used for
-- Applying table DDL and data updates to an already installed system.
--------------------------------------------------------------------------------
SET client_min_messages TO WARNING;

BEGIN;

DO $SCHEMA$
BEGIN

IF EXISTS (SELECT * FROM pg_namespace where LOWER(nspname) = '_patches') THEN
    RETURN;
END IF;

CREATE SCHEMA _patches AUTHORIZATION bde_dba;
GRANT USAGE ON SCHEMA _patches TO bde_admin;
COMMENT ON SCHEMA _patches IS 'Schema for LDS patch versioning';

CREATE TABLE _patches.applied_patches (
    patch_name TEXT NOT NULL PRIMARY KEY,
    datetime_applied TIMESTAMP NOT NULL DEFAULT now(),
    patch_sql TEXT[] NOT NULL
);

ALTER TABLE _patches.applied_patches OWNER TO bde_dba;

REVOKE ALL ON TABLE _patches.applied_patches FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE _patches.applied_patches TO bde_admin;

END
$SCHEMA$;

DO $$
DECLARE
   v_pcid    TEXT;
   v_schema  TEXT = '_patches';
BEGIN
    FOR v_pcid IN 
        SELECT v_schema || '.' || proname || '(' || pg_get_function_identity_arguments(oid) || ')'
        FROM pg_proc 
        WHERE pronamespace=(SELECT oid FROM pg_namespace WHERE nspname = v_schema)  
    LOOP
        EXECUTE 'DROP FUNCTION ' || v_pcid;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION _patches.apply_patch(
    p_patch_name TEXT,
    p_patch_sql  TEXT[]
)
RETURNS
    BOOLEAN AS
$$
DECLARE
    v_sql TEXT;
BEGIN
    -- Make sure that only one patch can be applied at a time
    LOCK TABLE _patches.applied_patches IN EXCLUSIVE MODE;

    IF EXISTS (
        SELECT patch_name
        FROM   _patches.applied_patches
        WHERE  patch_name = p_patch_name
    )
    THEN
        RAISE INFO 'Patch % is already applied', p_patch_name;
        RETURN FALSE;
    END IF;
    
    RAISE INFO 'Applying patch %', p_patch_name;
    
    BEGIN
        FOR v_sql IN SELECT * FROM unnest(p_patch_sql) LOOP
            RAISE DEBUG 'Running SQL: %', v_sql;
            EXECUTE v_sql;
        END LOOP;
    EXCEPTION
        WHEN others THEN
            RAISE EXCEPTION 'Could not applied % patch using %. ERROR: %',
                p_patch_name, v_sql, SQLERRM;
    END;

    INSERT INTO _patches.applied_patches(
        patch_name,
        datetime_applied,
        patch_sql
    )
    VALUES(
        p_patch_name,
        now(),
        p_patch_sql
    );
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION _patches.apply_patch(TEXT, TEXT[]) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION _patches.apply_patch(
    p_patch_name TEXT,
    p_patch_sql  TEXT
)
RETURNS
    BOOLEAN AS 
$$
    SELECT _patches.apply_patch($1, ARRAY[$2])
$$
    LANGUAGE sql;

ALTER FUNCTION _patches.apply_patch(TEXT, TEXT) OWNER TO bde_dba;

DO $$
DECLARE
    v_comment TEXT;
    v_pcid    TEXT;
    v_schema  TEXT = '_patches';
BEGIN
    FOR v_comment, v_pcid IN
        SELECT
            obj_description(oid, 'pg_proc'),
            proname || '(' || pg_get_function_identity_arguments(oid) || ')'
        FROM
            pg_proc
        WHERE
            pronamespace=(SELECT oid FROM pg_namespace WHERE nspname = v_schema)  AND
            proname NOT ILIKE '_createVersionComment'
    LOOP
        IF v_comment IS NULL THEN
            v_comment := '';
        ELSE
            v_comment := E'\n\n' || v_comment;
        END IF;
       
        v_comment := 'Version: ' ||  '$Id$' || E'\n' ||
                     'Installed: ' || to_char(current_timestamp,'YYYY-MM-DD HH:MI') ||
                    v_comment;
       
        EXECUTE 'COMMENT ON FUNCTION ' || v_schema || '.' || v_pcid || ' IS '
            || quote_literal( v_comment );
    END LOOP;
END
$$;

COMMIT;
