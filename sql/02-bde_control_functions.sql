--------------------------------------------------------------------------------
--
-- linz_bde_uploader - LINZ BDE uploader for PostgreSQL
--
-- Copyright 2016 Crown copyright (c)
-- Land Information New Zealand and the New Zealand Government.
-- All rights reserved
--
-- This software is released under the terms of the new BSD license. See the
-- LICENSE file for more information.
--
--------------------------------------------------------------------------------

SET SEARCH_PATH TO bde_control;

-- Note: The perl module BdeDatabase uses the parameter names to manage calling
-- functions
-- Names beginning bde_ are potentially exposed by the perl module
-- First parameter name "p_upload" means use the upload id
-- First parameter name "p_bde_schema" means use the schema name
-- See bde_GetBdeFunctions

-- Drop all existing functions in bde_control

SET client_min_messages TO WARNING;
BEGIN;

DO $$
DECLARE
   v_pcid    TEXT;
   v_schema  TEXT = 'bde_control';
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

-- Function to retrieve a list of functions that the script can use

CREATE OR REPLACE FUNCTION bde_GetBdeFunctions
(
    p_control_schema name
)
RETURNS TABLE
(
    proname name,
    nparam integer,
    isjobfunc integer,
    isschemafunc integer,
    returntype text
)
AS
$body$
    SELECT
        p.proname,
        p.pronargs::integer AS nparam,
        CASE WHEN LOWER(p.proargnames[1]) = 'p_upload' THEN
            1
        ELSE
            0
        END AS isjobfunc,
        CASE WHEN LOWER(p.proargnames[1]) = 'p_bde_schema' THEN
            1
        ELSE
            0
        END AS isschemafunc,
        CASE WHEN p.proretset THEN
            'TABLE' -- virtual tableset
        WHEN t.typname = 'record' THEN
            'RECORD' -- record (composite)
        ELSE
            'SINGLE' --built in datatype single value (int, real, varchar etc..)
        END AS returntype
    FROM pg_proc p
        JOIN pg_type t ON t.oid = p.prorettype
    WHERE p.pronamespace=
    (SELECT oid FROM pg_namespace WHERE LOWER(nspname) =  LOWER($1))
    AND p.proname ILIKE E'bde\\_%'
$body$
LANGUAGE sql;

ALTER FUNCTION bde_GetBdeFunctions(name) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION bde_CheckSchemaName
(
    p_bde_schema name
)
RETURNS
    name
AS
$body$
    SELECT nspname FROM pg_namespace WHERE LOWER(nspname) = LOWER($1)
$body$
LANGUAGE sql;

ALTER FUNCTION bde_CheckSchemaName(name) OWNER TO bde_dba;

-- Function to retrieve the last upload information for a table


CREATE OR REPLACE FUNCTION bde_LastUploadStats(
    p_bde_schema name,
    p_bde_table name,
    OUT last_upload_id integer,
    OUT last_upload_dataset character varying(14),
    OUT last_upload_type character(1),
    OUT last_upload_details text,
    OUT last_upload_time timestamp without time zone,
    OUT last_upload_bdetime timestamp without time zone,
    OUT last_level0_dataset character varying(14)
    )
AS
$body$
     SELECT
        last_upload_id,
        last_upload_dataset,
        last_upload_type,
        last_upload_details,
        last_upload_time,
        last_upload_bdetime,
        last_level0_dataset
    FROM
        bde_control.upload_table
    WHERE
        LOWER(schema_name) = LOWER($1) AND
        LOWER(table_name) = LOWER($2);
$body$
LANGUAGE sql;

ALTER FUNCTION bde_LastUploadStats(name, name) OWNER TO bde_dba;

-- Function to determine whether a job is still active

CREATE OR REPLACE FUNCTION bde_uploadIsActive (
    p_old_upload_id int
)
RETURNS INTEGER
AS
$body$
SELECT COUNT(*)::INTEGER FROM bde_control.upload
    WHERE id=$1 AND status='A';
$body$
language sql;

ALTER FUNCTION bde_uploadIsActive(int) OWNER TO bde_dba;

-- Function to determine whether any job is still active

CREATE OR REPLACE function bde_anyUploadIsActive (
)
RETURNS INTEGER
As
$body$
SELECT COUNT(*)::INTEGER FROM bde_control.upload
    WHERE status='A';
$body$
language sql;

ALTER FUNCTION bde_anyUploadIsActive() OWNER TO bde_dba;

-- Function to expire locks that are no longer current

CREATE OR REPLACE FUNCTION bde_ReleaseExpiredLocks(p_expiry_time REAL)
  RETURNS INTEGER
AS
$body$
DECLARE
    v_count INTEGER;
    v_expired RECORD;
BEGIN
    v_count := 0;
    FOR v_expired IN
        WITH ul( upl_id ) AS
       (SELECT
           upl_id_lock
        FROM
           bde_control.upload_table
        WHERE
           upl_id_lock IS NOT NULL
        UNION
        SELECT
           id
        FROM
           bde_control.upload
        WHERE
           status NOT IN ('E','C')
        UNION
        SELECT
           replace(nspname,'bde_upload_','')::integer
        FROM
           pg_namespace
        WHERE
           nspname LIKE 'bde_upload_%'
        )
    SELECT
        upl_id
    FROM
        ul
    WHERE
        NOT _bde_LockIsCurrent(upl_id,p_expiry_time)
    LOOP
        PERFORM _bde_ReleaseLocks(v_expired.upl_id);
    v_count := v_count+1;
    END LOOP;

    RETURN v_count;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_ReleaseExpiredLocks(REAL) OWNER TO bde_dba;

-- Function to delete information for jobs no longer required needed

CREATE OR REPLACE FUNCTION bde_RemoveOldJobData
(
    p_expiry_days INTEGER
)
RETURNS INTEGER
AS
$body$
DECLARE
    v_schema name;
BEGIN
    IF p_expiry_days > 0 THEN
        DELETE FROM bde_control.upload
        WHERE (clock_timestamp()::timestamp - end_time) >
            (p_expiry_days||'D')::interval
        AND id NOT IN ( SELECT last_upload_id FROM bde_control.upload_table )
        AND status <> 'A';
    END IF;

    DELETE FROM bde_control.upload_stats
    WHERE upl_id NOT IN (SELECT id FROM bde_control.upload);

    FOR v_schema IN
        SELECT nspname
        FROM pg_namespace
        WHERE _bde_IsTmpSchema(nspname)
        AND NOT _bde_IsActiveTmpSchema(nspname)
    LOOP
        BEGIN
            EXECUTE 'DROP SCHEMA ' || v_schema || ' CASCADE';
        EXCEPTION
        WHEN OTHERS THEN
        END;
    END LOOP;

    RETURN 0;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_RemoveOldJobData(INTEGER) OWNER TO bde_dba;

-- ----------------------------------------------------------------
-- Functions relating to a specific upload job


-- Create a new upload job.

-- Function to create an active upload job
-- Creates a schema to hold the data for the upload
-- and installs an _options table into the schema
-- The schema is destroyed by the _bde_ReleaseLocks function
-- Returns the id of the created job

CREATE OR REPLACE FUNCTION bde_CreateUpload(p_bde_schema name)
   RETURNS integer
AS
$body$

DECLARE
    v_upload INT;
    v_tmp_schema name;
    v_bde_schema name;
BEGIN
    v_bde_schema = bde_CheckSchemaName( p_bde_schema );
    IF v_bde_schema IS NULL THEN
         RETURN NULL;
    END IF;

    INSERT INTO bde_control.upload( schema_name, status )
    VALUES (v_bde_schema, 'A')
    RETURNING id INTO v_upload;

    v_tmp_schema := bde_TmpSchema(v_upload);
    EXECUTE 'CREATE SCHEMA ' || v_tmp_schema;
    EXECUTE 'CREATE TABLE ' || v_tmp_schema ||
        '._options ( option name primary key, value text )';

    PERFORM _bde_CreateEventLog(v_upload);
    PERFORM bde_SetOption(v_upload,'_dataset','(undefined dataset)');

    RETURN v_upload;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_CreateUpload(NAME) OWNER TO bde_dba;

-- Function to close the current job
-- param id is the job to finish
-- success is non zero for true, zero if there is an error

CREATE OR REPLACE FUNCTION bde_FinishUpload(
    p_upload INTEGER,
    p_error BOOLEAN
)
    RETURNS void
AS
$body$
DECLARE
    v_result CHAR(1);
BEGIN
    v_result := CASE WHEN p_error
        THEN
            'E'
        ELSE
            'C'
        END;

    UPDATE
        bde_control.upload
    SET
        end_time = clock_timestamp(),
        status = v_result
    WHERE
        id = p_upload;

    DROP TABLE IF EXISTS _bde_events;

    PERFORM _bde_ReleaseLocks(p_upload);
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_FinishUpload(INTEGER, BOOLEAN) OWNER TO bde_dba;

-- Function to refresh the lock on an upload.  Ideally this will be
-- called periodically during processing to ensure that it does not
-- expire.

CREATE OR REPLACE FUNCTION _bde_RefreshLock( p_upload INTEGER )
   RETURNS bool
AS
$body$
DECLARE
    v_count INT;
BEGIN
    UPDATE
        bde_control.upload
    SET
        end_time = clock_timestamp()
    WHERE
        status='A' AND
    id = p_upload;
    GET DIAGNOSTICS v_count=ROW_COUNT;
    RETURN v_count > 0;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_RefreshLock(INTEGER) OWNER TO bde_dba;

-- Function to release the locks relating to an upload.
-- This removes all entries in the status table, and deletes the
-- schema used by the job

CREATE OR REPLACE FUNCTION _bde_ReleaseLocks( p_upload INTEGER )
    RETURNS void
AS
$body$
BEGIN

    -- Change the status of the upload to not be active

    IF ( SELECT status FROM bde_control.upload WHERE id=p_upload )
        NOT IN ('E','C')
    THEN
        UPDATE bde_control.upload
        SET status = 'E'
        WHERE id = p_upload;

        RAISE INFO 'Expired lock deleted automatically';
    END IF;

    -- Release the lock ids on any tables locked by this upload

    UPDATE
        bde_control.upload_table
    SET
        upl_id_lock = NULL
    WHERE
        upl_id_lock = p_upload;

    -- Drop the schema used by the lock

    EXECUTE 'DROP SCHEMA IF EXISTS ' || bde_TmpSchema(p_upload) || ' CASCADE';
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_ReleaseLocks(INTEGER) OWNER TO bde_dba;

-- Function to check whether a lock is current
-- Returns 1 if the lock is current, 0 otherwise

CREATE OR REPLACE FUNCTION _bde_LockIsCurrent(
    p_upload INTEGER,
    p_expiry_time REAL
)
RETURNS
    BOOLEAN
AS
$body$
      SELECT
         COUNT(*) > 0
      FROM
         bde_control.upload
      WHERE
         id = $1
      AND
         status NOT IN ('E','C') AND
         (clock_timestamp()::timestamp - end_time) < ($2||'H')::interval
$body$
LANGUAGE sql;

ALTER FUNCTION _bde_LockIsCurrent(INTEGER, REAL) OWNER TO bde_dba;

-- Function to add a new table to the list of tables being maintained by
-- the BDE updates.
--
-- If the table is already in the list of tables being updated, then
-- this will check that the key column name is valid.  It can be updated to
-- only if the current value is null.  The error and warning tolerances will
-- be updated by this function.

CREATE OR REPLACE FUNCTION bde_AddTable(
    p_upload INTEGER,
    p_bde_table NAME,
    p_key_column NAME DEFAULT NULL,
    p_row_tol_error REAL DEFAULT NULL,
    p_row_tol_warning REAL DEFAULT NULL
)
RETURNS
    BOOLEAN
AS
$body$
DECLARE
    v_bde_schema NAME;
    v_bde_table NAME;
    v_key_column NAME;
    v_status BOOLEAN;
    v_tbl_id INTEGER;
    v_oid regclass;
BEGIN
    v_status := FALSE;
    v_bde_schema := bde_BdeSchema(p_upload);

    IF v_bde_schema IS NULL THEN
        RETURN 0;
    END IF;

    v_bde_table := LOWER(p_bde_table);
    v_key_column := LOWER(p_key_column);
    v_tbl_id := _bde_UploadTableId(p_upload,v_bde_table);
    v_oid := bde_control.bde_TableOid( v_bde_schema, v_bde_table );
    IF v_oid IS NULL THEN
        RAISE EXCEPTION 'Table ''%.%'' does not exist',
            v_bde_schema, v_bde_table;
    END IF;

    IF v_key_column IS NOT NULL AND
        NOT bde_control.bde_TableKeyIsValid( v_oid, v_key_column)
    THEN
        RAISE EXCEPTION 'Table ''%.%'' key ''%'' is not valid',
            v_bde_schema, v_bde_table, v_key_column;
    END IF;

    IF v_tbl_id IS NULL THEN
        INSERT INTO bde_control.upload_table (
            schema_name,
            table_name
        )
        VALUES(
            v_bde_schema,
            v_bde_table
        );
        v_tbl_id := lastval()::INTEGER;
        v_status := TRUE;
    END IF;

    -- Check that the key column is compatible with existing value
    -- (either the same, the existing value is null)

    IF COALESCE(v_key_column,'') <> (
        SELECT LOWER(COALESCE(key_column,v_key_column,''))
        FROM bde_control.upload_table
        WHERE id = v_tbl_id
        )
    THEN
        RAISE EXCEPTION 'Table ''%.%'' key ''%'' differs from current value',
            v_bde_schema, v_bde_table, v_key_column;
    END IF;

    -- Update information for table

    UPDATE bde_control.upload_table
    SET
        key_column=v_key_column,
        row_tol_error=p_row_tol_error,
        row_tol_warning=p_row_tol_warning
    WHERE
        id=v_tbl_id AND
        (
            COALESCE(key_column, '') <> COALESCE(v_key_column, '')  OR
            COALESCE(row_tol_error, 0) <> COALESCE(p_row_tol_error, 0) OR
            COALESCE(row_tol_warning, 0) <> COALESCE(p_row_tol_warning, 0)
        );

    RETURN v_status;
END;
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_AddTable(INTEGER, NAME, NAME, REAL, REAL) OWNER TO bde_dba;

-- Functions to begin and end uploading a table.  Simply create a transaction.
-- This should improve speed as it removes the need for write ahead logging
-- of tables created within the transaction.

CREATE OR REPLACE FUNCTION bde_BeginUploadTable(
    p_upload INTEGER,
    p_bde_table NAME,
    p_force_lock INT
)
RETURNS
    integer
AS
$body$
DECLARE
    v_locked INTEGER;
    v_lockOwner INTEGER;
BEGIN
    v_locked := _bde_LockTable( p_upload, p_bde_table );
    IF v_locked = 0 AND p_force_lock <> 0 THEN
        v_lockOwner = _bde_UnlockTable( p_upload, p_bde_table );
        RAISE WARNING 'Overriding lock held by % for table %',
            v_lockOwner, p_bde_table;
        v_locked := _bde_LockTable( p_upload, p_bde_table );
    END IF;

    RETURN v_locked;

END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_BeginUploadTable(INTEGER, NAME, INT) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION bde_EndUploadTable(
    p_upload INTEGER,
    p_bde_table NAME
)
RETURNS
    INTEGER
AS
$body$
BEGIN
    RETURN _bde_UnlockTable( p_upload, p_bde_table );
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_EndUploadTable(INTEGER, NAME) OWNER TO bde_dba;

-- Function to lock a table for an upload
--
-- Returns 1 if locked successfully, 0 if failed.


CREATE OR REPLACE FUNCTION _bde_LockTable( p_upload INTEGER, p_bde_table name )
    RETURNS INTEGER
AS
$body$
DECLARE
    v_tbl_id INTEGER;
    v_ok INTEGER;
BEGIN

    v_tbl_id := _bde_UploadTableId(p_upload,p_bde_table);

    IF v_tbl_id IS NULL THEN
        RAISE EXCEPTION
            'Can''t lock table %. First add the table to upload table registry',
            p_bde_table;
    END IF;

    UPDATE
        bde_control.upload_table
    SET
        upl_id_lock = p_upload
    WHERE
        id = v_tbl_id AND
        upl_id_lock IS NULL;

    v_ok := (
        SELECT
            COUNT(*)
        FROM
            bde_control.upload_table
        WHERE
            id = v_tbl_id AND
            upl_id_lock = p_upload
        );

     RETURN v_ok;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_LockTable(INTEGER, NAME) OWNER TO bde_dba;

-- Check whether we have a lock on a table

CREATE OR REPLACE FUNCTION _bde_HaveTableLock(
    p_upload INTEGER,
    p_bde_table NAME
)
RETURNS
    BOOL
AS
$body$
DECLARE
    v_tbl_id INTEGER;
BEGIN
    v_tbl_id := _bde_UploadTableId(p_upload, p_bde_table);
    RETURN EXISTS (
        SELECT * FROM bde_control.upload_table
        WHERE id=v_tbl_id AND upl_id_lock=p_upload
    );
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_HaveTableLock(INTEGER, NAME) OWNER TO bde_dba;

-- Function to unlock a table for an upload
--
-- Returns the id of the job that owned the lock


CREATE OR REPLACE FUNCTION _bde_UnlockTable(
    p_upload INTEGER,
    p_bde_table name
)
RETURNS
    INTEGER
AS
$body$
DECLARE
    v_tbl_id INTEGER;
    v_result INTEGER;
BEGIN
    v_tbl_id := _bde_UploadTableId(p_upload, p_bde_table);

    v_result := COALESCE(
        (select upl_id_lock FROM bde_control.upload_table WHERE id = v_tbl_id),
        0
    );

    UPDATE bde_control.upload_table
    SET    upl_id_lock = NULL
    WHERE  id = v_tbl_id;

    RETURN v_result;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_UnlockTable(INTEGER, NAME) OWNER TO bde_dba;

-- Function to obtain an exclusive database lock on a table, with a
-- timeout

CREATE OR REPLACE FUNCTION _bde_GetExclusiveLock(
    p_upload int,
    p_bde_table regclass
)
RETURNS
    VOID
AS
$body$
DECLARE
    v_sql TEXT;
    v_lock_owner TEXT;
    v_remaining INTEGER;
    v_failed bool;

BEGIN
    v_sql := bde_GetOption(p_upload,'exclusive_lock_timeout');
    v_remaining := 60;
    IF v_sql is not null AND v_sql <> '' THEN
        v_remaining := v_sql::int;
    END IF;
    v_sql := 'LOCK TABLE ' || p_bde_table || ' IN EXCLUSIVE MODE' ||
             (CASE WHEN v_remaining >= 0 THEN ' NOWAIT' ELSE '' END);

    v_failed := false;

    LOOP
        BEGIN
            EXECUTE v_sql;
        EXCEPTION
            WHEN OTHERS THEN
                IF v_remaining > 0 THEN
                    IF not v_failed THEN
                        RAISE DEBUG 'Waiting up to % seconds for lock on %',
                            v_remaining, p_bde_table;
                        v_failed := true;
                    END IF;
                    PERFORM pg_sleep(1);
                    v_remaining := v_remaining-1;
                    CONTINUE;
                END IF;
                v_lock_owner := (
                    SELECT
                        string_agg(distinct a.usename || ' (' || a.application_name || ')', ', ')
                    FROM
                        pg_stat_activity a
                        JOIN pg_locks l
                            ON l.pid = a.pid
                        JOIN pg_database d
                            ON l.database = d.oid
                    WHERE
                        d.datname = current_database() AND
                        l.relation = p_bde_table
                    );
                RAISE EXCEPTION
                    'Unable to acquire exclusive lock on % - currently lock held by %',
                    p_bde_table, v_lock_owner;
        END;
        EXIT;
    END LOOP;
    IF v_failed THEN
        RAISE DEBUG 'Lock on % acquired', p_bde_table;
    END IF;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_GetExclusiveLock(INTEGER, REGCLASS) OWNER TO bde_dba;

-- Set/reset an option for the upload job
-- (used to pass information between controlling script and bde SPL functions)

CREATE OR REPLACE FUNCTION bde_SetOption(
    p_upload INTEGER,
    p_option VARCHAR(255),
    p_value TEXT
    )
RETURNS INTEGER
AS
$body$
DECLARE
    v_c INTEGER;
    v_option_table varchar(255);
BEGIN
    v_option_table := bde_TmpSchema(p_upload) || '._options';
    IF p_value IS NULL THEN
        EXECUTE 'DELETE FROM ' || v_option_table || ' WHERE option=$1'
       USING p_option;
        GET DIAGNOSTICS v_c = ROW_COUNT;
    ELSE
        EXECUTE 'UPDATE ' || v_option_table || ' SET value=$2 WHERE option=$1'
            USING p_option,p_value;
        GET DIAGNOSTICS v_c = ROW_COUNT;
        IF v_c = 0 THEN
            EXECUTE 'INSERT INTO ' || v_option_table || ' VALUES($1,$2)'
                USING p_option,p_value;
            GET DIAGNOSTICS v_c = ROW_COUNT;
        END IF;
    END IF;
    RETURN v_c;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_SetOption(INTEGER, VARCHAR(255), TEXT) OWNER TO bde_dba;

-- Get the value of an option for the upload

CREATE OR REPLACE FUNCTION bde_GetOption(
    p_upload INTEGER,
    p_option VARCHAR(255)
    )
RETURNS TEXT
AS
$body$
DECLARE
    v_result TEXT;
    v_option_table varchar(255);
BEGIN
    v_option_table := bde_TmpSchema(p_upload) || '._options';
    EXECUTE 'SELECT value FROM ' || v_option_table || ' WHERE option=$1'
        INTO v_result
        USING p_option;
    RETURN v_result;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_GetOption(INTEGER, VARCHAR(255)) OWNER TO bde_dba;

-- Function to return the BDE schema for an upload

CREATE OR REPLACE FUNCTION bde_BdeSchema( p_upload INTEGER )
  RETURNS name
AS
$body$
BEGIN
    return (SELECT schema_name FROM bde_control.upload WHERE id=p_upload);
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_BdeSchema(INTEGER) OWNER TO bde_dba;

-- Function to return the scratch schema for an upload

CREATE OR REPLACE FUNCTION bde_TmpSchema( p_upload INTEGER )
  RETURNS name
AS
$body$
    SELECT ('bde_upload_' || $1)::name as result;
$body$
LANGUAGE sql;

ALTER FUNCTION bde_TmpSchema(INTEGER) OWNER TO bde_dba;

-- Function to determine if a namespace is a temp namespace

CREATE OR REPLACE FUNCTION _bde_IsTmpSchema( p_schema name )
  RETURNS BOOLEAN
AS
$body$
    SELECT $1 ilike 'bde_upload_%' as result;
$body$
LANGUAGE sql;

ALTER FUNCTION _bde_IsTmpSchema(NAME) OWNER TO bde_dba;

-- Function to determine if a namespace is an active temp namespace

CREATE OR REPLACE FUNCTION _bde_IsActiveTmpSchema( p_schema name )
  RETURNS BOOLEAN
AS
$body$
    SELECT $1 IN (
        select bde_TmpSchema(id) FROM bde_control.upload WHERE status='A'
    ) as result;
$body$
LANGUAGE sql;

ALTER FUNCTION _bde_IsActiveTmpSchema(NAME) OWNER TO bde_dba;

-- Function to get the bde table id for a table and upload

CREATE OR REPLACE FUNCTION _bde_UploadTableId(
    p_upload INTEGER,
    p_bde_table name
)
RETURNS
    INTEGER
AS
$body$
BEGIN
    RETURN (
        SELECT
            id
        FROM
            bde_control.upload_table
        WHERE
            LOWER(table_name) = LOWER(p_bde_table) AND
            schema_name = (
                SELECT schema_name from bde_control.upload where id = p_upload
            )
    );
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_UploadTableId(INTEGER, NAME) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION bde_TimestampEvent(
    p_upload INTEGER,
    p_event TEXT
)
RETURNS INTEGER
AS $$
DECLARE
    v_upload_id INTEGER;
BEGIN
    INSERT INTO _bde_events (upl_id, event)
    VALUES (p_upload, lower(p_event))
    RETURNING upl_id
    INTO v_upload_id;

    RETURN v_upload_id;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION bde_TimestampEvent(INTEGER, TEXT) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION _bde_CreateEventLog(
    p_upload INTEGER
)
RETURNS BOOLEAN
AS $$
DECLARE
    v_table_oid REGCLASS;
BEGIN
    v_table_oid := bde_TempTableOid('_bde_events');
    IF v_table_oid IS NULL THEN
        CREATE TEMP TABLE _bde_events
        (
            upl_id INTEGER,
            event TEXT NOT NULL,
            event_time TIMESTAMP NOT NULL DEFAULT clock_timestamp()
        );
    END IF;

    RETURN v_table_oid IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION _bde_CreateEventLog(INTEGER) OWNER TO bde_dba;

-- Function to return the duration of an event

CREATE OR REPLACE FUNCTION bde_EventDuration
(
    p_upload INTEGER,
    p_event TEXT
)
RETURNS INTERVAL
AS $$
DECLARE
    v_duration INTERVAL;
BEGIN
    SELECT max(event_time) - min(event_time)
    INTO   v_duration
    FROM   _bde_events
    WHERE  upl_id = $1
    AND    event = lower($2);

    RETURN v_duration;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION bde_EventDuration(INTEGER, TEXT) OWNER TO bde_dba;

-- Function to update the details for a table once a dataset is uploaded into it

CREATE OR REPLACE FUNCTION _bde_RecordDatasetLoaded(
    p_upload INTEGER,
    p_bde_table name,
    p_dataset VARCHAR(14),
    p_upload_type CHAR(1),
    p_incremental BOOLEAN,
    p_bdetime TIMESTAMP,
    p_details TEXT,
    p_nins BIGINT,
    p_nupd BIGINT,
    p_nnullupd BIGINT,
    p_ndel BIGINT
)
RETURNS
    INTEGER
AS
$body$
DECLARE
    v_tbl_id INTEGER;
    v_upload_time TIMESTAMP;
BEGIN
    v_tbl_id := _bde_UploadTableId(p_upload,p_bde_table);
    IF v_tbl_id IS NULL THEN
        RETURN 0;
    END IF;

    v_upload_time := clock_timestamp()::timestamp;

    -- Record the end of the upload timestamp (matching start event in
    -- bde_CreateWorkingCopy

    PERFORM bde_TimestampEvent(p_upload,p_dataset || ' ' || p_bde_table );

    UPDATE
        bde_control.upload_table
    SET
        last_upload_id = p_upload,
        last_upload_dataset = p_dataset,
        last_upload_type = p_upload_type,
        last_upload_incremental = p_incremental,
        last_upload_details = p_details,
        last_upload_time = v_upload_time,
        last_upload_bdetime = p_bdetime
    WHERE
        id = v_tbl_id;

    IF p_upload_type = '0' THEN
        UPDATE bde_control.upload_table
        SET    last_level0_dataset = p_dataset
        WHERE  id = v_tbl_id;
    END IF;

    INSERT INTO bde_control.upload_stats (
        upl_id,
        tbl_id,
        type,
        incremental,
        dataset,
        ninsert,
        nupdate,
        nnullupdate,
        ndelete,
        duration
        )
    VALUES (
        p_upload,
        v_tbl_id,
        p_upload_type,
        p_incremental,
        p_dataset,
        p_nins,
        p_nupd,
        p_nnullupd,
        p_ndel,
        bde_EventDuration(p_upload, p_dataset || ' ' || p_bde_table )
        );

    RAISE INFO 'Table % completed: % insertions, % updates, % deletions',
        p_bde_table, p_nins, p_nupd, p_ndel;
    RETURN 1;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_RecordDatasetLoaded(
    INTEGER,
    NAME,
    VARCHAR(14),
    CHAR(1),
    BOOLEAN,
    TIMESTAMP,
    TEXT,
    BIGINT,
    BIGINT,
    BIGINT,
    BIGINT
) OWNER TO bde_dba;

-- Prepare to upload a dataset
-- Removes any tables in the schema not used by the dataset

CREATE OR REPLACE FUNCTION bde_StartDataset (
    p_upload INTEGER,
    p_datasetname VARCHAR(14)
)
RETURNS
   INTEGER
AS
$body$
DECLARE
    v_tmp_schema name;
    v_tbl RECORD;
BEGIN
    PERFORM _bde_RefreshLock(p_upload);

    -- Clear out unwanted tables

    v_tmp_schema := bde_TmpSchema(p_upload);

    FOR v_tbl IN
        SELECT
            oid::regclass AS tbname
        FROM
            pg_class
        WHERE
            relkind = 'r' AND
            relnamespace = (
                SELECT oid
                FROM   pg_namespace
                WHERE  LOWER(nspname) = LOWER(v_tmp_schema)
            ) AND
            relname NOT IN ('_options')
    LOOP
        EXECUTE 'DROP TABLE ' || v_tbl.tbname;
    END LOOP;

    -- Save the dataset name in the options table

    PERFORM bde_SetOption(p_upload,'_dataset',p_datasetname);

    RETURN 1;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_StartDataset(INTEGER, VARCHAR(14)) OWNER TO bde_dba;

-- Get the regclass corresponding to a schema and table name

CREATE OR REPLACE FUNCTION bde_TableOid (
    p_schema_name name,
    p_table_name name
)
RETURNS
    REGCLASS
AS
$body$
BEGIN
    RETURN (
        SELECT oid::regclass
        FROM pg_class
        WHERE
            relkind = 'r' AND
            relnamespace = (
                SELECT oid
                FROM pg_namespace
                WHERE
                    LOWER(nspname) = LOWER(p_schema_name)
            ) AND
            LOWER(relname) = LOWER(p_table_name)
    );
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_TableOid(NAME, NAME) OWNER TO bde_dba;

-- Test that a table to be updated actually exists

CREATE OR REPLACE FUNCTION bde_BdeTableExists (
    p_upload INTEGER,
    p_tablename name
)
RETURNS
    BOOLEAN
AS
$body$
BEGIN
   RETURN bde_TableOid(bde_BdeSchema(p_upload),p_tablename) IS NOT NULL;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_BdeTableExists(INTEGER, NAME) OWNER TO bde_dba;

-- Test whether the table has been created already

CREATE OR REPLACE FUNCTION bde_TempTableExists (
    p_upload INTEGER,
    p_tablename name
)
RETURNS
    BOOLEAN
AS
$body$
BEGIN
    RETURN _bde_WorkingCopyTableOid(p_upload, p_tablename) IS NOT NULL;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_TempTableExists(INTEGER, NAME) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION _bde_WorkingCopyTableOid (
   p_upload INTEGER,
   p_table_name NAME
   )
RETURNS
   REGCLASS
AS
$body$
DECLARE
    v_table_oid REGCLASS;
BEGIN
    SELECT bde_TempTableOid(p_table_name)
    INTO   v_table_oid;

    IF v_table_oid IS NULL THEN
        v_table_oid := bde_TableOid(bde_TmpSchema(p_upload),p_table_name);
    END IF;

    RETURN v_table_oid;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_WorkingCopyTableOid(INTEGER, NAME) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION bde_TempTableOid(
   p_table_name NAME
)
RETURNS
   REGCLASS
AS $$
    SELECT
        c.oid::REGCLASS
    FROM
        pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE
        n.nspname LIKE 'pg_temp_%' AND
        pg_catalog.pg_table_is_visible(c.oid) AND
        c.relkind = 'r' AND
        c.relname = $1;
$$ LANGUAGE sql;

ALTER FUNCTION bde_TempTableOid(NAME) OWNER TO bde_dba;

-- Create a working version of a bde table in the temporary schema
-- This table is created without any index, etc - just the columns of
-- the specified type.

CREATE OR REPLACE FUNCTION bde_CreateWorkingCopy (
   p_upload INTEGER,
   p_table_name NAME,
   p_temp_table BOOLEAN
   )
RETURNS
   INTEGER
AS
$code$
DECLARE
    v_dataset varchar(14);
    v_tmp_schema name;
    v_bde_schema name;
    v_sql TEXT;
BEGIN

    IF NOT bde_BdeTableExists(p_upload,p_table_name) THEN
        RETURN 0;
    END IF;

    IF bde_TempTableExists(p_upload,p_table_name) THEN
        RETURN 1;
    END IF;

    v_dataset := bde_GetOption(p_upload,'_dataset');
    PERFORM bde_TimestampEvent(p_upload,v_dataset || ' ' || p_table_name );

    v_bde_schema = bde_BdeSchema(p_upload);

    v_sql := 'CREATE ';
    IF p_temp_table THEN
        v_sql := v_sql || 'TEMP TABLE ';
    ELSE
        v_tmp_schema = bde_TmpSchema(p_upload);
        v_sql := v_sql || 'TABLE ' || v_tmp_schema || '.';
    END IF;

    v_sql := v_sql || p_table_name
        || ' (LIKE ' || v_bde_schema || '.' || p_table_name
        || ' INCLUDING DEFAULTS)'
        || ' WITH (autovacuum_enabled=true, toast.autovacuum_enabled=true)';

    EXECUTE v_sql;

    RETURN 1;
END
$code$
LANGUAGE plpgsql;

ALTER FUNCTION bde_CreateWorkingCopy(INTEGER, NAME, BOOLEAN) OWNER TO bde_dba;

-- Create drops the copy of a bde table in the temporary schema

CREATE OR REPLACE FUNCTION bde_DropWorkingCopy (
    p_upload INTEGER,
    p_table_name NAME
)
RETURNS
    INTEGER
AS
$code$
DECLARE
    v_tmp_table REGCLASS;
BEGIN
    v_tmp_table := _bde_WorkingCopyTableOid(p_upload,p_table_name);
    IF v_tmp_table IS NOT NULL THEN
        EXECUTE 'DROP TABLE IF EXISTS ' || CAST(v_tmp_table AS TEXT);
    END IF;

    RETURN 1;
END
$code$
LANGUAGE plpgsql;

ALTER FUNCTION bde_DropWorkingCopy(INTEGER, NAME) OWNER TO bde_dba;

-- Select the subset of column names from a list of columns that
-- a table actually has.  The columns names are supplied and returned
-- as a pipe delimited list.

CREATE OR REPLACE FUNCTION bde_SelectValidColumns (
    p_upload INTEGER,
    p_table_name name,
    p_columns TEXT
    )
RETURNS
    TEXT
AS
$body$
DECLARE
   v_temptable regclass;
   v_colname name;
   v_usecolumns text;
BEGIN

    v_temptable := _bde_WorkingCopyTableOid(p_upload,p_table_name);

    v_usecolumns := '';

    FOR v_colname IN SELECT * FROM regexp_split_to_table(p_columns,E'\\|')
    LOOP
        IF EXISTS (
            SELECT *
            FROM   pg_attribute
            WHERE  attrelid=v_temptable
            AND    LOWER(attname) = LOWER(v_colname)
        ) THEN
            IF v_usecolumns != '' THEN
            v_usecolumns := v_usecolumns || '|';
        END IF;
            v_usecolumns := v_usecolumns || v_colname;
        END IF;
    END LOOP;

    RETURN v_usecolumns;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_SelectValidColumns(INTEGER, NAME, TEXT) OWNER TO bde_dba;

-- Function to convert pipe separated list of column names to quoted strings

CREATE OR REPLACE FUNCTION _bde_QuoteColumnNames( p_columns text )
RETURNS
    TEXT
AS
$body$
DECLARE
   v_qcolumns text;
   v_colname name;
BEGIN
   v_qcolumns := '';

   FOR v_colname IN SELECT * FROM regexp_split_to_table(p_columns,E'\\|')
   LOOP
       IF v_qcolumns != '' THEN
          v_qcolumns := v_qcolumns || ', ';
       END IF;
       v_qcolumns := v_qcolumns || quote_ident(v_colname);
   END LOOP;

   RETURN v_qcolumns;
END
$body$
LANGUAGE 'plpgsql';

ALTER FUNCTION _bde_QuoteColumnNames(TEXT) OWNER TO bde_dba;

-- Upload a data file to a table
-- Returns 1 on success, 0 on failure.  I don't think I can get more information
-- than this from the database regarding when and where it failed :-(

CREATE OR REPLACE FUNCTION bde_UploadDataToTempTable (
    p_upload INTEGER,
    p_table_name name,
    p_datafile text,
    p_columns text
)
RETURNS INTEGER
AS
$body$
DECLARE
    v_temptable regclass;
    v_columns text;
    v_sql text;
BEGIN
    -- A good time to ensure that the lock doesn't get revoked
    PERFORM _bde_RefreshLock(p_upload);

    RAISE DEBUG 'Loading file % into table %', p_datafile, p_table_name;

    v_temptable := _bde_WorkingCopyTableOid(p_upload,p_table_name);
    IF v_temptable IS NULL THEN
        RAISE EXCEPTION 'Cannot load file % into table % as working copy of table does not exist',
            p_datafile, p_table_name;
    END IF;

    v_columns := _bde_QuoteColumnNames(p_columns);

    v_sql := 'LOCK TABLE ' || v_temptable || ' IN ACCESS EXCLUSIVE MODE';
    EXECUTE v_sql;

    v_sql := 'COPY ' || v_temptable || '(' || v_columns || ') FROM ' ||
        quote_literal(p_datafile) || ' WITH DELIMITER ''|'' NULL AS ''''';
    RAISE DEBUG 'SQL: %', v_sql;
    EXECUTE v_sql;

    RETURN 1;
EXCEPTION
    WHEN others THEN
    RAISE EXCEPTION
        'Error encountered loading file % into table %. SQL: %. Error: %',
        p_datafile, p_table_name, v_sql, SQLERRM;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_UploadDataToTempTable(INTEGER, NAME, TEXT, TEXT)
    OWNER TO bde_dba;

-- Define the incremental change table name

CREATE OR REPLACE FUNCTION _bde_ChangeTableName()
RETURNS name
AS
$$
   SELECT '_incremental'::name
$$
LANGUAGE sql;

ALTER FUNCTION _bde_ChangeTableName() OWNER TO bde_dba;

-- Create an incremental change table for level 5 uploads to manage deleting
-- records

CREATE OR REPLACE FUNCTION bde_CreateL5ChangeTable (
    p_upload INTEGER,
    p_temp_table BOOLEAN
)
RETURNS
   NAME
AS
$body$
DECLARE
    v_incremental_table name;
    v_tmp_schema name;
    v_sql TEXT;
BEGIN

    v_incremental_table = _bde_ChangeTableName();
    IF bde_TempTableExists(p_upload, v_incremental_table) THEN
        RETURN v_incremental_table;
    END IF;

    v_sql := 'CREATE ';
    IF p_temp_table THEN
        v_sql := v_sql || 'TEMP TABLE ';
    ELSE
        v_tmp_schema := bde_TmpSchema(p_upload);
        v_sql := v_sql || 'TABLE ' || v_tmp_schema || '.';
    END IF;

    v_sql := v_sql || v_incremental_table || $$ (
        tablename VARCHAR(128) NOT NULL,
        tablekeyvalue INTEGER NOT NULL,
        action CHAR(1) NOT NULL
        ) with (autovacuum_enabled=false)$$;

    EXECUTE v_sql;

    PERFORM bde_SetOption(p_upload,'_inc_change_indexed','N');
    RETURN v_incremental_table;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_CreateL5ChangeTable(INTEGER, BOOLEAN) OWNER TO bde_dba;

-- Prepare incremental change table for use


CREATE OR REPLACE FUNCTION _bde_PrepareChangeTable (
   p_upload INTEGER,
   p_change_table regclass
   )
RETURNS
   INT
AS
$body$
BEGIN
    IF bde_GetOption(p_upload,'_inc_change_indexed') = 'Y' THEN
        RETURN 0;
    END IF;

    EXECUTE 'UPDATE ' || p_change_table || ' SET tablename = lower(tablename)
             WHERE tablename != lower(tablename)';
    EXECUTE 'CREATE INDEX _inc_tbl_id ON ' || p_change_table ||
        ' (tablename, tablekeyvalue)';
    EXECUTE 'ANALYZE ' || p_change_table;
    PERFORM bde_SetOption(p_upload,'_inc_change_indexed','Y');
    RETURN 1;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_PrepareChangeTable(INTEGER, REGCLASS) OWNER TO bde_dba;

-- Function to create a primary key join on two version of a table
-- Assumes a simple (non compound) primary key
-- Could probably be done better using temp table or other in memory
-- row set.
-- Also assume that these are "genuine" primary keys.  That is, records
-- are not updated by changing the primary key - the only changes to
-- primary keys are by insertion and deletion.

CREATE OR REPLACE FUNCTION _bde_PrimaryKeyJoin (
    p_bde_table regclass,
    p_prefix1 name,
    p_prefix2 name
    )
RETURNS
    text
AS
$body$
DECLARE
    v_indexid regclass;
    v_colname name;
    v_join text;
BEGIN
    v_join := '';
    v_indexid := (
        SELECT indexrelid::regclass FROM pg_index
        WHERE indrelid=p_bde_table AND indisprimary
    );
    FOR v_colname in (SELECT attname FROM pg_attribute WHERE attrelid=v_indexid)
    LOOP
        IF v_join <> '' THEN
        v_join := v_join || ' AND ';
        END IF;
    v_join := v_join || p_prefix1 || v_colname || ' = '
        || p_prefix2 || v_colname;
    END LOOP;

    RETURN v_join;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_PrimaryKeyJoin(REGCLASS, NAME, NAME) OWNER TO bde_dba;

-- Test that we have an valid key for applying incremental updates
-- Returns the key column name

CREATE OR REPLACE FUNCTION _bde_GetValidIncrementKey (
    p_upload INTEGER,
    p_table_name NAME
)
RETURNS NAME
AS
$body$
DECLARE
    v_schema      NAME;
    v_bde_table   REGCLASS;
    v_key_column  NAME;
    v_status      BOOLEAN;
BEGIN
    v_schema := bde_BdeSchema(p_upload);
    v_bde_table := bde_TableOid(v_schema, p_table_name);

    SELECT TBL.key_column
    INTO   v_key_column
    FROM   bde_control.upload_table TBL
    WHERE  TBL.schema_name = v_schema
    AND    TBL.table_name = p_table_name;

    IF NOT bde_control.bde_TableKeyIsValid(v_bde_table, v_key_column) THEN
        RAISE WARNING 'Table % key  % is invalid',
            p_table_name, v_key_column;
        v_key_column := NULL;
    END IF;

    RETURN v_key_column;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_GetValidIncrementKey(INTEGER, NAME) OWNER TO bde_dba;

-- Apply a level 5 update to a table.  Assumes that the incremental change table
-- and the upload temp table have been populated.

CREATE OR REPLACE FUNCTION bde_ApplyLevel5Update (
    p_upload               INTEGER,
    p_table_name           NAME,
    p_bdetime              TIMESTAMP,
    p_details              TEXT,
    p_fail_if_inconsistent BOOLEAN DEFAULT TRUE
    )
RETURNS INTEGER
AS $$
DECLARE
    v_msg                  TEXT;
    v_dataset              VARCHAR(14);
    v_tmptable             REGCLASS;
    v_changetable          REGCLASS;
    v_bdetable             REGCLASS;
    v_nins                 BIGINT;
    v_ndel                 BIGINT;
    v_nupd                 BIGINT;
    v_nnullupd             BIGINT;
    v_rcount               BIGINT;
    v_task                 TEXT;
    v_sql                  TEXT;
    v_distinct             TEXT;
    v_errmsg               TEXT;
    v_key_column           NAME;
BEGIN
    -- task is used in error reporting, updated throughout process to indicate
    -- what stage the update has got to.

    v_task := 'Setting up L5 update';

    -- A good time to ensure that the lock doesn't get revoked

    PERFORM _bde_RefreshLock(p_upload);

    -- If we don't have a key column, then cannot perform a Level 5 update
    v_key_column := _bde_GetValidIncrementKey( p_upload, p_table_name );
    IF v_key_column IS NULL THEN
        RAISE EXCEPTION
            'Cannot apply level 5 update % into table % as no valid key column is defined',
            v_dataset, p_table_name;
    END IF;

    v_dataset := bde_GetOption(p_upload,'_dataset');

    -- Get the tables we need

    v_tmptable := _bde_WorkingCopyTableOid(p_upload,p_table_name);
    v_bdetable := bde_TableOid(bde_BdeSchema(p_upload),p_table_name);
    v_changetable := _bde_WorkingCopyTableOid(p_upload,_bde_ChangeTableName());

    IF v_tmptable IS NULL OR v_bdetable IS NULL OR v_changetable IS NULL THEN
        RAISE EXCEPTION
            'Cannot apply level 5 update % into table % as one of the scratch, bde, or L5 change table doesn''t exist',
            v_dataset, p_table_name;
    END IF;

    -- Check that we have a table lock

    IF NOT _bde_HaveTableLock( p_upload, p_table_name ) THEN
        RAISE EXCEPTION
            'Cannot apply level 5 update % into table % as have not acquired a lock for this table',
            v_dataset, p_table_name;
    END IF;

    -- Add the primary key to the working table and analyze it
    -- If this works then don't need to use distinct with insert.

    v_task := 'Indexing working table';

    v_distinct := 'DISTINCT';
    BEGIN
        FOR v_sql IN
            SELECT conname || ' ' || pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conrelid = v_bdetable AND contype='p'
        LOOP
            v_sql := 'ALTER TABLE ' || v_tmptable || ' ADD CONSTRAINT '
                || v_sql;
            EXECUTE v_sql;
            v_sql := '';
            v_distinct := '';
        END LOOP;
    EXCEPTION
        WHEN others THEN
            RAISE WARNING 'Failed to add primary key to working table for %',
                p_table_name;
    END;

    EXECUTE 'ANALYZE ' || v_tmptable;

    v_ndel     := 0;
    v_nupd     := 0;
    v_nnullupd := 0;
    v_nins     := 0;

    v_task := 'Preparing incremental change table';

    PERFORM _bde_PrepareChangeTable(p_upload,v_changetable);

    EXECUTE 'SET LOCAL search_path TO ' || bde_TmpSchema(p_upload) ||
            ',' || current_setting('search_path');

    -- Build a list of incremental updates for this table into _tmp_inc_change
    -- This should make subsequent processing faster.

    v_task := 'Selecting incremental records';
    DROP TABLE IF EXISTS _tmp_inc_change;

    CREATE TEMP TABLE _tmp_inc_change (
        id INTEGER
    );

    v_rcount := bde_ExecuteTemplate(
        $sql$
            INSERT INTO _tmp_inc_change(
                id
            )
            SELECT
                tablekeyvalue
            FROM
                %1%
            WHERE
                tablename=%2%
        $sql$,
        array[v_changetable::text,quote_literal(lower(p_table_name))]
    );

    -- If no changed records for this table, then don't waste time
    -- processing them.

    IF v_rcount > 0 THEN

        -- Optimize use of _tmp_inc_change
        ALTER TABLE _tmp_inc_change ADD PRIMARY KEY (id);
        ANALYZE _tmp_inc_change;

        CREATE TEMP TABLE _tmp_inc_actions (
            id INTEGER,
            action CHAR(1)
        );

        -- Add index to incremental data table if it is not already defined

        IF NOT bde_TableKeyIsValid(v_tmptable, v_key_column) THEN
            v_sql := 'CREATE UNIQUE INDEX tmp_inc_key ON ' || v_tmptable ||
                '(' || quote_ident(v_key_column) || ')';
            EXECUTE v_sql;
            v_sql := 'ANALYZE ' || v_tmptable;
            EXECUTE v_sql;
            v_sql := '';
        END IF;

        v_task := 'Performing consistency checks of incremental data';

        v_rcount := _bde_FixChangedIncKeyRecords(
            v_bdetable, v_tmptable, v_key_column
        );
        IF v_rcount > 0 THEN
            RAISE WARNING
                '% rows have been identified that do not have a incremental action for %',
                v_rcount, p_table_name;
        END IF;

        v_task := 'Creating incremental row update actions';

        PERFORM _bde_CreateIncDeletes(v_bdetable, v_tmptable, v_key_column);
        PERFORM _bde_CreateIncInserts(v_bdetable, v_tmptable, v_key_column);
        PERFORM _bde_CreateIncUpdates(v_bdetable, v_tmptable, v_key_column);

        ALTER TABLE _tmp_inc_actions ADD PRIMARY KEY (id);
        ANALYZE _tmp_inc_actions;

        SELECT count(*) INTO v_nnullupd FROM _tmp_inc_actions WHERE action = '0';

        v_task := 'Applying incremental row updates';

        SELECT * FROM table_version._ver_apply_changes(
            v_bdetable, v_tmptable, '_tmp_inc_actions', v_key_column
        )
        INTO v_ndel, v_nins, v_nupd;

        DROP TABLE IF EXISTS _tmp_inc_actions;
    ELSE
        RAISE INFO 'There are no changes to apply for %', p_table_name;
    END IF;

    -- Record the update that has been applied

    v_task := 'Recording update statistics';

    PERFORM _bde_RecordDatasetLoaded(
        p_upload,
        p_table_name,
        v_dataset,
        '5',
        TRUE,
        p_bdetime,
        p_details,
        v_nins,
        v_nupd,
        v_nnullupd,
        v_ndel
    );

    -- Remove the scratch table

    v_task := 'Dropping temp tables';

    EXECUTE 'DROP TABLE ' || v_tmptable;
    DROP TABLE _tmp_inc_change;

    RETURN 1;

EXCEPTION
    WHEN others THEN
        v_errmsg := 'Level 5 update of table ' || p_table_name ||
            ' from dataset ' || v_dataset || ' failed in ' || v_task ||
            'Error:' || SQLERRM;
        IF v_sql <> '' THEN
            v_errmsg := v_errmsg || '. Last sql constructed: ' || v_sql;
        END IF;
    RAISE EXCEPTION '%', v_errmsg;
END;
$$ LANGUAGE plpgsql
SET search_path FROM CURRENT;

ALTER FUNCTION bde_ApplyLevel5Update(INTEGER, NAME, TIMESTAMP, TEXT, BOOLEAN)
    OWNER TO bde_dba;

-- Apply a level 0 update to a table.  Adds the indexes from the table, analyses
-- it, then drops the existing table and renames this one into the bde namespace

CREATE OR REPLACE FUNCTION bde_ApplyLevel0Update (
    p_upload      INTEGER,
    p_table_name  NAME,
    p_bdetime     TIMESTAMP,
    p_details     TEXT,
    p_incremental BOOLEAN
    )
RETURNS INTEGER
AS $$
DECLARE
    v_bde_schema  NAME;
    v_tmp_schema  NAME;
    v_key_column  NAME;
    v_dataset     VARCHAR(14);
    v_tmptable    REGCLASS;
    v_bdetable    REGCLASS;
    v_sql         TEXT;
    v_nins        BIGINT DEFAULT 0;
    v_ndel        BIGINT DEFAULT 0;
    v_nupd        BIGINT DEFAULT 0;
    v_task        TEXT;
    v_depsql      TEXT[];
BEGIN

    RAISE DEBUG 'bde_ApplyLevel0Update(%, %, %, %, %) called',
      p_upload, p_table_name, p_bdetime, p_details, p_incremental;

    v_task := 'Setting up L0 update';

    -- A good time to ensure that the lock doesn't get revoked
    PERFORM _bde_RefreshLock(p_upload);

    v_dataset := bde_GetOption(p_upload,'_dataset');

    -- Get the tables we need

    v_bde_schema := bde_BdeSchema(p_upload);
    v_bdetable := bde_TableOid(v_bde_schema,p_table_name);

    v_tmp_schema := bde_TmpSchema(p_upload);
    v_tmptable := _bde_WorkingCopyTableOid(p_upload,p_table_name);

    IF v_tmptable IS NULL OR v_bdetable IS NULL THEN
        RAISE EXCEPTION
            'Cannot apply level 0 update % into table % as either the scratch or the bde table doesn''t exist',
            v_dataset, p_table_name;
    END IF;

    -- Check that we have a lock for this table

    IF NOT _bde_HaveTableLock( p_upload, p_table_name ) THEN
        RAISE EXCEPTION
            'Cannot apply level 0 update % into table % as have not acquired a lock for this table',
            v_dataset, p_table_name;
        RETURN 0;
    END IF;

    -- Copy additional schema information (constraints, indexes, ownership,
    -- etc) from the bde table to the temp table

    v_task := 'Copying schema information to temp table';

    IF p_incremental THEN
        PERFORM _bde_CopyStatisticsInformation(p_upload,v_bdetable,v_tmptable);

        SELECT
            TBL.key_column
        INTO
            v_key_column
        FROM
            bde_control.upload_table TBL
        WHERE
            TBL.schema_name = v_bde_schema AND
            TBL.table_name  = p_table_name;

        v_sql := 'CREATE UNIQUE INDEX ' || v_tmptable || '_' || v_key_column ||
            ' ON ' || v_tmptable  || ' USING btree (' || v_key_column || ')';
        EXECUTE v_sql;
    ELSE
        PERFORM _bde_CopySchemaInformation(p_upload,v_bdetable,v_tmptable);
    END IF;

    -- Analyze the table

    v_task := 'Analyzing temp table ' || v_tmptable;

    RAISE INFO '%', v_task;
    EXECUTE 'ANALYZE ' || v_tmptable;

    IF p_incremental THEN
        v_task := 'Applying table differences';
        RAISE INFO '%', v_task;
        SELECT
            number_inserts,
            number_updates,
            number_deletes
        INTO
            v_nins,
            v_nupd,
            v_ndel
        FROM
            table_version.ver_apply_table_differences(
                v_bdetable, v_tmptable, v_key_column
            );

        IF v_nins <> 0 OR v_nupd <> 0 OR v_ndel <> 0 THEN
            IF v_nins <> 0 THEN
                RAISE WARNING 'level 0 incremental applied % inserts for % in %',
                    v_nins, p_table_name, v_dataset;
            END IF;
            IF v_nupd <> 0 THEN
                RAISE WARNING 'level 0 incremental applied % updates for % in %',
                    v_nupd, p_table_name, v_dataset;
            END IF;
            IF v_ndel <> 0 THEN
                RAISE WARNING 'level 0 incremental applied % deletes for % in %',
                    v_ndel, p_table_name, v_dataset;
            END IF;
            PERFORM bde_CheckTableCount(p_upload, p_table_name);
        END IF;

        RAISE INFO 'Dropping table %', v_tmptable;
        EXECUTE 'DROP TABLE ' || v_tmptable;
    ELSE
        -- Get dependent object SQL

        v_task := 'Retrieving dependent object information';
        RAISE INFO '%', v_task;

        v_depsql := _bde_GetDependentObjectSql(p_upload,v_bdetable);

        -- Is this too expensive on pg?

        v_task := 'Counting current and new version of table';
        RAISE INFO '%', v_task;

        EXECUTE 'SELECT COUNT(*) FROM ' || v_bdetable INTO v_ndel;
        EXECUTE 'SELECT COUNT(*) FROM ' || v_tmptable INTO v_nins;
        v_nupd := 0;

        -- Replace the BDE table with the new version

        v_task := 'Dropping current version of the table ' || v_bdetable;
        RAISE INFO '%', v_task;

        PERFORM _bde_GetExclusiveLock(p_upload,v_bdetable);
        v_sql := 'DROP TABLE ' || v_bdetable || ' CASCADE';
        RAISE DEBUG 'SQL: %',v_sql;
        EXECUTE v_sql;

        v_task := 'Renaming the new version to replace the current version';
        RAISE INFO '%', v_task;
        v_sql := 'ALTER TABLE ' || v_tmptable || ' SET SCHEMA ' || v_bde_schema;
        RAISE DEBUG 'Moving % into % schema: %', v_tmptable, v_bde_schema, v_sql;
        EXECUTE v_sql;

        -- Restore the dependent objects

        v_task := 'Restoring dependent objects';
        RAISE INFO '%', v_task;

        -- TODO: avoid the call to bde_ExecuteSqlArray,
        --       not really needed as we are catching
        --       exceptions ourselves
        PERFORM bde_ExecuteSqlArray(
            p_upload,v_task,v_depsql
        );
    END IF;

    -- Record the update that has been applied

    v_task := 'Recording the upload';
    RAISE INFO '%', v_task;
    PERFORM _bde_RecordDatasetLoaded(
        p_upload,
        p_table_name,
        v_dataset,
        '0',
        p_incremental,
        p_bdetime,
        p_details,
        v_nins,
        v_nupd,
        0,
        v_ndel
    );

    RETURN 1;

EXCEPTION
    WHEN others THEN
        RAISE EXCEPTION
            'Level 0 update of table % from dataset % failed in %. Error: %',
            p_table_name, v_dataset, v_task, SQLERRM;
END
$$ LANGUAGE plpgsql;

ALTER FUNCTION bde_ApplyLevel0Update(INTEGER, NAME, TIMESTAMP, TEXT, BOOLEAN)
    OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION bde_CheckTableCount(
    p_upload INTEGER,
    p_table_name NAME
)
RETURNS
    BOOLEAN
AS
$$
DECLARE
    v_schema_name   NAME;
    v_table         REGCLASS;
    v_current_count BIGINT;
    v_new_count     BIGINT;
    v_expected      BIGINT;
    v_tol_error     REAL;
    v_tol_warn      REAL;
    v_status        BOOLEAN;
BEGIN
    v_status := TRUE;
    v_schema_name := bde_control.bde_BdeSchema(p_upload);
    v_table := bde_control.bde_TableOid(v_schema_name, p_table_name);

    IF v_table IS NULL THEN
        RAISE EXCEPTION 'Table ''%'' does not exist', p_table_name;
    END IF;

    SELECT
        row_tol_error,
        row_tol_warning
    INTO
        v_tol_error,
        v_tol_warn
    FROM
        bde_control.upload_table
    WHERE
        table_name  = p_table_name AND
        schema_name = v_schema_name;

    SELECT
        CAST(reltuples AS BIGINT)
    INTO
        v_current_count
    FROM
        pg_class
    WHERE
        oid = CAST(v_table AS OID);

    EXECUTE 'SELECT COUNT(*) FROM ' || v_table
    INTO v_new_count;

    IF v_tol_error IS NOT NULL THEN
        v_expected := CAST((v_current_count * v_tol_error) AS BIGINT);
        IF v_new_count < v_expected THEN
            RAISE EXCEPTION
                '% has % rows, when at least % are expected',
                v_table, v_new_count, v_expected;
        END IF;
    END IF;

    IF v_status AND v_tol_warn IS NOT NULL THEN
        v_expected := CAST((v_current_count * v_tol_warn) AS BIGINT);
        IF v_new_count < v_expected THEN
            RAISE WARNING '% has % rows, when at least % are expected',
                v_table, v_new_count, v_expected;
        END IF;
    END IF;

    RETURN v_status;
END;
$$
LANGUAGE plpgsql;

ALTER FUNCTION bde_CheckTableCount(INTEGER, NAME) OWNER TO bde_dba;

-- Function to expand a template, replacing %i% with the i'th element of the
-- array of parameters.

CREATE OR REPLACE FUNCTION bde_ExpandTemplate (
    p_template TEXT,
    p_params TEXT[]
)
RETURNS TEXT
AS
$body$
DECLARE
    v_expanded TEXT;
BEGIN
    v_expanded := p_template;
    FOR i IN 1 .. array_length(p_params,1) LOOP
        v_expanded := REPLACE( v_expanded, '%' || i || '%', p_params[i]);
    END LOOP;
    RETURN v_expanded;
END
$body$
LANGUAGE 'plpgsql';

ALTER FUNCTION bde_ExpandTemplate(TEXT, TEXT[]) OWNER TO bde_dba;

-- Function to execute a template as SQL, returning the number of rows
-- from GET DIAGNOSTICS ..

CREATE OR REPLACE FUNCTION bde_ExecuteTemplate(
    p_template TEXT,
    p_params TEXT[]
)
RETURNS BIGINT
AS
$body$
DECLARE
    v_sql TEXT;
    v_count BIGINT;
BEGIN
    v_sql := bde_ExpandTemplate( p_template, p_params );
    BEGIN
        EXECUTE v_sql;
    EXCEPTION
        WHEN others THEN
            RAISE EXCEPTION E'Error executing template SQL: %\nError: %',
            v_sql, SQLERRM;
    END;
    GET DIAGNOSTICS v_count=ROW_COUNT;
    RETURN v_count;
END
$body$
LANGUAGE 'plpgsql';

ALTER FUNCTION bde_ExecuteTemplate(TEXT, TEXT[]) OWNER TO bde_dba;

-- This is required because not all of the BDE Informix tables have
-- immutable key columns. There are known cases in the LOL
-- application where primary values are swapped. Plus we need to account
-- for data maintenance scripts anyway.

CREATE OR REPLACE FUNCTION _bde_FixChangedIncKeyRecords(
    p_bde_table      REGCLASS,
    p_inc_data_table REGCLASS,
    p_key_column     NAME
)
RETURNS
    BIGINT
AS
$$
DECLARE
    v_changed     BIGINT := 0;
    v_count       BIGINT;
    v_sql         TEXT;
    v_unique_key  TEXT;
    v_table_cols  table_version.ATTRIBUTE[];
    v_col         table_version.ATTRIBUTE;
BEGIN
    v_table_cols := table_version._ver_get_table_unique_constraint_columns(
        p_bde_table,
        p_key_column,
        FALSE
    );

    CREATE TEMP TABLE missed_updates_keys (id INTEGER);

    FOR v_col IN SELECT * FROM unnest(v_table_cols) LOOP
        v_count := bde_ExecuteTemplate(
            $sql$
            INSERT INTO missed_updates_keys
            SELECT
                CUR.%3% AS old_key_value
            FROM
                _tmp_inc_change AS INC,
                %1% AS NEW_DAT,
                %2% AS CUR
            WHERE
                INC.id = NEW_DAT.%3% AND
                NEW_DAT.%4% = CUR.%4% AND
                NEW_DAT.%3% <> CUR.%3% AND
                NOT EXISTS (
                    SELECT MISS.id
                    FROM   missed_updates_keys MISS
                    WHERE  MISS.id = CUR.%3%
                )
            $sql$,
            ARRAY[
                p_inc_data_table::TEXT,
                p_bde_table::TEXT,
                quote_ident(p_key_column),
                v_col.att_name
            ]
        );
        ANALYSE missed_updates_keys;
    END LOOP;

    SELECT COUNT(*) INTO v_changed FROM missed_updates_keys;

    IF (v_changed > 0) THEN
        INSERT INTO _tmp_inc_change(
            id
        )
        SELECT
            MISS.id
        FROM
            missed_updates_keys MISS
            LEFT JOIN _tmp_inc_change CHG ON (MISS.id = CHG.id)
        WHERE
            CHG.id IS NULL;

        GET DIAGNOSTICS v_changed = ROW_COUNT;
    END IF;

    DROP TABLE missed_updates_keys;

    RETURN v_changed;
END;
$$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_FixChangedIncKeyRecords(REGCLASS, REGCLASS, NAME)
    OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION _bde_CreateIncDeletes(
    p_bde_table      REGCLASS,
    p_inc_data_table REGCLASS,
    p_key_column     NAME
)
RETURNS
    BIGINT
AS
$$
BEGIN
    RETURN bde_ExecuteTemplate (
        $sql$
            INSERT INTO _tmp_inc_actions
            SELECT
                CHG.id,
                'D'
            FROM
                _tmp_inc_change CHG
                JOIN %1% AS CUR ON (CHG.id = CUR.%3%)
                LEFT JOIN %2% AS INC_DAT ON (CHG.id = INC_DAT.%3%)
            WHERE
                INC_DAT.%3% IS NULL
        $sql$,
        ARRAY[
            p_bde_table::TEXT,
            p_inc_data_table::TEXT,
            quote_ident(p_key_column)
        ]
    );
END;
$$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_CreateIncDeletes(REGCLASS, REGCLASS, NAME)
    OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION _bde_CreateIncInserts(
    p_bde_table      REGCLASS,
    p_inc_data_table REGCLASS,
    p_key_column     NAME
)
RETURNS
    BIGINT
AS
$$
BEGIN
    RETURN bde_ExecuteTemplate (
        $sql$
            INSERT INTO _tmp_inc_actions
            SELECT
                CHG.id,
                'I'
            FROM
                _tmp_inc_change CHG
                LEFT JOIN %1% AS CUR ON (CHG.id = CUR.%3%)
                JOIN %2% AS INC_DAT ON (CHG.id = INC_DAT.%3%)
            WHERE
                CUR.%3% IS NULL
        $sql$,
        ARRAY[
            p_bde_table::TEXT,
            p_inc_data_table::TEXT,
            quote_ident(p_key_column)
        ]
    );
END;
$$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_CreateIncInserts(REGCLASS, REGCLASS, NAME)
    OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION _bde_CreateIncUpdates(
    p_bde_table      REGCLASS,
    p_inc_data_table REGCLASS,
    p_key_column     NAME
)
RETURNS
    BIGINT
AS
$$
DECLARE
    v_sql             TEXT;
    v_null_compare1   TEXT;
    v_null_compare2   TEXT;
    v_unique_compare1 TEXT = quote_literal('');
    v_unique_compare2 TEXT = quote_literal('');
    v_update_col_txt  TEXT;
    v_table_cols      table_version.ATTRIBUTE[];
    v_col             table_version.ATTRIBUTE;
BEGIN
    v_table_cols := table_version._ver_get_table_columns(p_bde_table);
    IF v_table_cols IS NULL THEN
        RAISE EXCEPTION 'Could not find any table columns for %', p_bde_table;
    END IF;
    v_null_compare1 = table_version._ver_get_compare_sql(v_table_cols, 'INC_DAT');
    v_null_compare2 = table_version._ver_get_compare_sql(v_table_cols, 'CUR');

    v_table_cols := table_version._ver_get_table_unique_constraint_columns(
        p_bde_table,
        p_key_column
    );
    IF v_table_cols IS NOT NULL THEN
        v_unique_compare1 = table_version._ver_get_compare_sql(v_table_cols, 'INC_DAT');
        v_unique_compare2 = table_version._ver_get_compare_sql(v_table_cols, 'CUR');
    END IF;

    -- Insert action of '0' for updates that don't change data
    -- Insert action of 'X' for updates which replace a unique constraint column
    -- These 'X' action are processed as a delete followed by an insert
    -- operation. This ensures that the key column uniqueness is not compromised
    -- during the update.

    RETURN bde_ExecuteTemplate (
        $sql$
            INSERT INTO _tmp_inc_actions
            SELECT
                CHG.id,
                CASE WHEN (%4%) = (%5%) THEN
                    '0'
                WHEN (%6%) <> (%7%) THEN
                    'X'
                ELSE
                    'U'
                END
            FROM
                _tmp_inc_change CHG
                JOIN %1% AS CUR ON (CHG.id = CUR.%3%)
                JOIN %2% AS INC_DAT ON (CHG.id = INC_DAT.%3%)
        $sql$,
        ARRAY[
            p_bde_table::TEXT,
            p_inc_data_table::TEXT,
            quote_ident(p_key_column),
            v_null_compare1,
            v_null_compare2,
            v_unique_compare1,
            v_unique_compare2
        ]
    );
END;
$$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_CreateIncUpdates(REGCLASS, REGCLASS, NAME)
    OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION _bde_GetDependentObjectSql(
    p_upload INTEGER,
    p_base regclass
)
RETURNS
    text[]
AS
$body$
BEGIN
    RETURN ARRAY[]::text[];
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_GetDependentObjectSql(INTEGER, REGCLASS) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION bde_control._bde_GetOwnerAccessSql(
    p_reference regclass,
    p_target regclass
)
RETURNS
    text[]
AS
$body$
DECLARE
    v_result text[];
    v_sql text;
    v_rights varchar(16)[];
    v_right varchar(16);
    v_rolename name;
    v_grant varchar(20);
BEGIN
    v_result := ARRAY[]::text[];
    v_sql := 'ALTER TABLE ' || p_target || ' OWNER TO ' ||
        quote_ident((
            SELECT pg_get_userbyid(relowner)
            FROM pg_class where oid = p_reference
        ));
    v_result := array_append(v_result,v_sql);

    v_rights := ARRAY[
        'SELECT',
        'INSERT',
        'UPDATE',
        'DELETE',
        'TRUNCATE',
        'REFERENCES',
        'TRIGGER'
    ];

    FOR v_rolename IN
        SELECT AUTH.rolname
        FROM   pg_shdepend DEP,
               pg_authid AUTH,
               pg_database DB
        WHERE  DEP.dbid = DB.oid
        AND    DEP.objid = p_reference
        AND    AUTH.oid = DEP.refobjid
        AND    DEP.deptype='a'
        AND    DB.datname = current_database()
    LOOP
        FOR v_right IN SELECT * FROM unnest(v_rights)
        LOOP
            v_sql := '';
            v_grant := '';
            IF has_table_privilege(
                v_rolename, p_reference, v_right || ' WITH GRANT OPTION'
            ) THEN
                v_sql := v_right;
                v_grant := ' WITH GRANT OPTION';
            ELSIF has_table_privilege(v_rolename, p_reference, v_right) THEN
                v_sql := v_right;
            END IF;
            IF v_sql <> '' THEN
                v_sql := 'GRANT ' || v_sql || ' ON TABLE ' || p_target ||
                    ' TO ' || v_rolename || v_grant;
                v_result := array_append(v_result, v_sql);
            END IF;
        END LOOP;
    END LOOP;

    RETURN v_result;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_GetOwnerAccessSql(REGCLASS, REGCLASS) OWNER TO bde_dba;

-- Execute each of the SQL statements in p_sqlarray,
-- intercepting any exception by re-throwing it with
-- custom message (p_task) and full SQL included in
-- the re-thrown exception message.
CREATE OR REPLACE FUNCTION bde_ExecuteSqlArray(
    p_upload INTEGER,
    p_task text,
    p_sqlarray text[]
)
RETURNS
    INTEGER
AS
$body$
DECLARE
    v_result INTEGER;
    v_sql text;
BEGIN
    v_result := 1;

    FOR v_sql IN select * from unnest(p_sqlarray)
    LOOP
        BEGIN
            RAISE INFO 'In task %, executing SQL: %', p_task, v_sql;
            EXECUTE v_sql;
        EXCEPTION
            WHEN others THEN
                v_result := 0;
                RAISE EXCEPTION 'Error in task %. ERROR: %. SQL: %',
                    p_task, SQLERRM, v_sql;
        END;
    END LOOP;
    RETURN v_result;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_ExecuteSqlArray(INTEGER, TEXT, TEXT[]) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION _bde_CopySchemaInformation(
    p_upload INTEGER,
    p_bdetable regclass,
    p_tmptable regclass
)
RETURNS
    INTEGER
AS
$body$
DECLARE
    v_sql text;
    v_task text;
BEGIN

    -- Apply each of the constraints

    v_task := 'Applying constraints to temp table';
    v_sql := '';

    FOR v_sql IN
        SELECT conname || ' ' || pg_get_constraintdef(oid)
        FROM   pg_constraint
        WHERE  conrelid = p_bdetable
    LOOP
        RAISE INFO 'Adding constraint %', v_sql;
        v_sql := 'ALTER TABLE ' || p_tmptable || ' ADD CONSTRAINT ' || v_sql;
        EXECUTE v_sql;
        v_sql := '';
    END LOOP;

    -- And each index

    v_task := 'Generating indexes for temp table';

    FOR v_sql IN
        SELECT pg_get_indexdef(indexrelid)
        FROM   pg_index
        WHERE  indrelid = p_bdetable
        AND    indexrelid NOT IN (
            SELECT conindid FROM pg_constraint
            WHERE  conrelid = p_bdetable
            AND    contype IN ('u', 'p')
        )
    LOOP
        v_sql := regexp_replace(
            v_sql,E'(^.*\\sON\\s).*?(\\sUSING\\s.*$)',E'\\1' ||
            p_tmptable::text || E'\\2'
        );
        RAISE INFO 'Creating index %', v_sql;
        EXECUTE v_sql;
        v_sql := '';
    END LOOP;

    -- Copy columns statistics information

    v_task := 'Copying column statistics information';

    PERFORM _bde_CopyStatisticsInformation(p_upload,p_bdetable,p_tmptable);

    -- Copy ownership and access rights

    v_task := 'Copying ownership and access information';

    PERFORM bde_ExecuteSqlArray(p_upload,v_task,
        _bde_GetOwnerAccessSql(p_bdetable,p_tmptable));

    RETURN 0;

EXCEPTION
    WHEN others THEN
        RAISE EXCEPTION E'Failed in task %\n%\n%',v_task,v_sql,SQLERRM;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_CopySchemaInformation(INTEGER, regclass, regclass)
    OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION _bde_CopyStatisticsInformation(
    p_upload INTEGER,
    p_bdetable regclass,
    p_tmptable regclass
)
RETURNS
    INTEGER
AS
$body$
DECLARE
    v_sql text;
BEGIN
    FOR v_sql IN
        SELECT 'ALTER TABLE  ' || p_tmptable ||
                ' ALTER COLUMN ' || attname ||
                ' SET STATISTICS ' ||  attstattarget
            FROM pg_attribute
            WHERE attrelid = p_bdetable
            AND attisdropped IS FALSE
            AND attnum > 0 AND attstattarget > 0
    LOOP
        RAISE DEBUG 'Setting col stats %', v_sql;
        EXECUTE v_sql;
        v_sql := '';
    END LOOP;

    RETURN 0;
END;
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_CopyStatisticsInformation(INTEGER, regclass, regclass)
    OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION _bde_RunBdeFunctions(
    p_upload integer,
    p_prefix name,
    p_description text
)
RETURNS INTEGER
AS
$body$
DECLARE
    v_bdeSchema name;
    v_procname name;
    v_nproc integer;
    v_intid oid;
BEGIN
    v_bdeSchema := bde_BdeSchema(p_upload);
    v_nproc := 0;
    v_intid := (select oid from pg_type where typname='int4');
    FOR v_procname IN
        SELECT proname
        FROM pg_proc
        WHERE
            pronamespace = (
                SELECT oid
                FROM   pg_namespace
                WHERE  LOWER(nspname) = LOWER(v_bdeSchema)
            ) AND
            proname LIKE p_prefix || '%' AND
            pronargs = 1 AND
            proargtypes[0] = v_intid AND
            prorettype = v_intid
    LOOP
        RAISE DEBUG 'Running % task %', p_description, v_procname;
        BEGIN
            EXECUTE 'SELECT ' || v_bdeSchema || '.' || v_procname || '(' ||
                p_upload || ')';
            v_nproc := v_nproc + 1;
        EXCEPTION
        WHEN others THEN
            RAISE EXCEPTION '% task % failed: %', p_description, v_procname, SQLERRM;
        END;
    END LOOP;
    RETURN v_nproc;
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION _bde_RunBdeFunctions(INTEGER, NAME, TEXT) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION bde_ApplyPostLevel0Functions(
    p_upload INTEGER
)
RETURNS INTEGER
AS
$body$
BEGIN
    RETURN _bde_RunBdeFunctions(p_upload,'bde_postlevel0_','Post level 0');
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_ApplyPostLevel0Functions(INTEGER) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION bde_ApplyPostUploadFunctions
(
    p_upload INTEGER
)
RETURNS INTEGER
AS
$body$
BEGIN
    RETURN _bde_RunBdeFunctions(p_upload,'bde_postupload_','Post upload');
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_ApplyPostUploadFunctions(INTEGER) OWNER TO bde_dba;

-- Function bde_TablesAffected
--
--  BOOLEAN bde_TablesAffected(p_upload,tables,test)
--
--  Tests tables that are affected by an upload.
--
--  p_upload is the upload id of interest
--
--  tables is an array or space separated string of tables to check.
--
--  test is a space separated string specifying the test to apply and
--  can include the following items:
--
--  'any'
--      The test will return true if any of the tables meet the
--      criteria.
--
--  'all'
--      The test will return true only if all specified tables pass.
--      This is the default if neither 'any' or 'all' are specified.
--
--  'level0'
--      The test will apply only to level 0 files loaded in the upload.
--      It will return false if there are no level 0 files have been
--      uploaded.
--
--  'level0_dataset'
--      The test will apply only to level 0 files in the upload, or
--      level 0 files in other uploads from the same level 0 dataset.
--
--  'loaded'
--      The test will be true if the table has been loaded, even if this
--      doesn't change any data. The default is only to return true if a
--      table has been changed by the upload.
--
--  'affected'
--      The test will only be true if the table has been updated
--
--
--  Using this function may require multiple calls, for example to see
--  if all tables are loaded and any are affected

CREATE OR REPLACE FUNCTION bde_TablesAffected
(
    p_upload INTEGER,
    p_tables name[],
    p_test TEXT
)
RETURNS
    BOOLEAN
AS
$body$
DECLARE
    v_loadedok BOOLEAN;
    v_l5ok BOOLEAN;
    v_anyok BOOLEAN;
    v_option VARCHAR(32);
    v_table name;
    v_pass BOOLEAN;
    v_ok BOOLEAN;
BEGIN
    v_loadedok := FALSE;
    v_l5ok := TRUE;
    v_anyok := FALSE;
    v_ok := FALSE;

    FOR v_option IN
        SELECT * FROM regexp_split_to_table(lower(p_test),E'\\s+')
    LOOP
        IF v_option = 'all' THEN
            v_anyok = FALSE;
        ELSIF v_option = 'any' THEN
            v_anyok = TRUE;
        ELSIF v_option = 'level0' THEN
            v_l5ok = FALSE;
        ELSIF v_option = 'level0_dataset' THEN
            v_l5ok = FAlSE;
            IF NOT EXISTS (
                SELECT dataset
                FROM bde_control.upload_stats
                WHERE upl_id = p_upload
                AND type='0'
            )
            THEN
                RETURN v_ok;
            END IF;
        ELSIF v_option = 'loaded' THEN
            v_loadedok = TRUE;
        ELSIF v_option = 'affected' THEN
            v_loadedok = FALSE;
        END IF;
    END LOOP;

    -- Inefficient implementation, but fine for the context in which
    -- it is used.

    FOR v_table IN
        SELECT * FROM unnest(p_tables)
    LOOP
        v_ok := (
            SELECT
                COUNT(*) > 0
            FROM
                bde_control.upload_stats sts JOIN
                bde_control.upload_table tbl
                    ON sts.tbl_id = tbl.id
            WHERE
                tbl.schema_name = (
                    SELECT schema_name
                    FROM bde_control.upload
                    WHERE id = p_upload) AND
                LOWER(tbl.table_name) =  LOWER(v_table) AND
                (sts.upl_id = p_upload OR sts.dataset IN (
                    SELECT dataset
                    FROM bde_control.upload_stats
                    WHERE upl_id = p_upload AND tbl_id = tbl.id)) AND
                (sts.type = '0' OR v_l5ok) AND
                (
                    v_loadedok OR sts.ninsert > 0 OR
                    sts.nupdate > 0 or sts.ndelete > 0
                )
            );
        IF v_ok AND v_anyok THEN
            EXIT;
        END IF;
        IF NOT v_ok AND NOT v_anyok THEN
            EXIT;
        END IF;
    END LOOP;

    RETURN v_ok;

END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_TablesAffected(INTEGER, NAME[], TEXT) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION bde_TablesAffected
(
    p_upload INTEGER,
    p_tables TEXT,
    p_test TEXT
)
RETURNS
    BOOLEAN
AS
$body$
BEGIN
    RETURN bde_TablesAffected(p_upload,
        regexp_split_to_array(lower(p_tables),E'\\s+'),
        p_test );
END
$body$
LANGUAGE plpgsql;

ALTER FUNCTION bde_TablesAffected(INTEGER, TEXT, TEXT) OWNER TO bde_dba;


CREATE OR REPLACE FUNCTION bde_TableKeyIsValid(
    p_table      REGCLASS,
    p_key_column NAME
)
RETURNS BOOLEAN AS
$$
DECLARE
    v_exists BOOLEAN;
BEGIN
    SELECT
        TRUE
    INTO
        v_exists
    FROM
        pg_index IDX,
        pg_attribute ATT
    WHERE
        IDX.indrelid = p_table AND
        ATT.attrelid = p_table AND
        ATT.attnum = ANY(IDX.indkey) AND
        ATT.attnotnull = TRUE AND
        IDX.indisunique = TRUE AND
        IDX.indexprs IS NULL AND
        IDX.indpred IS NULL AND
        format_type(ATT.atttypid, ATT.atttypmod) IN ('integer', 'bigint') AND
        array_length(IDX.indkey::INTEGER[], 1) = 1 AND
        LOWER(ATT.attname) = LOWER(p_key_column)
    ORDER BY
        IDX.indisprimary DESC;

    IF v_exists IS NULL THEN
        v_exists := FALSE;
    END IF;

    RETURN v_exists;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION bde_TableKeyIsValid(REGCLASS, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_TableKeyIsValid(REGCLASS, NAME) FROM PUBLIC;

CREATE OR REPLACE FUNCTION bde_CreateDatasetRevision(
    p_upload INTEGER
)
RETURNS INTEGER AS
$$
DECLARE
    v_revision   INTEGER;
    v_dataset    TEXT;
    v_dataset_ts TIMESTAMP;
BEGIN
    v_dataset := bde_GetOption(p_upload, '_dataset');
    IF v_dataset IS NULL OR v_dataset = '(undefined dataset)' THEN
        RAISE EXCEPTION 'A dataset has not been defined for this upload yet';
    END IF;

    BEGIN
        SELECT
            CAST(
                substr(v_dataset, 1, 4) || '-' ||
                substr(v_dataset, 5, 2) || '-' ||
                substr(v_dataset, 7, 2) || ' ' ||
                substr(v_dataset, 9, 2) || ':' ||
                substr(v_dataset, 11, 2) || ':' ||
                substr(v_dataset, 13, 2)
            AS TIMESTAMP)
        INTO
            v_dataset_ts;
    EXCEPTION
        WHEN others THEN
            RAISE EXCEPTION 'Dataset string ''%'' is malformed', v_dataset;
    END;

    SELECT table_version.ver_create_revision(
        'BDE upload for dataset ' || v_dataset,
        v_dataset_ts
    )
    INTO  v_revision;

    PERFORM bde_SetOption(p_upload, '_revision', CAST(v_revision AS TEXT));

    RETURN v_revision;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION bde_CreateDatasetRevision(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_CreateDatasetRevision(INTEGER)  FROM PUBLIC;

CREATE OR REPLACE FUNCTION bde_CompleteDatasetRevision(
    p_upload INTEGER
)
RETURNS BOOLEAN AS
$$
DECLARE
    v_revision       INTEGER;
    v_status         BOOLEAN;
    v_dataset        TEXT;
    v_tables_updated BIGINT;
BEGIN

    v_dataset := bde_GetOption(p_upload, '_dataset');

    RAISE INFO 'Completing dataset revision for dataset %', v_dataset;

    IF v_dataset IS NULL OR v_dataset = '(undefined dataset)' THEN
        RAISE EXCEPTION 'A dataset has not been defined for this upload yet';
    END IF;

    v_revision := CAST(bde_GetOption(p_upload, '_revision') AS INTEGER);
    IF v_revision IS NULL THEN
        RAISE EXCEPTION 'There is no revision in progress';
    END IF;

    SELECT table_version.ver_complete_revision()
    INTO   v_status;

    IF v_status THEN
        SELECT
            count(*)
        INTO
            v_tables_updated
        FROM
            bde_control.upload_stats STS
            JOIN bde_control.upload_table TBL ON (TBL.id = STS.tbl_id)
            JOIN table_version.ver_get_modified_tables(v_revision) MTB  ON (
                MTB.schema_name = TBL.schema_name AND
                MTB.table_name = TBL.table_name
            )
        WHERE
            STS.upl_id = p_upload AND
            STS.dataset = v_dataset;

        IF v_tables_updated = 0 THEN
            IF NOT (SELECT table_version.ver_delete_revision(v_revision)) THEN
                RAISE WARNING 'Can not delete unused revision %', v_revision;
            END IF;
        END IF;
    END IF;

    PERFORM bde_SetOption(p_upload,'_revision', NULL);

    RAISE INFO 'Finished completing dataset revision for dataset %', v_dataset;

    RETURN v_status;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION bde_CompleteDatasetRevision(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_CompleteDatasetRevision(INTEGER)  FROM PUBLIC;

DO $$
DECLARE
    v_comment TEXT;
    v_pcid    TEXT;
    v_schema  TEXT = 'bde_control';
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

        v_comment := 'Version: ' ||  '$Id$'
                    || E'\n' || 'Installed: ' ||
                    to_char(current_timestamp,'YYYY-MM-DD HH:MI') || v_comment;

        EXECUTE 'COMMENT ON FUNCTION ' || v_schema || '.' || v_pcid || ' IS '
            || quote_literal( v_comment );
    END LOOP;
END
$$;

COMMIT;
