--------------------------------------------------------------------------------
--
-- linz_bde_uploader -  LINZ BDE uploader for PostgreSQL
--
-- Copyright 2016 Crown copyright (c)
-- Land Information New Zealand and the New Zealand Government.
-- All rights reserved
--
-- This program is released under the terms of the new BSD license. See the
-- LICENSE file for more information.
--
--------------------------------------------------------------------------------
-- Creates system tables required for linz_bde_uploader
--------------------------------------------------------------------------------

DO $SCHEMA$
BEGIN

-- Utility function to implement CREATE INDEX IF NOT EXISTS for
-- PostgreSQL versions lower than 9.5 (where the syntax was introduced)
--
CREATE FUNCTION pg_temp.createIndexIfNotExists(p_name name, p_schema name, p_table name, p_column name)
RETURNS VOID LANGUAGE 'plpgsql' AS $$
BEGIN
    IF NOT EXISTS ( SELECT c.oid
                FROM pg_class c, pg_namespace n
                WHERE c.relname = p_name
                  AND c.relkind = 'i'
                  AND c.relnamespace = n.oid
                  AND n.nspname = p_schema )
    THEN
        EXECUTE format('CREATE INDEX %1I ON %2I.%3I (%4I)', p_name, p_schema, p_table, p_column);
        EXECUTE format('ALTER INDEX %1I OWNER TO bde_dba', p_name);
    END IF;
END;
$$;

CREATE SCHEMA IF NOT EXISTS bde_control;
ALTER SCHEMA bde_control OWNER TO bde_dba;

-- bde_control.upload
--

CREATE TABLE IF NOT EXISTS bde_control.upload
(
    id SERIAL NOT NULL PRIMARY KEY,
    schema_name name NOT NULL,
    start_time TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
    end_time TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
    status CHAR(1) NOT NULL DEFAULT 'U'
);

ALTER TABLE bde_control.upload OWNER TO bde_dba;

COMMENT ON TABLE bde_control.upload IS
$comment$
Defines an upload job.  Each upload job may upload multipled BDE datasets
to multiple tables.  The tables will all be in a single BDE schema, defined
in this table.

Each upload job is identified by an id.  When the upload is being applied
the working files are placed in a temporary schema named after this id
(bde_upload_##) where ## is the id.

end_time will be periodically updated during the running of the job and will
be used to determine if the job is still active.

status values are:
  U (uninitialized)
  A (active)
  C (completed successfully)
  E (completed with errors)

$comment$;

-- bde_control.tables

CREATE TABLE IF NOT EXISTS bde_control.upload_table
(
    id SERIAL NOT NULL PRIMARY KEY,
    schema_name name NOT NULL,
    table_name name NOT NULL,
    key_column NAME,
    last_upload_id INT,
    last_upload_dataset VARCHAR(14),
    last_upload_type CHAR(1),
    last_upload_incremental BOOLEAN,
    last_upload_details TEXT,
    last_upload_time TIMESTAMP,
    last_upload_bdetime TIMESTAMP,
    last_level0_dataset VARCHAR(14),
    upl_id_lock INT,
    row_tol_warning FLOAT CHECK (row_tol_warning BETWEEN 0 AND 1),
    row_tol_error FLOAT CHECK (row_tol_error BETWEEN 0 AND 1),
    UNIQUE (schema_name,table_name)
);

ALTER TABLE bde_control.upload_table OWNER TO bde_dba;

COMMENT ON TABLE bde_control.upload_table IS
'Tracks the status of uploads for each table.';

COMMENT ON COLUMN bde_control.upload_table.key_column IS $comment$
the name of a unique non-composite, not null
integer or bigint column used for identifying the table row for
incremental updates. This identifier must be the same as defined in
the cbe_tables.tablekeycolumn field of the Landonline INFORMIX
database.
$comment$;

COMMENT ON COLUMN bde_control.upload_table.last_upload_id IS $comment$
the id of the last upload job affecting this table,
referencing the id field of the upload table.
$comment$;

COMMENT ON COLUMN bde_control.upload_table.last_upload_dataset IS $comment$
the dataset id of the last level 5 or 0 uploaded
(since the level 0 upload will override any level 5 uploads that have
been applied).
$comment$;

COMMENT ON COLUMN bde_control.upload_table.last_upload_type IS $comment$
either 0 or 5.
$comment$;

COMMENT ON COLUMN bde_control.upload_table.last_upload_incremental IS $comment$
true if the table data was updated, and
false if the table data was completely refreshed from a level 0.
$comment$;

COMMENT ON COLUMN bde_control.upload_table.last_upload_details IS $comment$
a text string with details of the last upload (currently will contain
constituent files and end times, for checking L5 uploads against).
$comment$;

COMMENT ON COLUMN bde_control.upload_table.last_upload_time IS $comment$
records when the table upload was started.
$comment$;

COMMENT ON COLUMN bde_control.upload_table.last_upload_bdetime IS $comment$
timestamp found in the last BDE file uploaded.
$comment$;

COMMENT ON COLUMN bde_control.upload_table.last_level0_dataset IS $comment$
the dataset id of the last level 0 uploaded.
$comment$;

COMMENT ON COLUMN bde_control.upload_table.upl_id_lock IS $comment$
the id of an upload currently locking the table (this is
in the sense of a process lock, not a database lock).
$comment$;

COMMENT ON COLUMN bde_control.upload_table.row_tol_warning IS $comment$
the maximum tolerated change in row count during a -full-incremental
update  before a warning is raised, expressed as the ratio of new to
old rows count.
$comment$;

COMMENT ON COLUMN bde_control.upload_table.row_tol_error IS $comment$
the maximum tolerated change in row count during a -full-incremental
update  before an exception is thrown, expressed as
the ratio of new to old rows count.
$comment$;

-- upload_stats
CREATE TABLE IF NOT EXISTS bde_control.upload_stats
(
    id SERIAL NOT NULL PRIMARY KEY,
    upl_id INT NOT NULL,
    tbl_id INT NOT NULL,
    type CHAR(1) NOT NULL,
    incremental BOOLEAN NOT NULL DEFAULT TRUE,
    dataset VARCHAR(14) NOT NULL,
    upload_time TIMESTAMP NOT NULL DEFAULT clock_timestamp()::timestamp,
    duration INTERVAL,
    ninsert BIGINT NOT NULL DEFAULT 0,
    nupdate BIGINT NOT NULL DEFAULT 0,
    nnullupdate BIGINT NOT NULL DEFAULT 0,
    ndelete BIGINT NOT NULL DEFAULT 0
);

PERFORM pg_temp.createIndexIfNotExists('idx_sts_tbl', 'bde_control', 'upload_stats', 'tbl_id');
PERFORM pg_temp.createIndexIfNotExists('idx_sts_upl', 'bde_control', 'upload_stats', 'upl_id');

ALTER TABLE bde_control.upload_stats OWNER TO bde_dba;

COMMENT ON TABLE bde_control.upload_stats IS
$comment$
Statistics from uploads.
tbl_id is used to identify the table being uploaded.
type is the data set type, 0 (level 0) or 5 (level 5)
incremental is true if the table data was updated, and false if the table data was completely refreshed from a level 0
dataset is the name of the upload dataset (yyyymmddhhmmss)
upload_time is the time at which the upload was recorded
duration is the time of the event
ninsert is the number of records inserted
nupdate is the number of records updated
nnullupdate is the number records that had an incremental update but had no new data.
ndelete is the number of records deleted
$comment$;

--------------------------------------------------------------------------------
-- Fix up permissions on schema
--------------------------------------------------------------------------------

GRANT ALL ON SCHEMA bde_control TO bde_dba;
GRANT USAGE ON SCHEMA bde_control TO bde_admin;
GRANT USAGE ON SCHEMA bde_control TO bde_user;

REVOKE ALL
    ON ALL TABLES IN SCHEMA bde_control
    FROM public;

GRANT ALL
    ON ALL TABLES IN SCHEMA bde_control
    TO bde_dba;

GRANT SELECT, UPDATE, INSERT, DELETE
    ON ALL TABLES IN SCHEMA bde_control
    TO bde_admin;

GRANT SELECT
    ON ALL TABLES IN SCHEMA bde_control
    TO bde_user;

--------------------------------------------------------------------------------
-- Cleanup
--------------------------------------------------------------------------------

DROP FUNCTION pg_temp.createIndexIfNotExists(name, name, name, name);

END;
$SCHEMA$;
