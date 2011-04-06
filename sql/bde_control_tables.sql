--------------------------------------------------------------------------------
--
-- $Id$
--
-- linz_bde_loader -  LINZ BDE loader for PostgreSQL
--
-- Copyright 2011 Crown copyright (c)
-- Land Information New Zealand and the New Zealand Government.
-- All rights reserved
--
-- This program is released under the terms of the new BSD license. See the 
-- LICENSE file for more information.
--
--------------------------------------------------------------------------------
-- Creates system tables required for linz_bde_loader
--------------------------------------------------------------------------------
SET client_min_messages TO WARNING;

DO $SCHEMA$
BEGIN

IF EXISTS (SELECT * FROM pg_namespace where LOWER(nspname) = 'bde_control') THEN
    RETURN;
END IF;

CREATE SCHEMA bde_control AUTHORIZATION bde_dba;

GRANT USAGE ON SCHEMA bde_control TO bde_admin;
GRANT USAGE ON SCHEMA bde_control TO bde_user;

SET SEARCH_PATH TO bde_control, public;

-- bde_control.upload
--

CREATE TABLE upload
(
	id SERIAL NOT NULL PRIMARY KEY,
	schema_name name NOT NULL,
	start_time TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
	end_time TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
	status CHAR(1) NOT NULL DEFAULT 'U'
);

ALTER TABLE upload OWNER TO bde_dba;
REVOKE ALL ON TABLE upload FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE upload TO bde_admin;
GRANT SELECT ON TABLE upload TO bde_user;


COMMENT ON TABLE upload IS
$comment$
Defines an upload job.  Each upload job may upload multipled BDE datasets
to multiple tables.  The tables will all be in a single BDE schema, defined
in this table.

Each upload job is identified by an id.  When the upload is being applied
the working files are placed in a temporary schema named after this id 
(bde_upload_##) where ## is the id.

end_time will be periodically updated during the running of the job and will
be used to determine if the job is still active

status values are U (uninitiallized), A (active), C (completed successfully), and E (completed with errors)

$comment$;

-- bde_control.tables

CREATE TABLE upload_table
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

ALTER TABLE upload_table OWNER TO bde_dba;
REVOKE ALL ON TABLE upload_table FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE upload_table TO bde_admin;
GRANT SELECT ON TABLE upload_table TO bde_user;

COMMENT ON TABLE upload_table IS
$comment$
Tracks the status of uploads for each table
table_key_column is unique non-composite, not null integer or bigint column used for identifying the table row for incremental updates. This identifier must be the same as defined in the cbe_tables.tablekeycolumn
last_upload_id is the id of the last upload job affecting this table
last_upload_dataset is the dataset id of the last level 5 or 0 uploaded (since the level 0 upload will override any level 5 uploads that have been applied).
last_upload_type is either 0 or 5
last_upload_incremental is true if the table data was updated, and false if the table data was completely refreshed from a level 0
last_upload_details is a text string with details of the last upload
   (currently will contain constituent files and end times, for checking L5 uploads against.)
last_upload_time records when the upload was applied
last_upload_bdetime is a timestamp for the last BDE upload applied
last_level0_dataset is the dataset id of the last level 0 uploaded
upl_id_lock is the id of an upload currently locking the table (this is
  in the sense of a process lock, not a database lock)
row_tol_warning is the minimum ratio of the number of number of rows in the table after a level 0 update  to the number before the update before a warning is logged
row_tol_error is the minimum ratio of the number of number of rows in the table after a level 0 update  to the number before the update before an exception is thrown
$comment$;
-- upload_stats

CREATE TABLE upload_stats
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

CREATE INDEX idx_sts_tbl ON upload_stats ( tbl_id );
CREATE INDEX idx_sts_upl ON upload_stats ( upl_id );

ALTER TABLE upload_stats OWNER TO bde_dba;
REVOKE ALL ON TABLE upload_stats FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE upload_stats TO bde_admin;
GRANT SELECT ON TABLE upload_stats TO bde_user;

COMMENT ON TABLE upload_stats IS
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

-- upload_log

CREATE TABLE upload_log
(
	id SERIAL NOT NULL PRIMARY KEY,
	upl_id INT NOT NULL,
	type CHAR(1) NOT NULL DEFAULT 'I',
	message_time TIMESTAMP NOT NULL DEFAULT clock_timestamp()::timestamp,
	message TEXT NOT NULL
	
);

CREATE INDEX idx_log_upl ON upload_log( upl_id );

ALTER TABLE upload_log OWNER TO bde_dba;
REVOKE ALL ON TABLE upload_log FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE upload_log TO bde_admin;
GRANT SELECT ON TABLE upload_log TO bde_user;

COMMENT ON TABLE upload_log IS
$comment$
Log messages generated by uploads.  
Type can be one of I (information), W (warning), E (error)
Also number '1', '2', ... can be used to denote more verbose informational
messages
$comment$;

END;
$SCHEMA$
