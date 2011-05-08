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
-- Creates system tables required for table versioning support
--------------------------------------------------------------------------------
SET client_min_messages TO WARNING;

DO $SCHEMA$
BEGIN

IF EXISTS (SELECT * FROM pg_namespace where LOWER(nspname) = 'table_version') THEN
    RETURN;
END IF;

CREATE SCHEMA table_version AUTHORIZATION bde_dba;

SET search_path = table_version, public;

DROP TABLE IF EXISTS tables_changed;
DROP TABLE IF EXISTS versioned_tables;
DROP TABLE IF EXISTS revision;

CREATE TABLE revision (
    id SERIAL NOT NULL PRIMARY KEY,
    revision_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    start_time TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
    schema_change BOOLEAN NOT NULL,
    comment TEXT
);

PERFORM setval('table_version.revision_id_seq', 1000, true);

ALTER TABLE revision OWNER TO bde_dba;
REVOKE ALL ON TABLE revision FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE revision TO bde_admin;
GRANT SELECT ON TABLE revision TO bde_user;

COMMENT ON TABLE revision IS $$
Defines a revision represents a amendment to table or series of tables held within the 
database. Each revision is identified by an id.  

The revision_time is the datetime of the revision. In the context of LINZ BDE this datetime when
the data from unloaded from the Landonline database.

The start_time is the datetime of when the revision record was created.
$$;

CREATE TABLE versioned_tables (	
    id SERIAL NOT NULL PRIMARY KEY,
    schema_name NAME NOT NULL,
    table_name NAME NOT NULL,
    key_column VARCHAR(64) NOT NULL,
    versioned BOOLEAN NOT NULL,
    CONSTRAINT versioned_tables_name_key UNIQUE (schema_name, table_name)
);

ALTER TABLE versioned_tables OWNER TO bde_dba;
REVOKE ALL ON TABLE versioned_tables FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE versioned_tables TO bde_admin;
GRANT SELECT ON TABLE versioned_tables TO bde_user;

COMMENT ON TABLE versioned_tables IS $$
Defines if a table is versioned. Each table is identified by an id. 

The column used to define primary key
for the table is defined in key_column. This key does not actually have to be table primary key, rather it
needs to be a unique non-composite integer or bigint column.
$$;

CREATE TABLE tables_changed (
    revision INTEGER NOT NULL REFERENCES revision,
    table_id INTEGER NOT NULL REFERENCES versioned_tables,
    PRIMARY KEY (revision, table_id)
);

ALTER TABLE tables_changed OWNER TO bde_dba;
REVOKE ALL ON TABLE tables_changed FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE tables_changed TO bde_admin;
GRANT SELECT ON TABLE tables_changed TO bde_user;

COMMENT ON TABLE tables_changed IS $$
Defines which tables are modified by a given revision.
$$;


END;
$SCHEMA$;

