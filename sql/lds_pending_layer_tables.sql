-------------------------------------------------------------------------------
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
-- Creates LINZ Data Service (LDS) simplified Landonline layers tables
--------------------------------------------------------------------------------
SET client_min_messages TO WARNING;

DO $SCHEMA$
BEGIN

IF EXISTS (SELECT * FROM pg_namespace where LOWER(nspname) = 'lds_ext') THEN
    RETURN;
END IF;

CREATE SCHEMA lds_ext AUTHORIZATION bde_dba;

GRANT USAGE ON SCHEMA lds TO bde_admin;
GRANT USAGE ON SCHEMA lds TO bde_user;

COMMENT ON SCHEMA lds IS 'Schema for LDS pending simplified Landonline layers';

SET search_path = lds_ext, lds, bde, public;

--------------------------------------------------------------------------------
-- LDS table pending_parcels
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS pending_parcels CASCADE;

CREATE TABLE pending_parcels (
    id INTEGER NOT NULL,
    appellation VARCHAR(2048),
    affected_surveys VARCHAR(2048),
    parcel_intent VARCHAR(100) NOT NULL,
    topology_type VARCHAR(100) NOT NULL,
	status VARCHAR(25) NOT NULL,
    statutory_actions VARCHAR(4096),
    land_district VARCHAR(100) NOT NULL,
    titles VARCHAR(32768),
    survey_area NUMERIC(20, 4),
    calc_area NUMERIC(20, 0) NOT NULL
);
PERFORM AddGeometryColumn('pending_parcels', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE pending_parcels ADD PRIMARY KEY (id);
CREATE INDEX shx_all_pend_par_shape ON pending_parcels USING gist (shape);

ALTER TABLE pending_parcels OWNER TO bde_dba;

REVOKE ALL ON TABLE pending_parcels FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE pending_parcels TO bde_admin;
GRANT SELECT ON TABLE pending_parcels TO bde_user;

ANALYSE pending_parcels;

--------------------------------------------------------------------------------
-- LDS table pending_linear_parcels
--------------------------------------------------------------------------------


DROP TABLE IF EXISTS pending_linear_parcels CASCADE;

CREATE TABLE pending_linear_parcels (
    id INTEGER NOT NULL,
    appellation VARCHAR(2048),
    affected_surveys VARCHAR(2048),
    parcel_intent VARCHAR(100) NOT NULL,
    topology_type VARCHAR(100) NOT NULL,
	status VARCHAR(25) NOT NULL,
    statutory_actions VARCHAR(4096),
    land_district VARCHAR(100) NOT NULL,
    titles VARCHAR(32768),
    survey_area NUMERIC(20, 4),
    calc_area NUMERIC(20, 0)
);
PERFORM AddGeometryColumn('pending_linear_parcels', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE pending_linear_parcels ADD PRIMARY KEY (id);
CREATE INDEX shx_lin_pend_par_shape ON pending_linear_parcels USING gist (shape);

ALTER TABLE pending_linear_parcels OWNER TO bde_dba;

REVOKE ALL ON TABLE pending_linear_parcels FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE pending_linear_parcels TO bde_admin;
GRANT SELECT ON TABLE pending_linear_parcels TO bde_user;

ANALYSE pending_linear_parcels;

--------------------------------------------------------------------------------



END
$SCHEMA$;
