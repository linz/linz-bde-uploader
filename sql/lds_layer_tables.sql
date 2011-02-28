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
-- Creates LINZ Data Service (LDS) simplified Landonline layers tables
--------------------------------------------------------------------------------

DROP SCHEMA IF EXISTS lds CASCADE;
CREATE SCHEMA lds AUTHORIZATION bde_dba;

GRANT USAGE ON SCHEMA lds TO bde_admin;
GRANT USAGE ON SCHEMA lds TO bde_user;

COMMENT ON SCHEMA lds IS 'Schema for LDS simplified Landonline layers';

SET search_path = lds, bde, public;

--------------------------------------------------------------------------------
-- LDS table geodetic_marks
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS geodetic_marks CASCADE;

CREATE TABLE geodetic_marks (
    id INTEGER NOT NULL,
    geodetic_code CHAR(4) NOT NULL,
    current_mark_name TEXT,
    description TEXT,
    mark_type VARCHAR(4),
    beacon_type TEXT,
    mark_condition TEXT,
    "order" INTEGER NOT NULL,
    land_district VARCHAR(100),
    latitude NUMERIC(22,12) NOT NULL,
    longitude NUMERIC(22,12) NOT NULL,
    ellipsoidal_height NUMERIC(22,12) NULL
);
SELECT AddGeometryColumn('geodetic_marks', 'shape', 4167, 'POINT', 2);

ALTER TABLE geodetic_marks ADD PRIMARY KEY (id);
CREATE INDEX shx_geo_shape ON geodetic_marks USING gist (shape);

ALTER TABLE geodetic_marks OWNER TO bde_dba;

REVOKE ALL ON TABLE geodetic_marks FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE geodetic_marks TO bde_admin;
GRANT SELECT ON TABLE geodetic_marks TO bde_user;

--------------------------------------------------------------------------------
-- LDS table geodetic_vertical_marks
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS geodetic_vertical_marks CASCADE;

CREATE TABLE geodetic_vertical_marks(
    id INTEGER NOT NULL,
    nod_id INTEGER NOT NULL,
    geodetic_code CHAR(4) NOT NULL,
    current_mark_name TEXT,
    description TEXT,
    mark_type VARCHAR(4),
    beacon_type TEXT,
    mark_condition TEXT,
    "order" CHAR(2) NOT NULL,
    land_district VARCHAR(100),
    normal_orthometric_height NUMERIC(22, 12),
    coordinate_system VARCHAR(100) NOT NULL
);
SELECT AddGeometryColumn( 'geodetic_vertical_marks', 'shape', 4167, 'POINT', 2);

CREATE SEQUENCE geodetic_vertical_marks_id_seq;
ALTER TABLE geodetic_vertical_marks_id_seq OWNER TO bde_dba;
ALTER TABLE geodetic_vertical_marks ALTER COLUMN id SET DEFAULT nextval('geodetic_vertical_marks_id_seq');

ALTER TABLE geodetic_vertical_marks ADD PRIMARY KEY (id);
CREATE INDEX shx_geo_vert_shape ON geodetic_vertical_marks USING gist (shape);

ALTER TABLE geodetic_vertical_marks OWNER TO bde_dba;

REVOKE ALL ON TABLE geodetic_vertical_marks FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE geodetic_vertical_marks TO bde_admin;
GRANT SELECT ON TABLE geodetic_vertical_marks TO bde_user;

