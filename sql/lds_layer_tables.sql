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
SET client_min_messages TO WARNING;

DO $SCHEMA$
BEGIN

IF EXISTS (SELECT * FROM pg_namespace where LOWER(nspname) = 'lds') THEN
    RETURN;
END IF;

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
PERFORM AddGeometryColumn('geodetic_marks', 'shape', 4167, 'POINT', 2);

ALTER TABLE geodetic_marks ADD PRIMARY KEY (id);
CREATE INDEX shx_geo_shape ON geodetic_marks USING gist (shape);

ALTER TABLE geodetic_marks OWNER TO bde_dba;

REVOKE ALL ON TABLE geodetic_marks FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE geodetic_marks TO bde_admin;
GRANT SELECT ON TABLE geodetic_marks TO bde_user;

--------------------------------------------------------------------------------
-- LDS table geodetic_network_marks
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS geodetic_network_marks CASCADE;

CREATE TABLE geodetic_network_marks (
    id INTEGER NOT NULL,
    nod_id INTEGER NOT NULL,
    geodetic_code CHAR(4) NOT NULL,
    geodetic_network VARCHAR(4) NOT NULL,
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
PERFORM AddGeometryColumn('geodetic_network_marks', 'shape', 4167, 'POINT', 2);

CREATE SEQUENCE geodetic_network_marks_id_seq;
ALTER TABLE geodetic_network_marks_id_seq OWNER TO bde_dba;
ALTER TABLE geodetic_network_marks ALTER COLUMN id SET DEFAULT nextval('geodetic_network_marks_id_seq');

ALTER TABLE geodetic_network_marks ADD PRIMARY KEY (id);
CREATE UNIQUE INDEX idx_geo_net_mrk_nod_net ON geodetic_network_marks (nod_id, geodetic_network);
CREATE INDEX shx_geo_net_shape ON geodetic_network_marks USING gist (shape);

ALTER TABLE geodetic_network_marks OWNER TO bde_dba;

REVOKE ALL ON TABLE geodetic_network_marks FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE geodetic_network_marks TO bde_admin;
GRANT SELECT ON TABLE geodetic_network_marks TO bde_user;

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
PERFORM AddGeometryColumn('geodetic_vertical_marks', 'shape', 4167, 'POINT', 2);

ALTER TABLE geodetic_vertical_marks ADD UNIQUE (nod_id, coordinate_system);

CREATE SEQUENCE geodetic_vertical_marks_id_seq;
ALTER TABLE geodetic_vertical_marks_id_seq OWNER TO bde_dba;
ALTER TABLE geodetic_vertical_marks ALTER COLUMN id SET DEFAULT nextval('geodetic_vertical_marks_id_seq');

ALTER TABLE geodetic_vertical_marks ADD PRIMARY KEY (id);
CREATE INDEX shx_geo_vert_shape ON geodetic_vertical_marks USING gist (shape);

ALTER TABLE geodetic_vertical_marks OWNER TO bde_dba;

REVOKE ALL ON TABLE geodetic_vertical_marks FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE geodetic_vertical_marks TO bde_admin;
GRANT SELECT ON TABLE geodetic_vertical_marks TO bde_user;

--------------------------------------------------------------------------------
-- LDS table geodetic_antarctic_marks
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS geodetic_antarctic_marks CASCADE;

CREATE TABLE geodetic_antarctic_marks (
    id INTEGER NOT NULL,
    geodetic_code CHAR(4) NOT NULL,
    current_mark_name TEXT,
    description TEXT,
    mark_type VARCHAR(4),
    beacon_type TEXT,
    mark_condition TEXT,
    "order" INTEGER NOT NULL,
    latitude NUMERIC(22,12) NOT NULL,
    longitude NUMERIC(22,12) NOT NULL,
    ellipsoidal_height NUMERIC(22,12) NULL
);
PERFORM AddGeometryColumn('geodetic_antarctic_marks', 'shape', 4764, 'POINT', 2);

ALTER TABLE geodetic_antarctic_marks ADD PRIMARY KEY (id);
CREATE INDEX shx_geo_ant_shape ON geodetic_antarctic_marks USING gist (shape);

ALTER TABLE geodetic_antarctic_marks OWNER TO bde_dba;

REVOKE ALL ON TABLE geodetic_antarctic_marks FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE geodetic_antarctic_marks TO bde_admin;
GRANT SELECT ON TABLE geodetic_antarctic_marks TO bde_user;

--------------------------------------------------------------------------------
-- LDS table geodetic_antarctic_vertical_marks
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS geodetic_antarctic_vertical_marks CASCADE;

CREATE TABLE geodetic_antarctic_vertical_marks(
    id INTEGER NOT NULL,
    nod_id INTEGER NOT NULL,
    geodetic_code CHAR(4) NOT NULL,
    current_mark_name TEXT,
    description TEXT,
    mark_type VARCHAR(4),
    beacon_type TEXT,
    mark_condition TEXT,
    "order" CHAR(2) NOT NULL,
    normal_orthometric_height NUMERIC(22, 12),
    coordinate_system VARCHAR(100) NOT NULL
);
PERFORM AddGeometryColumn('geodetic_antarctic_vertical_marks', 'shape', 4764, 'POINT', 2);

ALTER TABLE geodetic_antarctic_vertical_marks ADD UNIQUE (nod_id, coordinate_system);

CREATE SEQUENCE geodetic_antarctic_vertical_marks_id_seq;
ALTER TABLE geodetic_antarctic_vertical_marks_id_seq OWNER TO bde_dba;
ALTER TABLE geodetic_antarctic_vertical_marks ALTER COLUMN id SET DEFAULT nextval('geodetic_antarctic_vertical_marks_id_seq');

ALTER TABLE geodetic_antarctic_vertical_marks ADD PRIMARY KEY (id);
CREATE INDEX shx_geo_ant_vert_shape ON geodetic_antarctic_vertical_marks USING gist (shape);

ALTER TABLE geodetic_antarctic_vertical_marks OWNER TO bde_dba;

REVOKE ALL ON TABLE geodetic_antarctic_vertical_marks FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE geodetic_antarctic_vertical_marks TO bde_admin;
GRANT SELECT ON TABLE geodetic_antarctic_vertical_marks TO bde_user;

--------------------------------------------------------------------------------
-- LDS table primary_parcels
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS primary_parcels CASCADE;

CREATE TABLE primary_parcels (
    id INTEGER NOT NULL,
    appellation TEXT,
    affected_surveys TEXT,
    parcel_intent TEXT NOT NULL,
    topology_type VARCHAR(100) NOT NULL,
    statutory_actions TEXT,
    land_district VARCHAR(100) NOT NULL,
    titles TEXT,
    survey_area NUMERIC(20, 4),
    calc_area NUMERIC(20, 4) NOT NULL
);
PERFORM AddGeometryColumn('primary_parcels', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE primary_parcels ADD PRIMARY KEY (id);
CREATE INDEX shx_all_prim_par_shape ON primary_parcels USING gist (shape);

ALTER TABLE primary_parcels OWNER TO bde_dba;

REVOKE ALL ON TABLE primary_parcels FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE primary_parcels TO bde_admin;
GRANT SELECT ON TABLE primary_parcels TO bde_user;

--------------------------------------------------------------------------------
-- LDS table land_parcels
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS land_parcels CASCADE;

CREATE TABLE land_parcels (
    id INTEGER NOT NULL,
    appellation TEXT,
    affected_surveys TEXT,
    parcel_intent TEXT NOT NULL,
    topology_type VARCHAR(100) NOT NULL,
    statutory_actions TEXT,
    land_district VARCHAR(100) NOT NULL,
    titles TEXT,
    survey_area NUMERIC(20, 4),
    calc_area NUMERIC(20, 4) NOT NULL
);
PERFORM AddGeometryColumn('land_parcels', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE land_parcels ADD PRIMARY KEY (id);
CREATE INDEX shx_lnd_par_shape ON land_parcels USING gist (shape);

ALTER TABLE land_parcels OWNER TO bde_dba;

REVOKE ALL ON TABLE land_parcels FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE land_parcels TO bde_admin;
GRANT SELECT ON TABLE land_parcels TO bde_user;

--------------------------------------------------------------------------------
-- LDS table hydro_parcels
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS hydro_parcels CASCADE;

CREATE TABLE hydro_parcels (
    id INTEGER NOT NULL,
    appellation TEXT,
    affected_surveys TEXT,
    parcel_intent TEXT NOT NULL,
    topology_type VARCHAR(100) NOT NULL,
    statutory_actions TEXT,
    land_district VARCHAR(100) NOT NULL,
    titles TEXT,
    survey_area NUMERIC(20, 4),
    calc_area NUMERIC(20, 4) NOT NULL
);
PERFORM AddGeometryColumn('hydro_parcels', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE hydro_parcels ADD PRIMARY KEY (id);
CREATE INDEX shx_hyd_par_shape ON hydro_parcels USING gist (shape);

ALTER TABLE hydro_parcels OWNER TO bde_dba;

REVOKE ALL ON TABLE hydro_parcels FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE hydro_parcels TO bde_admin;
GRANT SELECT ON TABLE hydro_parcels TO bde_user;

--------------------------------------------------------------------------------
-- LDS table road_parcels
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS road_parcels CASCADE;

CREATE TABLE road_parcels (
    id INTEGER NOT NULL,
    appellation TEXT,
    affected_surveys TEXT,
    parcel_intent TEXT NOT NULL,
    topology_type VARCHAR(100) NOT NULL,
    statutory_actions TEXT,
    land_district VARCHAR(100) NOT NULL,
    titles TEXT,
    survey_area NUMERIC(20, 4),
    calc_area NUMERIC(20, 4) NOT NULL
);
PERFORM AddGeometryColumn('road_parcels', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE road_parcels ADD PRIMARY KEY (id);
CREATE INDEX shx_road_par_shape ON road_parcels USING gist (shape);

ALTER TABLE road_parcels OWNER TO bde_dba;

REVOKE ALL ON TABLE road_parcels FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE road_parcels TO bde_admin;
GRANT SELECT ON TABLE road_parcels TO bde_user;

--------------------------------------------------------------------------------
-- LDS table non_primary_parcels
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS non_primary_parcels CASCADE;

CREATE TABLE non_primary_parcels (
    id INTEGER NOT NULL,
    appellation TEXT,
    affected_surveys TEXT,
    parcel_intent TEXT NOT NULL,
    topology_type VARCHAR(100) NOT NULL,
    statutory_actions TEXT,
    land_district VARCHAR(100) NOT NULL,
    titles TEXT,
    survey_area NUMERIC(20, 4),
    calc_area NUMERIC(20, 4) NOT NULL
);
PERFORM AddGeometryColumn('non_primary_parcels', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE non_primary_parcels ADD PRIMARY KEY (id);
CREATE INDEX shx_non_prim_par_shape ON non_primary_parcels USING gist (shape);

ALTER TABLE non_primary_parcels OWNER TO bde_dba;

REVOKE ALL ON TABLE non_primary_parcels FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE non_primary_parcels TO bde_admin;
GRANT SELECT ON TABLE non_primary_parcels TO bde_user;

--------------------------------------------------------------------------------
-- LDS table non_primary_linear_parcels
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS non_primary_linear_parcels CASCADE;

CREATE TABLE non_primary_linear_parcels (
    id INTEGER NOT NULL,
    appellation TEXT,
    affected_surveys TEXT,
    parcel_intent TEXT NOT NULL,
    topology_type VARCHAR(100) NOT NULL,
    statutory_actions TEXT,
    land_district VARCHAR(100) NOT NULL,
    titles TEXT,
    survey_area NUMERIC(20, 4),
    calc_area NUMERIC(20, 4)
);
PERFORM AddGeometryColumn('non_primary_linear_parcels', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE non_primary_linear_parcels ADD PRIMARY KEY (id);
CREATE INDEX shx_non_pril_par_shape ON non_primary_linear_parcels USING gist (shape);

ALTER TABLE non_primary_linear_parcels OWNER TO bde_dba;

REVOKE ALL ON TABLE non_primary_linear_parcels FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE non_primary_linear_parcels TO bde_admin;
GRANT SELECT ON TABLE non_primary_linear_parcels TO bde_user;

--------------------------------------------------------------------------------
-- LDS table strata_parcels
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS strata_parcels CASCADE;

CREATE TABLE strata_parcels (
    id INTEGER NOT NULL,
    appellation TEXT,
    affected_surveys TEXT,
    parcel_intent TEXT NOT NULL,
    topology_type VARCHAR(100) NOT NULL,
    statutory_actions TEXT,
    land_district VARCHAR(100) NOT NULL,
    titles TEXT,
    survey_area NUMERIC(20, 4),
    calc_area NUMERIC(20, 4) NOT NULL
);
PERFORM AddGeometryColumn('strata_parcels', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE strata_parcels ADD PRIMARY KEY (id);
CREATE INDEX shx_str_par_shape ON strata_parcels USING gist (shape);

ALTER TABLE strata_parcels OWNER TO bde_dba;

REVOKE ALL ON TABLE strata_parcels FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE strata_parcels TO bde_admin;
GRANT SELECT ON TABLE strata_parcels TO bde_user;

--------------------------------------------------------------------------------
-- LDS table titles
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS titles CASCADE;

CREATE TABLE titles (
    id INTEGER NOT NULL,
    title_no VARCHAR(20) NOT NULL,
    status VARCHAR(4) NOT NULL, 
    type TEXT NOT NULL,
    land_district VARCHAR(100) NOT NULL,
    issue_date TIMESTAMP NOT NULL,
    guarantee_status TEXT NOT NULL,
    estate_description TEXT,
    number_owners INT8 NOT NULL,
    spatial_extents_shared BOOLEAN NOT NULL
);
PERFORM AddGeometryColumn('titles', 'shape', 4167, 'MULTIPOLYGON', 2);

ALTER TABLE titles ADD PRIMARY KEY (id);
CREATE INDEX shx_title_shape ON titles USING gist (shape);

ALTER TABLE titles OWNER TO bde_dba;

REVOKE ALL ON TABLE titles FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE titles TO bde_admin;
GRANT SELECT ON TABLE titles TO bde_user;

--------------------------------------------------------------------------------
-- LDS table titles_plus
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS titles_plus CASCADE;

CREATE TABLE titles_plus (
    id INTEGER NOT NULL,
    title_no VARCHAR(20) NOT NULL,
    status VARCHAR(4) NOT NULL, 
    type TEXT NOT NULL,
    land_district VARCHAR(100) NOT NULL,
    issue_date TIMESTAMP NOT NULL,
    guarantee_status TEXT NOT NULL,
    estate_description TEXT,
    owners TEXT,
    spatial_extents_shared BOOLEAN NOT NULL
);
PERFORM AddGeometryColumn('titles_plus', 'shape', 4167, 'MULTIPOLYGON', 2);

ALTER TABLE titles_plus ADD PRIMARY KEY (id);
CREATE INDEX shx_title_plus_shape ON titles_plus USING gist (shape);

ALTER TABLE titles_plus OWNER TO bde_dba;

REVOKE ALL ON TABLE titles_plus FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE titles_plus TO bde_admin;
GRANT SELECT ON TABLE titles_plus TO bde_user;

--------------------------------------------------------------------------------
-- LDS table title_owners
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS title_owners CASCADE;

CREATE TABLE title_owners (
    id INTEGER NOT NULL,
    owner VARCHAR(250) NOT NULL,
    title_no VARCHAR(20) NOT NULL,
    title_status VARCHAR(4) NOT NULL, 
    land_district VARCHAR(100) NOT NULL,
    part_ownership BOOLEAN NOT NULL
);
PERFORM AddGeometryColumn('title_owners', 'shape', 4167, 'MULTIPOLYGON', 2);

ALTER TABLE title_owners ADD UNIQUE (owner, title_no);

CREATE SEQUENCE title_owners_id_seq;
ALTER TABLE title_owners_id_seq OWNER TO bde_dba;
ALTER TABLE title_owners ALTER COLUMN id SET DEFAULT nextval('title_owners_id_seq');

ALTER TABLE title_owners ADD PRIMARY KEY (id);
CREATE INDEX shx_owners_shape ON title_owners USING gist (shape);

ALTER TABLE title_owners OWNER TO bde_dba;

REVOKE ALL ON TABLE title_owners FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE title_owners TO bde_admin;
GRANT SELECT ON TABLE title_owners TO bde_user;

--------------------------------------------------------------------------------
-- LDS table road_centre_line
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS road_centre_line CASCADE;

CREATE TABLE road_centre_line (
    id INTEGER NOT NULL,
    "name" VARCHAR(100) NOT NULL,
    asp_location VARCHAR(100)
);
PERFORM AddGeometryColumn('road_centre_line', 'shape', 4167, 'MULTILINESTRING', 2);

ALTER TABLE road_centre_line ADD PRIMARY KEY (id);
CREATE INDEX shx_rcl_shape ON road_centre_line USING gist (shape);

ALTER TABLE road_centre_line OWNER TO bde_dba;

REVOKE ALL ON TABLE road_centre_line FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE road_centre_line TO bde_admin;
GRANT SELECT ON TABLE road_centre_line TO bde_user;

--------------------------------------------------------------------------------
-- LDS table road_centre_line_subsection
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS road_centre_line_subsection CASCADE;

CREATE TABLE road_centre_line_subsection (
    id INTEGER NOT NULL,
    road_id INTEGER NOT NULL,
    "name" VARCHAR(100) NOT NULL,
    asp_location VARCHAR(100),
    parcel_derived BOOLEAN NOT NULL
);
PERFORM AddGeometryColumn('road_centre_line_subsection', 'shape', 4167, 'LINESTRING', 2);

ALTER TABLE road_centre_line_subsection ADD PRIMARY KEY (id);
CREATE INDEX shx_rcls_shape ON road_centre_line_subsection USING gist (shape);

ALTER TABLE road_centre_line_subsection OWNER TO bde_dba;

REVOKE ALL ON TABLE road_centre_line_subsection FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE road_centre_line_subsection TO bde_admin;
GRANT SELECT ON TABLE road_centre_line_subsection TO bde_user;

--------------------------------------------------------------------------------
-- LDS table railway_centre_line
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS railway_centre_line CASCADE;

CREATE TABLE railway_centre_line (
    id INTEGER NOT NULL,
    "name" VARCHAR(100) NOT NULL
);
PERFORM AddGeometryColumn('railway_centre_line', 'shape', 4167, 'MULTILINESTRING', 2);

ALTER TABLE railway_centre_line ADD PRIMARY KEY (id);
CREATE INDEX shx_rlwy_cl_shape ON railway_centre_line USING gist (shape);

ALTER TABLE railway_centre_line OWNER TO bde_dba;

REVOKE ALL ON TABLE railway_centre_line FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE railway_centre_line TO bde_admin;
GRANT SELECT ON TABLE railway_centre_line TO bde_user;

--------------------------------------------------------------------------------
-- LDS table street_address
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS street_address CASCADE;

CREATE TABLE street_address (
    id INTEGER NOT NULL,
    address TEXT NOT NULL,
    house_number VARCHAR(25) NOT NULL,
    road_name VARCHAR(100) NOT NULL,
    asp_location VARCHAR(100)
);
PERFORM AddGeometryColumn('street_address', 'shape', 4167, 'POINT', 2);

ALTER TABLE street_address ADD PRIMARY KEY (id);
CREATE INDEX shx_sad_shape ON street_address USING gist (shape);

ALTER TABLE street_address OWNER TO bde_dba;

REVOKE ALL ON TABLE street_address FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE street_address TO bde_admin;
GRANT SELECT ON TABLE street_address TO bde_user;

--------------------------------------------------------------------------------
-- LDS table street_address
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS land_districts CASCADE;

CREATE TABLE land_districts (
    id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL
);
PERFORM AddGeometryColumn('land_districts', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE land_districts ADD PRIMARY KEY (id);
CREATE INDEX shx_land_districts_shape ON land_districts USING gist (shape);

ALTER TABLE land_districts OWNER TO bde_dba;

REVOKE ALL ON TABLE land_districts FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE land_districts TO bde_admin;
GRANT SELECT ON TABLE land_districts TO bde_user;

--------------------------------------------------------------------------------
-- LDS table survey_plans
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS survey_plans CASCADE;

CREATE TABLE survey_plans (
    id INTEGER NOT NULL,
    survey_reference VARCHAR(50) NOT NULL,
    land_district TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    survey_date DATE,
    purpose TEXT NOT NULL,
    type VARCHAR(100) NOT NULL,
    datum VARCHAR(10)
);
PERFORM AddGeometryColumn('survey_plans', 'shape', 4167, 'MULTIPOINT', 2);

ALTER TABLE survey_plans ADD PRIMARY KEY (id);
CREATE INDEX shx_sur_shape ON survey_plans USING gist (shape);

ALTER TABLE survey_plans OWNER TO bde_dba;

REVOKE ALL ON TABLE survey_plans FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE survey_plans TO bde_admin;
GRANT SELECT ON TABLE survey_plans TO bde_user;

--------------------------------------------------------------------------------
-- LDS table cadastral_adjustments
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS cadastral_adjustments CASCADE;

DROP TABLE IF EXISTS cadastral_adjustments CASCADE;
CREATE TABLE cadastral_adjustments (
    id INTEGER NOT NULL,
    date_adjusted TIMESTAMP NOT NULL,
    survey_reference TEXT,
    adjusted_nodes INTEGER NOT NULL
);
PERFORM AddGeometryColumn('cadastral_adjustments', 'shape', 4167, 'GEOMETRY', 2);

ALTER TABLE cadastral_adjustments ADD PRIMARY KEY (id);
CREATE INDEX shx_cad_adj_shape ON cadastral_adjustments USING gist (shape);

ALTER TABLE cadastral_adjustments OWNER TO bde_dba;

REVOKE ALL ON TABLE cadastral_adjustments FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE cadastral_adjustments TO bde_admin;
GRANT SELECT ON TABLE cadastral_adjustments TO bde_user;

--------------------------------------------------------------------------------
-- LDS table survey_observations
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS survey_observations CASCADE;

DROP TABLE IF EXISTS survey_observations CASCADE;
CREATE TABLE survey_observations (
    id INTEGER NOT NULL,
    nod_id_start INTEGER NOT NULL,
    nod_id_end INTEGER NOT NULL,
    obs_type TEXT NOT NULL,
    value NUMERIC(22,12) NOT NULL,
    value_label TEXT NOT NULL,
    surveyed_type TEXT,
    coordinate_system TEXT NOT NULL,
    ref_datetime TIMESTAMP NOT NULL,
    survey_reference TEXT
);
PERFORM AddGeometryColumn('survey_observations', 'shape', 4167, 'LINESTRING', 2);

ALTER TABLE survey_observations ADD PRIMARY KEY (id);
CREATE INDEX shx_sur_obs_shape ON survey_observations USING gist (shape);

ALTER TABLE survey_observations OWNER TO bde_dba;

REVOKE ALL ON TABLE survey_observations FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE survey_observations TO bde_admin;
GRANT SELECT ON TABLE survey_observations TO bde_user;

--------------------------------------------------------------------------------
-- LDS table survey_arc_observations
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS survey_arc_observations CASCADE;

DROP TABLE IF EXISTS survey_arc_observations CASCADE;
CREATE TABLE survey_arc_observations (
    id INTEGER NOT NULL,
    nod_id_start INTEGER NOT NULL,
    nod_id_end INTEGER NOT NULL,
    chord_bearing NUMERIC(22,12) NOT NULL,
    arc_length NUMERIC(22,12),
    arc_radius NUMERIC(22,12),
    arc_direction VARCHAR(4),
    surveyed_type TEXT,
    coordinate_system TEXT NOT NULL,
    ref_datetime TIMESTAMP NOT NULL,
    survey_reference TEXT NOT NULL,
    chord_bearing_label TEXT NOT NULL,
    arc_length_label TEXT,
    arc_radius_label TEXT
);
PERFORM AddGeometryColumn('survey_arc_observations', 'shape', 4167, 'LINESTRING', 2);

ALTER TABLE survey_arc_observations ADD PRIMARY KEY (id);
CREATE INDEX shx_sur_arc_obs_shape ON survey_arc_observations USING gist (shape);

ALTER TABLE survey_arc_observations OWNER TO bde_dba;

REVOKE ALL ON TABLE survey_arc_observations FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE survey_arc_observations TO bde_admin;
GRANT SELECT ON TABLE survey_arc_observations TO bde_user;

--------------------------------------------------------------------------------
-- LDS table parcel_vectors
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS parcel_vectors CASCADE;

DROP TABLE IF EXISTS parcel_vectors CASCADE;
CREATE TABLE parcel_vectors (
    id INTEGER NOT NULL,
    type TEXT NOT NULL,
    bearing NUMERIC(22,12),
    distance NUMERIC(22,12),
    bearing_label TEXT,
    distance_label TEXT
);
PERFORM AddGeometryColumn('parcel_vectors', 'shape', 4167, 'LINESTRING', 2);

ALTER TABLE parcel_vectors ADD PRIMARY KEY (id);
CREATE INDEX shx_par_vct_shape ON parcel_vectors USING gist (shape);

ALTER TABLE parcel_vectors OWNER TO bde_dba;

REVOKE ALL ON TABLE parcel_vectors FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE parcel_vectors TO bde_admin;
GRANT SELECT ON TABLE parcel_vectors TO bde_user;

--------------------------------------------------------------------------------
-- LDS table survey_network_marks
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS survey_network_marks CASCADE;

DROP TABLE IF EXISTS survey_network_marks CASCADE;
CREATE TABLE survey_network_marks (
    id INTEGER NOT NULL,
    geodetic_code CHAR(4),
    current_mark_name TEXT,
    description TEXT,
    mark_type VARCHAR(4),
    mark_condition TEXT,
    "order" INTEGER NOT NULL,
    nominal_accuracy NUMERIC(4,2),
    last_survey TEXT
);
PERFORM AddGeometryColumn('survey_network_marks', 'shape', 4167, 'POINT', 2);

ALTER TABLE survey_network_marks ADD PRIMARY KEY (id);
CREATE INDEX shx_csnm_shape ON survey_network_marks USING gist (shape);

ALTER TABLE survey_network_marks OWNER TO bde_dba;

REVOKE ALL ON TABLE survey_network_marks FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE survey_network_marks TO bde_admin;
GRANT SELECT ON TABLE survey_network_marks TO bde_user;

--------------------------------------------------------------------------------
-- LDS table survey_bdy_marks
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS survey_bdy_marks CASCADE;

DROP TABLE IF EXISTS survey_bdy_marks CASCADE;
CREATE TABLE survey_bdy_marks (
    id INTEGER NOT NULL,
    name TEXT,
    "order" INTEGER NOT NULL,
    nominal_accuracy NUMERIC(4,2),
    date_last_adjusted TIMESTAMP
);
PERFORM AddGeometryColumn('survey_bdy_marks', 'shape', 4167, 'POINT', 2);

ALTER TABLE survey_bdy_marks ADD PRIMARY KEY (id);
CREATE INDEX shx_cad_bdy_mrk_shape ON survey_bdy_marks USING gist (shape);

ALTER TABLE survey_bdy_marks OWNER TO bde_dba;

REVOKE ALL ON TABLE survey_bdy_marks FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE survey_bdy_marks TO bde_admin;
GRANT SELECT ON TABLE survey_bdy_marks TO bde_user;

--------------------------------------------------------------------------------
-- LDS table survey_non_bdy_marks
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS survey_non_bdy_marks CASCADE;

DROP TABLE IF EXISTS survey_non_bdy_marks CASCADE;
CREATE TABLE survey_non_bdy_marks (
    id INTEGER NOT NULL,
    name TEXT,
    "order" INTEGER NOT NULL,
    nominal_accuracy NUMERIC(4,2),
    date_last_adjusted TIMESTAMP
);
PERFORM AddGeometryColumn('survey_non_bdy_marks', 'shape', 4167, 'POINT', 2);

ALTER TABLE survey_non_bdy_marks ADD PRIMARY KEY (id);
CREATE INDEX shx_cad_nbdy_mrk_shape ON survey_non_bdy_marks USING gist (shape);

ALTER TABLE survey_non_bdy_marks OWNER TO bde_dba;

REVOKE ALL ON TABLE survey_non_bdy_marks FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE survey_non_bdy_marks TO bde_admin;
GRANT SELECT ON TABLE survey_non_bdy_marks TO bde_user;

END
$SCHEMA$;
