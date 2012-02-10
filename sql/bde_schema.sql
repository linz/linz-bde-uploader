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
-- Creates a PostgreSQL 8.4+ and PostGIS 1.0+ schema and set of table
-- definitions for BDE data
--------------------------------------------------------------------------------
SET client_min_messages TO WARNING;

DO $SCHEMA$
BEGIN

IF EXISTS (SELECT * FROM pg_namespace where LOWER(nspname) = 'bde') THEN
    RETURN;
END IF;

CREATE SCHEMA bde AUTHORIZATION bde_dba;

GRANT ALL ON SCHEMA bde TO bde_dba;
GRANT USAGE ON SCHEMA bde TO bde_admin;
GRANT USAGE ON SCHEMA bde TO bde_user;

SET search_path = bde, public;

--------------------------------------------------------------------------------
-- BDE table crs_action
--------------------------------------------------------------------------------

CREATE TABLE crs_action (
    tin_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    att_type VARCHAR(4) NOT NULL,
    system_action CHAR(1) NOT NULL,
    act_id_orig INTEGER,
    act_tin_id_orig INTEGER,
    ste_id INTEGER,
    mode VARCHAR(4),
    flags VARCHAR(4),
    source INTEGER,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_action
    ADD CONSTRAINT pkey_crs_action PRIMARY KEY (tin_id, id);

ALTER TABLE crs_action ALTER COLUMN tin_id SET STATISTICS 500;
ALTER TABLE crs_action ALTER COLUMN att_type SET STATISTICS 500;
ALTER TABLE crs_action ALTER COLUMN ste_id SET STATISTICS 500;
ALTER TABLE crs_action ALTER COLUMN audit_id SET STATISTICS 500;

ALTER TABLE crs_action OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_action FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_action TO bde_admin;
GRANT SELECT ON TABLE crs_action TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_action_type
--------------------------------------------------------------------------------

CREATE TABLE crs_action_type (
    type VARCHAR(4) NOT NULL,
    description VARCHAR(200) NOT NULL,
    system_action CHAR(1) NOT NULL,
    sob_name VARCHAR(50),
    existing_inst CHAR(1) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_action_type
    ADD CONSTRAINT pkey_crs_action_type PRIMARY KEY (type);

ALTER TABLE crs_action_type OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_action_type FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_action_type TO bde_admin;
GRANT SELECT ON TABLE crs_action_type TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_adj_obs_change
--------------------------------------------------------------------------------

CREATE TABLE crs_adj_obs_change (
    adj_id INTEGER NOT NULL,
    obn_id INTEGER NOT NULL,
    orig_status VARCHAR(4) NOT NULL,
    proposed_status VARCHAR(4),
    acc_multiplier NUMERIC(15,3),
    geodetic_class VARCHAR(4),
    residual_1 NUMERIC(22,12),
    residual_std_dev_1 NUMERIC(22,12),
    redundancy_fctr_1 NUMERIC(22,12),
    residual_2 NUMERIC(22,12),
    residual_std_dev_2 NUMERIC(22,12),
    redundancy_fctr_2 NUMERIC(22,12),
    residual_3 NUMERIC(22,12),
    residual_std_dev_3 NUMERIC(22,12),
    redundancy_fctr_3 NUMERIC(22,12),
    summary_residual NUMERIC(22,12),
    summary_std_dev NUMERIC(22,12),
    exclude CHAR(1),
    reliability VARCHAR(4),
    h_max_accuracy NUMERIC(22,12),
    h_min_accuracy NUMERIC(22,12),
    h_max_azimuth NUMERIC(22,12),
    v_accuracy NUMERIC(22,12),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_adj_obs_change
    ADD CONSTRAINT pkey_crs_adj_obs_change PRIMARY KEY (adj_id, obn_id);

ALTER TABLE crs_adj_obs_change ALTER COLUMN adj_id SET STATISTICS 1000;
ALTER TABLE crs_adj_obs_change ALTER COLUMN obn_id SET STATISTICS 1000;
ALTER TABLE crs_adj_obs_change ALTER COLUMN audit_id SET STATISTICS 1000;

ALTER TABLE crs_adj_obs_change OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_adj_obs_change FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_adj_obs_change TO bde_admin;
GRANT SELECT ON TABLE crs_adj_obs_change TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_adj_user_coef
--------------------------------------------------------------------------------

CREATE TABLE crs_adj_user_coef (
    adc_id INTEGER NOT NULL,
    adj_id INTEGER NOT NULL,
    value VARCHAR(255),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_adj_user_coef
    ADD CONSTRAINT pkey_crs_adj_user_coef PRIMARY KEY (adc_id, adj_id);

ALTER TABLE crs_adj_user_coef OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_adj_user_coef FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_adj_user_coef TO bde_admin;
GRANT SELECT ON TABLE crs_adj_user_coef TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_adjust_coef
--------------------------------------------------------------------------------

CREATE TABLE crs_adjust_coef (
    id INTEGER NOT NULL,
    adm_id INTEGER NOT NULL,
    default_value VARCHAR(255),
    description VARCHAR(100) NOT NULL,
    sequence INTEGER NOT NULL,
    coef_code VARCHAR(4) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_adjust_coef
    ADD CONSTRAINT pkey_crs_adjust_coef PRIMARY KEY (id);

ALTER TABLE crs_adjust_coef OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_adjust_coef FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_adjust_coef TO bde_admin;
GRANT SELECT ON TABLE crs_adjust_coef TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_adjust_method
--------------------------------------------------------------------------------

CREATE TABLE crs_adjust_method (
    id INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    software_used VARCHAR(4) NOT NULL,
    type VARCHAR(4) NOT NULL,
    name VARCHAR(30) NOT NULL,
    audit_id INTEGER NOT NULL,
    description VARCHAR(100)
);

ALTER TABLE ONLY crs_adjust_method
    ADD CONSTRAINT pkey_crs_adjust_method PRIMARY KEY (id);

ALTER TABLE crs_adjust_method OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_adjust_method FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_adjust_method TO bde_admin;
GRANT SELECT ON TABLE crs_adjust_method TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_adjustment_run
--------------------------------------------------------------------------------

CREATE TABLE crs_adjustment_run (
    id INTEGER NOT NULL,
    adm_id INTEGER NOT NULL,
    cos_id INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    usr_id_exec VARCHAR(20) NOT NULL,
    adjust_datetime TIMESTAMP,
    description VARCHAR(100),
    sum_sqrd_residuals NUMERIC(22,12),
    redundancy NUMERIC(22,12),
    wrk_id INTEGER,
    adj_nod_status_decom CHAR(1) NOT NULL,
    adj_obn_status_decom CHAR(1) NOT NULL,
    preview_datetime TIMESTAMP,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_adjustment_run
    ADD CONSTRAINT pkey_crs_adjustment_run PRIMARY KEY (id);

ALTER TABLE crs_adjustment_run ALTER COLUMN adm_id SET STATISTICS 250;
ALTER TABLE crs_adjustment_run ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_adjustment_run ALTER COLUMN cos_id SET STATISTICS 250;
ALTER TABLE crs_adjustment_run ALTER COLUMN id SET STATISTICS 250;
ALTER TABLE crs_adjustment_run ALTER COLUMN status SET STATISTICS 250;
ALTER TABLE crs_adjustment_run ALTER COLUMN usr_id_exec SET STATISTICS 250;
ALTER TABLE crs_adjustment_run ALTER COLUMN wrk_id SET STATISTICS 250;

ALTER TABLE crs_adjustment_run OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_adjustment_run FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_adjustment_run TO bde_admin;
GRANT SELECT ON TABLE crs_adjustment_run TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_adoption
--------------------------------------------------------------------------------

CREATE TABLE crs_adoption (
    obn_id_new INTEGER NOT NULL,
    obn_id_orig INTEGER,
    sur_wrk_id_orig INTEGER NOT NULL,
    factor_1 NUMERIC(22,12) NOT NULL,
    factor_2 NUMERIC(22,12),
    factor_3 NUMERIC(22,12),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_adoption
    ADD CONSTRAINT pkey_crs_adoption PRIMARY KEY (obn_id_new);

ALTER TABLE crs_adoption ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_adoption ALTER COLUMN obn_id_new SET STATISTICS 500;
ALTER TABLE crs_adoption ALTER COLUMN obn_id_orig SET STATISTICS 500;
ALTER TABLE crs_adoption ALTER COLUMN sur_wrk_id_orig SET STATISTICS 500;

ALTER TABLE crs_adoption OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_adoption FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_adoption TO bde_admin;
GRANT SELECT ON TABLE crs_adoption TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_affected_parcl
--------------------------------------------------------------------------------

CREATE TABLE crs_affected_parcl (
    sur_wrk_id INTEGER NOT NULL,
    par_id INTEGER NOT NULL,
    action VARCHAR(4) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_affected_parcl
    ADD CONSTRAINT pkey_crs_affected_parcl PRIMARY KEY (sur_wrk_id, par_id);

ALTER TABLE crs_affected_parcl ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_affected_parcl ALTER COLUMN par_id SET STATISTICS 500;
ALTER TABLE crs_affected_parcl ALTER COLUMN sur_wrk_id SET STATISTICS 500;

ALTER TABLE crs_affected_parcl OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_affected_parcl FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_affected_parcl TO bde_admin;
GRANT SELECT ON TABLE crs_affected_parcl TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_alias
--------------------------------------------------------------------------------

CREATE TABLE crs_alias (
    id INTEGER NOT NULL,
    prp_id INTEGER NOT NULL,
    surname VARCHAR(100) NOT NULL,
    other_names VARCHAR(100)
);

ALTER TABLE ONLY crs_alias
    ADD CONSTRAINT pkey_crs_alias PRIMARY KEY (id);

ALTER TABLE crs_alias OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_alias FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_alias TO bde_admin;
GRANT SELECT ON TABLE crs_alias TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_appellation
--------------------------------------------------------------------------------

CREATE TABLE crs_appellation (
    par_id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    title CHAR(1) NOT NULL,
    survey CHAR(1),
    status VARCHAR(4) NOT NULL,
    part_indicator VARCHAR(4) NOT NULL,
    maori_name VARCHAR(100),
    sub_type VARCHAR(4),
    appellation_value VARCHAR(60),
    parcel_type VARCHAR(4),
    parcel_value VARCHAR(60),
    second_parcel_type VARCHAR(4),
    second_prcl_value VARCHAR(60),
    block_number VARCHAR(15),
    sub_type_position VARCHAR(4),
    other_appellation VARCHAR(2048),
    act_id_crt INTEGER,
    act_tin_id_crt INTEGER,
    act_id_ext INTEGER,
    act_tin_id_ext INTEGER,
    id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_appellation
    ADD CONSTRAINT pkey_crs_appellation PRIMARY KEY (id);

ALTER TABLE crs_appellation ALTER COLUMN act_id_crt SET STATISTICS 500;
ALTER TABLE crs_appellation ALTER COLUMN act_id_ext SET STATISTICS 500;
ALTER TABLE crs_appellation ALTER COLUMN act_tin_id_crt SET STATISTICS 500;
ALTER TABLE crs_appellation ALTER COLUMN act_tin_id_ext SET STATISTICS 500;
ALTER TABLE crs_appellation ALTER COLUMN appellation_value SET STATISTICS 500;
ALTER TABLE crs_appellation ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_appellation ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_appellation ALTER COLUMN maori_name SET STATISTICS 500;
ALTER TABLE crs_appellation ALTER COLUMN other_appellation SET STATISTICS 500;
ALTER TABLE crs_appellation ALTER COLUMN parcel_value SET STATISTICS 500;
ALTER TABLE crs_appellation ALTER COLUMN par_id SET STATISTICS 500;

ALTER TABLE crs_appellation OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_appellation FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_appellation TO bde_admin;
GRANT SELECT ON TABLE crs_appellation TO bde_user;

/*
--------------------------------------------------------------------------------
-- BDE table crs_audit_detail
--------------------------------------------------------------------------------

CREATE TABLE crs_audit_detail (
    id INTEGER NOT NULL,
    "timestamp" TIMESTAMP NOT NULL,
    table_name VARCHAR(40) NOT NULL,
    action CHAR(1) NOT NULL,
    status VARCHAR(4),
    record_no INTEGER NOT NULL,
    identifier_1 VARCHAR(100),
    identifier_2 VARCHAR(100),
    identifier_3 VARCHAR(100),
    identifier_4 VARCHAR(100),
    identifier_5 VARCHAR(100),
    data_1 VARCHAR(100),
    data_2 VARCHAR(100),
    data_3 VARCHAR(100),
    data_4 VARCHAR(100),
    data_5 VARCHAR(100),
    trl_type VARCHAR(4),
    trl_id INTEGER,
    usr_id VARCHAR(20)
);

ALTER TABLE ONLY crs_audit_detail
    ADD CONSTRAINT pkey_crs_audit_detail PRIMARY KEY (record_no);

ALTER TABLE crs_audit_detail ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_audit_detail ALTER COLUMN record_no SET STATISTICS 500;
ALTER TABLE crs_audit_detail ALTER COLUMN table_name SET STATISTICS 500;

ALTER TABLE crs_audit_detail OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_audit_detail FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_audit_detail TO bde_admin;
GRANT SELECT ON TABLE crs_audit_detail TO bde_user;

*/

--------------------------------------------------------------------------------
-- BDE table crs_comprised_in
--------------------------------------------------------------------------------

CREATE TABLE crs_comprised_in (
    id INTEGER NOT NULL,
    wrk_id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    reference VARCHAR(20) NOT NULL,
    limited CHAR(1)
);

ALTER TABLE ONLY crs_comprised_in
    ADD CONSTRAINT pkey_crs_comprised_in PRIMARY KEY (id);

ALTER TABLE crs_comprised_in OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_comprised_in FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_comprised_in TO bde_admin;
GRANT SELECT ON TABLE crs_comprised_in TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_coordinate
--------------------------------------------------------------------------------

CREATE TABLE crs_coordinate (
    id INTEGER NOT NULL,
    cos_id INTEGER NOT NULL,
    nod_id INTEGER NOT NULL,
    ort_type_1 VARCHAR(4),
    ort_type_2 VARCHAR(4),
    ort_type_3 VARCHAR(4),
    status VARCHAR(4) NOT NULL,
    sdc_status CHAR(1) NOT NULL,
    source VARCHAR(4),
    value1 NUMERIC(22,12),
    value2 NUMERIC(22,12),
    value3 NUMERIC(22,12),
    wrk_id_created INTEGER,
    cor_id INTEGER,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_coordinate
    ADD CONSTRAINT pkey_crs_coordinate PRIMARY KEY (id);

ALTER TABLE crs_coordinate ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN cor_id SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN cos_id SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN nod_id SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN ort_type_1 SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN ort_type_2 SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN ort_type_3 SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN value1 SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN value2 SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN value3 SET STATISTICS 1000;
ALTER TABLE crs_coordinate ALTER COLUMN wrk_id_created SET STATISTICS 1000;

ALTER TABLE crs_coordinate OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_coordinate FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_coordinate TO bde_admin;
GRANT SELECT ON TABLE crs_coordinate TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_coordinate_sys
--------------------------------------------------------------------------------

CREATE TABLE crs_coordinate_sys (
    id INTEGER NOT NULL,
    cot_id INTEGER NOT NULL,
    dtm_id INTEGER NOT NULL,
    cos_id_adjust INTEGER,
    name VARCHAR(100) NOT NULL,
    initial_sta_name VARCHAR(100),
    code VARCHAR(10) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_coordinate_sys
    ADD CONSTRAINT pkey_crs_coordinate_sys PRIMARY KEY (id);

ALTER TABLE crs_coordinate_sys OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_coordinate_sys FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_coordinate_sys TO bde_admin;
GRANT SELECT ON TABLE crs_coordinate_sys TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_coordinate_tpe
--------------------------------------------------------------------------------

CREATE TABLE crs_coordinate_tpe (
    id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL,
    status VARCHAR(4) NOT NULL,
    ort_type_1 VARCHAR(4),
    ort_type_2 VARCHAR(4),
    ort_type_3 VARCHAR(4),
    category VARCHAR(4) NOT NULL,
    dimension VARCHAR(4) NOT NULL,
    ord_1_min NUMERIC(22,12),
    ord_1_max NUMERIC(22,12),
    ord_2_min NUMERIC(22,12),
    ord_2_max NUMERIC(22,12),
    ord_3_min NUMERIC(22,12),
    ord_3_max NUMERIC(22,12),
    data VARCHAR(4),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_coordinate_tpe
    ADD CONSTRAINT pkey_crs_coordinate_tpe PRIMARY KEY (id);

ALTER TABLE crs_coordinate_tpe OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_coordinate_tpe FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_coordinate_tpe TO bde_admin;
GRANT SELECT ON TABLE crs_coordinate_tpe TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_cor_precision
--------------------------------------------------------------------------------

CREATE TABLE crs_cor_precision (
    cor_id INTEGER NOT NULL,
    ort_type VARCHAR(4) NOT NULL,
    decimal_places INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_cor_precision
    ADD CONSTRAINT pkey_crs_cor_precision PRIMARY KEY (cor_id, ort_type);

ALTER TABLE crs_cor_precision OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_cor_precision FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_cor_precision TO bde_admin;
GRANT SELECT ON TABLE crs_cor_precision TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_cord_order
--------------------------------------------------------------------------------

CREATE TABLE crs_cord_order (
    id INTEGER NOT NULL,
    display VARCHAR(4) NOT NULL,
    description VARCHAR(100) NOT NULL,
    dtm_id INTEGER NOT NULL,
    order_group INTEGER,
    error NUMERIC(12,4) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_cord_order
    ADD CONSTRAINT pkey_crs_cord_order PRIMARY KEY (id);

ALTER TABLE crs_cord_order OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_cord_order FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_cord_order TO bde_admin;
GRANT SELECT ON TABLE crs_cord_order TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_datum
--------------------------------------------------------------------------------

CREATE TABLE crs_datum (
    id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(4) NOT NULL,
    dimension VARCHAR(4) NOT NULL,
    ref_datetime TIMESTAMP NOT NULL,
    status VARCHAR(4) NOT NULL,
    elp_id INTEGER,
    ref_datum_code VARCHAR(4) NOT NULL,
    code VARCHAR(10) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_datum
    ADD CONSTRAINT pkey_crs_datum PRIMARY KEY (id);

ALTER TABLE crs_datum OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_datum FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_datum TO bde_admin;
GRANT SELECT ON TABLE crs_datum TO bde_user;

/*
--------------------------------------------------------------------------------
-- BDE table crs_dealing
--------------------------------------------------------------------------------

CREATE TABLE crs_dealing (
    id INTEGER NOT NULL,
    ldt_loc_id INTEGER NOT NULL,
    off_code VARCHAR(4) NOT NULL,
    lodged_datetime TIMESTAMP NOT NULL,
    status VARCHAR(4) NOT NULL,
    usr_id_lodged VARCHAR(20) NOT NULL,
    principal_firm VARCHAR(200) NOT NULL,
    type VARCHAR(4) NOT NULL,
    dd_reason VARCHAR(4),
    usr_id_dept VARCHAR(20),
    fhr_id INTEGER,
    client_reference VARCHAR(25),
    dlg_id_rejected INTEGER,
    alt_id INTEGER,
    waiting_end_date DATE,
    dup_return_status CHAR(4),
    dtm_invalid CHAR(1),
    audit_id INTEGER NOT NULL,
    queue_state VARCHAR(4),
    rejected_datetime TIMESTAMP,
    usr_id_lodged_firm VARCHAR(20),
    queue_reason VARCHAR(255),
    register_attempts INTEGER
);

ALTER TABLE ONLY crs_dealing
    ADD CONSTRAINT pkey_crs_dealing PRIMARY KEY (id);

ALTER TABLE crs_dealing ALTER COLUMN alt_id SET STATISTICS 500;
ALTER TABLE crs_dealing ALTER COLUMN dlg_id_rejected SET STATISTICS 500;
ALTER TABLE crs_dealing ALTER COLUMN fhr_id SET STATISTICS 500;
ALTER TABLE crs_dealing ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_dealing ALTER COLUMN ldt_loc_id SET STATISTICS 500;
ALTER TABLE crs_dealing ALTER COLUMN off_code SET STATISTICS 500;
ALTER TABLE crs_dealing ALTER COLUMN queue_state SET STATISTICS 500;
ALTER TABLE crs_dealing ALTER COLUMN status SET STATISTICS 500;
ALTER TABLE crs_dealing ALTER COLUMN usr_id_dept SET STATISTICS 500;
ALTER TABLE crs_dealing ALTER COLUMN usr_id_lodged SET STATISTICS 500;
ALTER TABLE crs_dealing ALTER COLUMN usr_id_lodged_firm SET STATISTICS 500;

ALTER TABLE crs_dealing OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_dealing FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_dealing TO bde_admin;
GRANT SELECT ON TABLE crs_dealing TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_dealing_survey
--------------------------------------------------------------------------------

CREATE TABLE crs_dealing_survey (
    dlg_id INTEGER NOT NULL,
    sur_wrk_id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL
);

ALTER TABLE ONLY crs_dealing_survey
    ADD CONSTRAINT pkey_crs_dealing_survey PRIMARY KEY (dlg_id, sur_wrk_id);

ALTER TABLE crs_dealing_survey ALTER COLUMN dlg_id SET STATISTICS 250;
ALTER TABLE crs_dealing_survey ALTER COLUMN sur_wrk_id SET STATISTICS 250;

ALTER TABLE crs_dealing_survey OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_dealing_survey FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_dealing_survey TO bde_admin;
GRANT SELECT ON TABLE crs_dealing_survey TO bde_user;

*/
--------------------------------------------------------------------------------
-- BDE table crs_elect_place
--------------------------------------------------------------------------------

CREATE TABLE crs_elect_place (
    id INTEGER NOT NULL,
    alt_id INTEGER,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(100),
    status VARCHAR(4) NOT NULL,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'POINT'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_elect_place
    ADD CONSTRAINT pkey_crs_elect_place PRIMARY KEY (id);

ALTER TABLE crs_elect_place OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_elect_place FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_elect_place TO bde_admin;
GRANT SELECT ON TABLE crs_elect_place TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_ellipsoid
--------------------------------------------------------------------------------

CREATE TABLE crs_ellipsoid (
    id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL,
    semi_major_axis NUMERIC(22,12) NOT NULL,
    flattening NUMERIC(22,12) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_ellipsoid
    ADD CONSTRAINT pkey_crs_ellipsoid PRIMARY KEY (id);

ALTER TABLE crs_ellipsoid OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_ellipsoid FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_ellipsoid TO bde_admin;
GRANT SELECT ON TABLE crs_ellipsoid TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_enc_share
--------------------------------------------------------------------------------

CREATE TABLE crs_enc_share (
    id INTEGER NOT NULL,
    enc_id INTEGER,
    status VARCHAR(4),
    act_tin_id_crt INTEGER,
    act_id_crt INTEGER NOT NULL,
    act_id_ext INTEGER,
    act_tin_id_ext INTEGER,
    system_crt CHAR(1) NOT NULL,
    system_ext CHAR(1)
);

ALTER TABLE ONLY crs_enc_share
    ADD CONSTRAINT pkey_crs_enc_share PRIMARY KEY (id);

ALTER TABLE crs_enc_share ALTER COLUMN act_tin_id_crt SET STATISTICS 500;
ALTER TABLE crs_enc_share ALTER COLUMN enc_id SET STATISTICS 500;
ALTER TABLE crs_enc_share ALTER COLUMN id SET STATISTICS 500;

ALTER TABLE crs_enc_share OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_enc_share FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_enc_share TO bde_admin;
GRANT SELECT ON TABLE crs_enc_share TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_encumbrance
--------------------------------------------------------------------------------

CREATE TABLE crs_encumbrance (
    id INTEGER NOT NULL,
    status VARCHAR(4),
    act_tin_id_orig INTEGER,
    act_tin_id_crt INTEGER,
    act_id_crt INTEGER NOT NULL,
    act_id_orig INTEGER NOT NULL,
    ind_tan_holder CHAR(1),
    term VARCHAR(250)
);

ALTER TABLE ONLY crs_encumbrance
    ADD CONSTRAINT pkey_crs_encumbrance PRIMARY KEY (id);

ALTER TABLE crs_encumbrance ALTER COLUMN act_tin_id_crt SET STATISTICS 500;
ALTER TABLE crs_encumbrance ALTER COLUMN act_tin_id_orig SET STATISTICS 500;
ALTER TABLE crs_encumbrance ALTER COLUMN id SET STATISTICS 500;

ALTER TABLE crs_encumbrance OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_encumbrance FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_encumbrance TO bde_admin;
GRANT SELECT ON TABLE crs_encumbrance TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_encumbrancee
--------------------------------------------------------------------------------

CREATE TABLE crs_encumbrancee (
    id INTEGER NOT NULL,
    ens_id INTEGER,
    status VARCHAR(4),
    name VARCHAR(255),
    system_ext CHAR(1),
    usr_id VARCHAR(20)
);

ALTER TABLE ONLY crs_encumbrancee
    ADD CONSTRAINT pkey_crs_encumbrancee PRIMARY KEY (id);

ALTER TABLE crs_encumbrancee ALTER COLUMN ens_id SET STATISTICS 500;
ALTER TABLE crs_encumbrancee ALTER COLUMN id SET STATISTICS 500;

ALTER TABLE crs_encumbrancee OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_encumbrancee FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_encumbrancee TO bde_admin;
GRANT SELECT ON TABLE crs_encumbrancee TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_estate_share
--------------------------------------------------------------------------------

CREATE TABLE crs_estate_share (
    id INTEGER NOT NULL,
    ett_id INTEGER NOT NULL,
    share CHAR(100) NOT NULL,
    status VARCHAR(4) NOT NULL,
    share_memorial VARCHAR(17500),
    act_tin_id_crt INTEGER,
    act_id_crt INTEGER,
    act_id_ext INTEGER,
    act_tin_id_ext INTEGER,
    executorship VARCHAR(4),
    original_flag CHAR(1) NOT NULL,
    sort_order INTEGER,
    system_crt CHAR(1) NOT NULL,
    system_ext CHAR(1),
    transferee_group SMALLINT
);

ALTER TABLE ONLY crs_estate_share
    ADD CONSTRAINT pkey_crs_estate_share PRIMARY KEY (id);

ALTER TABLE crs_estate_share ALTER COLUMN act_tin_id_crt SET STATISTICS 500;
ALTER TABLE crs_estate_share ALTER COLUMN ett_id SET STATISTICS 500;
ALTER TABLE crs_estate_share ALTER COLUMN id SET STATISTICS 500;

ALTER TABLE crs_estate_share OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_estate_share FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_estate_share TO bde_admin;
GRANT SELECT ON TABLE crs_estate_share TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_feature_name
--------------------------------------------------------------------------------

CREATE TABLE crs_feature_name (
    id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    name VARCHAR(100) NOT NULL,
    status VARCHAR(4) NOT NULL,
    other_details VARCHAR(100),
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_feature_name
    ADD CONSTRAINT pkey_crs_feature_name PRIMARY KEY (id);

ALTER TABLE crs_feature_name OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_feature_name FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_feature_name TO bde_admin;
GRANT SELECT ON TABLE crs_feature_name TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_geodetic_network
--------------------------------------------------------------------------------

CREATE TABLE crs_geodetic_network  (
    id  INTEGER  NOT NULL,
    code VARCHAR(4),
    description VARCHAR(100)
);

ALTER TABLE ONLY crs_geodetic_network
    ADD CONSTRAINT pkey_crs_geodetic_network PRIMARY KEY (id);

ALTER TABLE crs_geodetic_network OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_geodetic_network FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_geodetic_network TO bde_admin;
GRANT SELECT ON TABLE crs_geodetic_network TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_geodetic_node_network
--------------------------------------------------------------------------------

CREATE TABLE crs_geodetic_node_network  (
    nod_id INTEGER NOT NULL,
    gdn_id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_geodetic_node_network
    ADD CONSTRAINT pkey_crs_geodetic_node_network PRIMARY KEY (nod_id, gdn_id);

ALTER TABLE crs_geodetic_node_network OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_geodetic_node_network FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_geodetic_node_network TO bde_admin;
GRANT SELECT ON TABLE crs_geodetic_node_network TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_image
--------------------------------------------------------------------------------

CREATE TABLE crs_image (
  id INTEGER NOT NULL,
  barcode_datetime TIMESTAMP,
  ims_id NUMERIC(32),
  ims_date DATE,
  pages INTEGER NOT NULL,
  centera_id VARCHAR(65),
  location CHAR(1)
);

ALTER TABLE ONLY crs_image
    ADD CONSTRAINT pkey_crs_image PRIMARY KEY (id);

ALTER TABLE crs_image OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_image FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_image TO bde_admin;
GRANT SELECT ON TABLE crs_image TO bde_user;

/*
--------------------------------------------------------------------------------
-- BDE table crs_inst_role
--------------------------------------------------------------------------------

CREATE TABLE crs_inst_role (
    tin_id INTEGER NOT NULL,
    inst_role VARCHAR(4) NOT NULL,
    sequence INTEGER,
    resp_for_release CHAR(1) NOT NULL,
    usr_id_contact VARCHAR(20),
    usr_id_cp VARCHAR(20),
    sig_id INTEGER,
    usr_id_firm_cp VARCHAR(20),
    usr_id_firm_cntct VARCHAR(20),
    resp_for_fees CHAR(1),
    multi_role_id INTEGER NOT NULL,
    signed_by VARCHAR(20),
    sign_count INTEGER,
    cp_no INTEGER
);

ALTER TABLE ONLY crs_inst_role
    ADD CONSTRAINT pkey_crs_inst_role PRIMARY KEY (tin_id, inst_role, multi_role_id);

ALTER TABLE crs_inst_role ALTER COLUMN inst_role SET STATISTICS 500;
ALTER TABLE crs_inst_role ALTER COLUMN multi_role_id SET STATISTICS 500;
ALTER TABLE crs_inst_role ALTER COLUMN resp_for_fees SET STATISTICS 500;
ALTER TABLE crs_inst_role ALTER COLUMN tin_id SET STATISTICS 500;
ALTER TABLE crs_inst_role ALTER COLUMN usr_id_contact SET STATISTICS 500;
ALTER TABLE crs_inst_role ALTER COLUMN usr_id_cp SET STATISTICS 500;
ALTER TABLE crs_inst_role ALTER COLUMN usr_id_firm_cntct SET STATISTICS 500;
ALTER TABLE crs_inst_role ALTER COLUMN usr_id_firm_cp SET STATISTICS 500;

ALTER TABLE crs_inst_role OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_inst_role FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_inst_role TO bde_admin;
GRANT SELECT ON TABLE crs_inst_role TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_job
--------------------------------------------------------------------------------

CREATE TABLE crs_job (
    id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    priority smallint NOT NULL,
    datetime_received TIMESTAMP NOT NULL,
    datetime_due TIMESTAMP NOT NULL,
    off_code_current VARCHAR(4),
    off_code_original VARCHAR(4),
    audit_id INTEGER NOT NULL,
    sub_type VARCHAR(4),
    candidate_flag CHAR(1)
);

ALTER TABLE ONLY crs_job
    ADD CONSTRAINT pkey_crs_job PRIMARY KEY (id);

ALTER TABLE crs_job ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_job ALTER COLUMN datetime_due SET STATISTICS 500;
ALTER TABLE crs_job ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_job ALTER COLUMN off_code_current SET STATISTICS 500;
ALTER TABLE crs_job ALTER COLUMN off_code_original SET STATISTICS 500;
ALTER TABLE crs_job ALTER COLUMN status SET STATISTICS 500;

ALTER TABLE crs_job OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_job FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_job TO bde_admin;
GRANT SELECT ON TABLE crs_job TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_job_task_list
--------------------------------------------------------------------------------

CREATE TABLE crs_job_task_list (
    tkl_id INTEGER NOT NULL,
    job_id INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    usr_id VARCHAR(20),
    "position" smallint,
    complexity INTEGER NOT NULL,
    audit_id INTEGER NOT NULL,
    date_completed TIMESTAMP
);

ALTER TABLE ONLY crs_job_task_list
    ADD CONSTRAINT pkey_crs_job_task_list PRIMARY KEY (tkl_id, job_id);

ALTER TABLE crs_job_task_list ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_job_task_list ALTER COLUMN date_completed SET STATISTICS 500;
ALTER TABLE crs_job_task_list ALTER COLUMN job_id SET STATISTICS 500;
ALTER TABLE crs_job_task_list ALTER COLUMN status SET STATISTICS 500;
ALTER TABLE crs_job_task_list ALTER COLUMN tkl_id SET STATISTICS 500;
ALTER TABLE crs_job_task_list ALTER COLUMN usr_id SET STATISTICS 500;

ALTER TABLE crs_job_task_list OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_job_task_list FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_job_task_list TO bde_admin;
GRANT SELECT ON TABLE crs_job_task_list TO bde_user;

*/
--------------------------------------------------------------------------------
-- BDE table crs_land_district
--------------------------------------------------------------------------------

CREATE TABLE crs_land_district (
    loc_id INTEGER NOT NULL,
    off_code VARCHAR(4) NOT NULL,
    "default" CHAR(1) NOT NULL,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_land_district
    ADD CONSTRAINT pkey_crs_land_district PRIMARY KEY (loc_id);

ALTER TABLE crs_land_district OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_land_district FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_land_district TO bde_admin;
GRANT SELECT ON TABLE crs_land_district TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_legal_desc
--------------------------------------------------------------------------------

CREATE TABLE crs_legal_desc (
    id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    total_area NUMERIC(22,12),
    ttl_title_no VARCHAR(20),
    legal_desc_text VARCHAR(2048),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_legal_desc
    ADD CONSTRAINT pkey_crs_legal_desc PRIMARY KEY (id);

ALTER TABLE crs_legal_desc ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_legal_desc ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_legal_desc ALTER COLUMN ttl_title_no SET STATISTICS 500;

ALTER TABLE crs_legal_desc OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_legal_desc FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_legal_desc TO bde_admin;
GRANT SELECT ON TABLE crs_legal_desc TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_legal_desc_prl
--------------------------------------------------------------------------------

CREATE TABLE crs_legal_desc_prl (
    lgd_id INTEGER NOT NULL,
    par_id INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    part_affected VARCHAR(4) NOT NULL,
    share CHAR(100) NOT NULL,
    audit_id INTEGER NOT NULL,
    sur_wrk_id_crt INTEGER
);

ALTER TABLE ONLY crs_legal_desc_prl
    ADD CONSTRAINT pkey_crs_legal_desc_prl PRIMARY KEY (lgd_id, par_id);

ALTER TABLE crs_legal_desc_prl ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_legal_desc_prl ALTER COLUMN lgd_id SET STATISTICS 500;
ALTER TABLE crs_legal_desc_prl ALTER COLUMN par_id SET STATISTICS 500;
ALTER TABLE crs_legal_desc_prl ALTER COLUMN sur_wrk_id_crt SET STATISTICS 500;

ALTER TABLE crs_legal_desc_prl OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_legal_desc_prl FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_legal_desc_prl TO bde_admin;
GRANT SELECT ON TABLE crs_legal_desc_prl TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_line
--------------------------------------------------------------------------------

CREATE TABLE crs_line (
    boundary CHAR(1) NOT NULL,
    type VARCHAR(4) NOT NULL,
    description VARCHAR(2048),
    nod_id_end INTEGER NOT NULL,
    nod_id_start INTEGER NOT NULL,
    arc_radius NUMERIC(22,12),
    arc_direction VARCHAR(4),
    arc_length NUMERIC(22,12),
    pnx_id_created INTEGER,
    dcdb_feature VARCHAR(12),
    id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'LINESTRING'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_line
    ADD CONSTRAINT pkey_crs_line PRIMARY KEY (id);

ALTER TABLE crs_line ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_line ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE crs_line ALTER COLUMN nod_id_end SET STATISTICS 1000;
ALTER TABLE crs_line ALTER COLUMN nod_id_start SET STATISTICS 1000;
ALTER TABLE crs_line ALTER COLUMN pnx_id_created SET STATISTICS 1000;

ALTER TABLE crs_line OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_line FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_line TO bde_admin;
GRANT SELECT ON TABLE crs_line TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_locality
--------------------------------------------------------------------------------

CREATE TABLE crs_locality (
    id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    name VARCHAR(100) NOT NULL,
    loc_id_parent INTEGER,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_locality
    ADD CONSTRAINT pkey_crs_locality PRIMARY KEY (id);

ALTER TABLE crs_locality OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_locality FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_locality TO bde_admin;
GRANT SELECT ON TABLE crs_locality TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_maintenance
--------------------------------------------------------------------------------

CREATE TABLE crs_maintenance (
    mrk_id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    "desc" VARCHAR(2048),
    complete_date DATE,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_maintenance
    ADD CONSTRAINT pkey_crs_maintenance PRIMARY KEY (mrk_id, type);

ALTER TABLE crs_maintenance OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_maintenance FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_maintenance TO bde_admin;
GRANT SELECT ON TABLE crs_maintenance TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_map_grid
--------------------------------------------------------------------------------

CREATE TABLE crs_map_grid (
    major_grid VARCHAR(4) NOT NULL,
    minor_grid VARCHAR(4) NOT NULL,
    shape GEOMETRY,
    se_row_id INTEGER,
    audit_id INTEGER NOT NULL,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'POLYGON'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_map_grid
    ADD CONSTRAINT pkey_crs_map_grid PRIMARY KEY (major_grid, minor_grid);

ALTER TABLE crs_map_grid OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_map_grid FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_map_grid TO bde_admin;
GRANT SELECT ON TABLE crs_map_grid TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_mark
--------------------------------------------------------------------------------

CREATE TABLE crs_mark (
    id INTEGER NOT NULL,
    nod_id INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    type VARCHAR(4) NOT NULL,
    "desc" VARCHAR(2048),
    category VARCHAR(4),
    country VARCHAR(4),
    beacon_type VARCHAR(4),
    protection_type VARCHAR(4),
    maintenance_level VARCHAR(4),
    mrk_id_dist INTEGER,
    disturbed CHAR(1) NOT NULL,
    disturbed_date TIMESTAMP,
    mrk_id_repl INTEGER,
    wrk_id_created INTEGER,
    replaced CHAR(1) NOT NULL,
    replaced_date TIMESTAMP,
    mark_annotation VARCHAR(50),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_mark
    ADD CONSTRAINT pkey_crs_mark PRIMARY KEY (id);

ALTER TABLE crs_mark ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_mark ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE crs_mark ALTER COLUMN mrk_id_dist SET STATISTICS 1000;
ALTER TABLE crs_mark ALTER COLUMN mrk_id_repl SET STATISTICS 1000;
ALTER TABLE crs_mark ALTER COLUMN nod_id SET STATISTICS 1000;
ALTER TABLE crs_mark ALTER COLUMN wrk_id_created SET STATISTICS 1000;

ALTER TABLE crs_mark OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_mark FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_mark TO bde_admin;
GRANT SELECT ON TABLE crs_mark TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_mark_name
--------------------------------------------------------------------------------

CREATE TABLE crs_mark_name (
    mrk_id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    name VARCHAR(100) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_mark_name
    ADD CONSTRAINT pkey_crs_mark_name PRIMARY KEY (mrk_id, type);

ALTER TABLE crs_mark_name ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_mark_name ALTER COLUMN mrk_id SET STATISTICS 500;
ALTER TABLE crs_mark_name ALTER COLUMN name SET STATISTICS 500;

ALTER TABLE crs_mark_name OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_mark_name FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_mark_name TO bde_admin;
GRANT SELECT ON TABLE crs_mark_name TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_mark_sup_doc
--------------------------------------------------------------------------------

CREATE TABLE crs_mark_sup_doc (
    mrk_id INTEGER NOT NULL,
    sud_id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_mark_sup_doc
    ADD CONSTRAINT pkey_crs_mark_sup_doc PRIMARY KEY (mrk_id, sud_id);

ALTER TABLE crs_mark_sup_doc ALTER COLUMN mrk_id SET STATISTICS 250;
ALTER TABLE crs_mark_sup_doc ALTER COLUMN sud_id SET STATISTICS 250;

ALTER TABLE crs_mark_sup_doc OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_mark_sup_doc FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_mark_sup_doc TO bde_admin;
GRANT SELECT ON TABLE crs_mark_sup_doc TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_mrk_phys_state
--------------------------------------------------------------------------------

CREATE TABLE crs_mrk_phys_state (
    mrk_id INTEGER NOT NULL,
    wrk_id INTEGER NOT NULL,
    "type" VARCHAR(4) NOT NULL,
    condition VARCHAR(4) NOT NULL,
    existing_mark CHAR(1) NOT NULL,
    status VARCHAR(4) NOT NULL,
    ref_datetime TIMESTAMP NOT NULL,
    pend_mark_status CHAR(4),
    pend_replaced CHAR(1),
    pend_disturbed CHAR(1),
    mrk_id_pend_rep INTEGER,
    mrk_id_pend_dist INTEGER,
    pend_dist_date TIMESTAMP,
    pend_repl_date TIMESTAMP,
    pend_mark_name VARCHAR(100),
    pend_mark_type VARCHAR(4),
    pend_mark_ann VARCHAR(50),
    description VARCHAR(2048),
    latest_condition VARCHAR(4),
    latest_cond_date TIMESTAMP,
    pend_altr_name VARCHAR(100),
    pend_bcon_type VARCHAR(4),
    pend_hist_name VARCHAR(100),
    pend_mrk_desc VARCHAR(2048),
    pend_othr_name VARCHAR(100),
    pend_prot_type VARCHAR(4),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_mrk_phys_state 
    ADD CONSTRAINT pkey_crs_mrk_phys_state PRIMARY KEY (wrk_id, "type", mrk_id);

ALTER TABLE crs_mrk_phys_state ALTER COLUMN mrk_id SET STATISTICS 500;
ALTER TABLE crs_mrk_phys_state ALTER COLUMN wrk_id SET STATISTICS 500;

ALTER TABLE crs_mrk_phys_state OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_mrk_phys_state FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_mrk_phys_state TO bde_admin;
GRANT SELECT ON TABLE crs_mrk_phys_state TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_mesh_blk
--------------------------------------------------------------------------------

CREATE TABLE crs_mesh_blk (
    id INTEGER NOT NULL,
    alt_id INTEGER,
    code VARCHAR(7) NOT NULL,
    start_datetime TIMESTAMP NOT NULL,
    end_datetime TIMESTAMP,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_mesh_blk
    ADD CONSTRAINT pkey_crs_mesh_blk PRIMARY KEY (id);

ALTER TABLE crs_mesh_blk ALTER COLUMN alt_id SET STATISTICS 250;
ALTER TABLE crs_mesh_blk ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_mesh_blk ALTER COLUMN id SET STATISTICS 250;

ALTER TABLE crs_mesh_blk OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_mesh_blk FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_mesh_blk TO bde_admin;
GRANT SELECT ON TABLE crs_mesh_blk TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_mesh_blk_area
--------------------------------------------------------------------------------

CREATE TABLE crs_mesh_blk_area (
    mbk_id INTEGER NOT NULL,
    stt_id INTEGER NOT NULL,
    alt_id INTEGER,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_mesh_blk_area
    ADD CONSTRAINT pkey_crs_mesh_blk_area PRIMARY KEY (mbk_id, stt_id);

ALTER TABLE crs_mesh_blk_area ALTER COLUMN alt_id SET STATISTICS 250;
ALTER TABLE crs_mesh_blk_area ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_mesh_blk_area ALTER COLUMN mbk_id SET STATISTICS 250;
ALTER TABLE crs_mesh_blk_area ALTER COLUMN stt_id SET STATISTICS 250;

ALTER TABLE crs_mesh_blk_area OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_mesh_blk_area FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_mesh_blk_area TO bde_admin;
GRANT SELECT ON TABLE crs_mesh_blk_area TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_mesh_blk_bdry
--------------------------------------------------------------------------------

CREATE TABLE crs_mesh_blk_bdry (
    mbk_id INTEGER NOT NULL,
    mbl_id INTEGER NOT NULL,
    alt_id INTEGER,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_mesh_blk_bdry
    ADD CONSTRAINT pkey_crs_mesh_blk_bdry PRIMARY KEY (mbk_id, mbl_id);

ALTER TABLE crs_mesh_blk_bdry ALTER COLUMN alt_id SET STATISTICS 500;
ALTER TABLE crs_mesh_blk_bdry ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_mesh_blk_bdry ALTER COLUMN mbk_id SET STATISTICS 500;
ALTER TABLE crs_mesh_blk_bdry ALTER COLUMN mbl_id SET STATISTICS 500;

ALTER TABLE crs_mesh_blk_bdry OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_mesh_blk_bdry FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_mesh_blk_bdry TO bde_admin;
GRANT SELECT ON TABLE crs_mesh_blk_bdry TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_mesh_blk_line
--------------------------------------------------------------------------------

CREATE TABLE crs_mesh_blk_line (
    id INTEGER NOT NULL,
    line_type VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    alt_id INTEGER,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'LINESTRING'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_mesh_blk_line
    ADD CONSTRAINT pkey_crs_mesh_blk_line PRIMARY KEY (id);

ALTER TABLE crs_mesh_blk_line ALTER COLUMN alt_id SET STATISTICS 500;
ALTER TABLE crs_mesh_blk_line ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_mesh_blk_line ALTER COLUMN id SET STATISTICS 500;

ALTER TABLE crs_mesh_blk_line OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_mesh_blk_line FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_mesh_blk_line TO bde_admin;
GRANT SELECT ON TABLE crs_mesh_blk_line TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_mesh_blk_place
--------------------------------------------------------------------------------

CREATE TABLE crs_mesh_blk_place (
    epl_id INTEGER NOT NULL,
    mbk_id INTEGER NOT NULL,
    alt_id INTEGER,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_mesh_blk_place
    ADD CONSTRAINT pkey_crs_mesh_blk_place PRIMARY KEY (epl_id, mbk_id);

ALTER TABLE crs_mesh_blk_place ALTER COLUMN alt_id SET STATISTICS 250;
ALTER TABLE crs_mesh_blk_place ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_mesh_blk_place ALTER COLUMN epl_id SET STATISTICS 250;
ALTER TABLE crs_mesh_blk_place ALTER COLUMN mbk_id SET STATISTICS 250;

ALTER TABLE crs_mesh_blk_place OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_mesh_blk_place FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_mesh_blk_place TO bde_admin;
GRANT SELECT ON TABLE crs_mesh_blk_place TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_network_plan
--------------------------------------------------------------------------------

CREATE TABLE crs_network_plan (
    id INTEGER NOT NULL,
    type VARCHAR(10) NOT NULL,
    status VARCHAR(4) NOT NULL,
    datum_order VARCHAR(4) NOT NULL,
    dtm_id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_network_plan
    ADD CONSTRAINT pkey_crs_network_plan PRIMARY KEY (id);

ALTER TABLE crs_network_plan OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_network_plan FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_network_plan TO bde_admin;
GRANT SELECT ON TABLE crs_network_plan TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_node
--------------------------------------------------------------------------------

CREATE TABLE crs_node (
    id INTEGER NOT NULL,
    cos_id_official INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    order_group_off INTEGER NOT NULL,
    sit_id INTEGER,
    alt_id INTEGER,
    wrk_id_created INTEGER,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'POINT'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_node
    ADD CONSTRAINT pkey_crs_node PRIMARY KEY (id);

ALTER TABLE crs_node ALTER COLUMN alt_id SET STATISTICS 1000;
ALTER TABLE crs_node ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_node ALTER COLUMN cos_id_official SET STATISTICS 1000;
ALTER TABLE crs_node ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE crs_node ALTER COLUMN sit_id SET STATISTICS 1000;
ALTER TABLE crs_node ALTER COLUMN wrk_id_created SET STATISTICS 1000;

ALTER TABLE crs_node OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_node FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_node TO bde_admin;
GRANT SELECT ON TABLE crs_node TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_node_prp_order
--------------------------------------------------------------------------------

CREATE TABLE crs_node_prp_order (
    dtm_id INTEGER NOT NULL,
    nod_id INTEGER NOT NULL,
    cor_id INTEGER,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_node_prp_order
    ADD CONSTRAINT pkey_crs_node_prp_order PRIMARY KEY (dtm_id, nod_id);

ALTER TABLE crs_node_prp_order OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_node_prp_order FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_node_prp_order TO bde_admin;
GRANT SELECT ON TABLE crs_node_prp_order TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_node_works
--------------------------------------------------------------------------------

CREATE TABLE crs_node_works (
    nod_id INTEGER NOT NULL,
    wrk_id INTEGER NOT NULL,
    pend_node_status VARCHAR(4),
    purpose VARCHAR(4),
    adopted CHAR(1),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_node_works
    ADD CONSTRAINT pkey_crs_node_works PRIMARY KEY (nod_id, wrk_id);

ALTER TABLE crs_node_works ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_node_works ALTER COLUMN nod_id SET STATISTICS 1000;
ALTER TABLE crs_node_works ALTER COLUMN wrk_id SET STATISTICS 1000;

ALTER TABLE crs_node_works OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_node_works FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_node_works TO bde_admin;
GRANT SELECT ON TABLE crs_node_works TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_nominal_index
--------------------------------------------------------------------------------

CREATE TABLE crs_nominal_index (
    id INTEGER NOT NULL,
    ttl_title_no VARCHAR(20) NOT NULL,
    status VARCHAR(4) NOT NULL,
    name_type VARCHAR(4) NOT NULL,
    corporate_name VARCHAR(250),
    surname VARCHAR(100),
    other_names VARCHAR(100),
    prp_id INTEGER,
    dlg_id_crt INTEGER,
    dlg_id_ext INTEGER,
    dlg_id_hst INTEGER,
    significance SMALLINT NOT NULL,
    system_ext CHAR(1)
);

ALTER TABLE ONLY crs_nominal_index
    ADD CONSTRAINT pkey_crs_nominal_index PRIMARY KEY (id);

ALTER TABLE crs_nominal_index ALTER COLUMN corporate_name SET STATISTICS 1000;
ALTER TABLE crs_nominal_index ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE crs_nominal_index ALTER COLUMN other_names SET STATISTICS 1000;
ALTER TABLE crs_nominal_index ALTER COLUMN prp_id SET STATISTICS 1000;
ALTER TABLE crs_nominal_index ALTER COLUMN surname SET STATISTICS 1000;
ALTER TABLE crs_nominal_index ALTER COLUMN ttl_title_no SET STATISTICS 1000;

ALTER TABLE crs_nominal_index OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_nominal_index FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_nominal_index TO bde_admin;
GRANT SELECT ON TABLE crs_nominal_index TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_obs_accuracy
--------------------------------------------------------------------------------

CREATE TABLE crs_obs_accuracy (
    obn_id1 INTEGER NOT NULL,
    obn_id2 INTEGER NOT NULL,
    value_11 DOUBLE PRECISION,
    value_12 DOUBLE PRECISION,
    value_13 DOUBLE PRECISION,
    value_21 DOUBLE PRECISION,
    value_22 DOUBLE PRECISION,
    value_23 DOUBLE PRECISION,
    value_31 DOUBLE PRECISION,
    value_32 DOUBLE PRECISION,
    value_33 DOUBLE PRECISION,
    id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_obs_accuracy
    ADD CONSTRAINT pkey_crs_obs_accuracy PRIMARY KEY (id);

ALTER TABLE crs_obs_accuracy ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_obs_accuracy ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE crs_obs_accuracy ALTER COLUMN obn_id1 SET STATISTICS 1000;
ALTER TABLE crs_obs_accuracy ALTER COLUMN obn_id2 SET STATISTICS 1000;

ALTER TABLE crs_obs_accuracy OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_obs_accuracy FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_obs_accuracy TO bde_admin;
GRANT SELECT ON TABLE crs_obs_accuracy TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_obs_elem_type
--------------------------------------------------------------------------------

CREATE TABLE crs_obs_elem_type (
    type VARCHAR(4) NOT NULL,
    description VARCHAR(100) NOT NULL,
    uom_code VARCHAR(4) NOT NULL,
    format_code VARCHAR(4) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_obs_elem_type
    ADD CONSTRAINT pkey_crs_obs_elem_type PRIMARY KEY (type);

ALTER TABLE crs_obs_elem_type OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_obs_elem_type FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_obs_elem_type TO bde_admin;
GRANT SELECT ON TABLE crs_obs_elem_type TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_obs_set
--------------------------------------------------------------------------------

CREATE TABLE crs_obs_set (
    id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    reason VARCHAR(100) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_obs_set
    ADD CONSTRAINT pkey_crs_obs_set PRIMARY KEY (id);

ALTER TABLE crs_obs_set OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_obs_set FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_obs_set TO bde_admin;
GRANT SELECT ON TABLE crs_obs_set TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_obs_type
--------------------------------------------------------------------------------

CREATE TABLE crs_obs_type (
    type VARCHAR(4) NOT NULL,
    sub_type VARCHAR(4) NOT NULL,
    vector_type VARCHAR(4) NOT NULL,
    oet_type_1 VARCHAR(4) NOT NULL,
    oet_type_2 VARCHAR(4),
    oet_type_3 VARCHAR(4),
    description VARCHAR(100) NOT NULL,
    datum_reqd CHAR(1),
    time_reqd CHAR(1),
    trajectory_reqd CHAR(1),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_obs_type
    ADD CONSTRAINT pkey_crs_obs_type PRIMARY KEY (type, sub_type);

ALTER TABLE crs_obs_type OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_obs_type FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_obs_type TO bde_admin;
GRANT SELECT ON TABLE crs_obs_type TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_observation
--------------------------------------------------------------------------------

CREATE TABLE crs_observation (
    id INTEGER NOT NULL,
    obt_type VARCHAR(4),
    obt_sub_type VARCHAR(4),
    stp_id_local INTEGER NOT NULL,
    stp_id_remote INTEGER,
    obs_id INTEGER,
    cos_id INTEGER,
    rdn_id INTEGER,
    vct_id INTEGER,
    ref_datetime TIMESTAMP NOT NULL,
    acc_multiplier NUMERIC(22,12),
    status VARCHAR(4) NOT NULL,
    geodetic_class VARCHAR(4),
    cadastral_class VARCHAR(4),
    surveyed_class VARCHAR(4),
    value_1 NUMERIC(22,12) NOT NULL,
    value_2 NUMERIC(22,12),
    value_3 NUMERIC(22,12),
    arc_radius NUMERIC(22,12),
    arc_direction VARCHAR(4),
    obn_id_amendment INTEGER,
    code VARCHAR(100),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_observation
    ADD CONSTRAINT pkey_crs_observation PRIMARY KEY (id);

ALTER TABLE crs_observation ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_observation ALTER COLUMN cos_id SET STATISTICS 1000;
ALTER TABLE crs_observation ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE crs_observation ALTER COLUMN obn_id_amendment SET STATISTICS 1000;
ALTER TABLE crs_observation ALTER COLUMN obs_id SET STATISTICS 1000;
ALTER TABLE crs_observation ALTER COLUMN obt_sub_type SET STATISTICS 1000;
ALTER TABLE crs_observation ALTER COLUMN obt_type SET STATISTICS 1000;
ALTER TABLE crs_observation ALTER COLUMN rdn_id SET STATISTICS 1000;
ALTER TABLE crs_observation ALTER COLUMN stp_id_local SET STATISTICS 1000;
ALTER TABLE crs_observation ALTER COLUMN stp_id_remote SET STATISTICS 1000;
ALTER TABLE crs_observation ALTER COLUMN vct_id SET STATISTICS 1000;

ALTER TABLE crs_observation OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_observation FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_observation TO bde_admin;
GRANT SELECT ON TABLE crs_observation TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_off_cord_sys
--------------------------------------------------------------------------------

CREATE TABLE crs_off_cord_sys (
    id INTEGER NOT NULL,
    cos_id INTEGER NOT NULL,
    description VARCHAR(100) NOT NULL,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'POLYGON'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_off_cord_sys
    ADD CONSTRAINT pkey_crs_off_cord_sys PRIMARY KEY (id);

ALTER TABLE crs_off_cord_sys OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_off_cord_sys FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_off_cord_sys TO bde_admin;
GRANT SELECT ON TABLE crs_off_cord_sys TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_office
--------------------------------------------------------------------------------

CREATE TABLE crs_office (
    code VARCHAR(4) NOT NULL,
    name VARCHAR(50) NOT NULL,
    rcs_name VARCHAR(50) NOT NULL,
    cis_name VARCHAR(50) NOT NULL,
    alloc_source_table VARCHAR(50) NOT NULL,
    display_in_dropdowns CHAR(1),
    display_in_treeview CHAR(1),
    fax VARCHAR(30),
    internet VARCHAR(100),
    postal_address VARCHAR(100),
    postal_address_prefix VARCHAR(100),
    postal_address_suffix VARCHAR(100),
    postal_address_town VARCHAR(100),
    postal_country CHAR(4),
    postal_dx_box VARCHAR(10),
    postal_postcode VARCHAR(6),
    printer_name VARCHAR(50),
    telephone VARCHAR(30),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_office
    ADD CONSTRAINT pkey_crs_office PRIMARY KEY (code);

ALTER TABLE crs_office OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_office FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_office TO bde_admin;
GRANT SELECT ON TABLE crs_office TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_ordinate_adj
--------------------------------------------------------------------------------

CREATE TABLE crs_ordinate_adj (
    adj_id INTEGER NOT NULL,
    coo_id_source INTEGER NOT NULL,
    sdc_status_prop CHAR(1) NOT NULL,
    coo_id_output INTEGER,
    "constraint" VARCHAR(4),
    rejected CHAR(1),
    adjust_fixed CHAR(1),
    cor_id_prop INTEGER,
    change_east NUMERIC(22,12),
    change_north NUMERIC(22,12),
    change_height NUMERIC(22,12),
    h_max_accuracy NUMERIC(22,12),
    h_min_accuracy NUMERIC(22,12),
    h_max_azimuth NUMERIC(22,12),
    v_accuracy NUMERIC(22,12),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_ordinate_adj
    ADD CONSTRAINT pkey_crs_ordinate_adj PRIMARY KEY (coo_id_source, adj_id);

ALTER TABLE crs_ordinate_adj ALTER COLUMN adj_id SET STATISTICS 1000;
ALTER TABLE crs_ordinate_adj ALTER COLUMN coo_id_output SET STATISTICS 1000;
ALTER TABLE crs_ordinate_adj ALTER COLUMN coo_id_source SET STATISTICS 1000;
ALTER TABLE crs_ordinate_adj ALTER COLUMN cor_id_prop SET STATISTICS 1000;

ALTER TABLE crs_ordinate_adj OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_ordinate_adj FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_ordinate_adj TO bde_admin;
GRANT SELECT ON TABLE crs_ordinate_adj TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_ordinate_type
--------------------------------------------------------------------------------

CREATE TABLE crs_ordinate_type (
    type VARCHAR(4) NOT NULL,
    uom_code VARCHAR(4) NOT NULL,
    description VARCHAR(100) NOT NULL,
    format_code VARCHAR(4) NOT NULL,
    mandatory CHAR(1) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_ordinate_type
    ADD CONSTRAINT pkey_crs_ordinate_type PRIMARY KEY (type);

ALTER TABLE crs_ordinate_type OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_ordinate_type FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_ordinate_type TO bde_admin;
GRANT SELECT ON TABLE crs_ordinate_type TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_parcel
--------------------------------------------------------------------------------

CREATE TABLE crs_parcel (
    id INTEGER NOT NULL,
    ldt_loc_id INTEGER NOT NULL,
    img_id INTEGER,
    fen_id INTEGER,
    toc_code VARCHAR(4) NOT NULL,
    alt_id INTEGER,
    area NUMERIC(20,4),
    nonsurvey_def VARCHAR(255),
    appellation_date TIMESTAMP,
    parcel_intent VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    total_area NUMERIC(20,4),
    calculated_area NUMERIC(20,4),
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_parcel
    ADD CONSTRAINT pkey_crs_parcel PRIMARY KEY (id);

ALTER TABLE crs_parcel ALTER COLUMN alt_id SET STATISTICS 500;
ALTER TABLE crs_parcel ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_parcel ALTER COLUMN fen_id SET STATISTICS 500;
ALTER TABLE crs_parcel ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_parcel ALTER COLUMN img_id SET STATISTICS 500;
ALTER TABLE crs_parcel ALTER COLUMN ldt_loc_id SET STATISTICS 500;
ALTER TABLE crs_parcel ALTER COLUMN toc_code SET STATISTICS 500;

ALTER TABLE crs_parcel OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_parcel FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_parcel TO bde_admin;
GRANT SELECT ON TABLE crs_parcel TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_parcel_bndry
--------------------------------------------------------------------------------

CREATE TABLE crs_parcel_bndry (
    pri_id INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    lin_id INTEGER NOT NULL,
    reversed CHAR(1) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_parcel_bndry
    ADD CONSTRAINT pkey_crs_parcel_bndry PRIMARY KEY (pri_id, sequence);

ALTER TABLE crs_parcel_bndry ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_parcel_bndry ALTER COLUMN lin_id SET STATISTICS 1000;
ALTER TABLE crs_parcel_bndry ALTER COLUMN pri_id SET STATISTICS 1000;

ALTER TABLE crs_parcel_bndry OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_parcel_bndry FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_parcel_bndry TO bde_admin;
GRANT SELECT ON TABLE crs_parcel_bndry TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_parcel_dimen
--------------------------------------------------------------------------------

CREATE TABLE crs_parcel_dimen (
    obn_id INTEGER NOT NULL,
    par_id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_parcel_dimen
    ADD CONSTRAINT pkey_crs_parcel_dimen PRIMARY KEY (obn_id, par_id);

ALTER TABLE crs_parcel_dimen ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_parcel_dimen ALTER COLUMN obn_id SET STATISTICS 1000;
ALTER TABLE crs_parcel_dimen ALTER COLUMN par_id SET STATISTICS 1000;

ALTER TABLE crs_parcel_dimen OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_parcel_dimen FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_parcel_dimen TO bde_admin;
GRANT SELECT ON TABLE crs_parcel_dimen TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_parcel_label
--------------------------------------------------------------------------------

CREATE TABLE crs_parcel_label (
    id INTEGER NOT NULL,
    par_id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'POINT'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_parcel_label
    ADD CONSTRAINT pkey_crs_parcel_label PRIMARY KEY (id);

ALTER TABLE crs_parcel_label ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_parcel_label ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_parcel_label ALTER COLUMN par_id SET STATISTICS 500;

ALTER TABLE crs_parcel_label OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_parcel_label FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_parcel_label TO bde_admin;
GRANT SELECT ON TABLE crs_parcel_label TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_parcel_ring
--------------------------------------------------------------------------------

CREATE TABLE crs_parcel_ring (
    id INTEGER NOT NULL,
    par_id INTEGER NOT NULL,
    pri_id_parent_ring INTEGER,
    is_ring CHAR(1) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_parcel_ring
    ADD CONSTRAINT pkey_crs_parcel_ring PRIMARY KEY (id);

ALTER TABLE crs_parcel_ring ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_parcel_ring ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_parcel_ring ALTER COLUMN par_id SET STATISTICS 500;
ALTER TABLE crs_parcel_ring ALTER COLUMN pri_id_parent_ring SET STATISTICS 500;

ALTER TABLE crs_parcel_ring OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_parcel_ring FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_parcel_ring TO bde_admin;
GRANT SELECT ON TABLE crs_parcel_ring TO bde_user;

/*
--------------------------------------------------------------------------------
-- BDE table crs_process
--------------------------------------------------------------------------------

CREATE TABLE crs_process (
    id INTEGER NOT NULL,
    job_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_process
    ADD CONSTRAINT pkey_crs_process PRIMARY KEY (id);

ALTER TABLE crs_process ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_process ALTER COLUMN job_id SET STATISTICS 500;

ALTER TABLE crs_process OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_process FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_process TO bde_admin;
GRANT SELECT ON TABLE crs_process TO bde_user;

*/

--------------------------------------------------------------------------------
-- BDE table crs_programme
--------------------------------------------------------------------------------

CREATE TABLE crs_programme (
    status VARCHAR(4) NOT NULL,
    type VARCHAR(4) NOT NULL,
    account_id VARCHAR(20),
    sched_start DATE,
    purpose VARCHAR(100),
    cost_centre VARCHAR(100),
    finance_year_date VARCHAR(9),
    usr_id VARCHAR(20) NOT NULL,
    sched_end DATE,
    nwp_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_programme
    ADD CONSTRAINT pkey_crs_programme PRIMARY KEY (id);

ALTER TABLE crs_programme OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_programme FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_programme TO bde_admin;
GRANT SELECT ON TABLE crs_programme TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_proprietor
--------------------------------------------------------------------------------

CREATE TABLE crs_proprietor (
    id INTEGER NOT NULL,
    ets_id INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    type VARCHAR(4) NOT NULL,
    corporate_name VARCHAR(250),
    prime_surname VARCHAR(100),
    prime_other_names VARCHAR(100),
    minor CHAR(1),
    minor_dob DATE,
    name_suffix VARCHAR(4),
    original_flag CHAR(1) NOT NULL,
    sort_order INTEGER,
    system_ext CHAR(1)
);

ALTER TABLE ONLY crs_proprietor
    ADD CONSTRAINT pkey_crs_proprietor PRIMARY KEY (id);

ALTER TABLE crs_proprietor ALTER COLUMN ets_id SET STATISTICS 500;
ALTER TABLE crs_proprietor ALTER COLUMN id SET STATISTICS 500;

ALTER TABLE crs_proprietor OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_proprietor FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_proprietor TO bde_admin;
GRANT SELECT ON TABLE crs_proprietor TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_reduct_meth
--------------------------------------------------------------------------------

CREATE TABLE crs_reduct_meth (
    id INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    description VARCHAR(100) NOT NULL,
    name VARCHAR(30) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_reduct_meth
    ADD CONSTRAINT pkey_crs_reduct_meth PRIMARY KEY (id);

ALTER TABLE crs_reduct_meth OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_reduct_meth FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_reduct_meth TO bde_admin;
GRANT SELECT ON TABLE crs_reduct_meth TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_reduct_run
--------------------------------------------------------------------------------

CREATE TABLE crs_reduct_run (
    id INTEGER NOT NULL,
    rdm_id INTEGER NOT NULL,
    datetime TIMESTAMP,
    description VARCHAR(100),
    traj_type VARCHAR(4),
    usr_id_exec VARCHAR(20),
    software_used VARCHAR(30),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_reduct_run
    ADD CONSTRAINT pkey_crs_reduct_run PRIMARY KEY (id);

ALTER TABLE crs_reduct_run OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_reduct_run FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_reduct_run TO bde_admin;
GRANT SELECT ON TABLE crs_reduct_run TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_ref_survey
--------------------------------------------------------------------------------

CREATE TABLE crs_ref_survey (
    sur_wrk_id_exist INTEGER NOT NULL,
    sur_wrk_id_new INTEGER NOT NULL,
    bearing_corr DECIMAL(16),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_ref_survey
    ADD CONSTRAINT pkey_crs_ref_survey PRIMARY KEY (sur_wrk_id_exist, sur_wrk_id_new);

ALTER TABLE crs_ref_survey ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_ref_survey ALTER COLUMN sur_wrk_id_exist SET STATISTICS 250;
ALTER TABLE crs_ref_survey ALTER COLUMN sur_wrk_id_new SET STATISTICS 250;

ALTER TABLE crs_ref_survey OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_ref_survey FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_ref_survey TO bde_admin;
GRANT SELECT ON TABLE crs_ref_survey TO bde_user;

/*
--------------------------------------------------------------------------------
-- BDE table crs_req_det
--------------------------------------------------------------------------------

CREATE TABLE crs_req_det (
    rqh_id INTEGER NOT NULL,
    line_no INTEGER NOT NULL,
    rqi_code VARCHAR(4),
    comment text,
    external CHAR(1) NOT NULL,
    complete CHAR(1) NOT NULL,
    tin_id INTEGER,
    reason text,
    fatal CHAR(1)
);

ALTER TABLE ONLY crs_req_det
    ADD CONSTRAINT pkey_crs_req_det PRIMARY KEY (rqh_id, line_no);

ALTER TABLE crs_req_det ALTER COLUMN line_no SET STATISTICS 500;
ALTER TABLE crs_req_det ALTER COLUMN rqh_id SET STATISTICS 500;
ALTER TABLE crs_req_det ALTER COLUMN rqi_code SET STATISTICS 500;
ALTER TABLE crs_req_det ALTER COLUMN tin_id SET STATISTICS 500;

ALTER TABLE crs_req_det OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_req_det FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_req_det TO bde_admin;
GRANT SELECT ON TABLE crs_req_det TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_req_hdr
--------------------------------------------------------------------------------

CREATE TABLE crs_req_hdr (
    id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    date_sent TIMESTAMP,
    sud_id INTEGER,
    wrk_id INTEGER,
    dlg_id INTEGER,
    usr_id VARCHAR(20),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_req_hdr
    ADD CONSTRAINT pkey_crs_req_hdr PRIMARY KEY (id);

ALTER TABLE crs_req_hdr ALTER COLUMN dlg_id SET STATISTICS 250;
ALTER TABLE crs_req_hdr ALTER COLUMN id SET STATISTICS 250;
ALTER TABLE crs_req_hdr ALTER COLUMN sud_id SET STATISTICS 250;
ALTER TABLE crs_req_hdr ALTER COLUMN usr_id SET STATISTICS 250;
ALTER TABLE crs_req_hdr ALTER COLUMN wrk_id SET STATISTICS 250;

ALTER TABLE crs_req_hdr OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_req_hdr FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_req_hdr TO bde_admin;
GRANT SELECT ON TABLE crs_req_hdr TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_req_item
--------------------------------------------------------------------------------

CREATE TABLE crs_req_item (
    code VARCHAR(4) NOT NULL,
    description text NOT NULL,
    type VARCHAR(4) NOT NULL,
    severity VARCHAR(4) NOT NULL,
    exec_type VARCHAR(4) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_req_item
    ADD CONSTRAINT pkey_crs_req_item PRIMARY KEY (code);

ALTER TABLE crs_req_item OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_req_item FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_req_item TO bde_admin;
GRANT SELECT ON TABLE crs_req_item TO bde_user;

*/
--------------------------------------------------------------------------------
-- BDE table crs_road_ctr_line
--------------------------------------------------------------------------------

CREATE TABLE crs_road_ctr_line (
    id INTEGER NOT NULL,
    alt_id INTEGER,
    status VARCHAR(4) NOT NULL,
    non_cadastral_rd CHAR(1) NOT NULL,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'LINESTRING'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_road_ctr_line
    ADD CONSTRAINT pkey_crs_road_ctr_line PRIMARY KEY (id);

ALTER TABLE crs_road_ctr_line ALTER COLUMN alt_id SET STATISTICS 250;
ALTER TABLE crs_road_ctr_line ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_road_ctr_line ALTER COLUMN id SET STATISTICS 250;

ALTER TABLE crs_road_ctr_line OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_road_ctr_line FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_road_ctr_line TO bde_admin;
GRANT SELECT ON TABLE crs_road_ctr_line TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_road_name
--------------------------------------------------------------------------------

CREATE TABLE crs_road_name (
    id INTEGER NOT NULL,
    alt_id INTEGER,
    type VARCHAR(4) NOT NULL,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(100),
    status VARCHAR(4) NOT NULL,
    unofficial_flag CHAR(1) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_road_name
    ADD CONSTRAINT pkey_crs_road_name PRIMARY KEY (id);

ALTER TABLE crs_road_name OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_road_name FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_road_name TO bde_admin;
GRANT SELECT ON TABLE crs_road_name TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_road_name_asc
--------------------------------------------------------------------------------

CREATE TABLE crs_road_name_asc (
    rna_id INTEGER NOT NULL,
    rcl_id INTEGER NOT NULL,
    alt_id INTEGER,
    priority INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_road_name_asc
    ADD CONSTRAINT pkey_crs_road_name_asc PRIMARY KEY (rna_id, rcl_id);

ALTER TABLE crs_road_name_asc ALTER COLUMN alt_id SET STATISTICS 250;
ALTER TABLE crs_road_name_asc ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_road_name_asc ALTER COLUMN rcl_id SET STATISTICS 250;
ALTER TABLE crs_road_name_asc ALTER COLUMN rna_id SET STATISTICS 250;

ALTER TABLE crs_road_name_asc OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_road_name_asc FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_road_name_asc TO bde_admin;
GRANT SELECT ON TABLE crs_road_name_asc TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_setup
--------------------------------------------------------------------------------

CREATE TABLE crs_setup (
    id INTEGER NOT NULL,
    nod_id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    valid_flag CHAR(1) NOT NULL,
    equipment_type VARCHAR(4),
    wrk_id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_setup
    ADD CONSTRAINT pkey_crs_setup PRIMARY KEY (id);

ALTER TABLE crs_setup ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_setup ALTER COLUMN equipment_type SET STATISTICS 1000;
ALTER TABLE crs_setup ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE crs_setup ALTER COLUMN nod_id SET STATISTICS 1000;
ALTER TABLE crs_setup ALTER COLUMN wrk_id SET STATISTICS 1000;

ALTER TABLE crs_setup OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_setup FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_setup TO bde_admin;
GRANT SELECT ON TABLE crs_setup TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_site
--------------------------------------------------------------------------------

CREATE TABLE crs_site (
    id INTEGER NOT NULL,
    type VARCHAR(4) NOT NULL,
    "desc" VARCHAR(2048),
    occupier VARCHAR(100),
    audit_id INTEGER NOT NULL,
    wrk_id_created INTEGER
);

ALTER TABLE ONLY crs_site
    ADD CONSTRAINT pkey_crs_site PRIMARY KEY (id);

ALTER TABLE crs_site ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_site ALTER COLUMN id SET STATISTICS 250;
ALTER TABLE crs_site ALTER COLUMN wrk_id_created SET STATISTICS 250;

ALTER TABLE crs_site OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_site FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_site TO bde_admin;
GRANT SELECT ON TABLE crs_site TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_site_locality
--------------------------------------------------------------------------------

CREATE TABLE crs_site_locality (
    sit_id INTEGER NOT NULL,
    loc_id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_site_locality
    ADD CONSTRAINT pkey_crs_site_locality PRIMARY KEY (sit_id, loc_id);

ALTER TABLE crs_site_locality ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_site_locality ALTER COLUMN loc_id SET STATISTICS 250;
ALTER TABLE crs_site_locality ALTER COLUMN sit_id SET STATISTICS 250;

ALTER TABLE crs_site_locality OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_site_locality FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_site_locality TO bde_admin;
GRANT SELECT ON TABLE crs_site_locality TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_stat_act_parcl
--------------------------------------------------------------------------------

CREATE TABLE crs_stat_act_parcl (
    sta_id INTEGER NOT NULL,
    par_id INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    action VARCHAR(4) NOT NULL,
    purpose VARCHAR(250),
    name VARCHAR(250),
    comments VARCHAR(250),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_stat_act_parcl
    ADD CONSTRAINT pkey_crs_stat_act_parcl PRIMARY KEY (sta_id, par_id);

ALTER TABLE crs_stat_act_parcl ALTER COLUMN par_id SET STATISTICS 250;
ALTER TABLE crs_stat_act_parcl ALTER COLUMN sta_id SET STATISTICS 250;

ALTER TABLE crs_stat_act_parcl OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_stat_act_parcl FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_stat_act_parcl TO bde_admin;
GRANT SELECT ON TABLE crs_stat_act_parcl TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_stat_version
--------------------------------------------------------------------------------

CREATE TABLE crs_stat_version (
    version INTEGER NOT NULL,
    area_class VARCHAR(4) NOT NULL,
    "desc" VARCHAR(50),
    statute_action VARCHAR(50) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_stat_version
    ADD CONSTRAINT pkey_crs_stat_version PRIMARY KEY (version, area_class);

ALTER TABLE crs_stat_version OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_stat_version FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_stat_version TO bde_admin;
GRANT SELECT ON TABLE crs_stat_version TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_statist_area
--------------------------------------------------------------------------------

CREATE TABLE crs_statist_area (
    id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL,
    name_abrev VARCHAR(18) NOT NULL,
    code VARCHAR(6) NOT NULL,
    status VARCHAR(4) NOT NULL,
    sav_version INTEGER NOT NULL,
    sav_area_class VARCHAR(4) NOT NULL,
    usr_id_firm_ta VARCHAR(20),
    alt_id INTEGER,
    se_row_id INTEGER,
    audit_id INTEGER NOT NULL,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'MULTIPOLYGON') OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_statist_area
    ADD CONSTRAINT pkey_crs_statist_area PRIMARY KEY (id);

ALTER TABLE crs_statist_area OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_statist_area FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_statist_area TO bde_admin;
GRANT SELECT ON TABLE crs_statist_area TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_statute
--------------------------------------------------------------------------------

CREATE TABLE crs_statute  (
    id INTEGER NOT NULL,
    section VARCHAR(100) NOT NULL,
    name_and_date VARCHAR(100) NOT NULL,
    still_in_force CHAR(1) NOT NULL,
    in_force_date DATE,
    repeal_date DATE,
    type VARCHAR(4),
    "default" CHAR(1),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_statute
    ADD CONSTRAINT pkey_crs_statute PRIMARY KEY (id);

ALTER TABLE crs_statute OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_statute FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_statute TO bde_admin;
GRANT SELECT ON TABLE crs_statute TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_statute_action
--------------------------------------------------------------------------------

CREATE TABLE crs_statute_action (
    type VARCHAR(4) NOT NULL,
    status VARCHAR(4),
    ste_id INTEGER,
    sur_wrk_id_vesting INTEGER,
    gazette_year INTEGER,
    gazette_page INTEGER,
    gazette_type VARCHAR(4),
    other_legality VARCHAR(250),
    recorded_date DATE,
    id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_statute_action
    ADD CONSTRAINT pkey_crs_statute_action PRIMARY KEY (id);

ALTER TABLE crs_statute_action ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_statute_action ALTER COLUMN id SET STATISTICS 250;
ALTER TABLE crs_statute_action ALTER COLUMN ste_id SET STATISTICS 250;
ALTER TABLE crs_statute_action ALTER COLUMN sur_wrk_id_vesting SET STATISTICS 250;

ALTER TABLE crs_statute_action OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_statute_action FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_statute_action TO bde_admin;
GRANT SELECT ON TABLE crs_statute_action TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_street_address
--------------------------------------------------------------------------------

CREATE TABLE crs_street_address (
    house_number VARCHAR(25) NOT NULL,
    range_low INTEGER NOT NULL,
    range_high INTEGER,
    status VARCHAR(4) NOT NULL,
    unofficial_flag CHAR(1) NOT NULL,
    rcl_id INTEGER NOT NULL,
    rna_id INTEGER NOT NULL,
    alt_id INTEGER,
    id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'POINT'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_street_address
    ADD CONSTRAINT pkey_crs_street_address PRIMARY KEY (id);

ALTER TABLE crs_street_address ALTER COLUMN alt_id SET STATISTICS 500;
ALTER TABLE crs_street_address ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_street_address ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_street_address ALTER COLUMN rcl_id SET STATISTICS 500;
ALTER TABLE crs_street_address ALTER COLUMN rna_id SET STATISTICS 500;

ALTER TABLE crs_street_address OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_street_address FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_street_address TO bde_admin;
GRANT SELECT ON TABLE crs_street_address TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_sur_admin_area
--------------------------------------------------------------------------------

CREATE TABLE crs_sur_admin_area (
    sur_wrk_id INTEGER NOT NULL,
    stt_id INTEGER NOT NULL,
    xstt_id INTEGER,
    eed_req_id INTEGER,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_sur_admin_area
    ADD CONSTRAINT pkey_crs_sur_admin_area PRIMARY KEY (sur_wrk_id, stt_id);

ALTER TABLE crs_sur_admin_area ALTER COLUMN audit_id SET STATISTICS 250;
ALTER TABLE crs_sur_admin_area ALTER COLUMN eed_req_id SET STATISTICS 250;
ALTER TABLE crs_sur_admin_area ALTER COLUMN stt_id SET STATISTICS 250;
ALTER TABLE crs_sur_admin_area ALTER COLUMN sur_wrk_id SET STATISTICS 250;
ALTER TABLE crs_sur_admin_area ALTER COLUMN xstt_id SET STATISTICS 250;

ALTER TABLE crs_sur_admin_area OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_sur_admin_area FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_sur_admin_area TO bde_admin;
GRANT SELECT ON TABLE crs_sur_admin_area TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_sur_plan_ref
--------------------------------------------------------------------------------

CREATE TABLE crs_sur_plan_ref (
    id INTEGER NOT NULL,
    wrk_id INTEGER,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_geotype_shape CHECK (((GeometryType(shape) = 'POINT'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_sur_plan_ref
    ADD CONSTRAINT pkey_crs_sur_plan_ref PRIMARY KEY (id);

ALTER TABLE crs_sur_plan_ref ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_sur_plan_ref ALTER COLUMN wrk_id SET STATISTICS 500;

ALTER TABLE crs_sur_plan_ref OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_sur_plan_ref FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_sur_plan_ref TO bde_admin;
GRANT SELECT ON TABLE crs_sur_plan_ref TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_survey
--------------------------------------------------------------------------------

CREATE TABLE crs_survey (
    wrk_id INTEGER NOT NULL,
    ldt_loc_id INTEGER NOT NULL,
    dataset_series CHAR(4) NOT NULL,
    dataset_id VARCHAR(20) NOT NULL,
    type_of_dataset CHAR(4) NOT NULL,
    data_source CHAR(4) NOT NULL,
    lodge_order INTEGER NOT NULL,
    dataset_suffix VARCHAR(7),
    surveyor_data_ref VARCHAR(100),
    survey_class CHAR(4),
    description VARCHAR(2048),
    usr_id_sol VARCHAR(20),
    survey_date DATE,
    certified_date DATE,
    registered_date DATE,
    chf_sur_amnd_date DATE,
    dlr_amnd_date DATE,
    cadastral_surv_acc CHAR(1),
    prior_wrk_id INTEGER,
    abey_prior_status CHAR(4),
    fhr_id INTEGER,
    pnx_id_submitted INTEGER,
    audit_id INTEGER NOT NULL,
    usr_id_sol_firm VARCHAR(20),
    sig_id INTEGER,
    xml_uploaded CHAR(1),
    xsv_id INTEGER
);

ALTER TABLE ONLY crs_survey
    ADD CONSTRAINT pkey_crs_survey PRIMARY KEY (wrk_id);

ALTER TABLE crs_survey ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_survey ALTER COLUMN dataset_id SET STATISTICS 500;
ALTER TABLE crs_survey ALTER COLUMN dataset_series SET STATISTICS 500;
ALTER TABLE crs_survey ALTER COLUMN dataset_suffix SET STATISTICS 500;
ALTER TABLE crs_survey ALTER COLUMN fhr_id SET STATISTICS 500;
ALTER TABLE crs_survey ALTER COLUMN ldt_loc_id SET STATISTICS 500;
ALTER TABLE crs_survey ALTER COLUMN pnx_id_submitted SET STATISTICS 500;
ALTER TABLE crs_survey ALTER COLUMN sig_id SET STATISTICS 500;
ALTER TABLE crs_survey ALTER COLUMN usr_id_sol SET STATISTICS 500;
ALTER TABLE crs_survey ALTER COLUMN usr_id_sol_firm SET STATISTICS 500;
ALTER TABLE crs_survey ALTER COLUMN wrk_id SET STATISTICS 500;

ALTER TABLE crs_survey OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_survey FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_survey TO bde_admin;
GRANT SELECT ON TABLE crs_survey TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_survey_image
--------------------------------------------------------------------------------

CREATE TABLE crs_survey_image (
    type VARCHAR(4) NOT NULL,
    sur_wrk_id INTEGER NOT NULL,
    img_id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_survey_image
    ADD CONSTRAINT pkey_crs_survey_image PRIMARY KEY (type, sur_wrk_id);

ALTER TABLE crs_survey_image OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_survey_image FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_survey_image TO bde_admin;
GRANT SELECT ON TABLE crs_survey_image TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_sys_code
--------------------------------------------------------------------------------

CREATE TABLE crs_sys_code (
    scg_code VARCHAR(4) NOT NULL,
    code VARCHAR(4) NOT NULL,
    "desc" VARCHAR(2048),
    status VARCHAR(4) NOT NULL,
    date_value DATE,
    char_value VARCHAR(2048),
    num_value NUMERIC(22,12),
    start_date DATE,
    end_date DATE,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_sys_code
    ADD CONSTRAINT pkey_crs_sys_code PRIMARY KEY (scg_code, code);

ALTER TABLE crs_sys_code OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_sys_code FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_sys_code TO bde_admin;
GRANT SELECT ON TABLE crs_sys_code TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_sys_code_group
--------------------------------------------------------------------------------

CREATE TABLE crs_sys_code_group (
    "desc" VARCHAR(100) NOT NULL,
    user_create_flag CHAR(1) NOT NULL,
    user_modify_flag CHAR(1) NOT NULL,
    user_delete_flag CHAR(1) NOT NULL,
    user_view_flag CHAR(1) NOT NULL,
    data_type CHAR(1) NOT NULL,
    group_type VARCHAR(1),
    code VARCHAR(4) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_sys_code_group
    ADD CONSTRAINT pkey_crs_sys_code_group PRIMARY KEY (code);

ALTER TABLE crs_sys_code_group OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_sys_code_group FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_sys_code_group TO bde_admin;
GRANT SELECT ON TABLE crs_sys_code_group TO bde_user;

/*
--------------------------------------------------------------------------------
-- BDE table crs_task
--------------------------------------------------------------------------------

CREATE TABLE crs_task (
    id INTEGER NOT NULL,
    skp_id INTEGER,
    sob_name VARCHAR(50),
    sob_mdi_name VARCHAR(50),
    description VARCHAR(100) NOT NULL,
    status VARCHAR(4) NOT NULL
);

ALTER TABLE ONLY crs_task
    ADD CONSTRAINT pkey_crs_task PRIMARY KEY (id);

ALTER TABLE crs_task OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_task FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_task TO bde_admin;
GRANT SELECT ON TABLE crs_task TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_task_list
--------------------------------------------------------------------------------

CREATE TABLE crs_task_list (
    id INTEGER NOT NULL,
    trt_grp VARCHAR(4),
    trt_type VARCHAR(4),
    std_position smallint NOT NULL,
    estimated_effort NUMERIC(4,2) NOT NULL,
    optional CHAR(1),
    tsk_id INTEGER,
    status CHAR(4) NOT NULL,
    complexity INTEGER NOT NULL,
    user_continuity VARCHAR(1) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_task_list
    ADD CONSTRAINT pkey_crs_task_list PRIMARY KEY (id);

ALTER TABLE crs_task_list OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_task_list FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_task_list TO bde_admin;
GRANT SELECT ON TABLE crs_task_list TO bde_user;

*/
--------------------------------------------------------------------------------
-- BDE table crs_title
--------------------------------------------------------------------------------

CREATE TABLE crs_title (
    title_no VARCHAR(20) NOT NULL,
    ldt_loc_id INTEGER NOT NULL,
    register_type VARCHAR(4) NOT NULL,
    ste_id INTEGER NOT NULL,
    issue_date TIMESTAMP NOT NULL,
    guarantee_status VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    duplicate CHAR(1) NOT NULL,
    duplicate_version smallint NOT NULL,
    duplicate_status VARCHAR(4) NOT NULL,
    type VARCHAR(4) NOT NULL,
    provisional CHAR(1) NOT NULL,
    sur_wrk_id INTEGER,
    sur_wrk_id_preallc INTEGER,
    ttl_title_no_srs VARCHAR(20),
    conversion_reason VARCHAR(4),
    protect_start DATE,
    protect_end DATE,
    protect_reference VARCHAR(100),
    phy_prod_no INTEGER,
    dlg_id INTEGER,
    alt_id INTEGER,
    audit_id INTEGER NOT NULL,
    maori_land CHAR(1),
    no_survivorship CHAR(1)
);

ALTER TABLE ONLY crs_title
    ADD CONSTRAINT pkey_crs_title PRIMARY KEY (title_no);

ALTER TABLE crs_title ALTER COLUMN alt_id SET STATISTICS 500;
ALTER TABLE crs_title ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_title ALTER COLUMN dlg_id SET STATISTICS 500;
ALTER TABLE crs_title ALTER COLUMN ldt_loc_id SET STATISTICS 500;
ALTER TABLE crs_title ALTER COLUMN phy_prod_no SET STATISTICS 500;
ALTER TABLE crs_title ALTER COLUMN ste_id SET STATISTICS 500;
ALTER TABLE crs_title ALTER COLUMN sur_wrk_id SET STATISTICS 500;
ALTER TABLE crs_title ALTER COLUMN sur_wrk_id_preallc SET STATISTICS 500;
ALTER TABLE crs_title ALTER COLUMN title_no SET STATISTICS 500;
ALTER TABLE crs_title ALTER COLUMN ttl_title_no_srs SET STATISTICS 500;

ALTER TABLE crs_title OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_title FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_title TO bde_admin;
GRANT SELECT ON TABLE crs_title TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_title_action
--------------------------------------------------------------------------------

CREATE TABLE crs_title_action (
    ttl_title_no VARCHAR(20) NOT NULL,
    act_tin_id INTEGER NOT NULL,
    act_id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_title_action
    ADD CONSTRAINT pkey_crs_title_action PRIMARY KEY (ttl_title_no, act_tin_id, act_id);

ALTER TABLE crs_title_action ALTER COLUMN ttl_title_no SET STATISTICS 1000;
ALTER TABLE crs_title_action ALTER COLUMN act_tin_id SET STATISTICS 1000;
ALTER TABLE crs_title_action ALTER COLUMN act_id SET STATISTICS 1000;
ALTER TABLE crs_title_action ALTER COLUMN audit_id SET STATISTICS 1000;

ALTER TABLE crs_title_action OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_title_action FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_title_action TO bde_admin;
GRANT SELECT ON TABLE crs_title_action TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_title_doc_ref
--------------------------------------------------------------------------------

CREATE TABLE crs_title_doc_ref (
    id INTEGER NOT NULL,
    type VARCHAR(4),
    tin_id INTEGER,
    reference_no VARCHAR(15)
);

ALTER TABLE ONLY crs_title_doc_ref
    ADD CONSTRAINT pkey_crs_title_doc_ref PRIMARY KEY (id);

ALTER TABLE crs_title_doc_ref ALTER COLUMN id SET STATISTICS 250;

ALTER TABLE crs_title_doc_ref OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_title_doc_ref FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_title_doc_ref TO bde_admin;
GRANT SELECT ON TABLE crs_title_doc_ref TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_title_estate
--------------------------------------------------------------------------------

CREATE TABLE crs_title_estate (
    id INTEGER NOT NULL,
    ttl_title_no VARCHAR(20) NOT NULL,
    type VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    lgd_id INTEGER NOT NULL,
    share CHAR(100) NOT NULL,
    timeshare_week_no VARCHAR(20),
    purpose VARCHAR(255),
    act_tin_id_crt INTEGER,
    act_id_crt INTEGER,
    act_id_ext INTEGER,
    act_tin_id_ext INTEGER,
    original_flag CHAR(1) NOT NULL,
    term VARCHAR(255),
    tin_id_orig INTEGER
);

ALTER TABLE ONLY crs_title_estate
    ADD CONSTRAINT pkey_crs_title_estate PRIMARY KEY (id);

ALTER TABLE crs_title_estate ALTER COLUMN act_tin_id_crt SET STATISTICS 500;
ALTER TABLE crs_title_estate ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_title_estate ALTER COLUMN lgd_id SET STATISTICS 500;
ALTER TABLE crs_title_estate ALTER COLUMN ttl_title_no SET STATISTICS 500;

ALTER TABLE crs_title_estate OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_title_estate FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_title_estate TO bde_admin;
GRANT SELECT ON TABLE crs_title_estate TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_title_mem_text
--------------------------------------------------------------------------------

CREATE TABLE crs_title_mem_text (
    ttm_id INTEGER NOT NULL,
    sequence_no INTEGER NOT NULL,
    curr_hist_flag VARCHAR(4) NOT NULL,
    std_text VARCHAR(18000),
    col_1_text VARCHAR(2048),
    col_2_text VARCHAR(2048),
    col_3_text VARCHAR(2048),
    col_4_text VARCHAR(2048),
    col_5_text VARCHAR(2048),
    col_6_text VARCHAR(2048),
    col_7_text VARCHAR(2048),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_title_mem_text
    ADD CONSTRAINT pkey_crs_title_mem_text PRIMARY KEY (ttm_id, sequence_no);

ALTER TABLE crs_title_mem_text ALTER COLUMN ttm_id SET STATISTICS 1000;
ALTER TABLE crs_title_mem_text ALTER COLUMN sequence_no SET STATISTICS 1000;
ALTER TABLE crs_title_mem_text ALTER COLUMN audit_id SET STATISTICS 1000;

ALTER TABLE crs_title_mem_text OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_title_mem_text FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_title_mem_text TO bde_admin;
GRANT SELECT ON TABLE crs_title_mem_text TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_title_memorial
--------------------------------------------------------------------------------

CREATE TABLE crs_title_memorial (
    id INTEGER NOT NULL,
    ttl_title_no VARCHAR(20) NOT NULL,
    mmt_code VARCHAR(10) NOT NULL,
    act_id_orig INTEGER NOT NULL,
    act_tin_id_orig INTEGER NOT NULL,
    act_id_crt INTEGER NOT NULL,
    act_tin_id_crt INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    user_changed CHAR(1) NOT NULL,
    text_type VARCHAR(4) NOT NULL,
    register_only_mem CHAR(1),                         
    prev_further_reg CHAR(1),                         
    curr_hist_flag VARCHAR(4) NOT NULL,
    "default" CHAR(1) NOT NULL,
    number_of_cols INTEGER,
    col_1_size INTEGER,
    col_2_size INTEGER,
    col_3_size INTEGER,
    col_4_size INTEGER,
    col_5_size INTEGER,
    col_6_size INTEGER,
    col_7_size INTEGER,
    act_id_ext INTEGER,
    act_tin_id_ext INTEGER
);

ALTER TABLE ONLY crs_title_memorial
    ADD CONSTRAINT pkey_crs_title_memorial PRIMARY KEY (id);

ALTER TABLE crs_title_memorial ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE crs_title_memorial ALTER COLUMN ttl_title_no SET STATISTICS 1000;
ALTER TABLE crs_title_memorial ALTER COLUMN mmt_code SET STATISTICS 1000;
ALTER TABLE crs_title_memorial ALTER COLUMN act_tin_id_crt SET STATISTICS 1000;
ALTER TABLE crs_title_memorial ALTER COLUMN act_id_crt SET STATISTICS 1000;
ALTER TABLE crs_title_memorial ALTER COLUMN act_tin_id_orig SET STATISTICS 1000;
ALTER TABLE crs_title_memorial ALTER COLUMN act_id_orig SET STATISTICS 1000;
ALTER TABLE crs_title_memorial ALTER COLUMN act_tin_id_ext SET STATISTICS 1000;
ALTER TABLE crs_title_memorial ALTER COLUMN act_id_ext SET STATISTICS 1000;

ALTER TABLE crs_title_memorial OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_title_memorial FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_title_memorial TO bde_admin;
GRANT SELECT ON TABLE crs_title_memorial TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_topology_class
--------------------------------------------------------------------------------

CREATE TABLE crs_topology_class  (
    code VARCHAR(4) NOT NULL,
    type VARCHAR(4) NOT NULL,
    name VARCHAR(100) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_topology_class
    ADD CONSTRAINT pkey_crs_topology_class PRIMARY KEY (code);

ALTER TABLE crs_topology_class OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_topology_class FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_topology_class TO bde_admin;
GRANT SELECT ON TABLE crs_topology_class TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_transact_type
--------------------------------------------------------------------------------

CREATE TABLE crs_transact_type (
    grp VARCHAR(4) NOT NULL,
    type VARCHAR(4) NOT NULL,
    description VARCHAR(100) NOT NULL,
    category VARCHAR(4),
    plan_type VARCHAR(4),
    unit_plan CHAR(1),
    prov_alloc_ct CHAR(1),
    ct_duplicate_req VARCHAR(1),
    register_only_mem CHAR(1),
    prevents_reg CHAR(1),
    audit_id INTEGER NOT NULL,
    curr CHAR(1),
    tan_required CHAR(1),
    creates_tan CHAR(1),
    electronic CHAR(1),
    fees_exempt_allw CHAR(1),
    trt_type_discrg VARCHAR(200),
    trt_grp_discrg VARCHAR(4),
    sob_name VARCHAR(50),
    holder VARCHAR(4),
    linked_to CHAR(1),
    tran_id_reqd CHAR(1),
    advertise CHAR(1),
    default_reg_status VARCHAR(4),
    a_and_i_required CHAR(1),
    always_image_button CHAR(1),
    always_text_button CHAR(1),
    current_lodge_method CHAR(4),
    default_lodge_method CHAR(4),
    dep_plan_instrument CHAR(1),
    discrg_type VARCHAR(1),
    display_sequence INTEGER NOT NULL,
    encee_holder VARCHAR(4),
    encor_holder VARCHAR(4),
    internal_only CHAR(1),
    internal_request CHAR(1),
    lead_processor CHAR(1),
    new_title_instrument CHAR(1),
    no_title_req CHAR(1),
    partial_discharge VARCHAR(200),
    post_reg_default CHAR(1),
    post_reg_view CHAR(1),
    request_workflow_assignment VARCHAR(4),
    short_name CHAR(100),
    submitting_firm_only CHAR(1),
    view_in_search_tree CHAR(1)
);

ALTER TABLE ONLY crs_transact_type
    ADD CONSTRAINT pkey_crs_transact_type PRIMARY KEY (grp, type);

ALTER TABLE crs_transact_type OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_transact_type FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_transact_type TO bde_admin;
GRANT SELECT ON TABLE crs_transact_type TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_ttl_enc
--------------------------------------------------------------------------------

CREATE TABLE crs_ttl_enc (
    id INTEGER NOT NULL,
    ttl_title_no VARCHAR(20) NOT NULL,
    enc_id INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    act_tin_id_crt INTEGER NOT NULL,
    act_id_crt INTEGER NOT NULL,
    act_id_ext INTEGER,
    act_tin_id_ext INTEGER
);

ALTER TABLE ONLY crs_ttl_enc
    ADD CONSTRAINT pkey_crs_ttl_enc PRIMARY KEY (id);

ALTER TABLE crs_ttl_enc ALTER COLUMN act_tin_id_crt SET STATISTICS 500;
ALTER TABLE crs_ttl_enc ALTER COLUMN enc_id SET STATISTICS 500;
ALTER TABLE crs_ttl_enc ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_ttl_enc ALTER COLUMN ttl_title_no SET STATISTICS 500;

ALTER TABLE crs_ttl_enc OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_ttl_enc FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_ttl_enc TO bde_admin;
GRANT SELECT ON TABLE crs_ttl_enc TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_ttl_hierarchy
--------------------------------------------------------------------------------

CREATE TABLE crs_ttl_hierarchy (
    id INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    ttl_title_no_prior VARCHAR(20),
    ttl_title_no_flw VARCHAR(20) NOT NULL,
    tdr_id INTEGER,
    act_tin_id_crt INTEGER,
    act_id_crt INTEGER,
    act_id_ext INTEGER,
    act_tin_id_ext INTEGER
);

ALTER TABLE ONLY crs_ttl_hierarchy
    ADD CONSTRAINT pkey_crs_ttl_hierarchy PRIMARY KEY (id);

ALTER TABLE crs_ttl_hierarchy ALTER COLUMN act_tin_id_crt SET STATISTICS 500;
ALTER TABLE crs_ttl_hierarchy ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_ttl_hierarchy ALTER COLUMN tdr_id SET STATISTICS 500;
ALTER TABLE crs_ttl_hierarchy ALTER COLUMN ttl_title_no_flw SET STATISTICS 500;
ALTER TABLE crs_ttl_hierarchy ALTER COLUMN ttl_title_no_prior SET STATISTICS 500;

ALTER TABLE crs_ttl_hierarchy OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_ttl_hierarchy FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_ttl_hierarchy TO bde_admin;
GRANT SELECT ON TABLE crs_ttl_hierarchy TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_ttl_inst
--------------------------------------------------------------------------------

CREATE TABLE crs_ttl_inst (
    id INTEGER NOT NULL,
    inst_no VARCHAR(30) NOT NULL,
    trt_grp VARCHAR(4) NOT NULL,
    trt_type VARCHAR(4) NOT NULL,
    ldt_loc_id INTEGER NOT NULL,
    status VARCHAR(4) NOT NULL,
    lodged_datetime TIMESTAMP NOT NULL,
    dlg_id INTEGER,
    priority_no INTEGER,
    img_id INTEGER,
    pro_id INTEGER,
    completion_date DATE,
    usr_id_approve VARCHAR(20),
    tin_id_parent INTEGER,
    audit_id INTEGER NOT NULL,
    dm_covenant_flag CHAR(1),
    advertise CHAR(1),
    image_count INTEGER,
    img_id_sec INTEGER,
    inst_ldg_type VARCHAR(4),
    next_lodge_new_req CHAR(1) NOT NULL,
    next_lodge_prev_req_cnt INTEGER NOT NULL,
    reject_resub_no INTEGER,
    req_changed CHAR(1),
    requisition_resub_no INTEGER,
    ttin_id INTEGER,
    ttin_new_rej CHAR(1) NOT NULL
);

ALTER TABLE ONLY crs_ttl_inst
    ADD CONSTRAINT pkey_crs_ttl_inst PRIMARY KEY (id);

ALTER TABLE crs_ttl_inst ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_ttl_inst ALTER COLUMN dlg_id SET STATISTICS 500;
ALTER TABLE crs_ttl_inst ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_ttl_inst ALTER COLUMN img_id SET STATISTICS 500;
ALTER TABLE crs_ttl_inst ALTER COLUMN inst_no SET STATISTICS 500;
ALTER TABLE crs_ttl_inst ALTER COLUMN ldt_loc_id SET STATISTICS 500;
ALTER TABLE crs_ttl_inst ALTER COLUMN pro_id SET STATISTICS 500;
ALTER TABLE crs_ttl_inst ALTER COLUMN tin_id_parent SET STATISTICS 500;
ALTER TABLE crs_ttl_inst ALTER COLUMN trt_grp SET STATISTICS 500;
ALTER TABLE crs_ttl_inst ALTER COLUMN trt_type SET STATISTICS 500;
ALTER TABLE crs_ttl_inst ALTER COLUMN usr_id_approve SET STATISTICS 500;

ALTER TABLE crs_ttl_inst OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_ttl_inst FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_ttl_inst TO bde_admin;
GRANT SELECT ON TABLE crs_ttl_inst TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_ttl_inst_title
--------------------------------------------------------------------------------

CREATE TABLE crs_ttl_inst_title (
    tin_id INTEGER NOT NULL,
    ttl_title_no VARCHAR(20) NOT NULL,
    created_by_inst CHAR(1),
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_ttl_inst_title
    ADD CONSTRAINT pkey_crs_ttl_inst_title PRIMARY KEY (tin_id, ttl_title_no);

ALTER TABLE crs_ttl_inst_title ALTER COLUMN tin_id SET STATISTICS 1000;
ALTER TABLE crs_ttl_inst_title ALTER COLUMN ttl_title_no SET STATISTICS 1000;

ALTER TABLE crs_ttl_inst_title OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_ttl_inst_title FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_ttl_inst_title TO bde_admin;
GRANT SELECT ON TABLE crs_ttl_inst_title TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_unit_of_meas
--------------------------------------------------------------------------------

CREATE TABLE crs_unit_of_meas (
    code VARCHAR(4) NOT NULL,
    description VARCHAR(100) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_unit_of_meas
    ADD CONSTRAINT pkey_crs_unit_of_meas PRIMARY KEY (code);

ALTER TABLE crs_unit_of_meas OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_unit_of_meas FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_unit_of_meas TO bde_admin;
GRANT SELECT ON TABLE crs_unit_of_meas TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_user
--------------------------------------------------------------------------------

CREATE TABLE crs_user (
    id VARCHAR(20) NOT NULL,
    type VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    title VARCHAR(4),
    given_names VARCHAR(30),
    surname VARCHAR(30),
    off_code VARCHAR(4),
    usr_id_coordinator VARCHAR(20),
    corporate_name VARCHAR(100),
    contact_title VARCHAR(4),
    contact_given_name VARCHAR(30),
    contact_surname VARCHAR(30),
    email_address VARCHAR(100),
    cus_credit_status VARCHAR(4),
    cus_account_ref VARCHAR(20),
    geo_accr_status VARCHAR(4),
    suv_auth_ref VARCHAR(10),
    int_employee_code VARCHAR(10),
    sup_agency_type VARCHAR(4),
    int_max_hold INTEGER,
    prob_status VARCHAR(4),
    audit_id INTEGER NOT NULL,
    login VARCHAR(1) NOT NULL,
    login_type VARCHAR(4),
    default_theme INTEGER,
    news_account_no VARCHAR(50),
    land_district VARCHAR(4),
    cus_acc_credit_lim NUMERIC(10,2),
    cus_acc_balance NUMERIC(10,2),
    linked_tan_firm VARCHAR(20),
    usr_id_parent VARCHAR(20),
    system_manager CHAR(1) NOT NULL,
    quick_code VARCHAR(4),
    scrambled CHAR(1),
    addr_country VARCHAR(4),
    addr_street VARCHAR(100),
    addr_town VARCHAR(100),
    fax VARCHAR(20),
    mobile_phone VARCHAR(20),
    phone VARCHAR(20),
    postal_address VARCHAR(100),
    postal_address_town VARCHAR(100),
    postal_country VARCHAR(4),
    postal_dx_box VARCHAR(10),
    postal_postcode VARCHAR(10),
    postal_recipient_prefix VARCHAR(100),
    postal_recipient_suffix VARCHAR(100),
    preferred_name VARCHAR(200),
    single_pref_contact CHAR(1) NOT NULL,
    sup_competency_det VARCHAR(2048)
);

ALTER TABLE ONLY crs_user
    ADD CONSTRAINT pkey_crs_user PRIMARY KEY (id);

ALTER TABLE crs_user OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_user FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_user TO bde_admin;
GRANT SELECT ON TABLE crs_user TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_vector
--------------------------------------------------------------------------------

CREATE TABLE crs_vector (
    type VARCHAR(4) NOT NULL,
    nod_id_start INTEGER NOT NULL,
    nod_id_end INTEGER,
    length NUMERIC(22,12) NOT NULL,
    source INTEGER NOT NULL,
    id INTEGER NOT NULL,
    audit_id INTEGER NOT NULL,
    se_row_id INTEGER,
    shape GEOMETRY,
    CONSTRAINT enforce_dims_shape CHECK ((public.ndims(shape) = 2)),
    CONSTRAINT enforce_srid_shape CHECK ((srid(shape) = 4167))
);

ALTER TABLE ONLY crs_vector
    ADD CONSTRAINT pkey_crs_vector PRIMARY KEY (id);

ALTER TABLE crs_vector ALTER COLUMN audit_id SET STATISTICS 1000;
ALTER TABLE crs_vector ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE crs_vector ALTER COLUMN nod_id_end SET STATISTICS 1000;
ALTER TABLE crs_vector ALTER COLUMN nod_id_start SET STATISTICS 1000;

ALTER TABLE crs_vector OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_vector FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_vector TO bde_admin;
GRANT SELECT ON TABLE crs_vector TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_vertx_sequence
--------------------------------------------------------------------------------

CREATE table crs_vertx_sequence (
    lin_id INTEGER NOT NULL,
    sequence SMALLINT NOT NULL,
    value1 NUMERIC(22,12) NOT NULL,
    value2 NUMERIC(22,12) NOT NULL,
    audit_id INTEGER NOT NULL
);

ALTER TABLE ONLY crs_vertx_sequence
    ADD CONSTRAINT pkey_crs_vertx_sequence PRIMARY KEY (lin_id, sequence);

ALTER TABLE crs_vertx_sequence ALTER COLUMN lin_id SET STATISTICS 1000;

ALTER TABLE crs_vertx_sequence OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_vertx_sequence FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_vertx_sequence TO bde_admin;
GRANT SELECT ON TABLE crs_vertx_sequence TO bde_user;

--------------------------------------------------------------------------------
-- BDE table crs_work
--------------------------------------------------------------------------------

CREATE TABLE crs_work (
    id INTEGER NOT NULL,
    trt_grp VARCHAR(4) NOT NULL,
    trt_type VARCHAR(4) NOT NULL,
    status VARCHAR(4) NOT NULL,
    con_id INTEGER,
    pro_id INTEGER,
    usr_id_firm VARCHAR(20),
    usr_id_principal VARCHAR(20),
    cel_id INTEGER,
    project_name VARCHAR(100),
    invoice VARCHAR(20),
    external_work_id INTEGER,
    view_txn CHAR(1),
    restricted CHAR(1),
    lodged_date TIMESTAMP,
    authorised_date TIMESTAMP,
    usr_id_authorised VARCHAR(20),
    validated_date DATE,
    usr_id_validated VARCHAR(20),
    cos_id INTEGER,
    data_loaded CHAR(1),
    run_auto_rules CHAR(1),
    alt_id INTEGER,
    audit_id INTEGER,
    usr_id_prin_firm VARCHAR(20),
    manual_rules VARCHAR(1) NOT NULL,
    annotations TEXT,
    trv_id INTEGER
);

ALTER TABLE ONLY crs_work
    ADD CONSTRAINT pkey_crs_work PRIMARY KEY (id);

ALTER TABLE crs_work ALTER COLUMN alt_id SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN audit_id SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN authorised_date SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN cel_id SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN con_id SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN cos_id SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN lodged_date SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN pro_id SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN trt_grp SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN trt_type SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN usr_id_authorised SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN usr_id_firm SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN usr_id_principal SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN usr_id_prin_firm SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN usr_id_validated SET STATISTICS 500;
ALTER TABLE crs_work ALTER COLUMN validated_date SET STATISTICS 500;

ALTER TABLE crs_work OWNER TO bde_dba;

REVOKE ALL ON TABLE crs_work FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE crs_work TO bde_admin;
GRANT SELECT ON TABLE crs_work TO bde_user;

--------------------------------------------------------------------------------
-- Spatial table geometry column metdata. Use don't use the AddGeometryColumn()
-- function for the table definitions, because we want to control the geometry
-- type for tables with mixed geometry types.
--------------------------------------------------------------------------------

DELETE FROM geometry_columns
WHERE f_table_schema = current_schema();

INSERT INTO geometry_columns (
    f_table_catalog, f_table_schema, f_table_name, f_geometry_column, coord_dimension, srid, type
)
VALUES
    ('', current_schema(), 'crs_elect_place',    'shape', 2, 4167, 'POINT'),
    ('', current_schema(), 'crs_feature_name',   'shape', 2, 4167, 'GEOMETRY'),
    ('', current_schema(), 'crs_land_district',  'shape', 2, 4167, 'POLYGON'),
    ('', current_schema(), 'crs_line',           'shape', 2, 4167, 'LINESTRING'),
    ('', current_schema(), 'crs_locality',       'shape', 2, 4167, 'POLYGON'),
    ('', current_schema(), 'crs_map_grid',       'shape', 2, 4167, 'POLYGON'),
    ('', current_schema(), 'crs_mesh_blk',       'shape', 2, 4167, 'MULTIPOLYGON'),
    ('', current_schema(), 'crs_mesh_blk_line',  'shape', 2, 4167, 'LINESTRING'),
    ('', current_schema(), 'crs_node',           'shape', 2, 4167, 'POINT'),
    ('', current_schema(), 'crs_off_cord_sys',   'shape', 2, 4167, 'POLYGON'),
    ('', current_schema(), 'crs_parcel',         'shape', 2, 4167, 'MULTIPOLYGON'),
    ('', current_schema(), 'crs_parcel_label',   'shape', 2, 4167, 'POINT'),
    ('', current_schema(), 'crs_road_ctr_line',  'shape', 2, 4167, 'LINESTRING'),
    ('', current_schema(), 'crs_statist_area',   'shape', 2, 4167, 'MULTIPOLYGON'),
    ('', current_schema(), 'crs_street_address', 'shape', 2, 4167, 'POINT'),
    ('', current_schema(), 'crs_sur_plan_ref',   'shape', 2, 4167, 'POINT'),
    ('', current_schema(), 'crs_vector',         'shape', 2, 4167, 'LINESTRING');

END;
$SCHEMA$;


