--------------------------------------------------------------------------------
--
-- $Id$
--
-- linz_bde_loader - LINZ BDE loader for PostgreSQL
--
-- Copyright 2014 Crown copyright (c)
-- Land Information New Zealand and the New Zealand Government.
-- All rights reserved
--
-- This software is released under the terms of the new BSD license. See the 
-- LICENSE file for more information.
--
--------------------------------------------------------------------------------
-- Creates LINZ Data Service (LDS) extended Landonline layers tables
--------------------------------------------------------------------------------
SET client_min_messages TO WARNING;

DO $SCHEMA$
BEGIN

IF EXISTS (SELECT * FROM pg_namespace where LOWER(nspname) = 'bde_ext') THEN
	RETURN;
END IF;

CREATE SCHEMA bde_ext AUTHORIZATION bde_dba;

GRANT USAGE ON SCHEMA bde_ext TO bde_admin;
GRANT USAGE ON SCHEMA bde_ext TO bde_user;

COMMENT ON SCHEMA bde_ext IS 'Schema for LDS full Landonline layers that require public filtering';

SET search_path = bde_ext, public;

-- =============================================================================
-- A D J U S T M E N T   R U N
-- =============================================================================

DROP TABLE IF EXISTS adjustment_run CASCADE;
CREATE TABLE adjustment_run
(
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
  audit_id INTEGER NOT NULL,
  CONSTRAINT pkey_adjustment_run PRIMARY KEY (id)
);

ALTER TABLE adjustment_run OWNER TO bde_dba;
REVOKE ALL ON TABLE adjustment_run FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE adjustment_run TO bde_admin;
GRANT SELECT ON TABLE adjustment_run TO bde_user;

-- =============================================================================
-- P R O P R I E T O R
-- =============================================================================
DROP TABLE IF EXISTS proprietor CASCADE;
CREATE TABLE proprietor
(
  id INTEGER NOT NULL,
  ets_id INTEGER NOT NULL,
  status VARCHAR(4) NOT NULL,
  type VARCHAR(4) NOT NULL,
  corporate_name VARCHAR(250),
  prime_surname VARCHAR(100),
  prime_other_names VARCHAR(100),
  name_suffix VARCHAR(4),
  original_flag CHAR(1) NOT NULL,
  CONSTRAINT pkey_proprietor PRIMARY KEY (id)
);

ALTER TABLE proprietor OWNER TO bde_dba;
REVOKE ALL ON TABLE proprietor FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE proprietor TO bde_admin;
GRANT SELECT ON TABLE proprietor TO bde_user;

-- =============================================================================
-- E N C U M B R A N C E E
-- =============================================================================
DROP TABLE IF EXISTS encumbrancee CASCADE;
CREATE TABLE encumbrancee
(
  id INTEGER NOT NULL,
  ens_id INTEGER,
  status VARCHAR(4),
  name VARCHAR(255),
  CONSTRAINT pkey_encumbrancee PRIMARY KEY (id)
);

ALTER TABLE encumbrancee OWNER TO bde_dba;
REVOKE ALL ON TABLE encumbrancee FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE encumbrancee TO bde_admin;
GRANT SELECT ON TABLE encumbrancee TO bde_user;

-- =============================================================================
-- N O M I N A L   I N D E X 
-- =============================================================================
DROP TABLE IF EXISTS nominal_index CASCADE;
CREATE TABLE nominal_index
(
  id INTEGER NOT NULL,
  ttl_title_no VARCHAR(20) NOT NULL,
  status VARCHAR(4) NOT NULL,
  name_type VARCHAR(4) NOT NULL,
  corporate_name VARCHAR(250),
  surname VARCHAR(100),
  other_names VARCHAR(100),
  prp_id INTEGER,
  CONSTRAINT pkey_nominal_index PRIMARY KEY (id)
);

ALTER TABLE nominal_index OWNER TO bde_dba;
REVOKE ALL ON TABLE nominal_index FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE nominal_index TO bde_admin;
GRANT SELECT ON TABLE nominal_index TO bde_user;

-- =============================================================================
-- A L I A S 
-- =============================================================================
DROP TABLE IF EXISTS alias CASCADE;
CREATE TABLE alias
(
  id INTEGER NOT NULL,
  prp_id INTEGER NOT NULL,
  surname VARCHAR(100) NOT NULL,
  other_names VARCHAR(100),
  CONSTRAINT pkey_alias PRIMARY KEY (id )
);

ALTER TABLE alias OWNER TO bde_dba;
REVOKE ALL ON TABLE alias FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE alias TO bde_admin;
GRANT SELECT ON TABLE alias TO bde_user;

-- =============================================================================
-- E N C   S H A R E
-- =============================================================================
DROP TABLE IF EXISTS enc_share CASCADE;
CREATE TABLE enc_share
(
  id INTEGER NOT NULL,
  enc_id INTEGER,
  status VARCHAR(4),
  act_tin_id_crt INTEGER,
  act_id_crt INTEGER NOT NULL,
  act_id_ext INTEGER,
  act_tin_id_ext INTEGER,
  system_crt CHAR(1) NOT NULL,
  system_ext CHAR(1),
  CONSTRAINT pkey_enc_share PRIMARY KEY (id )
);

ALTER TABLE enc_share OWNER TO bde_dba;
REVOKE ALL ON TABLE enc_share FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE enc_share TO bde_admin;
GRANT SELECT ON TABLE enc_share TO bde_user;

-- =============================================================================
-- E N C U M B R A N C E
-- =============================================================================
DROP TABLE IF EXISTS encumbrance CASCADE;
CREATE TABLE encumbrance
(
  id INTEGER NOT NULL,
  status VARCHAR(4),
  act_tin_id_orig INTEGER,
  act_tin_id_crt INTEGER,
  act_id_crt INTEGER NOT NULL,
  act_id_orig INTEGER NOT NULL,
  term VARCHAR(250),
  CONSTRAINT pkey_encumbrance PRIMARY KEY (id)
);

ALTER TABLE encumbrance OWNER TO bde_dba;
REVOKE ALL ON TABLE encumbrance FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE encumbrance TO bde_admin;
GRANT SELECT ON TABLE encumbrance TO bde_user;

-- =============================================================================
-- E S T A T E   S H A R E
-- =============================================================================
DROP TABLE IF EXISTS estate_share CASCADE;
CREATE TABLE estate_share
(
  id INTEGER NOT NULL,
  ett_id INTEGER NOT NULL,
  share VARCHAR(100) NOT NULL,
  status VARCHAR(4) NOT NULL,
  share_memorial VARCHAR(17500),
  act_tin_id_crt INTEGER,
  act_id_crt INTEGER,
  executorship VARCHAR(4),
  original_flag CHAR(1) NOT NULL,
  system_crt CHAR(1) NOT NULL,
  CONSTRAINT pkey_estate_share PRIMARY KEY (id )
);

ALTER TABLE estate_share OWNER TO bde_dba;
REVOKE ALL ON TABLE estate_share FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE estate_share TO bde_admin;
GRANT SELECT ON TABLE estate_share TO bde_user;

-- =============================================================================
-- L E G A L   D E S C
-- =============================================================================
DROP TABLE IF EXISTS legal_desc CASCADE;
CREATE TABLE legal_desc
(
  id INTEGER NOT NULL,
  type VARCHAR(4) NOT NULL,
  status VARCHAR(4) NOT NULL,
  total_area NUMERIC(22,12),
  ttl_title_no VARCHAR(20),
  legal_desc_text VARCHAR(2048),
  audit_id INTEGER NOT NULL,
  CONSTRAINT pkey_legal_desc PRIMARY KEY (id)
);

ALTER TABLE legal_desc OWNER TO bde_dba;
REVOKE ALL ON TABLE legal_desc FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE legal_desc TO bde_admin;
GRANT SELECT ON TABLE legal_desc TO bde_user;

-- =============================================================================
-- L I N E
-- =============================================================================
DROP TABLE IF EXISTS line CASCADE;
CREATE TABLE line
(
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
  CONSTRAINT pkey_line PRIMARY KEY (id)
);

PERFORM AddGeometryColumn('line', 'shape', 4167, 'LINESTRING', 2);
CREATE INDEX shx_line_shape ON line USING gist (shape);

ALTER TABLE line OWNER TO bde_dba;
REVOKE ALL ON TABLE line FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE line TO bde_admin;
GRANT SELECT ON TABLE line TO bde_user;


-- =============================================================================
-- M A I N T E N A N C E
-- =============================================================================
DROP TABLE IF EXISTS maintenance CASCADE;
CREATE TABLE maintenance
(
  mrk_id INTEGER NOT NULL,
  type VARCHAR(4) NOT NULL,
  status VARCHAR(4) NOT NULL,
  "desc" VARCHAR(2048),
  complete_date DATE,
  audit_id INTEGER NOT NULL,
  CONSTRAINT maintenance_pkey PRIMARY KEY (audit_id),
  CONSTRAINT maintenance_mrk_id_type_key UNIQUE (mrk_id , type)
);

ALTER TABLE maintenance OWNER TO bde_dba;
REVOKE ALL ON TABLE maintenance FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE maintenance TO bde_admin;
GRANT SELECT ON TABLE maintenance TO bde_user;

-- =============================================================================
-- M A R K
-- =============================================================================
DROP TABLE IF EXISTS mark CASCADE;
CREATE TABLE mark
(
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
  audit_id INTEGER NOT NULL,
  CONSTRAINT pkey_mark PRIMARY KEY (id )
);

ALTER TABLE mark OWNER TO bde_dba;
REVOKE ALL ON TABLE mark FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE mark TO bde_admin;
GRANT SELECT ON TABLE mark TO bde_user;

-- =============================================================================
-- M A R K  N A M E
-- =============================================================================
DROP TABLE IF EXISTS mark_name CASCADE;
CREATE TABLE mark_name
(
  mrk_id INTEGER NOT NULL,
  type VARCHAR(4) NOT NULL,
  name VARCHAR(100) NOT NULL,
  audit_id INTEGER NOT NULL,
  CONSTRAINT mark_name_pkey PRIMARY KEY (audit_id ),
  CONSTRAINT mark_name_mrk_id_type_key UNIQUE (mrk_id , type )
);

ALTER TABLE mark_name OWNER TO bde_dba;
REVOKE ALL ON TABLE mark_name FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE mark_name TO bde_admin;
GRANT SELECT ON TABLE mark_name TO bde_user;

-- =============================================================================
-- M A R K   P H Y S   S T A T E
-- =============================================================================
DROP TABLE IF EXISTS mark_phys_state CASCADE;
CREATE TABLE mark_phys_state
(
  mrk_id INTEGER NOT NULL,
  wrk_id INTEGER NOT NULL,
  type VARCHAR(4) NOT NULL,
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
  audit_id INTEGER NOT NULL,
  CONSTRAINT mark_phys_state_pkey PRIMARY KEY (audit_id),
  CONSTRAINT mark_phys_state_mrk_id_type_wrk_id_key UNIQUE (mrk_id ,type ,wrk_id)
);

ALTER TABLE mark_phys_state OWNER TO bde_dba;
REVOKE ALL ON TABLE mark_phys_state FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE mark_phys_state TO bde_admin;
GRANT SELECT ON TABLE mark_phys_state TO bde_user;

-- =============================================================================
-- N O D E
-- =============================================================================
DROP TABLE IF EXISTS node CASCADE;
CREATE TABLE node
(
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
  CONSTRAINT pkey_node PRIMARY KEY (id)
);

PERFORM AddGeometryColumn('node', 'shape', 4167, 'POINT', 2);
CREATE INDEX shx_node_shape ON node USING gist (shape);

ALTER TABLE node OWNER TO bde_dba;
REVOKE ALL ON TABLE node FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE node TO bde_admin;
GRANT SELECT ON TABLE node TO bde_user;

-- =============================================================================
-- N O D E  P R P  O R D E R
-- =============================================================================
DROP TABLE IF EXISTS node_prp_order CASCADE;
CREATE TABLE node_prp_order
(
  dtm_id INTEGER NOT NULL,
  nod_id INTEGER NOT NULL,
  cor_id INTEGER,
  audit_id INTEGER NOT NULL,
  CONSTRAINT node_prp_order_pkey PRIMARY KEY (audit_id),
  CONSTRAINT node_prp_order_dtm_id_nod_id_key UNIQUE (dtm_id , nod_id)
);

ALTER TABLE node_prp_order OWNER TO bde_dba;
REVOKE ALL ON TABLE node_prp_order FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE node_prp_order TO bde_admin;
GRANT SELECT ON TABLE node_prp_order TO bde_user;

-- =============================================================================
-- P A R C E L
-- =============================================================================
DROP TABLE IF EXISTS parcel CASCADE;
CREATE TABLE parcel
(
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
  CONSTRAINT parcel_pkey PRIMARY KEY (id)
);

PERFORM AddGeometryColumn('parcel', 'shape', 4167, 'GEOMETRY', 2);
CREATE INDEX shx_parcel_shape ON parcel USING gist (shape);

ALTER TABLE parcel OWNER TO bde_dba;
REVOKE ALL ON TABLE parcel FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE parcel TO bde_admin;
GRANT SELECT ON TABLE parcel TO bde_user;

-- =============================================================================
-- P A R C E L  L S
-- =============================================================================
DROP TABLE IF EXISTS parcel_ls CASCADE;
CREATE TABLE parcel_ls
(
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
  CONSTRAINT pkey_parcel_ls PRIMARY KEY (id)
);

PERFORM AddGeometryColumn('parcel_ls', 'shape', 4167, 'LINESTRING', 2);
CREATE INDEX shx_parcel_ls_shape ON parcel_ls USING gist (shape);

ALTER TABLE parcel_ls OWNER TO bde_dba;
REVOKE ALL ON TABLE parcel_ls FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE parcel_ls TO bde_admin;
GRANT SELECT ON TABLE parcel_ls TO bde_user;

-- =============================================================================
-- P A R C E L  L A B E L
-- =============================================================================
DROP TABLE IF EXISTS parcel_label CASCADE;
CREATE TABLE parcel_label
(
  id INTEGER NOT NULL,
  par_id INTEGER NOT NULL,
  se_row_id INTEGER NOT NULL,
  audit_id INTEGER NOT NULL,
  CONSTRAINT parcel_label_pkey PRIMARY KEY (id)
);

PERFORM AddGeometryColumn('parcel_label', 'shape', 4167, 'POINT', 2);
CREATE INDEX shx_parcel_label_shape ON parcel_label USING gist (shape);

ALTER TABLE parcel_label OWNER TO bde_dba;
REVOKE ALL ON TABLE parcel_label FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE parcel_label TO bde_admin;
GRANT SELECT ON TABLE parcel_label TO bde_user;

-- =============================================================================
-- P A R C E L  D I M E N
-- =============================================================================
DROP TABLE IF EXISTS parcel_dimen CASCADE;
CREATE TABLE parcel_dimen
(
  obn_id INTEGER NOT NULL,
  par_id INTEGER NOT NULL,
  audit_id INTEGER NOT NULL,
  CONSTRAINT parcel_dimen_pkey PRIMARY KEY (audit_id),
  CONSTRAINT parcel_dimen_obn_id_par_id_key UNIQUE (obn_id , par_id)
);

ALTER TABLE parcel_dimen OWNER TO bde_dba;
REVOKE ALL ON TABLE parcel_dimen FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE parcel_dimen TO bde_admin;
GRANT SELECT ON TABLE parcel_dimen TO bde_user;

-- =============================================================================
-- P A R C E L  R I N G
-- =============================================================================
DROP TABLE IF EXISTS parcel_ring CASCADE;
CREATE TABLE parcel_ring
(
  id INTEGER NOT NULL,
  par_id INTEGER NOT NULL,
  pri_id_parent_ring INTEGER,
  is_ring CHAR(1) NOT NULL,
  audit_id INTEGER NOT NULL,
  CONSTRAINT pkey_parcel_ring PRIMARY KEY (id)
);

ALTER TABLE parcel_ring OWNER TO bde_dba;
REVOKE ALL ON TABLE parcel_ring FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE parcel_ring TO bde_admin;
GRANT SELECT ON TABLE parcel_ring TO bde_user;


-- =============================================================================
-- S T A T   V E R S I O N
-- =============================================================================
DROP TABLE IF EXISTS stat_version CASCADE;
CREATE TABLE stat_version
(
  version INTEGER NOT NULL,
  area_class VARCHAR(4) NOT NULL,
  "desc" VARCHAR(50),
  statute_action VARCHAR(50) NOT NULL,
  start_date date NOT NULL,
  end_date DATE,
  audit_id INTEGER NOT NULL,
  CONSTRAINT stat_version_pkey PRIMARY KEY (audit_id),
  CONSTRAINT stat_version_area_class_version_key UNIQUE (area_class , version)
);

ALTER TABLE stat_version OWNER TO bde_dba;
REVOKE ALL ON TABLE stat_version FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE stat_version TO bde_admin;
GRANT SELECT ON TABLE stat_version TO bde_user;

-- =============================================================================
-- S T A T I S T   A R E A
-- =============================================================================
DROP TABLE IF EXISTS statist_area CASCADE;
CREATE TABLE statist_area
(
  id INTEGER NOT NULL,
  name VARCHAR(100) NOT NULL,
  name_abrev VARCHAR(18) NOT NULL,
  code VARCHAR(6) NOT NULL,
  status VARCHAR(4) NOT NULL,
  sav_version INTEGER NOT NULL,
  sav_area_class VARCHAR(4) NOT NULL,
  alt_id INTEGER,
  se_row_id INTEGER,
  audit_id INTEGER NOT NULL,
  CONSTRAINT pkey_statist_area PRIMARY KEY (id)
);

ALTER TABLE statist_area OWNER TO bde_dba;
REVOKE ALL ON TABLE statist_area FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE statist_area TO bde_admin;
GRANT SELECT ON TABLE statist_area TO bde_user;

-- =============================================================================
-- S U R V E Y
-- =============================================================================
DROP TABLE IF EXISTS survey CASCADE;
CREATE TABLE survey
(
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
  CONSTRAINT pkey_survey PRIMARY KEY (wrk_id)
);

ALTER TABLE survey OWNER TO bde_dba;
REVOKE ALL ON TABLE survey FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE survey TO bde_admin;
GRANT SELECT ON TABLE survey TO bde_user;

-- =============================================================================
-- T I T L E
-- =============================================================================
DROP TABLE IF EXISTS title CASCADE;
CREATE TABLE title
(
  title_no VARCHAR(20) NOT NULL,
  ldt_loc_id INTEGER NOT NULL,
  register_type VARCHAR(4) NOT NULL,
  ste_id INTEGER NOT NULL,
  issue_date TIMESTAMP NOT NULL,
  guarantee_status VARCHAR(4) NOT NULL,
  status VARCHAR(4) NOT NULL,
  type VARCHAR(4) NOT NULL,
  provisional CHAR(1) NOT NULL,
  sur_wrk_id INTEGER,
  ttl_title_no_srs VARCHAR(20),
  ttl_title_no_head_srs VARCHAR(20),
  maori_land CHAR(1),
  audit_id INTEGER NOT NULL,
  CONSTRAINT title_pkey PRIMARY KEY (audit_id),
  CONSTRAINT title_title_no_key UNIQUE (title_no)
);

ALTER TABLE title OWNER TO bde_dba;
REVOKE ALL ON TABLE title FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE title TO bde_admin;
GRANT SELECT ON TABLE title TO bde_user;

-- =============================================================================
-- T I T L E   A C T I O N
-- =============================================================================
DROP TABLE IF EXISTS title_action CASCADE;
CREATE TABLE title_action
(
  ttl_title_no VARCHAR(20) NOT NULL,
  act_tin_id INTEGER NOT NULL,
  act_id INTEGER NOT NULL,
  audit_id INTEGER NOT NULL,
  CONSTRAINT title_action_pkey PRIMARY KEY (audit_id),
  CONSTRAINT title_action_act_id_act_tin_id_ttl_title_no_key UNIQUE (act_id , act_tin_id , ttl_title_no)
);

ALTER TABLE title_action OWNER TO bde_dba;
REVOKE ALL ON TABLE title_action FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE title_action TO bde_admin;
GRANT SELECT ON TABLE title_action TO bde_user;

-- =============================================================================
-- T I T L E   D O C   R E F 
-- =============================================================================
DROP TABLE IF EXISTS title_doc_ref CASCADE;
CREATE TABLE title_doc_ref
(
  id INTEGER NOT NULL,
  type VARCHAR(4),
  tin_id INTEGER,
  reference_no VARCHAR(15),
  CONSTRAINT pkey_title_doc_ref PRIMARY KEY (id)
);

ALTER TABLE title_doc_ref OWNER TO bde_dba;
REVOKE ALL ON TABLE title_doc_ref FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE title_doc_ref TO bde_admin;
GRANT SELECT ON TABLE title_doc_ref TO bde_user;

-- =============================================================================
-- T I T L E   E S T A T E
-- =============================================================================
DROP TABLE IF EXISTS title_estate CASCADE;
CREATE TABLE title_estate
(
  id INTEGER NOT NULL,
  ttl_title_no VARCHAR(20) NOT NULL,
  type VARCHAR(4) NOT NULL,
  status VARCHAR(4) NOT NULL,
  lgd_id INTEGER NOT NULL,
  share VARCHAR(100) NOT NULL,
  timeshare_week_no VARCHAR(20),
  purpose VARCHAR(255),
  act_tin_id_crt INTEGER,
  act_id_crt INTEGER,
  original_flag CHAR(1) NOT NULL,
  term VARCHAR(255),
  tin_id_orig INTEGER,
  CONSTRAINT pkey_title_estate PRIMARY KEY (id)
);

ALTER TABLE title_estate OWNER TO bde_dba;
REVOKE ALL ON TABLE title_estate FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE title_estate TO bde_admin;
GRANT SELECT ON TABLE title_estate TO bde_user;

-- =============================================================================
-- T I T L E   M E M   T E X T 
-- =============================================================================
DROP TABLE IF EXISTS title_mem_text CASCADE;
CREATE TABLE title_mem_text
(
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
  audit_id INTEGER NOT NULL,
  CONSTRAINT title_mem_text_pkey PRIMARY KEY (audit_id),
  CONSTRAINT title_mem_text_sequence_no_ttm_id_key UNIQUE (sequence_no , ttm_id)
);

ALTER TABLE title_mem_text OWNER TO bde_dba;
REVOKE ALL ON TABLE title_mem_text FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE title_mem_text TO bde_admin;
GRANT SELECT ON TABLE title_mem_text TO bde_user;

-- =============================================================================
-- T I T L E   M E M O R I A L
-- =============================================================================
DROP TABLE IF EXISTS title_memorial CASCADE;
CREATE TABLE title_memorial
(
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
  act_tin_id_ext INTEGER,
  CONSTRAINT pkey_title_memorial PRIMARY KEY (id)
);

ALTER TABLE title_memorial OWNER TO bde_dba;
REVOKE ALL ON TABLE title_memorial FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE title_memorial TO bde_admin;
GRANT SELECT ON TABLE title_memorial TO bde_user;

-- =============================================================================
-- T I T L E   P A R C E L   A S S O C I A T I O N
-- =============================================================================
DROP TABLE IF EXISTS title_parcel_association CASCADE;
CREATE TABLE title_parcel_association
(
  id INTEGER NOT NULL,
  ttl_title_no VARCHAR(20) NOT NULL,
  par_id INTEGER NOT NULL,
  source VARCHAR(4) NOT NULL,
  CONSTRAINT pkey_title_parcel_association PRIMARY KEY (id)
);

ALTER TABLE title_parcel_association OWNER TO bde_dba;
REVOKE ALL ON TABLE title_parcel_association FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE title_parcel_association TO bde_admin;
GRANT SELECT ON TABLE title_parcel_association TO bde_user;

-- =============================================================================
-- T I T L E   T R A N S A C T   T Y P E 
-- =============================================================================
DROP TABLE IF EXISTS transact_type CASCADE;
CREATE TABLE transact_type
(
  grp VARCHAR(4) NOT NULL,
  type VARCHAR(4) NOT NULL,
  description VARCHAR(100) NOT NULL,
  audit_id INTEGER NOT NULL,
  CONSTRAINT transact_type_pkey PRIMARY KEY (audit_id),
  CONSTRAINT transact_type_grp_type_key UNIQUE (grp , type)
);

ALTER TABLE transact_type OWNER TO bde_dba;
REVOKE ALL ON TABLE transact_type FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE transact_type TO bde_admin;
GRANT SELECT ON TABLE transact_type TO bde_user;

-- =============================================================================
-- T T L   E N C 
-- =============================================================================
DROP TABLE IF EXISTS ttl_enc CASCADE;
CREATE TABLE ttl_enc
(
  id INTEGER NOT NULL,
  ttl_title_no VARCHAR(20) NOT NULL,
  enc_id INTEGER NOT NULL,
  status VARCHAR(4) NOT NULL,
  act_tin_id_crt INTEGER NOT NULL,
  act_id_crt INTEGER NOT NULL,
  CONSTRAINT pkey_ttl_enc PRIMARY KEY (id)
);

ALTER TABLE ttl_enc OWNER TO bde_dba;
REVOKE ALL ON TABLE ttl_enc FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE ttl_enc TO bde_admin;
GRANT SELECT ON TABLE ttl_enc TO bde_user;

-- =============================================================================
-- T T L   H I E R A R C H Y
-- =============================================================================
DROP TABLE IF EXISTS ttl_hierarchy CASCADE;
CREATE TABLE ttl_hierarchy
(
  id INTEGER NOT NULL,
  status VARCHAR(4) NOT NULL,
  ttl_title_no_prior VARCHAR(20),
  ttl_title_no_flw VARCHAR(20) NOT NULL,
  tdr_id INTEGER,
  act_tin_id_crt INTEGER,
  act_id_crt INTEGER,
  CONSTRAINT pkey_ttl_hierarchy PRIMARY KEY (id)
);

ALTER TABLE ttl_hierarchy OWNER TO bde_dba;
REVOKE ALL ON TABLE ttl_hierarchy FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE ttl_hierarchy TO bde_admin;
GRANT SELECT ON TABLE ttl_hierarchy TO bde_user;

-- =============================================================================
-- T T L   I N S T
-- =============================================================================
DROP TABLE IF EXISTS ttl_inst CASCADE;
CREATE TABLE ttl_inst
(
  id INTEGER NOT NULL,
  inst_no VARCHAR(30) NOT NULL,
  trt_grp VARCHAR(4) NOT NULL,
  trt_type VARCHAR(4) NOT NULL,
  ldt_loc_id INTEGER NOT NULL,
  status VARCHAR(4) NOT NULL,
  lodged_datetime TIMESTAMP NOT NULL,
  dlg_id INTEGER,
  priority_no INTEGER,
  tin_id_parent INTEGER,
  audit_id INTEGER NOT NULL,
  CONSTRAINT pkey_ttl_inst PRIMARY KEY (id)
);

ALTER TABLE ttl_inst OWNER TO bde_dba;
REVOKE ALL ON TABLE ttl_inst FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE ttl_inst TO bde_admin;
GRANT SELECT ON TABLE ttl_inst TO bde_user;

-- =============================================================================
-- T T L   I N S T   T I T L E
-- =============================================================================
DROP TABLE IF EXISTS ttl_inst_title CASCADE;
CREATE TABLE ttl_inst_title
(
  tin_id INTEGER NOT NULL,
  ttl_title_no VARCHAR(20) NOT NULL,
  audit_id INTEGER NOT NULL,
  CONSTRAINT ttl_inst_title_pkey PRIMARY KEY (audit_id),
  CONSTRAINT ttl_inst_title_tin_id_ttl_title_no_key UNIQUE (tin_id , ttl_title_no)
);

ALTER TABLE ttl_inst_title OWNER TO bde_dba;
REVOKE ALL ON TABLE ttl_inst_title FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE ttl_inst_title TO bde_admin;
GRANT SELECT ON TABLE ttl_inst_title TO bde_user;

-- =============================================================================
-- U S E R
-- =============================================================================
DROP TABLE IF EXISTS "user" CASCADE;
CREATE TABLE "user"
(
  id VARCHAR(20) NOT NULL,
  type VARCHAR(4) NOT NULL,
  status VARCHAR(4) NOT NULL,
  title VARCHAR(4),
  given_names VARCHAR(30),
  surname VARCHAR(30),
  corporate_name VARCHAR(100),
  audit_id INTEGER NOT NULL,
  CONSTRAINT user_pkey PRIMARY KEY (audit_id),
  CONSTRAINT user_id_key UNIQUE (id)
);

ALTER TABLE "user" OWNER TO bde_dba;
REVOKE ALL ON TABLE "user" FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE "user" TO bde_admin;
GRANT SELECT ON TABLE "user" TO bde_user;

-- =============================================================================
-- V E C T O R   L S 
-- =============================================================================
DROP TABLE IF EXISTS vector_ls CASCADE;
CREATE TABLE vector_ls
(
  type VARCHAR(4) NOT NULL,
  nod_id_start INTEGER NOT NULL,
  nod_id_end INTEGER,
  length NUMERIC(22,12) NOT NULL,
  source INTEGER NOT NULL,
  id INTEGER NOT NULL,
  audit_id INTEGER NOT NULL,
  se_row_id INTEGER,
  CONSTRAINT pkey_vector_ls PRIMARY KEY (id)
);

PERFORM AddGeometryColumn('vector_ls', 'shape', 4167, 'LINESTRING', 2);
CREATE INDEX shx_vector_ls_shape ON vector_ls USING gist (shape);

ALTER TABLE vector_ls OWNER TO bde_dba;
REVOKE ALL ON TABLE vector_ls FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE vector_ls TO bde_admin;
GRANT SELECT ON TABLE vector_ls TO bde_user;

-- =============================================================================
-- V E C T O R   P T
-- =============================================================================
DROP TABLE IF EXISTS vector_pt CASCADE;
CREATE TABLE vector_pt
(
  type VARCHAR(4) NOT NULL,
  nod_id_start INTEGER NOT NULL,
  nod_id_end INTEGER,
  length NUMERIC(22,12) NOT NULL,
  source INTEGER NOT NULL,
  id INTEGER NOT NULL,
  audit_id INTEGER NOT NULL,
  se_row_id INTEGER,
  CONSTRAINT pkey_vector_pt PRIMARY KEY (id)
);

PERFORM AddGeometryColumn('vector_pt', 'shape', 4167, 'POINT', 2);
CREATE INDEX shx_vector_pt_shape ON vector_pt USING gist (shape);

ALTER TABLE vector_pt OWNER TO bde_dba;
REVOKE ALL ON TABLE vector_pt FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE vector_pt TO bde_admin;
GRANT SELECT ON TABLE vector_pt TO bde_user;

-- =============================================================================
-- W O R K
-- =============================================================================
DROP TABLE IF EXISTS work CASCADE;
CREATE TABLE work
(
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
  CONSTRAINT pkey_work PRIMARY KEY (id)
);

ALTER TABLE work OWNER TO bde_dba;
REVOKE ALL ON TABLE work FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE work TO bde_admin;
GRANT SELECT ON TABLE work TO bde_user;

-- =============================================================================
-- S T R E E T   A D D R E S S   E X T E R N A L
-- =============================================================================

DROP TABLE IF EXISTS street_address_ext CASCADE;
CREATE TABLE street_address_ext
(
  house_number VARCHAR(25) NOT NULL,
  range_low INTEGER NOT NULL,
  range_high INTEGER,
  status VARCHAR(4) NOT NULL,
  unofficial_flag CHAR(1) NOT NULL,
  rcl_id INTEGER NOT NULL,
  rna_id INTEGER NOT NULL,
  alt_id INTEGER,
  id INTEGER NOT NULL,
  sufi INTEGER,
  audit_id INTEGER NOT NULL,
  se_row_id INTEGER,
  CONSTRAINT pkey_street_address_ext PRIMARY KEY (id)
);

PERFORM AddGeometryColumn('street_address_ext', 'shape', 4167, 'POINT', 2);
CREATE INDEX shx_street_address_ext_shape ON street_address_ext USING gist (shape);

ALTER TABLE street_address_ext OWNER TO bde_dba;
REVOKE ALL ON TABLE street_address_ext FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE street_address_ext TO bde_admin;
GRANT SELECT ON TABLE street_address_ext TO bde_user;

-- =============================================================================
-- F E A T U R E   N A M E   P T
-- =============================================================================

DROP TABLE IF EXISTS feature_name_pt CASCADE;
CREATE TABLE feature_name_pt
(
  id integer NOT NULL,
  type character varying(4) NOT NULL,
  name character varying(100) NOT NULL,
  status character varying(4) NOT NULL,
  other_details character varying(100),
  audit_id integer NOT NULL,
  se_row_id integer,
  CONSTRAINT pkey_crs_feature_name_pt PRIMARY KEY (id)
);

PERFORM AddGeometryColumn('feature_name_pt', 'shape', 4167, 'POINT', 2);

ALTER TABLE feature_name_pt OWNER TO bde_dba;
REVOKE ALL ON TABLE feature_name_pt FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE feature_name_pt TO bde_admin;
GRANT SELECT ON TABLE feature_name_pt TO bde_user;


-- =============================================================================
-- F E A T U R E   N A M E   P O L Y
-- =============================================================================

DROP TABLE IF EXISTS feature_name_poly CASCADE;
CREATE TABLE feature_name_poly
(
  id integer NOT NULL,
  type character varying(4) NOT NULL,
  name character varying(100) NOT NULL,
  status character varying(4) NOT NULL,
  other_details character varying(100),
  audit_id integer NOT NULL,
  se_row_id integer,
  CONSTRAINT pkey_crs_feature_name_poly PRIMARY KEY (id)
);

PERFORM AddGeometryColumn('feature_name_poly', 'shape', 4167, 'POLYGON', 2);

ALTER TABLE feature_name_poly OWNER TO bde_dba;
REVOKE ALL ON TABLE feature_name_poly FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE feature_name_poly TO bde_admin;
GRANT SELECT ON TABLE feature_name_poly TO bde_user;


-- =============================================================================
-- C O O R D I N A T E
-- =============================================================================

DROP TABLE IF EXISTS coordinate CASCADE;
CREATE TABLE coordinate
(
  id integer NOT NULL,
  cos_id integer NOT NULL,
  nod_id integer NOT NULL,
  ort_type_1 character varying(4),
  ort_type_2 character varying(4),
  ort_type_3 character varying(4),
  status character varying(4) NOT NULL,
  sdc_status character(1) NOT NULL,
  source character varying(4),
  value1 numeric(22,12),
  value2 numeric(22,12),
  value3 numeric(22,12),
  wrk_id_created integer,
  cor_id integer,
  audit_id integer NOT NULL,
  CONSTRAINT pkey_coordinate PRIMARY KEY (id)
);

ALTER TABLE coordinate OWNER TO bde_dba;
REVOKE ALL ON TABLE coordinate FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE coordinate TO bde_admin;
GRANT SELECT ON TABLE coordinate TO bde_user;

-- =============================================================================
-- O F F I C E
-- =============================================================================
DROP TABLE IF EXISTS office CASCADE;
CREATE TABLE office
(
  code VARCHAR(4) NOT NULL,
  name VARCHAR(50) NOT NULL,
  rcs_name VARCHAR(50) NOT NULL,
  cis_name VARCHAR(50) NOT NULL,
  alloc_source_table VARCHAR(50) NOT NULL,
  audit_id integer NOT NULL,
  CONSTRAINT crs_office_pkey PRIMARY KEY (audit_id),
  CONSTRAINT crs_office_code_key UNIQUE (code)
);
ALTER TABLE office OWNER TO bde_dba;
REVOKE ALL ON TABLE office FROM PUBLIC;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE office TO bde_admin;
GRANT SELECT ON TABLE office TO bde_user;

END
$SCHEMA$;
