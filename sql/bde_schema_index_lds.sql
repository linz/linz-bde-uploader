-------------------------------------------------------------------------------
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
-------------------------------------------------------------------------------
-- BDE indexes required for LDS
-------------------------------------------------------------------------------
SET client_min_messages TO WARNING;
SET search_path = bde, public;

BEGIN;
-------------------------------------------------------------------------------
-- crs_action
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_act_aud_id ON crs_action USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_action_type
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_att_aud_id ON crs_action_type USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_adj_obs_change
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_aoc_aud_id ON crs_adj_obs_change USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_adj_user_coef
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_auc_aud_id ON crs_adj_user_coef USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_adjust_coef
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_adc_aud_id ON crs_adjust_coef USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_adjust_method
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_adm_aud_id ON crs_adjust_method USING btree (audit_id);
CREATE UNIQUE INDEX idx_adm_name ON crs_adjust_method USING btree (name);

-------------------------------------------------------------------------------
-- crs_adjustment_run
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_adj_aud_id ON crs_adjustment_run USING btree (audit_id);
CREATE INDEX fk_adj_adm ON crs_adjustment_run USING btree (adm_id);
CREATE INDEX fk_adj_wrk ON crs_adjustment_run USING btree (wrk_id);

-------------------------------------------------------------------------------
-- crs_adoption
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_adp_aud_id ON crs_adoption USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_affected_parcl
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_afp_aud_id ON crs_affected_parcl USING btree (audit_id);
CREATE INDEX fk_afp_par ON crs_affected_parcl USING btree (par_id);
CREATE INDEX fk_afp_sur ON crs_affected_parcl USING btree (sur_wrk_id);

-------------------------------------------------------------------------------
-- crs_alias
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_appellation
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_app_aud_id ON crs_appellation USING btree (audit_id);
CREATE INDEX fk_app_par ON crs_appellation USING btree (par_id);

-------------------------------------------------------------------------------
-- crs_comprised_in
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_coordinate
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_coo_aud_id ON crs_coordinate USING btree (audit_id);
CREATE INDEX fk_coo_cor ON crs_coordinate USING btree (cor_id);
CREATE INDEX fk_coo_cos ON crs_coordinate USING btree (cos_id);
CREATE INDEX fk_coo_nod ON crs_coordinate USING btree (nod_id);

-------------------------------------------------------------------------------
-- crs_coordinate_sys
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_cos_aud_id ON crs_coordinate_sys USING btree (audit_id);
CREATE INDEX fk_cos_cot ON crs_coordinate_sys USING btree (cot_id);
CREATE INDEX fk_cos_dtm ON crs_coordinate_sys USING btree (dtm_id);

-------------------------------------------------------------------------------
-- crs_coordinate_tpe
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_cot_aud_id ON crs_coordinate_tpe USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_cor_precision
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_cop_aud_id ON crs_cor_precision USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_cord_order
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_cor_aud_id ON crs_cord_order USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_datum
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_dtm_aud_id ON crs_datum USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_elect_place
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_epl_aud_id ON crs_elect_place USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_ellipsoid
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_elp_aud_id ON crs_ellipsoid USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_enc_share
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_encumbrance
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_encumbrancee
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_estate_share
-------------------------------------------------------------------------------
CREATE INDEX fk_tle_ess ON crs_estate_share USING btree (ett_id);

-------------------------------------------------------------------------------
-- crs_feature_name
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_fen_aud_id ON crs_feature_name USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_geodetic_node_network
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_gnn_aud_id ON crs_geodetic_node_network USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_image
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_land_district
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_ldt_aud_id ON crs_land_district USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_legal_desc
-------------------------------------------------------------------------------
CREATE INDEX fk_lgd_ttl ON crs_legal_desc USING btree (ttl_title_no);
CREATE UNIQUE INDEX idx_lgd_aud_id ON crs_legal_desc USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_legal_desc_prl
-------------------------------------------------------------------------------
CREATE INDEX fk_rap_par ON crs_legal_desc_prl USING btree (par_id);
CREATE INDEX fk_rap_rar ON crs_legal_desc_prl USING btree (lgd_id);
CREATE UNIQUE INDEX idx_lgp_aud_id ON crs_legal_desc_prl USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_line
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_lin_aud_id ON crs_line USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_locality
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_loc_aud_id ON crs_locality USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_maintenance
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_mnt_aud_id ON crs_maintenance USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_map_grid
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_map_aud_id ON crs_map_grid USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mark
-------------------------------------------------------------------------------
CREATE INDEX fk_mrk_nod ON crs_mark USING btree (nod_id);

-------------------------------------------------------------------------------
-- crs_mark_name
-------------------------------------------------------------------------------
CREATE INDEX fk_mkn_mrk ON crs_mark_name USING btree (mrk_id);
CREATE UNIQUE INDEX idx_mkn_aud_id ON crs_mark_name USING btree (audit_id);
CREATE INDEX idx_mkn_type_code ON crs_mark_name USING btree (type) WHERE UPPER(type) = 'CODE';
CREATE INDEX idx_mkn_type ON crs_mark_name USING btree ("type");

-------------------------------------------------------------------------------
-- crs_mark_sup_doc
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_msd_aud_id ON crs_mark_sup_doc USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mesh_blk
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_mbk_aud_id ON crs_mesh_blk USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mesh_blk_area
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_mba_aud_id ON crs_mesh_blk_area USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mesh_blk_bdry
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_mbb_aud_id ON crs_mesh_blk_bdry USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mesh_blk_line
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_mbl_aud_id ON crs_mesh_blk_line USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mesh_blk_place
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_mpr_aud_id ON crs_mesh_blk_place USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mrk_phys_state
-------------------------------------------------------------------------------
CREATE INDEX fk_mps_mrk ON crs_mrk_phys_state USING btree (mrk_id);
CREATE INDEX fk_mps_wrk ON crs_mrk_phys_state USING btree (wrk_id);
CREATE UNIQUE INDEX idx_mps_aud_id ON crs_mrk_phys_state USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_network_plan
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_nwp_aud_id ON crs_network_plan USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_node
-------------------------------------------------------------------------------
CREATE INDEX fk_nod_cos ON crs_node USING btree (cos_id_official);
CREATE INDEX fk_nod_sit ON crs_node USING btree (sit_id);
CREATE UNIQUE INDEX idx_nod_aud_id ON crs_node USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_node_prp_order
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_npo_aud_id ON crs_node_prp_order USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_node_works
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_now_aud_id ON crs_node_works USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_nominal_index
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_obs_accuracy
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_oba_aud_id ON crs_obs_accuracy USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_obs_elem_type
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_oet_aud_id ON crs_obs_elem_type USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_obs_set
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_obs_aud_id ON crs_obs_set USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_obs_type
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_obt_aud_id ON crs_obs_type USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_observation
-------------------------------------------------------------------------------
CREATE INDEX fk_obn_cos ON crs_observation USING btree (cos_id);
CREATE INDEX fk_obn_obt ON crs_observation USING btree (obt_type, obt_sub_type);
CREATE INDEX fk_obn_stp1 ON crs_observation USING btree (stp_id_local);
CREATE INDEX fk_obn_vct ON crs_observation USING btree (vct_id);
CREATE UNIQUE INDEX idx_obn_aud_id ON crs_observation USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_off_cord_sys
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_ocs_aud_id ON crs_off_cord_sys USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_office
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_ofc_aud_id ON crs_office USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_ordinate_adj
-------------------------------------------------------------------------------
CREATE INDEX fk_orj_coo_output ON crs_ordinate_adj USING btree (coo_id_output);
CREATE INDEX idx_orj_adj_coo ON crs_ordinate_adj USING btree (adj_id, coo_id_output);
CREATE UNIQUE INDEX idx_orj_aud_id ON crs_ordinate_adj USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_ordinate_type
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_ort_aud_id ON crs_ordinate_type USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_parcel
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_par_aud_id ON crs_parcel USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_parcel_bndry
-------------------------------------------------------------------------------
CREATE INDEX fk_pab_lin ON crs_parcel_bndry USING btree (lin_id);
CREATE INDEX fk_pab_pri ON crs_parcel_bndry USING btree (pri_id);
CREATE UNIQUE INDEX idx_pab_aud_id ON crs_parcel_bndry USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_parcel_dimen
-------------------------------------------------------------------------------
CREATE INDEX fk_pdi_obn ON crs_parcel_dimen USING btree (obn_id);
CREATE INDEX fk_pdi_par ON crs_parcel_dimen USING btree (par_id);
CREATE UNIQUE INDEX idx_pdi_aud_id ON crs_parcel_dimen USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_parcel_label
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_plb_aud_id ON crs_parcel_label USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_parcel_ring
-------------------------------------------------------------------------------
CREATE INDEX fk_pri_par ON crs_parcel_ring USING btree (par_id);
CREATE UNIQUE INDEX idx_pri_aud_id ON crs_parcel_ring USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_programme
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_pgm_aud_id ON crs_programme USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_proprietor
-------------------------------------------------------------------------------
CREATE INDEX fk_ess_prp ON crs_proprietor USING btree (ets_id);

-------------------------------------------------------------------------------
-- crs_reduct_meth
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_rdm_aud_id ON crs_reduct_meth USING btree (audit_id);
CREATE UNIQUE INDEX idx_rdm_name ON crs_reduct_meth USING btree (name);

-------------------------------------------------------------------------------
-- crs_reduct_run
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_rdn_aud_id ON crs_reduct_run USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_ref_survey
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_rsu_aud_id ON crs_ref_survey USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_road_ctr_line
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_rcl_aud_id ON crs_road_ctr_line USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_road_name
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_rna_aud_id ON crs_road_name USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_road_name_asc
-------------------------------------------------------------------------------
CREATE INDEX fk_rns_rcl ON crs_road_name_asc USING btree (rcl_id);
CREATE INDEX fk_rns_rna ON crs_road_name_asc USING btree (rna_id);
CREATE UNIQUE INDEX idx_rns_aud_id ON crs_road_name_asc USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_setup
-------------------------------------------------------------------------------
CREATE INDEX fk_stp_wrk ON crs_setup USING btree (wrk_id);
CREATE UNIQUE INDEX idx_stp_aud_id ON crs_setup USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_site
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_sit_aud_id ON crs_site USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_site_locality
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_slo_aud_id ON crs_site_locality USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_stat_act_parcl
-------------------------------------------------------------------------------
CREATE INDEX fk_sap_par ON crs_stat_act_parcl USING btree (par_id);
CREATE UNIQUE INDEX fk_sap_aud ON crs_stat_act_parcl USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_stat_version
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_sav_aud_id ON crs_stat_version USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_statist_area
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_stt_aud_id ON crs_statist_area USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_statute
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_ste_ak1 ON crs_statute USING btree (section, name_and_date);
CREATE UNIQUE INDEX idx_ste_aud_id ON crs_statute USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_statute_action
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_sta_aud_id ON crs_statute_action USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_street_address
-------------------------------------------------------------------------------
CREATE INDEX fk_sad_rna ON crs_street_address USING btree (rna_id);
CREATE UNIQUE INDEX idx_sad_aud_id ON crs_street_address USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_sur_admin_area
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_saa_aud_id ON crs_sur_admin_area USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_sur_plan_ref
-------------------------------------------------------------------------------
CREATE INDEX fk_wrk_id ON crs_sur_plan_ref USING btree (wrk_id);

-------------------------------------------------------------------------------
-- crs_survey
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_sur_idx ON crs_survey USING btree (dataset_id, dataset_series, ldt_loc_id, dataset_suffix);
CREATE UNIQUE INDEX idx_sur_aud_id ON crs_survey USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_survey_image
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_sim_aud_id ON crs_survey_image USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_sys_code
-------------------------------------------------------------------------------
CREATE INDEX fk_sco_scg ON crs_sys_code USING btree (scg_code);
CREATE UNIQUE INDEX fk_sco_scg_code ON crs_sys_code USING btree (scg_code, code);
CREATE UNIQUE INDEX idx_sco_aud_id ON crs_sys_code USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_sys_code_group
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_scg_aud_id ON crs_sys_code_group USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_title
-------------------------------------------------------------------------------
CREATE INDEX fk_ttl_psd ON crs_title USING btree (protect_start);
CREATE INDEX fk_ttl_ped ON crs_title USING btree (protect_end);
CREATE UNIQUE INDEX idx_ttl_aud_id ON crs_title USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_title_action
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_tta_aud_id ON crs_title_action USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_title_estate
-------------------------------------------------------------------------------
CREATE INDEX fk_ett_lgd ON crs_title_estate USING btree (lgd_id);
CREATE INDEX fk_ttl_ett ON crs_title_estate USING btree (ttl_title_no);

-------------------------------------------------------------------------------
-- crs_title_mem_text
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_tmt_aud_id ON crs_title_mem_text USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_title_memorial
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_topology_class
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_top_aud_id ON crs_topology_class USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_transact_type
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_crs_tran_desc ON crs_transact_type USING btree (grp, description, "type");
CREATE UNIQUE INDEX idx_trt_aud_id ON crs_transact_type USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_ttl_enc
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_ttl_hierarchy
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_ttl_inst
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- crs_ttl_inst_title
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_tnt_aud_id ON crs_ttl_inst_title USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_unit_of_meas
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_uom_aud_id ON crs_unit_of_meas USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_user
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_usr_aud_id ON crs_user USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_vector
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_vct_ak1 ON crs_vector USING btree ("type", nod_id_start, nod_id_end);
CREATE UNIQUE INDEX idx_vct_aud_id ON crs_vector USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_vertx_sequence
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_vts_aud_id ON crs_vertx_sequence USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_work
-------------------------------------------------------------------------------
CREATE INDEX fk_wrk_cos ON crs_work USING btree (cos_id);
CREATE UNIQUE INDEX idx_wrk_aud_id ON crs_work USING btree (audit_id);

COMMIT;
