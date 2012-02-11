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
-- BDE indexes
-------------------------------------------------------------------------------
SET client_min_messages TO WARNING;
SET search_path = bde, public;

BEGIN;
-------------------------------------------------------------------------------
-- crs_action
-------------------------------------------------------------------------------
CREATE INDEX fk_act_tin ON crs_action USING btree (tin_id);
CREATE INDEX fk_act_att ON crs_action USING btree (att_type);
CREATE INDEX fk_act_ste ON crs_action USING btree (ste_id);
CREATE INDEX fk_act_act ON crs_action USING btree (act_tin_id_orig, act_id_orig);
CREATE UNIQUE INDEX idx_act_aud_id ON crs_action USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_action_type
-------------------------------------------------------------------------------
CREATE INDEX fk_att_sob ON crs_action_type USING btree (sob_name);
CREATE UNIQUE INDEX idx_att_aud_id ON crs_action_type USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_adj_obs_change
-------------------------------------------------------------------------------
CREATE INDEX fk_aoc_adj ON crs_adj_obs_change USING btree (adj_id);
CREATE INDEX fk_aoc_obn ON crs_adj_obs_change USING btree (obn_id);
CREATE UNIQUE INDEX idx_aoc_aud_id ON crs_adj_obs_change USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_adj_user_coef
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_auc_aud_id ON crs_adj_user_coef USING btree (audit_id);
CREATE INDEX fk_auc_adc ON crs_adj_user_coef USING btree (adc_id);
CREATE INDEX fk_auc_adj ON crs_adj_user_coef USING btree (adj_id);

-------------------------------------------------------------------------------
-- crs_adjust_coef
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_adc_aud_id ON crs_adjust_coef USING btree (audit_id);
CREATE INDEX fk_adc_adm ON crs_adjust_coef USING btree (adm_id);

-------------------------------------------------------------------------------
-- crs_adjust_method
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_adm_aud_id ON crs_adjust_method USING btree (audit_id);
CREATE UNIQUE INDEX idx_adm_name ON crs_adjust_method USING btree (name);

-------------------------------------------------------------------------------
-- crs_adjustment_run
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_adj_aud_id ON crs_adjustment_run USING btree (audit_id);
CREATE INDEX idx_adj_status ON crs_adjustment_run USING btree (status);
CREATE INDEX fk_adj_adm ON crs_adjustment_run USING btree (adm_id);
CREATE INDEX fk_adj_cos ON crs_adjustment_run USING btree (cos_id);
CREATE INDEX fk_adj_usr ON crs_adjustment_run USING btree (usr_id_exec);
CREATE INDEX fk_adj_wrk ON crs_adjustment_run USING btree (wrk_id);

-------------------------------------------------------------------------------
-- crs_adoption
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_adp_aud_id ON crs_adoption USING btree (audit_id);
CREATE INDEX fk_adp_obn_orig ON crs_adoption USING btree (obn_id_orig);
CREATE INDEX fk_adp_sur ON crs_adoption USING btree (sur_wrk_id_orig);

-------------------------------------------------------------------------------
-- crs_affected_parcl
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_afp_aud_id ON crs_affected_parcl USING btree (audit_id);
CREATE INDEX fk_afp_par ON crs_affected_parcl USING btree (par_id);
CREATE INDEX fk_afp_sur ON crs_affected_parcl USING btree (sur_wrk_id);

-------------------------------------------------------------------------------
-- crs_alias
-------------------------------------------------------------------------------
CREATE INDEX fk_ali_prp ON crs_alias USING btree (prp_id);

-------------------------------------------------------------------------------
-- crs_appellation
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_app_aud_id ON crs_appellation USING btree (audit_id);
CREATE INDEX idx_fi_app_general ON crs_appellation USING btree (appellation_value, parcel_value);
CREATE INDEX idx_fi_app_maori ON crs_appellation USING btree (maori_name, parcel_value);
CREATE INDEX idx_fi_app_other ON crs_appellation USING btree (other_appellation);
CREATE INDEX fk_app_act_crt ON crs_appellation USING btree (act_tin_id_crt, act_id_crt);
CREATE INDEX fk_app_act_ext ON crs_appellation USING btree (act_tin_id_ext, act_id_ext);
CREATE INDEX fk_app_par ON crs_appellation USING btree (par_id);

/*
--------------------------------------------------------------------------------
-- table crs_audit_detail
--------------------------------------------------------------------------------
CREATE INDEX idx_crs_audit_detail ON crs_audit_detail USING btree (id, table_name);
CREATE INDEX idx_aud_table_name ON crs_audit_detail USING btree (table_name, "timestamp");

*/

-------------------------------------------------------------------------------
-- crs_comprised_in
-------------------------------------------------------------------------------
CREATE INDEX fk_cmp_wrk ON crs_comprised_in USING btree (wrk_id);

-------------------------------------------------------------------------------
-- crs_coordinate
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_coo_aud_id ON crs_coordinate USING btree (audit_id);
CREATE INDEX idx_coo_value1 ON crs_coordinate USING btree (value1);
CREATE INDEX idx_coo_value2 ON crs_coordinate USING btree (value2);
CREATE INDEX idx_coo_value3 ON crs_coordinate USING btree (value3);
CREATE INDEX fk_coo_cor ON crs_coordinate USING btree (cor_id);
CREATE INDEX fk_coo_cos ON crs_coordinate USING btree (cos_id);
CREATE INDEX fk_coo_nod ON crs_coordinate USING btree (nod_id);
CREATE INDEX fk_coo_ort1 ON crs_coordinate USING btree (ort_type_1);
CREATE INDEX fk_coo_ort2 ON crs_coordinate USING btree (ort_type_2);
CREATE INDEX fk_coo_ort3 ON crs_coordinate USING btree (ort_type_3);
CREATE INDEX fk_coo_wrk ON crs_coordinate USING btree (wrk_id_created);

-------------------------------------------------------------------------------
-- crs_coordinate_sys
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_cos_aud_id ON crs_coordinate_sys USING btree (audit_id);
CREATE INDEX fk_cos_cos ON crs_coordinate_sys USING btree (cos_id_adjust);
CREATE INDEX fk_cos_cot ON crs_coordinate_sys USING btree (cot_id);
CREATE INDEX fk_cos_dtm ON crs_coordinate_sys USING btree (dtm_id);

-------------------------------------------------------------------------------
-- crs_coordinate_tpe
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_cot_aud_id ON crs_coordinate_tpe USING btree (audit_id);
CREATE INDEX fk_cot_ort1 ON crs_coordinate_tpe USING btree (ort_type_1);
CREATE INDEX fk_cot_ort2 ON crs_coordinate_tpe USING btree (ort_type_2);
CREATE INDEX fk_cot_ort3 ON crs_coordinate_tpe USING btree (ort_type_3);

-------------------------------------------------------------------------------
-- crs_cor_precision
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_cop_aud_id ON crs_cor_precision USING btree (audit_id);
CREATE INDEX fk_cop_cor ON crs_cor_precision USING btree (cor_id);
CREATE INDEX fk_cop_ort ON crs_cor_precision USING btree (ort_type);

-------------------------------------------------------------------------------
-- crs_cord_order
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_cor_aud_id ON crs_cord_order USING btree (audit_id);
CREATE INDEX fk_cor_dtm ON crs_cord_order USING btree (dtm_id);

-------------------------------------------------------------------------------
-- crs_datum
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_dtm_aud_id ON crs_datum USING btree (audit_id);
CREATE INDEX fk_dtm_elp ON crs_datum USING btree (elp_id);

/*
--------------------------------------------------------------------------------
-- crs_dealing_survey
--------------------------------------------------------------------------------
CREATE INDEX fk_dsu_dlg ON crs_dealing_survey USING btree (dlg_id);
CREATE INDEX fk_dsu_sur ON crs_dealing_survey USING btree (sur_wrk_id);

*/

-------------------------------------------------------------------------------
-- crs_elect_place
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_epl_aud_id ON crs_elect_place USING btree (audit_id);
CREATE INDEX shx_epl_shape ON crs_elect_place USING gist (shape);
CREATE INDEX fk_epl_alt ON crs_elect_place USING btree (alt_id);

-------------------------------------------------------------------------------
-- crs_ellipsoid
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_elp_aud_id ON crs_ellipsoid USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_enc_share
-------------------------------------------------------------------------------
CREATE INDEX fk_enc_ecs ON crs_enc_share USING btree (enc_id);
CREATE INDEX idx_ens_act_crt ON crs_enc_share USING btree (act_tin_id_crt);

-------------------------------------------------------------------------------
-- crs_encumbrance
-------------------------------------------------------------------------------
CREATE INDEX fk_enc_crt ON crs_encumbrance USING btree (act_tin_id_crt);
CREATE INDEX fk_enc_orig ON crs_encumbrance USING btree (act_tin_id_orig);

-------------------------------------------------------------------------------
-- crs_encumbrancee
-------------------------------------------------------------------------------
CREATE INDEX fk_ene_ens ON crs_encumbrancee USING btree (ens_id);

-------------------------------------------------------------------------------
-- crs_estate_share
-------------------------------------------------------------------------------
CREATE INDEX fk_ets_act_crt ON crs_estate_share USING btree (act_tin_id_crt);
CREATE INDEX fk_tle_ess ON crs_estate_share USING btree (ett_id);

-------------------------------------------------------------------------------
-- crs_feature_name
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_fen_aud_id ON crs_feature_name USING btree (audit_id);
CREATE INDEX shx_fen_shape ON crs_feature_name USING gist (shape);

-------------------------------------------------------------------------------
-- crs_geodetic_node_network
-------------------------------------------------------------------------------
CREATE INDEX fk_gnn_gdn ON crs_geodetic_node_network USING btree (gdn_id);
CREATE UNIQUE INDEX idx_gnn_aud_id ON crs_geodetic_node_network USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_image
-------------------------------------------------------------------------------
CREATE INDEX idx_ims_id ON crs_image USING btree (ims_id);
CREATE INDEX idx_ims_centera_id ON crs_image USING btree (centera_id);

/*
--------------------------------------------------------------------------------
-- crs_job_task_list
--------------------------------------------------------------------------------
CREATE INDEX fk_jtl_job ON crs_job_task_list USING btree (job_id);
CREATE INDEX fk_jtl_tkl ON crs_job_task_list USING btree (tkl_id);
CREATE INDEX fk_jtl_usr ON crs_job_task_list USING btree (usr_id);
CREATE INDEX idx_jtl_date_comp ON crs_job_task_list USING btree (date_completed);
CREATE INDEX idx_crs_jtl_stat ON crs_job_task_list USING btree (status, usr_id);
CREATE UNIQUE INDEX idx_jtl_audit_id ON crs_job_task_list USING btree (audit_id);

*/

-------------------------------------------------------------------------------
-- crs_land_district
-------------------------------------------------------------------------------
CREATE INDEX fk_ldt_off ON crs_land_district USING btree (off_code);
CREATE UNIQUE INDEX idx_ldt_aud_id ON crs_land_district USING btree (audit_id);
CREATE INDEX shx_ldt_shape ON crs_land_district USING gist (shape);

-------------------------------------------------------------------------------
-- crs_legal_desc
-------------------------------------------------------------------------------
CREATE INDEX fk_lgd_ttl ON crs_legal_desc USING btree (ttl_title_no);
CREATE UNIQUE INDEX idx_lgd_aud_id ON crs_legal_desc USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_legal_desc_prl
-------------------------------------------------------------------------------
CREATE INDEX fk_lgp_sur ON crs_legal_desc_prl USING btree (sur_wrk_id_crt);
CREATE INDEX fk_rap_par ON crs_legal_desc_prl USING btree (par_id);
CREATE INDEX fk_rap_rar ON crs_legal_desc_prl USING btree (lgd_id);
CREATE UNIQUE INDEX idx_lgp_aud_id ON crs_legal_desc_prl USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_line
-------------------------------------------------------------------------------
CREATE INDEX fk_lin_nod_end ON crs_line USING btree (nod_id_end);
CREATE INDEX fk_lin_nod_start ON crs_line USING btree (nod_id_start);
CREATE INDEX fk_lin_pnx ON crs_line USING btree (pnx_id_created);
CREATE UNIQUE INDEX idx_lin_aud_id ON crs_line USING btree (audit_id);
CREATE INDEX shx_lin_shape ON crs_line USING gist (shape);

-------------------------------------------------------------------------------
-- crs_locality
-------------------------------------------------------------------------------
CREATE INDEX fk_loc_loc ON crs_locality USING btree (loc_id_parent);
CREATE UNIQUE INDEX idx_loc_aud_id ON crs_locality USING btree (audit_id);
CREATE INDEX shx_loc_shape ON crs_locality USING gist (shape);

-------------------------------------------------------------------------------
-- crs_maintenance
-------------------------------------------------------------------------------
CREATE INDEX fk_mnt_mrk ON crs_maintenance USING btree (mrk_id);
CREATE UNIQUE INDEX idx_mnt_aud_id ON crs_maintenance USING btree (audit_id);
CREATE INDEX idx_mnt_status ON crs_maintenance USING btree (status);

-------------------------------------------------------------------------------
-- crs_map_grid
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_map_aud_id ON crs_map_grid USING btree (audit_id);
CREATE INDEX shx_map_shape ON crs_map_grid USING gist (shape);

-------------------------------------------------------------------------------
-- crs_mark
-------------------------------------------------------------------------------
CREATE INDEX fk_mark_wrk ON crs_mark USING btree (wrk_id_created);
CREATE INDEX fk_mrk_mrk_dist ON crs_mark USING btree (mrk_id_dist);
CREATE INDEX fk_mrk_mrk_rep ON crs_mark USING btree (mrk_id_repl);
CREATE INDEX fk_mrk_nod ON crs_mark USING btree (nod_id);
CREATE UNIQUE INDEX idx_mrk_aud_id ON crs_mark USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mark_name
-------------------------------------------------------------------------------
CREATE INDEX fk_mkn_mrk ON crs_mark_name USING btree (mrk_id);
CREATE UNIQUE INDEX idx_mkn_aud_id ON crs_mark_name USING btree (audit_id);
CREATE INDEX idx_mkn_name ON crs_mark_name USING btree (name);
CREATE INDEX idx_mkn_type_code ON crs_mark_name USING btree (type) WHERE UPPER(type) = 'CODE';
CREATE INDEX idx_mkn_type ON crs_mark_name USING btree ("type");

-------------------------------------------------------------------------------
-- crs_mark_sup_doc
-------------------------------------------------------------------------------
CREATE INDEX fk_msd_mrk ON crs_mark_sup_doc USING btree (mrk_id);
CREATE INDEX fk_msd_sud ON crs_mark_sup_doc USING btree (sud_id);
CREATE UNIQUE INDEX idx_msd_aud_id ON crs_mark_sup_doc USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mesh_blk
-------------------------------------------------------------------------------
CREATE INDEX fk_mbk_alt ON crs_mesh_blk USING btree (alt_id);
CREATE UNIQUE INDEX idx_mbk_aud_id ON crs_mesh_blk USING btree (audit_id);
CREATE INDEX shx_mbk_shape ON crs_mesh_blk USING gist (shape);

-------------------------------------------------------------------------------
-- crs_mesh_blk_area
-------------------------------------------------------------------------------
CREATE INDEX fk_mba_alt ON crs_mesh_blk_area USING btree (alt_id);
CREATE INDEX fk_mba_mbk ON crs_mesh_blk_area USING btree (mbk_id);
CREATE INDEX fk_mba_stt ON crs_mesh_blk_area USING btree (stt_id);
CREATE UNIQUE INDEX idx_mba_aud_id ON crs_mesh_blk_area USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mesh_blk_bdry
-------------------------------------------------------------------------------
CREATE INDEX fk_mbb_alt ON crs_mesh_blk_bdry USING btree (alt_id);
CREATE INDEX fk_mbb_mbk ON crs_mesh_blk_bdry USING btree (mbk_id);
CREATE INDEX fk_mbb_mbl ON crs_mesh_blk_bdry USING btree (mbl_id);
CREATE UNIQUE INDEX idx_mbb_aud_id ON crs_mesh_blk_bdry USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_mesh_blk_line
-------------------------------------------------------------------------------
CREATE INDEX fk_mbl_alt ON crs_mesh_blk_line USING btree (alt_id);
CREATE UNIQUE INDEX idx_mbl_aud_id ON crs_mesh_blk_line USING btree (audit_id);
CREATE INDEX shx_mbl_shape ON crs_mesh_blk_line USING gist (shape);

-------------------------------------------------------------------------------
-- crs_mesh_blk_place
-------------------------------------------------------------------------------
CREATE INDEX fk_mpr_alt ON crs_mesh_blk_place USING btree (alt_id);
CREATE INDEX fk_mpr_epl ON crs_mesh_blk_place USING btree (epl_id);
CREATE INDEX fk_mpr_mbk ON crs_mesh_blk_place USING btree (mbk_id);
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
CREATE INDEX fk_nwp_dtm ON crs_network_plan USING btree (dtm_id);
CREATE UNIQUE INDEX idx_nwp_aud_id ON crs_network_plan USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_node
-------------------------------------------------------------------------------
CREATE INDEX fk_nod_alt ON crs_node USING btree (alt_id);
CREATE INDEX fk_nod_cos ON crs_node USING btree (cos_id_official);
CREATE INDEX fk_nod_ogo ON crs_node USING btree (order_group_off);
CREATE INDEX fk_nod_sit ON crs_node USING btree (sit_id);
CREATE INDEX fk_nod_wrk ON crs_node USING btree (wrk_id_created);
CREATE UNIQUE INDEX idx_nod_aud_id ON crs_node USING btree (audit_id);
CREATE INDEX shx_nod_shape ON crs_node USING gist (shape);

-------------------------------------------------------------------------------
-- crs_node_prp_order
-------------------------------------------------------------------------------
CREATE INDEX fk_nwp_nod ON crs_node_prp_order USING btree (nod_id);
CREATE INDEX fk_npo_dtm ON crs_node_prp_order USING btree (dtm_id);
CREATE INDEX fk_nwp_cor ON crs_node_prp_order USING btree (cor_id);
CREATE UNIQUE INDEX idx_npo_aud_id ON crs_node_prp_order USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_node_works
-------------------------------------------------------------------------------
CREATE INDEX fk_now_nod ON crs_node_works USING btree (nod_id);
CREATE INDEX fk_now_wrk ON crs_node_works USING btree (wrk_id);
CREATE UNIQUE INDEX idx_now_aud_id ON crs_node_works USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_nominal_index
-------------------------------------------------------------------------------
CREATE INDEX idx_nmi_corp_name ON crs_nominal_index USING btree (corporate_name);
CREATE INDEX idx_nmi_other_names ON crs_nominal_index USING btree (other_names);
CREATE INDEX idx_nmi_surname ON crs_nominal_index USING btree (surname, other_names);
CREATE INDEX fk_nmi_prp ON crs_nominal_index USING btree (prp_id);
CREATE INDEX fk_prh_ttl ON crs_nominal_index USING btree (ttl_title_no);

-------------------------------------------------------------------------------
-- crs_obs_accuracy
-------------------------------------------------------------------------------
CREATE INDEX fk_oba_obn2 ON crs_obs_accuracy USING btree (obn_id1);
CREATE INDEX fk_oba_obn1 ON crs_obs_accuracy USING btree (obn_id2);
CREATE UNIQUE INDEX idx_oba_aud_id ON crs_obs_accuracy USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_obs_elem_type
-------------------------------------------------------------------------------
CREATE INDEX fk_uom_oet ON crs_obs_elem_type USING btree (uom_code);
CREATE UNIQUE INDEX idx_oet_aud_id ON crs_obs_elem_type USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_obs_set
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_obs_aud_id ON crs_obs_set USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_obs_type
-------------------------------------------------------------------------------
CREATE INDEX fk_obt_oet1 ON crs_obs_type USING btree (oet_type_1);
CREATE INDEX fk_obt_oet2 ON crs_obs_type USING btree (oet_type_2);
CREATE INDEX fk_obt_oet3 ON crs_obs_type USING btree (oet_type_3);
CREATE UNIQUE INDEX idx_obt_aud_id ON crs_obs_type USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_observation
-------------------------------------------------------------------------------
CREATE INDEX fk_obn_cos ON crs_observation USING btree (cos_id);
CREATE INDEX fk_obn_obn ON crs_observation USING btree (obn_id_amendment);
CREATE INDEX fk_obn_obt ON crs_observation USING btree (obt_type, obt_sub_type);
CREATE INDEX fk_obn_rdn ON crs_observation USING btree (rdn_id);
CREATE INDEX fk_obn_stp1 ON crs_observation USING btree (stp_id_local);
CREATE INDEX fk_obn_stp2 ON crs_observation USING btree (stp_id_remote);
CREATE INDEX fk_obn_vct ON crs_observation USING btree (vct_id);
CREATE INDEX fk_obs_obn ON crs_observation USING btree (obs_id);
CREATE UNIQUE INDEX idx_obn_aud_id ON crs_observation USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_off_cord_sys
-------------------------------------------------------------------------------
CREATE INDEX fk_ocs_cos ON crs_off_cord_sys USING btree (cos_id);
CREATE UNIQUE INDEX idx_ocs_aud_id ON crs_off_cord_sys USING btree (audit_id);
CREATE INDEX shx_ocs_shape ON crs_off_cord_sys USING gist (shape);

-------------------------------------------------------------------------------
-- crs_office
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_ofc_aud_id ON crs_office USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_ordinate_adj
-------------------------------------------------------------------------------
CREATE INDEX fk_orj_adj ON crs_ordinate_adj USING btree (adj_id);
CREATE INDEX fk_orj_coo_output ON crs_ordinate_adj USING btree (coo_id_output);
CREATE INDEX fk_orj_coo_source ON crs_ordinate_adj USING btree (coo_id_source);
CREATE INDEX fk_orj_cor ON crs_ordinate_adj USING btree (cor_id_prop);
CREATE INDEX idx_orj_adj_coo ON crs_ordinate_adj USING btree (adj_id, coo_id_output);
CREATE UNIQUE INDEX idx_orj_aud_id ON crs_ordinate_adj USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_ordinate_type
-------------------------------------------------------------------------------
CREATE INDEX fk_ort_uom ON crs_ordinate_type USING btree (uom_code);
CREATE UNIQUE INDEX idx_ort_aud_id ON crs_ordinate_type USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_parcel
-------------------------------------------------------------------------------
CREATE INDEX fk_par_alt ON crs_parcel USING btree (alt_id);
CREATE INDEX fk_par_fen ON crs_parcel USING btree (fen_id);
CREATE INDEX fk_par_img ON crs_parcel USING btree (img_id);
CREATE INDEX fk_par_ldt ON crs_parcel USING btree (ldt_loc_id);
CREATE INDEX fk_par_toc ON crs_parcel USING btree (toc_code);
CREATE UNIQUE INDEX idx_par_aud_id ON crs_parcel USING btree (audit_id);
CREATE INDEX shx_par_shape ON crs_parcel USING gist (shape);

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
CREATE INDEX fk_lb1_par ON crs_parcel_label USING btree (par_id);
CREATE UNIQUE INDEX idx_plb_aud_id ON crs_parcel_label USING btree (audit_id);
CREATE INDEX shx_plb_shape ON crs_parcel_label USING gist (shape);

-------------------------------------------------------------------------------
-- crs_parcel_ring
-------------------------------------------------------------------------------
CREATE INDEX fk_pri_par ON crs_parcel_ring USING btree (par_id);
CREATE INDEX fk_pri_pri ON crs_parcel_ring USING btree (pri_id_parent_ring);
CREATE UNIQUE INDEX idx_pri_aud_id ON crs_parcel_ring USING btree (audit_id);

/*
--------------------------------------------------------------------------------
-- crs_process
--------------------------------------------------------------------------------
CREATE INDEX fk_pro_job ON crs_process USING btree (job_id);

*/

-------------------------------------------------------------------------------
-- crs_programme
-------------------------------------------------------------------------------
CREATE INDEX fk_pgm_usr ON crs_programme USING btree (usr_id);
CREATE INDEX fk_pgm_nwp ON crs_programme USING btree (nwp_id);
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
CREATE INDEX fk_rdn_rdm ON crs_reduct_run USING btree (rdm_id);
CREATE INDEX fk_rdn_usr ON crs_reduct_run USING btree (usr_id_exec);
CREATE UNIQUE INDEX idx_rdn_aud_id ON crs_reduct_run USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_ref_survey
-------------------------------------------------------------------------------
CREATE INDEX fk_rsu_sur_frm ON crs_ref_survey USING btree (sur_wrk_id_new);
CREATE INDEX fk_rsu_sur_to ON crs_ref_survey USING btree (sur_wrk_id_exist);
CREATE UNIQUE INDEX idx_rsu_aud_id ON crs_ref_survey USING btree (audit_id);

/*

--------------------------------------------------------------------------------
-- BDE table crs_req_det
--------------------------------------------------------------------------------

CREATE INDEX fk_rqd_rqh ON crs_req_det USING btree (rqh_id);
CREATE INDEX fk_rqd_rqi ON crs_req_det USING btree (rqi_code);
CREATE INDEX fk_rqd_tin ON crs_req_det USING btree (tin_id);

--------------------------------------------------------------------------------
-- BDE table crs_req_hdr
--------------------------------------------------------------------------------

CREATE INDEX fk_rqh_dlg ON crs_req_hdr USING btree (dlg_id);
CREATE INDEX fk_rqh_sud ON crs_req_hdr USING btree (sud_id);
CREATE INDEX fk_rqh_usr ON crs_req_hdr USING btree (usr_id);
CREATE INDEX fk_rqh_wrk ON crs_req_hdr USING btree (wrk_id);
CREATE UNIQUE INDEX fk_rqh_aud ON crs_req_hdr USING btree (audit_id);

--------------------------------------------------------------------------------
-- crs_req_item
--------------------------------------------------------------------------------
CREATE UNIQUE INDEX fk_rqi_aud ON crs_req_item USING btree (audit_id);

*/

-------------------------------------------------------------------------------
-- crs_road_ctr_line
-------------------------------------------------------------------------------
CREATE INDEX fk_rcl_alt ON crs_road_ctr_line USING btree (alt_id);
CREATE UNIQUE INDEX idx_rcl_aud_id ON crs_road_ctr_line USING btree (audit_id);
CREATE INDEX shx_rcl_shape ON crs_road_ctr_line USING gist (shape);

-------------------------------------------------------------------------------
-- crs_road_name
-------------------------------------------------------------------------------
CREATE INDEX fk_rna_alt ON crs_road_name USING btree (alt_id);
CREATE INDEX idx_ix_rna_name ON crs_road_name USING btree (name);
CREATE UNIQUE INDEX idx_rna_aud_id ON crs_road_name USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_road_name_asc
-------------------------------------------------------------------------------
CREATE INDEX fk_rns_alt ON crs_road_name_asc USING btree (alt_id);
CREATE INDEX fk_rns_rcl ON crs_road_name_asc USING btree (rcl_id);
CREATE INDEX fk_rns_rna ON crs_road_name_asc USING btree (rna_id);
CREATE UNIQUE INDEX idx_rns_aud_id ON crs_road_name_asc USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_setup
-------------------------------------------------------------------------------
CREATE INDEX fk_stp_nod ON crs_setup USING btree (nod_id);
CREATE INDEX fk_stp_wrk ON crs_setup USING btree (wrk_id);
CREATE UNIQUE INDEX idx_stp_aud_id ON crs_setup USING btree (audit_id);
CREATE INDEX idx_stp_equip_type ON crs_setup USING btree (equipment_type);

-------------------------------------------------------------------------------
-- crs_site
-------------------------------------------------------------------------------
CREATE INDEX idx_sit_wrk_id_created ON crs_site USING btree (wrk_id_created);
CREATE UNIQUE INDEX idx_sit_aud_id ON crs_site USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_site_locality
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_slo_aud_id ON crs_site_locality USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_stat_act_parcl
-------------------------------------------------------------------------------
CREATE INDEX fk_sap_par ON crs_stat_act_parcl USING btree (par_id);
CREATE INDEX fk_sap_sta ON crs_stat_act_parcl USING btree (sta_id);
CREATE UNIQUE INDEX fk_sap_aud ON crs_stat_act_parcl USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_stat_version
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_sav_aud_id ON crs_stat_version USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_statist_area
-------------------------------------------------------------------------------
CREATE INDEX fk_saa_alt ON crs_statist_area USING btree (alt_id);
CREATE INDEX fk_stt_sav ON crs_statist_area USING btree (sav_version, sav_area_class);
CREATE INDEX fk_stt_usr ON crs_statist_area USING btree (usr_id_firm_ta);
CREATE UNIQUE INDEX idx_stt_aud_id ON crs_statist_area USING btree (audit_id);
CREATE INDEX shx_stt_shape ON crs_statist_area USING gist (shape);

-------------------------------------------------------------------------------
-- crs_statute
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_ste_ak1 ON crs_statute USING btree (section, name_and_date);
CREATE UNIQUE INDEX idx_ste_aud_id ON crs_statute USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_statute_action
-------------------------------------------------------------------------------
CREATE INDEX fk_sta_ste ON crs_statute_action USING btree (ste_id);
CREATE INDEX fk_sta_sur ON crs_statute_action USING btree (sur_wrk_id_vesting);
CREATE UNIQUE INDEX idx_sta_aud_id ON crs_statute_action USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_street_address
-------------------------------------------------------------------------------
CREATE INDEX fk_sad_alt ON crs_street_address USING btree (alt_id);
CREATE INDEX fk_sad_rcl ON crs_street_address USING btree (rcl_id);
CREATE INDEX fk_sad_rna ON crs_street_address USING btree (rna_id);
CREATE UNIQUE INDEX idx_sad_aud_id ON crs_street_address USING btree (audit_id);
CREATE INDEX shx_sad_shape ON crs_street_address USING gist (shape);

-------------------------------------------------------------------------------
-- crs_sur_admin_area
-------------------------------------------------------------------------------
CREATE INDEX fk_saa_stt ON crs_sur_admin_area USING btree (stt_id);
CREATE INDEX fk_saa_sur ON crs_sur_admin_area USING btree (sur_wrk_id);
CREATE INDEX fk_saa_xstt ON crs_sur_admin_area USING btree (eed_req_id, xstt_id);
CREATE UNIQUE INDEX idx_saa_aud_id ON crs_sur_admin_area USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_sur_plan_ref
-------------------------------------------------------------------------------
CREATE INDEX fk_wrk_id ON crs_sur_plan_ref USING btree (wrk_id);
CREATE INDEX shx_spf_shape ON crs_sur_plan_ref USING gist (shape);

-------------------------------------------------------------------------------
-- crs_survey
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_sur_idx ON crs_survey USING btree (dataset_id, dataset_series, ldt_loc_id, dataset_suffix);
CREATE INDEX fk_sur_fhr ON crs_survey USING btree (fhr_id);
CREATE INDEX fk_sur_ldt ON crs_survey USING btree (ldt_loc_id);
CREATE INDEX fk_sur_pnx ON crs_survey USING btree (pnx_id_submitted);
CREATE INDEX fk_sur_sig ON crs_survey USING btree (sig_id);
CREATE INDEX fk_sur_usr_firm_sol ON crs_survey USING btree (usr_id_sol_firm);
CREATE INDEX fk_sur_usr_sol ON crs_survey USING btree (usr_id_sol);
CREATE UNIQUE INDEX idx_sur_aud_id ON crs_survey USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_survey_image
-------------------------------------------------------------------------------
CREATE INDEX fk_sim_img ON crs_survey_image USING btree (img_id);
CREATE INDEX fk_sim_sur ON crs_survey_image USING btree (sur_wrk_id);
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

/*
--------------------------------------------------------------------------------
-- crs_task_list
--------------------------------------------------------------------------------
CREATE INDEX fk_tkl_trt ON crs_task_list USING btree (trt_grp, trt_type);
CREATE INDEX fk_tkl_tsk ON crs_task_list USING btree (tsk_id);
CREATE UNIQUE INDEX idx_tkl_audit_id ON crs_task_list USING btree (audit_id);

*/

-------------------------------------------------------------------------------
-- crs_title
-------------------------------------------------------------------------------
CREATE INDEX fk_ttl_alt ON crs_title USING btree (alt_id);
CREATE INDEX fk_ttl_dlg ON crs_title USING btree (dlg_id);
CREATE INDEX fk_ttl_ldt ON crs_title USING btree (ldt_loc_id);
CREATE INDEX fk_ttl_phy ON crs_title USING btree (phy_prod_no);
CREATE INDEX fk_ttl_ste ON crs_title USING btree (ste_id);
CREATE INDEX fk_ttl_sur ON crs_title USING btree (sur_wrk_id);
CREATE INDEX fk_ttl_ttl ON crs_title USING btree (ttl_title_no_srs);
CREATE INDEX fk_ttl_wrk ON crs_title USING btree (sur_wrk_id_preallc);
CREATE INDEX fk_ttl_psd ON crs_title USING btree (protect_start);
CREATE INDEX fk_ttl_ped ON crs_title USING btree (protect_end);
CREATE UNIQUE INDEX idx_ttl_aud_id ON crs_title USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_title_action
-------------------------------------------------------------------------------
CREATE INDEX fk_tta_ttl ON crs_title_action USING btree (ttl_title_no);
CREATE INDEX fk_tta_act ON crs_title_action USING btree (act_tin_id, act_id);
CREATE UNIQUE INDEX idx_tta_aud_id ON crs_title_action USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_title_estate
-------------------------------------------------------------------------------
CREATE INDEX fk_ett_act_crt ON crs_title_estate USING btree (act_tin_id_crt);
CREATE INDEX fk_ett_lgd ON crs_title_estate USING btree (lgd_id);
CREATE INDEX fk_ttl_ett ON crs_title_estate USING btree (ttl_title_no);

-------------------------------------------------------------------------------
-- crs_title_mem_text
-------------------------------------------------------------------------------
CREATE INDEX fk_tmt_ttm ON crs_title_mem_text USING btree (ttm_id);
CREATE UNIQUE INDEX idx_tmt_aud_id ON crs_title_mem_text USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_title_memorial
-------------------------------------------------------------------------------
CREATE INDEX fk_ttl_ttm ON crs_title_memorial USING btree (ttl_title_no);
CREATE INDEX fk_ttm_mmt ON crs_title_memorial USING btree (mmt_code);
CREATE INDEX fk_ttm_act_crt ON crs_title_memorial USING btree (act_tin_id_crt, act_id_crt);
CREATE INDEX fk_ttm_act_orig ON crs_title_memorial USING btree (act_tin_id_orig, act_id_orig);
CREATE INDEX fk_ttm_act_ext ON crs_title_memorial USING btree (act_tin_id_ext, act_id_ext);

-------------------------------------------------------------------------------
-- crs_topology_class
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_top_aud_id ON crs_topology_class USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_transact_type
-------------------------------------------------------------------------------
CREATE INDEX fk_trt_sob ON crs_transact_type USING btree (sob_name);
CREATE INDEX fk_trt_trt_dischar ON crs_transact_type USING btree (trt_grp_discrg, trt_type_discrg);
CREATE UNIQUE INDEX idx_crs_tran_desc ON crs_transact_type USING btree (grp, description, "type");
CREATE UNIQUE INDEX idx_trt_aud_id ON crs_transact_type USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_ttl_enc
-------------------------------------------------------------------------------
CREATE INDEX fk_tte_enc ON crs_ttl_enc USING btree (enc_id);
CREATE INDEX fk_tte_ttl ON crs_ttl_enc USING btree (ttl_title_no);
CREATE INDEX idx_tin_usr ON crs_ttl_enc USING btree (act_tin_id_crt);

-------------------------------------------------------------------------------
-- crs_ttl_hierarchy
-------------------------------------------------------------------------------
CREATE INDEX fk_tlh_tdr ON crs_ttl_hierarchy USING btree (tdr_id);
CREATE INDEX fk_tlh_ttl_flw ON crs_ttl_hierarchy USING btree (ttl_title_no_flw);
CREATE INDEX fk_tlh_ttl_prior ON crs_ttl_hierarchy USING btree (ttl_title_no_prior);
CREATE INDEX idx_act_tin_id_crt ON crs_ttl_hierarchy USING btree (act_tin_id_crt);

-------------------------------------------------------------------------------
-- crs_ttl_inst
-------------------------------------------------------------------------------
CREATE INDEX idx_tin_inst_no ON crs_ttl_inst USING btree (inst_no);
CREATE INDEX fk_tin_dlg ON crs_ttl_inst USING btree (dlg_id);
CREATE INDEX fk_tin_img ON crs_ttl_inst USING btree (img_id);
CREATE INDEX fk_tin_ldt ON crs_ttl_inst USING btree (ldt_loc_id);
CREATE INDEX fk_tin_pro ON crs_ttl_inst USING btree (pro_id);
CREATE INDEX fk_tin_tin ON crs_ttl_inst USING btree (tin_id_parent);
CREATE INDEX fk_tin_trt ON crs_ttl_inst USING btree (trt_grp, trt_type);
CREATE INDEX fk_tin_usr ON crs_ttl_inst USING btree (usr_id_approve);
CREATE UNIQUE INDEX idx_tin_aud_id ON crs_ttl_inst USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_ttl_inst_title
-------------------------------------------------------------------------------
CREATE INDEX fk_tnt_tin ON crs_ttl_inst_title USING btree (tin_id);
CREATE INDEX fk_tnt_ttl ON crs_ttl_inst_title USING btree (ttl_title_no);
CREATE UNIQUE INDEX idx_tnt_aud_id ON crs_ttl_inst_title USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_unit_of_meas
-------------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_uom_aud_id ON crs_unit_of_meas USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_user
-------------------------------------------------------------------------------
CREATE INDEX fk_usr_off ON crs_user USING btree (off_code);
CREATE INDEX fk_usr_usr ON crs_user USING btree (usr_id_coordinator);
CREATE INDEX fk_usr_usr_parent ON crs_user USING btree (usr_id_parent);
CREATE UNIQUE INDEX idx_usr_aud_id ON crs_user USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_vector
-------------------------------------------------------------------------------
CREATE INDEX fk_vct_nod_end ON crs_vector USING btree (nod_id_end);
CREATE INDEX fk_vct_nod_start ON crs_vector USING btree (nod_id_start);
CREATE UNIQUE INDEX idx_vct_ak1 ON crs_vector USING btree ("type", nod_id_start, nod_id_end);
CREATE UNIQUE INDEX idx_vct_aud_id ON crs_vector USING btree (audit_id);
CREATE INDEX shx_vct_shape ON crs_vector USING gist (shape);

-------------------------------------------------------------------------------
-- crs_vertx_sequence
-------------------------------------------------------------------------------
CREATE INDEX fk_vts_lin_id ON crs_vertx_sequence USING btree (lin_id);
CREATE UNIQUE INDEX idx_vts_aud_id ON crs_vertx_sequence USING btree (audit_id);

-------------------------------------------------------------------------------
-- crs_work
-------------------------------------------------------------------------------
CREATE INDEX fk_wrk_alt ON crs_work USING btree (alt_id);
CREATE INDEX fk_wrk_auth_date ON crs_work USING btree (authorised_date);
CREATE INDEX fk_wrk_cel ON crs_work USING btree (cel_id);
CREATE INDEX fk_wrk_con ON crs_work USING btree (con_id);
CREATE INDEX fk_wrk_cos ON crs_work USING btree (cos_id);
CREATE INDEX fk_wrk_lodged_date ON crs_work USING btree (lodged_date);
CREATE INDEX fk_wrk_pro ON crs_work USING btree (pro_id);
CREATE INDEX fk_wrk_trt ON crs_work USING btree (trt_grp, trt_type);
CREATE INDEX fk_wrk_usr ON crs_work USING btree (usr_id_firm);
CREATE INDEX fk_wrk_usr_auth ON crs_work USING btree (usr_id_authorised);
CREATE INDEX fk_wrk_usr_firm_prin ON crs_work USING btree (usr_id_prin_firm);
CREATE INDEX fk_wrk_usr_prpd ON crs_work USING btree (usr_id_principal);
CREATE INDEX fk_wrk_usr_val ON crs_work USING btree (usr_id_validated);
CREATE INDEX fk_wrk_val_date ON crs_work USING btree (validated_date);
CREATE UNIQUE INDEX idx_wrk_aud_id ON crs_work USING btree (audit_id);

COMMIT;
