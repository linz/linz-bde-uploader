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
-- Creates functions to maintain LDS Full Landonline layers
--------------------------------------------------------------------------------
--

SET client_min_messages TO WARNING;
BEGIN;

SET SEARCH_PATH = bde_ext, lds, bde, bde_control, public;

DO $$
DECLARE
   v_pcid    TEXT;
   v_schema  TEXT = 'bde_ext';
BEGIN
    FOR v_pcid IN 
        SELECT v_schema || '.' || proname || '(' || pg_get_function_identity_arguments(oid) || ')'
        FROM pg_proc 
        WHERE pronamespace=(SELECT oid FROM pg_namespace WHERE nspname = v_schema)
    LOOP
        EXECUTE 'DROP FUNCTION ' || v_pcid;
    END LOOP;
END;
$$;


-- ########################################################################################################

-- ***NOTE*** This function needs to be called in the linz_bde_upload perl script i.e.
-- """dataset_load_end_sql <<EOT
-- SELECT lds.LDS_MaintainAllFBDELayers({{id}});
-- SELECT bde_CompleteDatasetRevision({{id}});
-- EOT"""

CREATE OR REPLACE FUNCTION LDS_MaintainAllFBDELayers(
    p_upload_id INTEGER
)
RETURNS
    INTEGER AS $$
DECLARE
    v_dataset        TEXT;
BEGIN
    v_dataset := COALESCE(
        bde_control.bde_GetOption(p_upload_id, '_dataset'),
        '(undefined dataset)'
    );
    
    RAISE INFO 'Maintaining FBDE layers for dataset %', v_dataset;
    
    PERFORM LDS_MaintainFBDELayers(p_upload_id);
    
    RAISE INFO 'Finished maintaining FBDE layers %', v_dataset;
    
    RETURN 1;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER;

ALTER FUNCTION LDS_MaintainAllFBDELayers(integer) SET search_path=bde_ext, lds, bde, bde_control, public;
ALTER FUNCTION LDS_MaintainAllFBDELayers(integer) OWNER TO bde_dba;


CREATE OR REPLACE FUNCTION LDS_MaintainFBDELayers(
    p_upload INTEGER
)
RETURNS
    INTEGER AS $$
DECLARE
    v_message          TEXT;
    v_bad_code_count   BIGINT;
    v_bad_code_string  TEXT;
    v_table            REGCLASS;
    v_data_diff_sql    TEXT;
    v_data_insert_sql  TEXT;
BEGIN
    RAISE INFO 'Starting maintenance on titles FBDE layers';
    
    IF (
        NOT (
            SELECT bde_control.bde_TablesAffected(
                p_upload,
                ARRAY[
                    'crs_alias',
                    'crs_adjustment_run',
                    'crs_proprietor',
                    'crs_encumbrancee',
                    'crs_encumbrance',
                    'crs_nominal_index',
                    'crs_action',
                    'crs_enc_share',
                    'crs_estate_share',
                    'crs_legal_desc',
                    'crs_line',
                    'crs_maintenance',
                    'crs_mark',
                    'crs_mark_name',
                    'crs_mrk_phys_state',
                    'crs_node',
                    'crs_node_prp_order',
                    'crs_parcel',
                    'crs_parcel_dimen',
                    'crs_parcel_label',
                    'crs_parcel_ring',
                    'crs_parcel_bndry',
                    'crs_stat_version',
                    'crs_statist_area',
                    'crs_survey',
                    'crs_title',
                    'crs_title_action',
                    'crs_title_doc_ref',
                    'crs_title_estate',
                    'crs_title_memorial',
                    'crs_title_mem_text',
                    'crs_transact_type',
                    'crs_ttl_enc',
                    'crs_ttl_hierarchy',
                    'crs_ttl_inst',
                    'crs_ttl_inst_title',
                    'crs_user',
                    'crs_vector',
                    'crs_work',
                    'crs_street_address',
                    'crs_feature_name',
                    'crs_coordinate',
                    'cbe_title_parcel_association'
                ],
                'any affected'
            )
        )                    
        AND LDS.LDS_TableHasData('bde', 'crs_alias')
        AND LDS.LDS_TableHasData('bde', 'crs_adjustment_run')
        AND LDS.LDS_TableHasData('bde', 'crs_proprietor')
        AND LDS.LDS_TableHasData('bde', 'crs_encumbrancee')
        AND LDS.LDS_TableHasData('bde', 'crs_encumbrance')
        AND LDS.LDS_TableHasData('bde', 'crs_nominal_index')
        AND LDS.LDS_TableHasData('bde', 'crs_action')
        AND LDS.LDS_TableHasData('bde', 'crs_enc_share')
        AND LDS.LDS_TableHasData('bde', 'crs_estate_share')
        AND LDS.LDS_TableHasData('bde', 'crs_legal_desc')
        AND LDS.LDS_TableHasData('bde', 'crs_line')
        AND LDS.LDS_TableHasData('bde', 'crs_maintenance')
        AND LDS.LDS_TableHasData('bde', 'crs_mark')
        AND LDS.LDS_TableHasData('bde', 'crs_mark_name')
        AND LDS.LDS_TableHasData('bde', 'crs_mrk_phys_state')
        AND LDS.LDS_TableHasData('bde', 'crs_node')
        AND LDS.LDS_TableHasData('bde', 'crs_node_prp_order')
        AND LDS.LDS_TableHasData('bde', 'crs_parcel')
        AND LDS.LDS_TableHasData('bde', 'crs_parcel_dimen')
        AND LDS.LDS_TableHasData('bde', 'crs_parcel_label')
        AND LDS.LDS_TableHasData('bde', 'crs_parcel_ring')
        AND LDS.LDS_TableHasData('bde', 'crs_parcel_bndry')
        AND LDS.LDS_TableHasData('bde', 'crs_stat_version')
        AND LDS.LDS_TableHasData('bde', 'crs_statist_area')
        AND LDS.LDS_TableHasData('bde', 'crs_survey')
        AND LDS.LDS_TableHasData('bde', 'crs_title')
        AND LDS.LDS_TableHasData('bde', 'crs_title_action')
        AND LDS.LDS_TableHasData('bde', 'crs_title_doc_ref')
        AND LDS.LDS_TableHasData('bde', 'crs_title_estate')
        AND LDS.LDS_TableHasData('bde', 'crs_title_memorial')
        AND LDS.LDS_TableHasData('bde', 'crs_title_mem_text')
        AND LDS.LDS_TableHasData('bde', 'crs_transact_type')
        AND LDS.LDS_TableHasData('bde', 'crs_ttl_enc')
        AND LDS.LDS_TableHasData('bde', 'crs_ttl_hierarchy')
        AND LDS.LDS_TableHasData('bde', 'crs_ttl_inst')
        AND LDS.LDS_TableHasData('bde', 'crs_ttl_inst_title')
        AND LDS.LDS_TableHasData('bde', 'crs_user')
        AND LDS.LDS_TableHasData('bde', 'crs_vector')
        AND LDS.LDS_TableHasData('bde', 'crs_work')
        AND LDS.LDS_TableHasData('bde', 'crs_street_address')
        AND LDS.LDS_TableHasData('bde', 'crs_feature_name')
        AND LDS.LDS_TableHasData('bde', 'crs_coordinate')
        AND LDS.LDS_TableHasData('bde', 'cbe_title_parcel_association')
    )
    THEN
        RAISE INFO
            'Maintain FBDE layers has been skipped as no relating tables were affected by the upload';
        RETURN 1;
    END IF;
    
    ----------------------------------------------------------------------------
    -- temporary titles tables
    ----------------------------------------------------------------------------
    
    PERFORM LDS_CreateTitleExclusionTables(p_upload);
    
    ----------------------------------------------------------------------------
    -- alias layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'alias');

    RAISE DEBUG 'Started creating temp tables for %', v_table;    
    
    CREATE TEMPORARY TABLE dvl_prp 
    (title_no VARCHAR, prp_id INTEGER)
    ON COMMIT DROP;
    
    INSERT INTO dvl_prp
    SELECT
        DVL.title_no AS title_no,
        PRP.id AS prp_id
    FROM
        tmp_protected_titles DVL
    JOIN crs_title_estate ETT ON DVL.title_no = ETT.ttl_title_no
    JOIN crs_estate_share ETS ON ETT.id = ETS.ett_id
    JOIN crs_proprietor PRP ON ETS.id = PRP.ets_id;
    
    ANALYSE dvl_prp;

    CREATE TEMPORARY TABLE exclude_prp 
    (prp_id INTEGER)
    ON COMMIT DROP;

    INSERT INTO exclude_prp
    SELECT 
        PRP.id as prp_id
    FROM
        tmp_excluded_titles EXL
    JOIN crs_title_estate ETT ON EXL.title_no = ETT.ttl_title_no
    JOIN crs_estate_share ETS ON ETT.id = ETS.ett_id
    JOIN crs_proprietor PRP ON ETS.id = PRP.ets_id;
    
    ANALYSE exclude_prp;

    RAISE DEBUG 'Finished creating temp tables for %', v_table;

    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id, 
            prp_id, 
            surname, 
            other_names
        )
        SELECT
            ALS.id,
            ALS.prp_id,
            -- Mask protected titles
            CASE WHEN D2P.title_no IS NOT NULL THEN lds.LDS_GetProtectedText(D2P.title_no) ELSE ALS.surname END AS surname,
            CASE WHEN D2P.title_no IS NOT NULL THEN NULL ELSE ALS.other_names END AS other_names 
        FROM crs_alias ALS
        JOIN crs_proprietor PRP ON ALS.prp_id = PRP.id
        LEFT JOIN DVL_PRP D2P ON PRP.id = D2P.prp_id 
        WHERE PRP.status <> 'LDGE'
        -- Completely exclude training titles and pending titles
        AND PRP.id NOT IN (SELECT prp_id FROM exclude_prp)
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- adjustment run
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'adjustment_run');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            adm_id,
            cos_id,
            status,  
            usr_id_exec,
            adjust_datetime,
            description,
            sum_sqrd_residuals,
            redundancy,
            wrk_id,
            audit_id
        )
        SELECT 
            ADJ.id,
            ADJ.adm_id,
            ADJ.cos_id,
            ADJ.status,
            ADJ.usr_id_exec,
            ADJ.adjust_datetime,
            ADJ.description,
            ADJ.sum_sqrd_residuals,
            ADJ.redundancy,
            ADJ.wrk_id,
            ADJ.audit_id
        FROM
            crs_adjustment_run ADJ
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- proprietor layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'proprietor');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
                id, 
                ets_id, 
                status, 
                type,
                prime_surname,
                prime_other_names,
                corporate_name,
                name_suffix,
                original_flag
        )
        SELECT prp.id, prp.ets_id, prp.status, prp.type, 
                CASE
                    WHEN d2p.title_no IS NOT NULL THEN NULL::VARCHAR
                    ELSE prp.prime_surname
                END AS prime_surname, 
                CASE
                    WHEN d2p.title_no IS NOT NULL THEN NULL::VARCHAR
                    ELSE prp.prime_other_names
                END AS prime_other_names, 
                CASE
                    WHEN d2p.title_no IS NOT NULL THEN lds.LDS_GetProtectedText(d2p.title_no)
                    ELSE prp.corporate_name
                END AS corporate_name, prp.name_suffix, prp.original_flag
        FROM crs_proprietor prp
        LEFT JOIN dvl_prp d2p ON prp.id = d2p.prp_id
        WHERE prp.status <> 'LDGE' 
        AND prp.id NOT IN ( SELECT exclude_prp.prp_id FROM exclude_prp)
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- encumbrancee layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'encumbrancee');
    
    v_data_insert_sql := $sql$
    INSERT INTO %1% ( 
        id, 
        ens_id, 
        status, 
        name
    )
    WITH 
    TTE(ttl_title_no,enc_id)
    AS (
        SELECT DISTINCT
        ttl_title_no,enc_id 
        FROM crs_ttl_enc TE
        JOIN crs_title T ON TE.ttl_title_no = T.title_no
        WHERE T.status <> 'PEND'
    ),
    TRN_ENC(id) 
    AS (
        SELECT DISTINCT
        ENE.id AS id
        FROM tmp_training_titles TRN
        JOIN TTE ON TRN.title_no = TTE.ttl_title_no
        JOIN crs_enc_share ENS ON ENS.enc_id = TTE.enc_id
        JOIN crs_encumbrancee ENE ON ENE.ens_id = ENS.id
    ),
    ENE(id,ens_id,status,name)
    AS (
        SELECT
        id,ens_id,status,name 
        FROM crs_encumbrancee 
        WHERE status <> 'LDGE'
        AND id NOT IN (SELECT id FROM TRN_ENC)
    )
    SELECT DISTINCT            
        ENE.id, 
        ENE.ens_id, 
        ENE.status,  
        ENE.name
    FROM ENE
    JOIN crs_enc_share ENS ON ENS.id = ENE.ens_id
    JOIN TTE ON TTE.enc_id = ENS.enc_id
    ORDER BY id;
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- encumbrance layer
    ----------------------------------------------------------------------------


    v_table := LDS.LDS_GetTable('bde_ext', 'encumbrance');
    
    v_data_insert_sql := $sql$
    INSERT INTO %1% ( 
        id, 
        status, 
        act_tin_id_orig,
        act_tin_id_crt,
        act_id_crt,
        act_id_orig,
        term
    )
    WITH TRN(enc_id)
    AS (
        SELECT TTE.enc_id 
        FROM crs_ttl_enc TTE
        JOIN tmp_training_titles TRN 
        ON TRN.title_no = TTE.ttl_title_no
    )
    SELECT            
        id,
        status, 
        act_tin_id_orig,
        act_tin_id_crt,
        act_id_crt,
        act_id_orig,
        term
    FROM crs_encumbrance
    WHERE id NOT IN (SELECT enc_id FROM TRN)
    AND status <> 'LDGE'
    ORDER BY id;
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- nominal_index layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'nominal_index');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            ttl_title_no, 
            prp_id, 
            id, 
            status, 
            name_type, 
            surname,
            other_names,
            corporate_name
        )
        SELECT 
            NMI.ttl_title_no, 
            NMI.prp_id, 
            NMI.id, 
            NMI.status, 
            NMI.name_type, 
            CASE WHEN DVL.title_no IS NOT NULL THEN NULL ELSE NMI.surname END AS surname,
            CASE WHEN DVL.title_no IS NOT NULL THEN NULL ELSE NMI.other_names END AS other_names, 
            CASE WHEN DVL.title_no IS NOT NULL THEN lds.LDS_GetProtectedText(DVL.title_no) ELSE NMI.corporate_name END as corporate_name
        FROM crs_nominal_index NMI
        LEFT JOIN tmp_protected_titles DVL 
        ON NMI.ttl_title_no = DVL.title_no 
        WHERE NMI.status <> 'LDGE'
        AND NMI.ttl_title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
        ORDER BY id;
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- enc share layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'enc_share');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id, 
            enc_id, 
            status, 
            act_tin_id_crt,
            act_id_crt,
            act_id_ext,
            act_tin_id_ext,
            system_crt,
            system_ext
        )
        WITH TRN_ENS (id) 
        AS(
            SELECT
                ENS.id AS id
            FROM tmp_training_titles TRN
            JOIN crs_ttl_enc TTE ON TTE.ttl_title_no = TRN.title_no
            JOIN crs_encumbrance ENC ON TTE.enc_id = ENC.id
            JOIN crs_enc_share ENS ON ENS.enc_id = ENC.id
        )
        SELECT 
            ENS.id, 
            ENS.enc_id, 
            ENS.status, 
            ENS.act_tin_id_crt, 
            ENS.act_id_crt, 
            ENS.act_id_ext,
            ENS.act_tin_id_ext,
            ENS.system_crt,
            ENS.system_ext
        FROM crs_enc_share ENS
        WHERE ENS.status <> 'LDGE' 
        AND ENS.id NOT IN (SELECT id FROM TRN_ENS)
        ORDER BY id;
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    
    ----------------------------------------------------------------------------
    -- estate share layer
    ----------------------------------------------------------------------------
    
    v_table := LDS.LDS_GetTable('bde_ext', 'estate_share');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id, 
            ett_id, 
            status, 
            share, 
            act_tin_id_crt, 
            original_flag, 
            system_crt, 
            executorship, 
            act_id_crt, 
            share_memorial
        )
        WITH EXL_ETS (id)
        AS(
            SELECT ETS.id
            FROM tmp_excluded_titles EXL
            JOIN crs_title TTL ON TTL.title_no = EXL.title_no
            JOIN crs_title_estate ETT ON ETT.ttl_title_no = TTL.title_no
            JOIN crs_estate_share ETS ON ETS.ett_id = ett.id
        )
        SELECT 
            ETS.id, 
            ETS.ett_id, 
            ETS.status, 
            ETS.share, 
            ETS.act_tin_id_crt, 
            ETS.original_flag, 
            ETS.system_crt, 
            ETS.executorship, 
            ETS.act_id_crt, 
            ETS.share_memorial
        FROM crs_estate_share ETS
        WHERE ETS.status <> 'LDGE' 
        AND ETS.id NOT IN (SELECT id FROM EXL_ETS)
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    
    ----------------------------------------------------------------------------
    -- legal desc layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'legal_desc');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            id, 
            type, 
            status, 
            ttl_title_no, 
            audit_id, 
            total_area, 
            legal_desc_text
        )
        SELECT 
            LGD.id, 
            LGD.type, 
            LGD.status, 
            LGD.ttl_title_no, 
            LGD.audit_id,
            LGD.total_area,
            LGD.legal_desc_text
        FROM crs_legal_desc LGD
        WHERE LGD.status <> 'LDGE'
        AND (
            LGD.ttl_title_no IS NULL 
            OR LGD.ttl_title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
        )
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    
    ----------------------------------------------------------------------------
    -- line layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'line');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            id,  
            boundary, 
            type, 
            nod_id_end, 
            nod_id_start, 
            arc_radius, 
            arc_direction, 
            arc_length, 
            pnx_id_created, 
            dcdb_feature, 
            se_row_id, 
            audit_id, 
            description, 
            shape
        )
        WITH PAB_LIN (lin_id)
        AS(
            SELECT pab.lin_id
            FROM crs_parcel_bndry PAB
            JOIN crs_parcel_ring PRI ON PRI.id = PAB.pri_id
            JOIN crs_parcel PAR ON PAR.id = PRI.par_id
            WHERE PAR.status IN ('CURR','SHST')
        ),
        LIN_ORD (
            id,
            boundary,
            type,
            nod_id_end,
            nod_id_start,
            arc_radius,
            arc_direction,
            arc_length,
            pnx_id_created,
            dcdb_feature,
            se_row_id,
            audit_id,
            description,
            shape )
        AS(
            SELECT
                id,
                boundary,
                type,
                nod_id_end,
                nod_id_start,
                arc_radius,
                arc_direction,
                arc_length,
                pnx_id_created,
                dcdb_feature,
                se_row_id,
                audit_id,
                description,
                shape 
            FROM crs_line)
        SELECT 
            LIN_ORD.id, 
            LIN_ORD.boundary, 
            LIN_ORD.type, 
            LIN_ORD.nod_id_end, 
            LIN_ORD.nod_id_start, 
            LIN_ORD.arc_radius, 
            LIN_ORD.arc_direction, 
            LIN_ORD.arc_length, 
            LIN_ORD.pnx_id_created, 
            LIN_ORD.dcdb_feature, 
            LIN_ORD.se_row_id, 
            LIN_ORD.audit_id, 
            LIN_ORD.description, 
            LIN_ORD.shape
        FROM LIN_ORD
        WHERE LIN_ORD.id IN (SELECT lin_id FROM PAB_LIN
        ORDER BY id);
        
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    
    ----------------------------------------------------------------------------
    -- maintenance layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'maintenance');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            mrk_id, 
            type, 
            status, 
            complete_date, 
            audit_id, 
            "desc"
        )
        WITH MNT_MRK(id)
        AS (
            SELECT mrk.id
            FROM crs_mark MRK
            JOIN crs_maintenance MNT ON MRK.id = MNT.mrk_id 
            AND MRK.status <> 'PEND'
        )
        SELECT 
            MNT.mrk_id, 
            MNT.type, 
            MNT.status, 
            MNT.complete_date, 
            MNT.audit_id, 
            MNT."desc"
        FROM crs_maintenance MNT
        WHERE MNT.mrk_id in (SELECT id FROM MNT_MRK)
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    
    ----------------------------------------------------------------------------
    -- mark layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'mark');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            id, 
            nod_id, 
            status, 
            type, 
            category, 
            beacon_type, 
            protection_type, 
            maintenance_level, 
            mrk_id_dist, 
            disturbed, 
            disturbed_date, 
            mrk_id_repl, 
            replaced, 
            replaced_date, 
            mark_annotation, 
            wrk_id_created, 
            audit_id, 
            "desc"
        )
        SELECT 
            MRK.id, 
            MRK.nod_id, 
            MRK.status, 
            MRK.type, 
            MRK.category, 
            MRK.beacon_type, 
            MRK.protection_type, 
            MRK.maintenance_level, 
            MRK.mrk_id_dist, 
            MRK.disturbed, 
            MRK.disturbed_date, 
            MRK.mrk_id_repl, 
            MRK.replaced, 
            MRK.replaced_date, 
            MRK.mark_annotation, 
            MRK.wrk_id_created, 
            MRK.audit_id, 
            MRK."desc"
        FROM crs_mark MRK
        WHERE MRK.status <> 'PEND'
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    
    ----------------------------------------------------------------------------
    -- mark name layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'mark_name');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            mrk_id, 
            type, 
            name, 
            audit_id
        )
        SELECT 
            MKN.mrk_id, 
            MKN.type, 
            MKN.name, 
            MKN.audit_id
        FROM crs_mark_name MKN
        JOIN crs_mark MRK ON MKN.mrk_id = MRK.id
        WHERE MRK.status <> 'PEND'
        ORDER BY audit_id;;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );


    ----------------------------------------------------------------------------
    -- mark phys state layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'mark_phys_state');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            mrk_id, 
            wrk_id, 
            type, 
            condition, 
            existing_mark, 
            status, 
            ref_datetime, 
            pend_mark_status, 
            pend_replaced, 
            pend_disturbed, 
            mrk_id_pend_rep, 
            mrk_id_pend_dist, 
            pend_dist_date, 
            pend_repl_date, 
            pend_mark_name, 
            pend_mark_type, 
            pend_mark_ann, 
            latest_condition, 
            latest_cond_date, 
            audit_id, 
            description
        )
        SELECT 
            MPS.mrk_id, 
            MPS.wrk_id, 
            MPS.type, 
            MPS.condition, 
            MPS.existing_mark, 
            MPS.status, 
            MPS.ref_datetime, 
            MPS.pend_mark_status, 
            MPS.pend_replaced, 
            MPS.pend_disturbed, 
            MPS.mrk_id_pend_rep, 
            MPS.mrk_id_pend_dist, 
            MPS.pend_dist_date, 
            MPS.pend_repl_date, 
            MPS.pend_mark_name, 
            MPS.pend_mark_type, 
            MPS.pend_mark_ann, 
            MPS.latest_condition, 
            MPS.latest_cond_date, 
            MPS.audit_id, 
            MPS.description
        FROM crs_mrk_phys_state MPS
        JOIN crs_mark MRK ON MRK.id = MPS.mrk_id 
        WHERE MPS.status <> 'PROV'
        AND MRK.status <> 'PEND'
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    
    ----------------------------------------------------------------------------
    -- node layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'node');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            id, 
            cos_id_official, 
            type, 
            status, 
            order_group_off, 
            sit_id, 
            wrk_id_created, 
            alt_id, 
            se_row_id, 
            audit_id, 
            shape
        )
        SELECT
            NOD.id, 
            NOD.cos_id_official, 
            NOD.type, 
            NOD.status, 
            NOD.order_group_off, 
            NOD.sit_id, 
            NOD.wrk_id_created, 
            NOD.alt_id, 
            NOD.se_row_id, 
            NOD.audit_id, 
            NOD.shape
        FROM crs_node NOD
        WHERE NOD.status <> 'PEND'
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- node prp order layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'node_prp_order');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            dtm_id, 
            nod_id, 
            cor_id, 
            audit_id
        )
        SELECT DISTINCT
            NPO.dtm_id, 
            NPO.nod_id, 
            NPO.cor_id, 
            NPO.audit_id
        FROM crs_node_prp_order NPO
        JOIN crs_mark MRK ON NPO.nod_id = MRK.nod_id
        WHERE MRK.status <> 'PEND'
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- parcel layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'parcel');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            id, 
            ldt_loc_id, 
            img_id, 
            fen_id, 
            toc_code, 
            alt_id, 
            area, 
            nonsurvey_def, 
            appellation_date, 
            parcel_intent, 
            status, 
            total_area, 
            calculated_area, 
            se_row_id, 
            audit_id, 
            shape
        )
        SELECT 
            PAR.id, 
            PAR.ldt_loc_id, 
            PAR.img_id, 
            PAR.fen_id, 
            PAR.toc_code, 
            PAR.alt_id, 
            PAR.area, 
            PAR.nonsurvey_def, 
            PAR.appellation_date, 
            PAR.parcel_intent, 
            PAR.status, 
            PAR.total_area, 
            PAR.calculated_area, 
            PAR.se_row_id, 
            PAR.audit_id, 
            CASE WHEN ST_IsValid(PAR.shape) THEN
                PAR.shape
            ELSE 
                ST_Buffer(PAR.shape, 0)
            END AS shape
        FROM crs_parcel PAR
        WHERE PAR.status IN ('CURR', 'SHST')
        AND (PAR.shape IS NULL OR ST_GeometryType(PAR.shape) IN ('ST_MultiPolygon','ST_Polygon'))
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- parcel label layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'parcel_label');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            id,
            par_id,
            se_row_id,
            audit_id,
            shape
        )
        SELECT 
            PDL.id,
            PDL.par_id,
            PDL.se_row_id,
            PDL.audit_id,
            PDL.shape
        FROM crs_parcel_label PDL
        WHERE (
            EXISTS(
                SELECT *
                FROM crs_parcel PAR
                WHERE PAR.id = PDL.par_id
                AND PAR.status IN ('CURR','SHST')
            )
        )
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- parcel dimen layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'parcel_dimen');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            obn_id, 
            par_id, 
            audit_id
        )
        SELECT 
            PDM.obn_id, 
            PDM.par_id, 
            PDM.audit_id
        FROM crs_parcel_dimen PDM
        WHERE (
            EXISTS ( 
                SELECT PAR.id
                FROM crs_parcel PAR
                WHERE PAR.id = PDM.par_id 
                AND PAR.status IN ('CURR', 'SHST')
                )
            )
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- parcel ls layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'parcel_ls');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            id, 
            ldt_loc_id, 
            img_id, 
            fen_id, 
            toc_code, 
            alt_id, 
            area, 
            nonsurvey_def, 
            appellation_date, 
            parcel_intent, 
            status, 
            total_area, 
            calculated_area, 
            se_row_id, 
            audit_id, 
            shape
        )
        SELECT 
            PAR.id, 
            PAR.ldt_loc_id, 
            PAR.img_id, 
            PAR.fen_id, 
            PAR.toc_code, 
            PAR.alt_id, 
            PAR.area, 
            PAR.nonsurvey_def, 
            PAR.appellation_date, 
            PAR.parcel_intent, 
            PAR.status, 
            PAR.total_area, 
            PAR.calculated_area, 
            PAR.se_row_id, 
            PAR.audit_id, 
            PAR.shape
        FROM crs_parcel PAR
        WHERE PAR.status IN ('CURR', 'SHST')
        AND ST_GeometryType(PAR.shape) = 'ST_LineString'
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- parcel ring layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'parcel_ring');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id, 
            par_id, 
            pri_id_parent_ring, 
            is_ring, 
            audit_id
        )
        -- create or replace view ext_parcel_ring2 as
        SELECT 
            PRN.id, 
            PRN.par_id, 
            PRN.pri_id_parent_ring, 
            PRN.is_ring, 
            PRN.audit_id
        FROM crs_parcel_ring PRN
        WHERE PRN.par_id IN
            (
            SELECT PAR.id
            FROM crs_parcel PAR
            WHERE PAR.status IN ('CURR', 'SHST')
            )
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- stat version layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'stat_version');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            version, 
            area_class, 
            "desc", 
            statute_action, 
            start_date, 
            end_date, 
            audit_id
        )
        SELECT 
            SVR.version, 
            SVR.area_class, 
            SVR."desc", 
            SVR.statute_action, 
            SVR.start_date, 
            SVR.end_date, 
            SVR.audit_id
        FROM crs_stat_version SVR
        WHERE SVR.area_class= 'TA'
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- statist area layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'statist_area');
    
    v_data_insert_sql := $sql$
    
        INSERT INTO %1% (
            id, 
            sav_area_class, 
            sav_version, 
            name,
            name_abrev, 
            code, 
            status, 
            alt_id,
            se_row_id,
            audit_id
        )
        SELECT 
            STA.id, 
            STA.sav_area_class, 
            STA.sav_version, 
            STA.name, 
            STA.name_abrev, 
            STA.code, 
            STA.status, 
            STA.alt_id,
            STA.se_row_id,
            STA.audit_id
        FROM crs_statist_area STA
        WHERE STA.sav_area_class = 'TA'
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- survey layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'survey');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            wrk_id, 
            ldt_loc_id, 
            dataset_series, 
            dataset_id, 
            type_of_dataset, 
            data_source, 
            lodge_order, 
            dataset_suffix, 
            surveyor_data_ref, 
            survey_class, 
            description, 
            usr_id_sol, 
            survey_date, 
            certified_date, 
            registered_date, 
            chf_sur_amnd_date, 
            dlr_amnd_date, 
            cadastral_surv_acc, 
            prior_wrk_id, 
            abey_prior_status,
            fhr_id,
            pnx_id_submitted,
            audit_id
        )
        SELECT 
            SUR.wrk_id, 
            SUR.ldt_loc_id, 
            SUR.dataset_series, 
            SUR.dataset_id, 
            SUR.type_of_dataset, 
            SUR.data_source, 
            SUR.lodge_order, 
            SUR.dataset_suffix, 
            SUR.surveyor_data_ref, 
            SUR.survey_class, 
            SUR.description, 
            SUR.usr_id_sol, 
            SUR.survey_date, 
            SUR.certified_date, 
            SUR.registered_date, 
            SUR.chf_sur_amnd_date, 
            SUR.dlr_amnd_date, 
            SUR.cadastral_surv_acc, 
            SUR.prior_wrk_id, 
            SUR.abey_prior_status,
            SUR.fhr_id,
            SUR.pnx_id_submitted,
            SUR.audit_id
        FROM crs_survey SUR
        WHERE SUR.wrk_id IN (
                SELECT WRK.id
                FROM crs_work WRK
                WHERE WRK.restricted = 'N'
        )
        ORDER BY wrk_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
        ----------------------------------------------------------------------------
    -- title layer (2)
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'title');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            title_no, 
            ldt_loc_id, 
            status, 
            issue_date, 
            register_type, 
            type, 
            audit_id, 
            ste_id, 
            guarantee_status, 
            provisional, 
            sur_wrk_id, 
            ttl_title_no_srs,
            ttl_title_no_head_srs
        )
        SELECT 
            TTL.title_no, 
            TTL.ldt_loc_id, 
            TTL.status, 
            TTL.issue_date, 
            TTL.register_type, 
            TTL.type, 
            TTL.audit_id, 
            TTL.ste_id, 
            TTL.guarantee_status, 
            TTL.provisional, 
            TTL.sur_wrk_id, 
            TTL.ttl_title_no_srs,
            NULL --TODO: add column to crs_title for TTL.ttl_title_no_head_srs
        FROM crs_title TTL
        WHERE TTL.title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
        AND TTL.status <> 'PEND'
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- title action layer (2)
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'title_action');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            ttl_title_no, 
            act_tin_id, 
            act_id,
            audit_id
        )
        SELECT 
            TTA.ttl_title_no, 
            TTA.act_tin_id, 
            TTA.act_id,
            TTA.audit_id
        FROM crs_title_action TTA
        LEFT OUTER JOIN crs_title TTL ON TTA.ttl_title_no = TTL.title_no
        WHERE TTA.ttl_title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
        
    
    ----------------------------------------------------------------------------
    -- title doc ref layer (1)
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'title_doc_ref');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            type, 
            reference_no, 
            id, 
            tin_id
        )
        SELECT 
            TDR.type, 
            TDR.reference_no, 
            TDR.id, 
            TDR.tin_id
        FROM crs_title_doc_ref TDR
        WHERE (
            EXISTS (
                SELECT TLH.id
                    FROM crs_ttl_hierarchy TLH
                    WHERE TDR.id = TLH.tdr_id 
                    AND TLH.status = 'REGD' 
                    AND NOT (
                    EXISTS ( 
                        SELECT TRN.title_no
                        FROM tmp_training_titles TRN
                        WHERE (TRN.title_no = TLH.ttl_title_no_prior 
                            OR TRN.title_no = TLH.ttl_title_no_flw) 
                    )
                )
            )
        )
        ORDER BY id;;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );    
    
    ----------------------------------------------------------------------------
    -- title estate layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'title_estate');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            ttl_title_no, 
            type, 
            status, 
            share, 
            purpose, 
            timeshare_week_no, 
            lgd_id, 
            id, 
            act_tin_id_crt, 
            original_flag, 
            tin_id_orig, 
            term, 
            act_id_crt
        )
        SELECT 
            ETT.ttl_title_no, 
            ETT.type, 
            ETT.status, 
            ETT.share, 
            ETT.purpose, 
            ETT.timeshare_week_no, 
            ETT.lgd_id, 
            ETT.id, 
            ETT.act_tin_id_crt, 
            ETT.original_flag, 
            ETT.tin_id_orig, 
            ETT.term, 
            ETT.act_id_crt
        FROM crs_title_estate ETT
        LEFT OUTER JOIN crs_title TTL
            ON ETT.ttl_title_no = TTL.title_no 
        WHERE ETT.status <> 'LDGE' 
            AND ETT.ttl_title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
        
    
    ----------------------------------------------------------------------------
    -- title mem text layer (3) {using temp tables}
    -- this temp tables are used to speed up the query, not because the temp is used elsewhere
    ----------------------------------------------------------------------------
    
    v_table := LDS.LDS_GetTable('bde_ext', 'title_mem_text');
    
    RAISE DEBUG 'Started creating temp tables for %', v_table;

    CREATE TEMPORARY TABLE dvl_mem 
    (title_no VARCHAR, mem_id INTEGER)
    ON COMMIT DROP;

    INSERT INTO dvl_mem
    SELECT
        DVL.title_no AS title_no,
        M.id AS mem_id
    FROM
        tmp_protected_titles DVL
    JOIN crs_title_memorial M ON DVL.title_no = M.ttl_title_no;
    
    ANALYSE dvl_mem;
    
    ----------------------------------------------------------------------------
    -- title mem text layer (3) {using temp tables}
    -- this temp tables are used to speed up the query, not because the temp is used elsewhere
    ----------------------------------------------------------------------------
    
    v_table := LDS.LDS_GetTable('bde_ext', 'title_mem_text');
    
    RAISE DEBUG 'Started creating temp tables for %', v_table;

    CREATE TEMPORARY TABLE ttm_ldg 
    (id int)
    ON COMMIT DROP;

    INSERT INTO ttm_ldg
    SELECT TTM.id FROM crs_title_memorial TTM
    WHERE TTM.status <> 'LDGE' 
    AND TTM.ttl_title_no NOT IN (SELECT title_no FROM tmp_excluded_titles);
    
    CREATE INDEX ttm_ldg_idx ON ttm_ldg (id);
    
    ANALYSE ttm_ldg;
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            ttm_id, 
            sequence_no, 
            curr_hist_flag, 
            std_text, 
            col_1_text, 
            col_2_text, 
            col_3_text, 
            col_4_text, 
            col_5_text, 
            col_6_text, 
            col_7_text,
            audit_id
        )
        SELECT 
            TMT.ttm_id, 
            TMT.sequence_no, 
            TMT.curr_hist_flag, 
            CASE WHEN DVL_MEM.title_no IS NOT NULL AND TRT.grp = 'TINT' AND TRT.type IN ('JFH','DD','CN','UAPP','X','T','TSM')
                THEN TIN.inst_no || ' ' || TRT.description || ' - '|| to_char(TIN.lodged_datetime, 'DD.MM.YYYY') || ' at ' || to_char(TIN.lodged_datetime, 'HH:MI am')
            ELSE TMT.std_text
            END AS std_text, 
            TMT.col_1_text, 
            TMT.col_2_text, 
            TMT.col_3_text, 
            TMT.col_4_text, 
            TMT.col_5_text, 
            TMT.col_6_text, 
            TMT.col_7_text,
            TMT.audit_id
        FROM crs_title_mem_text TMT
        LEFT JOIN DVL_MEM ON DVL_MEM.mem_id = TMT.ttm_id 
        LEFT JOIN crs_title_memorial TTM ON TMT.ttm_id = TTM.id
        LEFT JOIN crs_ttl_inst TIN ON TTM.act_tin_id_crt = TIN.id
        LEFT JOIN crs_transact_type TRT ON (TRT.grp = TIN.trt_grp AND TRT.type = TIN.trt_type)
        WHERE TMT.ttm_id IN (SELECT id FROM ttm_ldg)
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- title memorial layer (2)
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'title_memorial');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id, 
            ttl_title_no, 
            mmt_code, 
            act_id_orig, 
            act_tin_id_orig, 
            act_id_crt, 
            act_tin_id_crt, 
            status, 
            user_changed, 
            text_type, 
            register_only_mem, 
            prev_further_reg, 
            curr_hist_flag, 
            "default", 
            number_of_cols, 
            col_1_size, 
            col_2_size, 
            col_3_size, 
            col_4_size, 
            col_5_size, 
            col_6_size, 
            col_7_size, 
            act_id_ext, 
            act_tin_id_ext
        )
        SELECT 
            TTM.id, 
            TTM.ttl_title_no, 
            TTM.mmt_code, 
            TTM.act_id_orig, 
            TTM.act_tin_id_orig, 
            TTM.act_id_crt, 
            TTM.act_tin_id_crt, 
            TTM.status, 
            TTM.user_changed, 
            TTM.text_type, 
            TTM.register_only_mem, 
            TTM.prev_further_reg, 
            TTM.curr_hist_flag, 
            TTM."default", 
            TTM.number_of_cols, 
            TTM.col_1_size, 
            TTM.col_2_size, 
            TTM.col_3_size, 
            TTM.col_4_size, 
            TTM.col_5_size, 
            TTM.col_6_size, 
            TTM.col_7_size, 
            TTM.act_id_ext, 
            TTM.act_tin_id_ext
        FROM crs_title_memorial TTM
        WHERE TTM.status <> 'LDGE' 
        AND TTM.ttl_title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );    
    
    ----------------------------------------------------------------------------
    -- title parcel association layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'title_parcel_association');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            ttl_title_no, 
            par_id, 
            source
        )
        SELECT 
            TPA.id,
            TPA.ttl_title_no, 
            TPA.par_id, 
            TPA.source
        FROM cbe_title_parcel_association TPA
        WHERE TPA.status = 'VALD'
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );    
    
    ----------------------------------------------------------------------------
    -- title transact type layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'transact_type');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            grp, 
            type, 
            description, 
            audit_id
        )
        SELECT 
            TRT.grp, 
            TRT.type, 
            TRT.description, 
            TRT.audit_id
        FROM crs_transact_type TRT
        WHERE TRT.grp IN ('TINT', 'WRKT')
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
   
    ----------------------------------------------------------------------------
    -- ttl enc layer (2)
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'ttl_enc');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            ttl_title_no, 
            enc_id, 
            status, 
            id, 
            act_tin_id_crt, 
            act_id_crt
        )
        SELECT 
            TLE.ttl_title_no, 
            TLE.enc_id, 
            TLE.status, 
            TLE.id, 
            TLE.act_tin_id_crt, 
            TLE.act_id_crt
        FROM crs_ttl_enc TLE
        LEFT OUTER JOIN crs_title T ON TLE.ttl_title_no = T.title_no
        WHERE TLE.status <> 'LDGE'
        AND TLE.ttl_title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
        ORDER BY id;

    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- title hierarchy layer (2)
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'ttl_hierarchy');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id, 
            status, 
            ttl_title_no_prior, 
            ttl_title_no_flw, 
            tdr_id, 
            act_tin_id_crt, 
            act_id_crt
        )
        SELECT DISTINCT
            TLH.id, 
            TLH.status, 
            TLH.ttl_title_no_prior, 
            TLH.ttl_title_no_flw, 
            TLH.tdr_id, 
            TLH.act_tin_id_crt, 
            TLH.act_id_crt
        FROM crs_ttl_hierarchy TLH
        LEFT OUTER JOIN crs_title FLW 
            ON TLH.ttl_title_no_flw = FLW.title_no OR TLH.ttl_title_no_prior = FLW.title_no
        LEFT OUTER JOIN crs_title PRI 
            ON TLH.ttl_title_no_prior = PRI.title_no OR TLH.ttl_title_no_prior = PRI.title_no
        WHERE TLH.status = 'REGD'
        AND (FLW.title_no NOT IN (SELECT title_no FROM tmp_excluded_titles) OR FLW.title_no IS NULL)
        AND (PRI.title_no NOT IN (SELECT title_no FROM tmp_excluded_titles) OR PRI.title_no IS NULL)
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- ttl inst layer (2)
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'ttl_inst');


    v_data_insert_sql := $sql$
        INSERT INTO %1% (
                id,
                dlg_id, 
                inst_no, 
                priority_no, 
                ldt_loc_id, 
                lodged_datetime, 
                status, 
                trt_grp, 
                trt_type, 
                audit_id, 
                tin_id_parent
        )
        SELECT
                TIN.id, 
                TIN.dlg_id, 
                TIN.inst_no, 
                TIN.priority_no, 
                TIN.ldt_loc_id, 
                TIN.lodged_datetime, 
                TIN.status, 
                TIN.trt_grp, 
                TIN.trt_type, 
                TIN.audit_id, 
                TIN.tin_id_parent
        FROM crs_ttl_inst TIN
        INNER JOIN (
                SELECT tin_id FROM crs_action
                UNION
                SELECT tin_id FROM crs_ttl_inst_title
            ) TIN_IDS 
            ON TIN_IDS.tin_id = TIN.id
        ORDER BY id;
        $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- ttl inst title layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'ttl_inst_title');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            tin_id, 
            ttl_title_no,
            audit_id
        )
        SELECT
            TIT.tin_id, 
            TIT.ttl_title_no,
            TIT.audit_id
        FROM crs_ttl_inst_title TIT
        WHERE TIT.ttl_title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    
    ----------------------------------------------------------------------------
    -- user layer (2)
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'user');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id, 
            type, 
            status, 
            title, 
            given_names, 
            surname, 
            corporate_name, 
            audit_id
        )
        WITH WRK_3ID(id)
        AS (
            SELECT usr_id_principal AS id
            FROM crs_work
            UNION 
            SELECT usr_id_firm AS id
            FROM crs_work
            UNION
            SELECT usr_id_prin_firm AS id
            FROM crs_work
        )
        SELECT 
            USR.id, 
            USR.type, 
            USR.status, 
            USR.title, 
            USR.given_names, 
            USR.surname, 
            USR.corporate_name, 
            USR.audit_id
        FROM crs_user USR
        WHERE USR.id IN (SELECT id FROM WRK_3ID)
        ORDER BY audit_id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- vector ls layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'vector_ls');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            type, 
            nod_id_start, 
            nod_id_end, 
            source, 
            se_row_id, 
            id, 
            audit_id, 
            length, 
            shape
        )
        -- create or replace view ext_vector_ls as
        SELECT 
            VEC.type, 
            VEC.nod_id_start, 
            VEC.nod_id_end, 
            VEC.source, 
            VEC.se_row_id, 
            VEC.id, 
            VEC.audit_id, 
            VEC.length, 
            VEC.shape
        FROM crs_vector VEC
        WHERE ST_GeometryType(VEC.shape) = 'ST_LineString' OR VEC.shape IS NULL
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );    
    
    ----------------------------------------------------------------------------
    -- vector pt layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'vector_pt');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            type, 
            nod_id_start, 
            nod_id_end, 
            source, 
            se_row_id, 
            id, 
            audit_id, 
            length, 
            shape
        )
        -- create or replace view ext_vector_ls as
        SELECT 
            VEC.type, 
            VEC.nod_id_start, 
            VEC.nod_id_end, 
            VEC.source, 
            VEC.se_row_id, 
            VEC.id, 
            VEC.audit_id, 
            VEC.length, 
            VEC.shape
        FROM crs_vector VEC
        WHERE ST_GeometryType(VEC.shape) = 'ST_Point'
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- work layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'work');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id, 
            trt_grp, 
            trt_type, 
            status, 
            con_id, 
            pro_id, 
            usr_id_firm, 
            usr_id_principal, 
            cel_id, 
            project_name, 
            invoice, 
            external_work_id, 
            view_txn, 
            restricted, 
            lodged_date, 
            authorised_date,
            usr_id_authorised,
            validated_date,
            usr_id_validated,
            cos_id, 
            data_loaded, 
            run_auto_rules, 
            alt_id, 
            audit_id, 
            usr_id_prin_firm
        )
        SELECT 
            WRK.id, 
            WRK.trt_grp, 
            WRK.trt_type, 
            WRK.status, 
            WRK.con_id, 
            WRK.pro_id, 
            WRK.usr_id_firm, 
            WRK.usr_id_principal, 
            WRK.cel_id, 
            WRK.project_name, 
            WRK.invoice, 
            WRK.external_work_id, 
            WRK.view_txn, 
            WRK.restricted, 
            WRK.lodged_date, 
            WRK.authorised_date,
            WRK.usr_id_authorised,
            WRK.validated_date,
            WRK.usr_id_validated,
            WRK.cos_id, 
            WRK.data_loaded, 
            WRK.run_auto_rules, 
            WRK.alt_id, 
            WRK.audit_id, 
            WRK.usr_id_prin_firm
        FROM crs_work WRK
        WHERE WRK.restricted = 'N'
        ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- street address layer
    ----------------------------------------------------------------------------
    
    v_table := LDS.LDS_GetTable('bde_ext', 'street_address_ext');
    
    v_data_insert_sql := $sql$
    INSERT INTO %1% (
        house_number,
        range_low,
        range_high,
        status,
        unofficial_flag,
        rcl_id,
        rna_id,
        alt_id,
        id,
        audit_id,
        se_row_id,
        shape
    )
    SELECT 
        SAD.house_number,
        SAD.range_low,
        SAD.range_high,
        SAD.status,
        SAD.unofficial_flag,
        SAD.rcl_id,
        SAD.rna_id,
        SAD.alt_id,
        SAD.id,
        SAD.audit_id,
        SAD.se_row_id,
        SAD.shape
    FROM crs_street_address SAD
    WHERE SAD.house_number != 'UNH' 
    AND SAD.range_low != 0
    ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- feature name pt layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'feature_name_pt');
    
    v_data_insert_sql := $sql$
    INSERT INTO %1% (
		id,
		type,
		name,
		status,
		other_details,
		se_row_id,
		audit_id,
		shape
	)
	SELECT 
		FEN.id,
		FEN.type,
		FEN.name,
		FEN.status,
		FEN.other_details,
		FEN.se_row_id,
		FEN.audit_id,
		FEN.shape
	FROM crs_feature_name FEN
    WHERE (FEN.shape IS NULL OR ST_GeometryType(FEN.shape) = 'ST_Point')
    ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- feature name poly layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'feature_name_poly');
    
    v_data_insert_sql := $sql$
    
      INSERT INTO %1% (
		id,
		type,
		name,
		status,
		other_details,
		se_row_id,
		audit_id,
		shape
	 )
	 SELECT 
		FEN.id,
		FEN.type,
		FEN.name,
		FEN.status,
		FEN.other_details,
		FEN.se_row_id,
		FEN.audit_id,
		FEN.shape
	 FROM crs_feature_name FEN
     WHERE ST_GeometryType(FEN.shape) = 'ST_Polygon'
     ORDER BY id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );


    ----------------------------------------------------------------------------
    -- coordinate layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('bde_ext', 'coordinate');
    
    v_data_insert_sql := $sql$
    
      INSERT INTO %1% (
		id,
		cos_id,
		nod_id,
		ort_type_1,
		ort_type_2,
		ort_type_3,
		status,
		sdc_status,
		source,
		value1,
		value2,
		value3,
		wrk_id_created,
		cor_id,
		audit_id
	)
	SELECT
		COO.id,
		COO.cos_id,
		COO.nod_id,
		COO.ort_type_1,
		COO.ort_type_2,
		COO.ort_type_3,
		COO.status,
		COO.sdc_status,
		COO.source,
		COO.value1,
		COO.value2,
		COO.value3,
		COO.wrk_id_created,
		COO.cor_id,
		COO.audit_id
	FROM
		crs_coordinate COO
	WHERE
		COO.cos_id = 109
		AND COO.status = 'AUTH'
    ORDER BY id;
 $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    PERFORM LDS_DropTitleExclusionTables(p_upload);
        
    RAISE INFO 'Finished maintenance on FBDE layers';
    
    RETURN 1;
    
EXCEPTION
    WHEN others THEN
        RAISE EXCEPTION 'Could not maintain FBDE layers, ERROR %',
            SQLERRM;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_MaintainFBDELayers(INTEGER) OWNER TO bde_dba;

DO $$
DECLARE
    v_comment TEXT;
    v_pcid    TEXT;
    v_schema  TEXT = 'bde_ext';
BEGIN
    FOR v_comment, v_pcid IN
        SELECT
            obj_description(oid, 'pg_proc'),
            proname || '(' || pg_get_function_identity_arguments(oid) || ')'
        FROM
            pg_proc
        WHERE
            pronamespace=(SELECT oid FROM pg_namespace WHERE nspname = v_schema)  AND
            proname NOT ILIKE '_createVersionComment'
    LOOP
        IF v_comment IS NULL THEN
            v_comment := '';
        ELSE
            v_comment := E'\n\n' || v_comment;
        END IF;
       
        v_comment := 'Version: ' ||  '$Id$'
                    || E'\n' || 'Installed: ' ||
                    to_char(current_timestamp,'YYYY-MM-DD HH:MI') || v_comment;
       
        EXECUTE 'COMMENT ON FUNCTION ' || v_schema || '.' || v_pcid || ' IS '
            || quote_literal( v_comment );
    END LOOP;
END
$$;

COMMIT;
