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
-- Creates functions to maintain LDS simplified layers
--------------------------------------------------------------------------------
SET client_min_messages TO WARNING;
BEGIN;

SET SEARCH_PATH = lds_ext, lds, bde, bde_control, public;

DO $$
DECLARE
   v_pcid    TEXT;
   v_schema  TEXT = 'lds';
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


-- ############################################################################################################
-- ############################################################################################################
-- ############################################################################################################


CREATE OR REPLACE FUNCTION LDS_MaintainSimplifiedPendingLayers(
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
    RAISE INFO 'Starting maintenance on pending simplified layers';
    
    IF (
        NOT (
            SELECT bde_control.bde_TablesAffected(
                p_upload,
                ARRAY[
                	'crs_title',
                	'cbe_title_parcel_association',
                	'crs_stat_act_parcl',
                	'crs_sys_code',
                	'crs_parcel',
                	'crs_locality',
                	'crs_affected_parcl',
                    'crs_topology_class'
                ],
                'any affected'
            )
        )
        AND LDS_EXT.LDS_TableHasData('lds_ext', 'pending_parcels')
        AND LDS_EXT.LDS_TableHasData('lds_ext', 'pending_linear_parcels')
    )
    THEN
        RAISE INFO
            'Maintain pending simplified layers has been skipped as no relating tables were affected by the upload';
        RETURN 1;
    END IF;
    
    -- Upper is used on the mark type to
    -- force the planner to do a seq scan on crs_mark_name.

    
    ----------------------------------------------------------------------------
    -- temp tables for pending layers
    ----------------------------------------------------------------------------    
    
    DROP TABLE IF EXISTS tmp_excluded_titles;
    CREATE TEMP TABLE tmp_excluded_titles AS
    SELECT
        DISTINCT TTL.title_no
    FROM
        crs_title            TTL
    WHERE
    (
        title_no LIKE 'WNTRAIN%' OR
        title_no = 'WNTESTDATAONLY'
    );
    
    ALTER TABLE tmp_excluded_titles ADD PRIMARY KEY (title_no);
    ANALYSE tmp_excluded_titles;

    DROP TABLE IF EXISTS tmp_protected_titles;
    CREATE TEMP TABLE tmp_protected_titles AS
    SELECT
        TTL.title_no
    FROM
        crs_title TTL
        LEFT JOIN tmp_excluded_titles EXL ON TTL.title_no = EXL.title_no
    WHERE
    (
        (
            TTL.protect_start <= CURRENT_DATE AND
            (
                TTL.protect_end  >= CURRENT_DATE OR 
                TTL.protect_end IS NULL
            )
        ) OR
        (
            TTL.protect_start IS NULL AND
            TTL.protect_end   >= CURRENT_DATE
        )
    ) AND
    EXL.title_no IS NULL;

    ALTER TABLE tmp_protected_titles ADD PRIMARY KEY (title_no);
    ANALYSE tmp_protected_titles;

    --RAISE DEBUG 'Started creating temp table tmp_title_parcel_associations';

    DROP TABLE IF EXISTS tmp_title_parcel_associations;
    CREATE TEMP TABLE tmp_title_parcel_associations AS
    SELECT
        id,
        ttl_title_no AS title_no,
        par_id,
        CAST (
            CASE source
            WHEN 'LINZ' THEN 'LINZ'
            WHEN 'LOL' THEN 'LINZ'
            WHEN 'EXTL' THEN 'External'
            END
        AS VARCHAR(8) ) AS source
    FROM
        cbe_title_parcel_association
    WHERE
        status = 'VALD' AND
        ttl_title_no NOT IN (SELECT title_no FROM tmp_excluded_titles);
    
    ALTER TABLE tmp_title_parcel_associations ADD PRIMARY KEY (par_id, title_no);
    
    ANALYSE tmp_title_parcel_associations;

    --RAISE DEBUG 'Started creating temp table tmp_par_stat_action';

    DROP TABLE IF EXISTS tmp_par_stat_action;
    CREATE TEMP TABLE tmp_par_stat_action AS
    SELECT
        SAP.audit_id as id,
        SAP.par_id,
        SAPS.char_value as status,
        SAPA.char_value AS action,
        bde_get_par_stat_act(SAP.sta_id, SAP.par_id) as statutory_action
    FROM
        crs_stat_act_parcl SAP
        LEFT JOIN  crs_sys_code SAPA ON SAPA.scg_code ='SAPA' AND SAP.action = SAPA.code
        LEFT JOIN  crs_sys_code SAPS ON SAPS.scg_code ='SAPS' AND SAP.status = SAPS.code
    ORDER BY SAP.par_id;
    
    ANALYSE tmp_par_stat_action;

    -- create aggregated listing of statutory actions for the parcels layers
    DROP TABLE IF EXISTS tmp_par_stat_action_agg;
    CREATE TEMP TABLE tmp_par_stat_action_agg AS
    SELECT
        PSA.par_id,
        string_agg(
            '[' ||  PSA.action || '] ' ||  PSA.statutory_action,
            E'\r\n'
            ORDER BY
                '[' ||  PSA.action || '] ' ||  PSA.statutory_action
        ) AS statutory_actions
    FROM
        tmp_par_stat_action PSA
    WHERE
        PSA.status = 'Current'
    GROUP BY
        PSA.par_id;

    ALTER TABLE tmp_par_stat_action_agg ADD PRIMARY KEY (par_id);
    ANALYSE tmp_par_stat_action_agg;

    perform LDS.LDS_CreateSurveyPlansTable(p_upload);
    
    -- make region data for determining how to calc areas
    DROP TABLE IF EXISTS tmp_world_regions;
    CREATE TEMP TABLE tmp_world_regions (
        id INTEGER NOT NULL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        shape GEOMETRY NOT NULL
    );
    
    INSERT INTO tmp_world_regions (id, name, shape) VALUES
    ( 1, 'chathams',   'SRID=4167;POLYGON((182 -43,185 -43,185 -45,182 -45,182 -43))' ),
    ( 2, 'nz',         'SRID=4167;POLYGON((166 -34,179 -34,179 -48,166 -48,166 -34))' );
    
    CREATE INDEX tmp_world_regions_shpx ON tmp_world_regions USING gist (shape);
    ANALYSE tmp_world_regions;

    --RAISE DEBUG 'Started creating temp table tmp_parcel_geoms';
    
    -- Some Landonline parcel polygons have rings that self-intersect, typically
    -- banana polygons. So here we use the buffer 0 trick to build a polygon
    -- that is structurally identical but follows OGC topology rules.
    DROP TABLE IF EXISTS tmp_parcel_geoms;
    CREATE TEMP TABLE tmp_parcel_geoms AS
    SELECT
        PAR.id as par_id,
        CASE WHEN ST_IsValid(PAR.shape) THEN
            PAR.shape
        ELSE 
            ST_Buffer(PAR.shape, 0)
        END AS shape
    FROM
        crs_parcel PAR
    WHERE
        PAR.status = 'PEND';
        
    ALTER TABLE tmp_parcel_geoms ADD PRIMARY KEY(par_id);
    ANALYSE tmp_parcel_geoms;
    
    --RAISE DEBUG 'Started creating temp table tmp_current_parcels';

    DROP TABLE IF EXISTS tmp_current_parcels;
    CREATE TEMP TABLE tmp_current_parcels AS
    SELECT
        PAR.id,
        bde_get_combined_appellation(PAR.id, 'N') AS appellation,
        string_agg(
            DISTINCT 
                SUR.survey_reference,
            ', '
            ORDER BY
                SUR.survey_reference
        ) AS affected_surveys,
        PAR.parcel_intent,
        PAR.toc_code,
        PAR.status,
        PSA.statutory_actions,
        LOC.name AS land_district,
        string_agg(DISTINCT TTL.title_no, ', ' ORDER BY TTL.title_no ASC) AS titles,
        COALESCE(PAR.total_area, PAR.area) AS survey_area,
        CASE WHEN WDR.name = 'chathams' THEN
            CAST(ST_Area(ST_Transform(GEOM.shape, 3793)) AS NUMERIC(20, 4))
        ELSE
            CAST(ST_Area(ST_Transform(GEOM.shape, 2193)) AS NUMERIC(20, 0))
        END AS calc_area,
        GEOM.shape
    FROM
        tmp_world_regions WDR,
        crs_parcel PAR
        JOIN tmp_parcel_geoms GEOM ON PAR.id = GEOM.par_id
        JOIN crs_locality LOC ON PAR.ldt_loc_id = LOC.id
        LEFT JOIN tmp_title_parcel_associations TTL ON PAR.id = TTL.par_id
        LEFT JOIN tmp_par_stat_action_agg PSA ON PAR.id = PSA.par_id
        LEFT JOIN crs_affected_parcl AFP ON PAR.id = AFP.par_id
        LEFT JOIN tmp_survey_plans SUR ON AFP.sur_wrk_id = SUR.wrk_id
    WHERE
        PAR.status = 'PEND' AND
        (ST_Contains(WDR.shape, PAR.shape) OR PAR.shape IS NULL)
    GROUP BY
        1, 2, 4, 5, 6, 7, 8, 10, 11, 12
    ORDER BY
        PAR.id;

    -------------------------------------
    --- P E N D I N G   P A R C E L S ---
    -------------------------------------		
    
    --------------------------------------------------------------------------------
	-- LDS table pending_parcels
	--------------------------------------------------------------------------------
	
	v_table := LDS_EXT.LDS_GetTable('lds_ext', 'pending_parcels');
    
    v_data_insert_sql := $sql$
	INSERT INTO %1%(
	    id,
	    appellation,
	    affected_surveys,
	    parcel_intent,
	    topology_type,
	    status,
	    statutory_actions,
	    land_district,
	    titles,
	    survey_area,
	    calc_area,
	    shape
	)
	SELECT
	    PAR.id,
	    PAR.appellation,
	    PAR.affected_surveys,
	    COALESCE(SYSP.char_value, PAR.parcel_intent) AS parcel_intent,
	    TOC.name AS topology_type,
		SYSS.char_value AS status,
	    PAR.statutory_actions,
	    PAR.land_district,
	    PAR.titles,
	    PAR.survey_area,
	    PAR.calc_area,
	    PAR.shape
	FROM
	    tmp_current_parcels PAR
	    JOIN crs_topology_class TOC ON PAR.toc_code = TOC.code
	    LEFT JOIN crs_sys_code SYSP ON PAR.parcel_intent = SYSP.code AND SYSP.scg_code = 'PARI'
		LEFT JOIN crs_sys_code SYSS ON PAR.status = SYSS.code AND SYSS.scg_code = 'PARS'
	WHERE
	    PAR.toc_code IN ('PRIM', 'SECO', 'TERT', 'STRA')  AND
	    ST_GeometryType(PAR.shape) IN ('ST_MultiPolygon', 'ST_Polygon');
	$sql$;
	
	PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
	
	--------------------------------------------------------------------------------
	-- LDS table pending_linear_parcels
	--------------------------------------------------------------------------------

	v_table := LDS_EXT.LDS_GetTable('lds_ext', 'pending_linear_parcels');
    
    v_data_insert_sql := $sql$
	INSERT INTO %1%(
	    id,
	    appellation,
	    affected_surveys,
	    parcel_intent,
	    topology_type,
		status,
	    statutory_actions,
	    land_district,
	    titles,
	    survey_area,
	    calc_area,
	    shape
	)
	SELECT
	    PAR.id,
	    PAR.appellation,
	    PAR.affected_surveys,
	    COALESCE(SYSP.char_value, PAR.parcel_intent) AS parcel_intent,
	    TOC.name AS topology_type,
		SYSS.char_value AS status,
	    PAR.statutory_actions,
	    PAR.land_district,
	    PAR.titles,
	    PAR.survey_area,
	    NULL,
	    PAR.shape
	FROM
	    tmp_current_parcels PAR
	    JOIN crs_topology_class TOC ON PAR.toc_code = TOC.code
	    LEFT JOIN crs_sys_code SYSP ON PAR.parcel_intent = SYSP.code AND SYSP.scg_code = 'PARI'
		LEFT JOIN crs_sys_code SYSS ON PAR.status = SYSS.code AND SYSS.scg_code = 'PARS'
	WHERE
	    PAR.toc_code IN ('SECL', 'TECL') AND
	    ST_GeometryType(PAR.shape) IN ('ST_LineString', 'ST_MultiLineString');
	$sql$;
	
	PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
	
	RAISE INFO 'Finished maintenance on pending parcel layers';

    ------------------------------------
    ------------------------------------
    ------------------------------------

    RAISE INFO 'Finished maintenance on pending parcel simplified layers';
    
    RETURN 1;
    
EXCEPTION
    WHEN others THEN
        RAISE EXCEPTION 'Could not maintain pending parcel layers, ERROR %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_MaintainSimplifiedPendingLayers(INTEGER) OWNER TO bde_dba;


--###########################################################################################################
--###########################################################################################################
--###########################################################################################################


ALTER FUNCTION LDS_MaintainSimplifiedPendingLayers(INTEGER) OWNER TO bde_dba;

DO $$
DECLARE
    v_comment TEXT;
    v_pcid    TEXT;
    v_schema  TEXT = 'lds';
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
