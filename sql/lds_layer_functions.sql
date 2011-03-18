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
SET SEARCH_PATH = lds, bde_control, public;

DO $$
DECLARE
   pcid text;
BEGIN
    FOR pcid IN 
        SELECT proname || '(' || pg_get_function_identity_arguments(oid) || ')'
        FROM pg_proc 
        WHERE pronamespace=(SELECT oid FROM pg_namespace WHERE nspname ILIKE 'lds')  
    LOOP
        EXECUTE 'DROP FUNCTION ' || pcid;
    END LOOP;
END
$$;


CREATE OR REPLACE FUNCTION LDS_MaintainSimplifiedLayers(
    p_upload_id INTEGER
)
RETURNS
    INTEGER AS $$
BEGIN    
    PERFORM LDS.LDS_MaintainSimplifiedGeodeticLayers(p_upload_id);
    PERFORM LDS.LDS_MaintainSimplifiedElectoralLayers(p_upload_id);
    PERFORM LDS.LDS_MaintainSimplifiedParcelLayers(p_upload_id);
    PERFORM LDS.LDS_MaintainSimplifiedSurveyLayers(p_upload_id);
    
    RETURN 1;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER;

ALTER FUNCTION LDS_MaintainSimplifiedLayers(integer) SET search_path=lds, bde, bde_control, public;


CREATE OR REPLACE FUNCTION LDS_GetTable(
    p_schema NAME,
    p_table_name NAME
)
RETURNS
    REGCLASS AS $$
DECLARE
    v_table REGCLASS;
BEGIN
    v_table := bde_control.bde_TableOid(p_schema, p_table_name);
    
    IF v_table IS NULL THEN
        RAISE EXCEPTION '%.% table does not exist', p_schema, p_table_name;
    END IF;
    
    RETURN v_table;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_GetTable(NAME, NAME) OWNER TO bde_dba;


CREATE OR REPLACE FUNCTION LDS_UpdateSimplifiedTable(
    p_upload            INTEGER,
    p_table             REGCLASS,
    p_data_diff_tmpl    TEXT,
    p_data_insert_tmpl  TEXT
)
RETURNS
    BOOLEAN AS $$
DECLARE
    v_count          BIGINT;
    v_key_column     NAME;
    v_indexes        TEXT[];
    v_temp_copy      REGCLASS;
BEGIN
    IF LDS.LDS_TableHasData(p_table) THEN
        PERFORM bde_control.bde_WriteUploadLog(
            p_upload,
            '3',
            'Started creating new version of table ' || p_table  || ' for differencing'
        );
        
        SELECT LDS.LDS_CreateTempCopy(p_table)
        INTO   v_temp_copy;
        
        v_count := bde_control.bde_ExecuteTemplate(
            p_data_diff_tmpl,
            ARRAY[v_temp_copy::TEXT, p_table::TEXT]
        );
        
        SELECT LDS.LDS_ApplyPrimaryKeyFrom(p_table, v_temp_copy)
        INTO   v_key_column;
        
        EXECUTE 'ANALYSE ' || v_temp_copy;

        PERFORM bde_control.bde_WriteUploadLog(
            p_upload,
            '3',
            'Finished creating new version of table ' || p_table ||
            ' for differencing. ' || v_count || ' rows were created'
        );
        
        PERFORM LDS.LDS_ApplyTableDifferences(p_upload, p_table, v_temp_copy, v_key_column);
        
        EXECUTE 'DROP TABLE ' || v_temp_copy;
    ELSE
        PERFORM bde_control.bde_WriteUploadLog(
            p_upload,
            '3',
            'Started creating new version of table ' || p_table
        );
        
        SELECT LDS.LDS_GetTableContrainstsAndIndexes(p_table)
        INTO   v_indexes;

        PERFORM LDS.LDS_DropTableContrainstsAndIndexes(p_table);
        
        v_count := bde_control.bde_ExecuteTemplate(
            p_data_insert_tmpl,
            ARRAY[p_table::TEXT]
        );
        
        PERFORM bde_control.bde_ExecuteSqlArray(
            p_upload,
            'Applying Indexes and constraints',
            v_indexes
        );
        
        EXECUTE 'ANALYSE ' || p_table;
        
        PERFORM bde_control.bde_WriteUploadLog(
            p_upload,
            '3',
            'Finished creating new version of table ' || p_table  ||
            '. ' ||  v_count || ' rows were created'
        );
    END IF;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_UpdateSimplifiedTable(INTEGER, REGCLASS, TEXT, TEXT) OWNER TO bde_dba;


CREATE OR REPLACE FUNCTION LDS_TableHasData(
    p_table REGCLASS
)
RETURNS
    BOOLEAN AS $$
DECLARE
    v_exists BOOLEAN;
BEGIN
    EXECUTE 'SELECT EXISTS (SELECT * FROM ' || p_table::TEXT || ' LIMIT 1)'
    INTO v_exists;
    
    RETURN v_exists;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION LDS_TableHasData(
    p_schema NAME,
    p_table_name NAME
)
RETURNS
    BOOLEAN AS $$
    SELECT LDS.LDS_TableHasData(LDS.LDS_GetTable($1, $2));
$$ LANGUAGE sql;

ALTER FUNCTION LDS_TableHasData(NAME, NAME) OWNER TO bde_dba;


CREATE OR REPLACE FUNCTION LDS_CreateTempCopy(
    p_table REGCLASS
)
RETURNS
    REGCLASS AS $$
DECLARE
    v_table_name NAME;
    v_table_oid  REGCLASS;
    v_sql        TEXT;
BEGIN
    IF NOT EXISTS(SELECT * FROM pg_class where oid = p_table) THEN
        RAISE EXCEPTION 'Input table % does not exist', p_table;
    END IF;
    
    v_table_name := REPLACE(p_table::TEXT, '.', '_') || E'_\$\$';

    v_sql := 'CREATE TEMP TABLE ' || v_table_name
        || ' (LIKE ' || p_table::TEXT
        || ' INCLUDING DEFAULTS)';
    
    EXECUTE v_sql;

    SELECT bde_TempTableOid(v_table_name)
    INTO   v_table_oid;

    IF v_table_oid IS NULL THEN
        RAISE EXCEPTION 'Created temp working copy % does not exist', v_table_name;
    END IF;
    
    RETURN v_table_oid;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_CreateTempCopy(REGCLASS) OWNER TO bde_dba;


CREATE OR REPLACE FUNCTION LDS_ApplyPrimaryKeyFrom(
    p_table_from REGCLASS,
    p_table_dest REGCLASS
)
RETURNS
    NAME AS $$
DECLARE
    v_key_column TEXT;
    v_sql        TEXT;
BEGIN
    SELECT               
        ATT.attname
    INTO
        v_key_column
    FROM
        pg_index     IDX,
        pg_class     CLS,
        pg_attribute ATT
    WHERE 
        CLS.oid = p_table_from AND
        IDX.indrelid = CLS.oid AND
        ATT.attrelid = CLS.oid AND 
        ATT.attnum = any(IDX.indkey) AND
        IDX.indisprimary AND
        array_length(IDX.indkey, 1) = 1;
    
    IF NOT bde_control.bde_TableKeyIsValid(p_table_from, v_key_column) THEN
        RAISE EXCEPTION 'Table % does not have a valid primary key', p_table_from;
    END IF;

    BEGIN
        v_sql := 'ALTER TABLE ' || p_table_dest || ' ADD PRIMARY KEY (' || v_key_column || ')';
        EXECUTE v_sql;
    EXCEPTION
        WHEN others THEN
            RAISE EXCEPTION 'Error applying primary key SQL %, ERROR %',  v_sql, SQLERRM;
    END;
 
    RETURN v_key_column;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_ApplyPrimaryKeyFrom(REGCLASS, REGCLASS) OWNER TO bde_dba;


CREATE OR REPLACE FUNCTION LDS_GetTableContrainstsAndIndexes(p_table REGCLASS) RETURNS TEXT[] AS $$
DECLARE
    v_objects TEXT[] = '{}';
    v_sql     TEXT;
BEGIN
    FOR v_sql IN
        SELECT conname || ' ' || pg_get_constraintdef(oid)
        FROM pg_constraint WHERE conrelid = p_table  
    LOOP
       v_sql := 'ALTER TABLE ' || p_table || ' ADD CONSTRAINT ' || v_sql;
       v_objects := v_objects || v_sql;
    END LOOP;

    FOR v_sql IN
        SELECT pg_get_indexdef(indexrelid)
        FROM   pg_index
        WHERE  indrelid = p_table
        AND    NOT indisprimary 
    LOOP
        v_objects := v_objects || v_sql;
    END LOOP;
    
    RETURN v_objects;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_GetTableContrainstsAndIndexes(REGCLASS) OWNER TO bde_dba;


CREATE OR REPLACE FUNCTION LDS_DropTableContrainstsAndIndexes(p_table REGCLASS) RETURNS BOOLEAN AS $$
DECLARE
    v_sql     TEXT;
    v_name    TEXT;
BEGIN
    FOR v_name IN
        SELECT conname
        FROM   pg_constraint
        WHERE  conrelid = p_table
    LOOP
       v_sql := 'ALTER TABLE ' || p_table || ' DROP CONSTRAINT ' || v_name;
       EXECUTE v_sql;
    END LOOP;

    FOR v_name IN
        SELECT indexrelid::REGCLASS
        FROM   pg_index
        WHERE  indrelid = p_table
    LOOP
        v_sql := 'DROP INDEX ' || v_name;
        EXECUTE v_sql;
    END LOOP;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_DropTableContrainstsAndIndexes(REGCLASS) OWNER TO bde_dba;


CREATE OR REPLACE FUNCTION LDS_ApplyTableDifferences(
    p_upload     INTEGER,
    p_table      REGCLASS,
    p_temp_copy  REGCLASS,
    p_key_column NAME
)
RETURNS
    BOOLEAN AS $$
DECLARE
    v_nins  BIGINT;
    v_ndel  BIGINT;
    v_nupd  BIGINT;
BEGIN
    SELECT
        number_inserts,
        number_updates,
        number_deletes
    INTO
        v_nins,
        v_nupd,
        v_ndel
    FROM
        bde_control.bde_ApplyTableDifferences(
            p_upload, p_table, p_temp_copy, p_key_column
        );
    
    PERFORM bde_control.bde_WriteUploadLog(p_upload,'1',
        'Finished updating ' || p_table  || ' '
        || v_ndel || ' deletes, '
        || v_nins || ' inserts and '
        || v_nupd || ' updates'
    );
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_ApplyTableDifferences(INTEGER, REGCLASS, REGCLASS, NAME) OWNER TO bde_dba;


CREATE OR REPLACE FUNCTION LDS_MaintainSimplifiedGeodeticLayers(
    p_upload INTEGER
)
RETURNS
    INTEGER AS $$
DECLARE
    v_bad_code_count   BIGINT;
    v_bad_code_string  TEXT;
    v_table            REGCLASS;
    v_data_diff_sql    TEXT;
    v_data_insert_sql  TEXT;
BEGIN
    PERFORM bde_control.bde_WriteUploadLog(
        p_upload, 
        '2',
        'Starting maintenance on geodetic simplified layers'
    );

    IF (
        NOT (
            SELECT bde_control.bde_TablesAffected(
                p_upload,
                ARRAY[
                    'crs_node',
                    'crs_mark',
                    'crs_mark_name',
                    'crs_coordinate',
                    'crs_cord_order',
                    'crs_coordinate_tpe',
                    'crs_coordinate_sys',
                    'crs_site_locality',
                    'crs_locality',
                    'crs_mrk_phys_state',
                    'crs_sys_code'
                ],
                'any affected'
            )
        )
        AND LDS.LDS_TableHasData('lds', 'geodetic_marks')
        AND LDS.LDS_TableHasData('lds', 'geodetic_vertical_marks')
        AND LDS.LDS_TableHasData('lds', 'geodetic_antarctic_marks')
        AND LDS.LDS_TableHasData('lds', 'geodetic_antarctic_vertical_marks')
    )
    THEN
        PERFORM bde_control.bde_WriteUploadLog(
            p_upload, 
            '1',
            'Maintain geodetic simplified layers has been skipped as no ' ||
            'relating tables were affected by the upload'
        );
        RETURN 1;
    END IF;
    
    -- Upper is used on the mark type to
    -- force the planner to do a seq scan on crs_mark_name.
    CREATE TEMP TABLE tmp_geo_nodes AS
    SELECT DISTINCT
        MRK.nod_id,
        MKN.name AS geodetic_code
    FROM
        bde.crs_mark MRK,
        bde.crs_mark_name MKN
    WHERE
        MRK.id = MKN.mrk_id AND
        UPPER(MKN.type) = 'CODE';

    SELECT
        COUNT(*),
        string_agg(E'\'' || geodetic_code || E'\'', ', ')
    INTO
        v_bad_code_count,
        v_bad_code_string
    FROM
        tmp_geo_nodes
    WHERE
        (LENGTH(geodetic_code) <> 4 OR LENGTH(trim(both ' ' FROM geodetic_code)) <> 4);

    IF (v_bad_code_count > 0) THEN
        PERFORM bde_control.bde_WriteUploadLog(
            p_upload, 
            'W',
            'The following malformed geodetic codes have been detected: ' ||
            v_bad_code_string || '. Any of these codes that are still ' ||
            'malformed after white space has been trimmed will be removed ' ||
            'from the geodetic layers.'
        );
        
        UPDATE
            tmp_geo_nodes
        SET
            geodetic_code = trim(both ' ' FROM geodetic_code)
        WHERE
            LENGTH(trim(both ' ' FROM geodetic_code)) <> LENGTH(geodetic_code);
    
        DELETE FROM tmp_geo_nodes
        WHERE LENGTH(geodetic_code) <> 4;
    END IF;

    ALTER TABLE tmp_geo_nodes ADD PRIMARY KEY(nod_id);
    
    ANALYSE tmp_geo_nodes;
    
    ----------------------------------------------------------------------------
    -- geodetic_marks layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'geodetic_marks');

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating temp tables for ' || v_table
    );
    
    -- The windowing partition will prioritise the commissioned, non-replaced marks for each node.
    CREATE TEMP TABLE tmp_geodetic_marks AS
    SELECT
        row_number() OVER (ORDER BY GEO.geodetic_code, MRK.status, MRK.replaced) AS row_number,
        NOD.id,
        GEO.geodetic_code, 
        MKN.name AS current_mark_name, 
        MRK.desc AS description, 
        RTRIM(mrk.type) AS mark_type,
        SCOB.char_value AS beacon_type,
        SCOC.char_value AS mark_condition,
        CAST(COR.display AS INTEGER) AS "order",
        LOC.Name AS land_district,
        COO.value1 AS latitude,
        COO.value2 AS longitude,
        COO.value3 AS ellipsoidal_height,
        NOD.cos_id_official,
        CASE WHEN NOD.cos_id_official = 142 THEN
            ST_SetSRID(ST_MakePoint(COO.value2, COO.value1), 4764)
        ELSE
            NOD.shape
        END AS shape
    FROM
        tmp_geo_nodes GEO
        JOIN crs_node NOD ON NOD.id = GEO.NOD_ID 
        JOIN crs_mark MRK ON MRK.nod_id = NOD.id
        LEFT JOIN crs_mark_name mkn ON MRK.id = MKN.mrk_id AND MKN.type = 'CURR' 
        LEFT JOIN crs_site_locality SLO ON SLO.sit_id = NOD.sit_id 
        LEFT JOIN crs_locality LOC ON LOC.id = SLO.loc_id AND LOC.type = 'LDST' 
        JOIN crs_coordinate COO ON NOD.cos_id_official = COO.cos_id AND NOD.id = COO.nod_id AND COO.status = 'AUTH'
        JOIN crs_cord_order COR ON COO.cor_id = COR.id
        LEFT JOIN crs_mrk_phys_state MPSM ON MPSM.mrk_id = MRK.id AND MPSM.type = 'MARK' AND MPSM.status = 'CURR'
        LEFT JOIN crs_mrk_phys_state MPSB ON MPSB.mrk_id = MRK.id and MPSB.type = 'BCON' and MPSB.status = 'CURR' 
        LEFT JOIN crs_sys_code SCOC ON MPSM.condition = SCOC.code AND SCOC.scg_code = 'MPSC'
        LEFT JOIN crs_sys_code SCOB ON MRK.beacon_type = SCOB.code AND SCOB.scg_code = 'MRKE'
    WHERE
        NOD.status = 'AUTH' AND
        NOD.cos_id_official IN (109, 142);
    
    DELETE FROM
        tmp_geodetic_marks
    WHERE
        row_number NOT IN (SELECT MIN(row_number) FROM tmp_geodetic_marks GROUP BY geodetic_code);

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Finished creating temp tables for ' || v_table
    );
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            geodetic_code,
            current_mark_name,
            description,
            mark_type,
            beacon_type,
            mark_condition,
            "order",
            land_district,
            latitude,
            longitude,
            ellipsoidal_height,
            shape
        )
        SELECT
            id,
            geodetic_code,
            current_mark_name,
            description,
            mark_type,
            beacon_type,
            mark_condition,
            "order",
            land_district,
            latitude,
            longitude,
            ellipsoidal_height,
            shape
        FROM
            tmp_geodetic_marks
        WHERE
            cos_id_official = 109
        ORDER BY
            id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- geodetic_antarctic_marks layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'geodetic_antarctic_marks');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            geodetic_code,
            current_mark_name,
            description,
            mark_type,
            beacon_type,
            mark_condition,
            "order",
            latitude,
            longitude,
            ellipsoidal_height,
            shape
        )
        SELECT
            id,
            geodetic_code,
            current_mark_name,
            description,
            mark_type,
            beacon_type,
            mark_condition,
            "order",
            latitude,
            longitude,
            ellipsoidal_height,
            shape
        FROM
            tmp_geodetic_marks
        WHERE
            cos_id_official = 142
        ORDER BY
            id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    DROP TABLE IF EXISTS tmp_geodetic_marks;
    
    ----------------------------------------------------------------------------
    -- geodetic_vertical_marks layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'geodetic_vertical_marks');
    
    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating temp tables for ' || v_table
    );
    
    -- The windowing partition will prioritise the commissioned, non-replaced marks for each node.
    CREATE TEMP TABLE tmp_geodetic_vertical_mark AS
    SELECT
        row_number() OVER (ORDER BY COS.name, GEO.geodetic_code, MRK.status, MRK.replaced) AS row_number,
        NOD.id AS nod_id,
        GEO.geodetic_code,
        MKN.name AS current_mark_name,
        MRK.desc AS description,
        RTRIM(mrk.type) AS mark_type,
        SCOB.char_value AS beacon_type,
        SCOC.char_value AS mark_condition,
        COR.display AS "order",
        LOC.Name AS land_district,
        COO.value3 AS normal_orthometric_height,
        COS.name AS coordinate_system,
        NOD.cos_id_official,
        CASE WHEN NOD.cos_id_official = 142 THEN
            ST_SetSRID(ST_MakePoint(COO.value2, COO.value1), 4764)
        ELSE
            NOD.shape
        END AS shape
    FROM
        tmp_geo_nodes GEO
        JOIN crs_node NOD ON NOD.id = GEO.NOD_ID 
        JOIN crs_mark MRK ON MRK.nod_id = NOD.id
        LEFT JOIN crs_mark_name mkn ON MRK.id = MKN.mrk_id AND MKN.type = 'CURR' 
        LEFT JOIN crs_site_locality SLO ON SLO.sit_id = NOD.sit_id 
        LEFT JOIN crs_locality LOC ON LOC.id = SLO.loc_id AND LOC.type = 'LDST' 
        JOIN crs_coordinate COO ON COO.nod_id = NOD.id AND COO.status = 'AUTH'
        JOIN crs_coordinate_sys COS ON COO.cos_id = COS.id
        JOIN crs_coordinate_tpe COT ON COT.id = COS.cot_id
        JOIN crs_cord_order COR ON COO.cor_id = COR.id
        LEFT JOIN crs_mrk_phys_state MPSM ON MPSM.mrk_id = MRK.id AND MPSM.type = 'MARK' AND MPSM.status = 'CURR'
        LEFT JOIN crs_mrk_phys_state MPSB ON MPSB.mrk_id = MRK.id and MPSB.type = 'BCON' and MPSB.status = 'CURR' 
        LEFT JOIN crs_sys_code SCOC ON MPSM.condition = SCOC.code AND SCOC.scg_code = 'MPSC'
        LEFT JOIN crs_sys_code SCOB ON MRK.beacon_type = SCOB.code AND SCOB.scg_code = 'MRKE'
    WHERE
        COT.dimension = 'HEGT' AND
        NOD.status = 'AUTH' AND
        MRK.status = 'COMM'  AND
        NOD.cos_id_official IN (109, 142);
    
    DELETE FROM
        tmp_geodetic_vertical_mark
    WHERE
        row_number NOT IN (SELECT MIN(row_number) FROM tmp_geodetic_vertical_mark GROUP BY coordinate_system, nod_id);

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Finished creating temp tables for ' || v_table
    );
    
    v_data_diff_sql := $sql$
        INSERT INTO %1% (
            id,
            nod_id,
            geodetic_code, 
            current_mark_name, 
            description, 
            mark_type,
            beacon_type,
            mark_condition,
            "order",
            land_district,
            normal_orthometric_height,
            coordinate_system,
            shape
        )
        SELECT
            COALESCE(ORG.id, nextval('lds.geodetic_vertical_marks_id_seq')),
            TMP.nod_id,
            TMP.geodetic_code, 
            TMP.current_mark_name, 
            TMP.description, 
            TMP.mark_type,
            TMP.beacon_type,
            TMP.mark_condition,
            TMP."order",
            TMP.land_district,
            TMP.normal_orthometric_height,
            TMP.coordinate_system,
            TMP.shape
        FROM
            tmp_geodetic_vertical_mark AS TMP
            LEFT JOIN %2% AS ORG ON (ORG.nod_id = TMP.nod_id AND ORG.coordinate_system = TMP.coordinate_system)
        WHERE
            cos_id_official = 109
        ORDER BY
            TMP.nod_id,
            TMP.coordinate_system
    $sql$;
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            nod_id,
            geodetic_code, 
            current_mark_name, 
            description, 
            mark_type,
            beacon_type,
            mark_condition,
            "order",
            land_district,
            normal_orthometric_height,
            coordinate_system,
            shape
        )
        SELECT
            nod_id,
            geodetic_code, 
            current_mark_name, 
            description, 
            mark_type,
            beacon_type,
            mark_condition,
            "order",
            land_district,
            normal_orthometric_height,
            coordinate_system,
            shape
        FROM
            tmp_geodetic_vertical_mark
        WHERE
            cos_id_official = 109
        ORDER BY
            nod_id,
            coordinate_system
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_diff_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- geodetic_antarctic_vertical_marks layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'geodetic_antarctic_vertical_marks');
    
    v_data_diff_sql := $sql$
        INSERT INTO %1% (
            id,
            nod_id,
            geodetic_code, 
            current_mark_name, 
            description, 
            mark_type,
            beacon_type,
            mark_condition,
            "order",
            normal_orthometric_height,
            coordinate_system,
            shape
        )
        SELECT
            COALESCE(ORG.id, nextval('lds.geodetic_antarctic_vertical_marks_id_seq')),
            TMP.nod_id,
            TMP.geodetic_code, 
            TMP.current_mark_name, 
            TMP.description, 
            TMP.mark_type,
            TMP.beacon_type,
            TMP.mark_condition,
            TMP."order",
            TMP.normal_orthometric_height,
            TMP.coordinate_system,
            TMP.shape
        FROM
            tmp_geodetic_vertical_mark AS TMP
            LEFT JOIN %2% AS ORG ON (ORG.nod_id = TMP.nod_id AND ORG.coordinate_system = TMP.coordinate_system)
        WHERE
            cos_id_official = 142
        ORDER BY
            TMP.nod_id,
            TMP.coordinate_system
    $sql$;
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            nod_id,
            geodetic_code, 
            current_mark_name, 
            description, 
            mark_type,
            beacon_type,
            mark_condition,
            "order",
            normal_orthometric_height,
            coordinate_system,
            shape
        )
        SELECT
            nod_id,
            geodetic_code, 
            current_mark_name, 
            description, 
            mark_type,
            beacon_type,
            mark_condition,
            "order",
            normal_orthometric_height,
            coordinate_system,
            shape
        FROM
            tmp_geodetic_vertical_mark
        WHERE
            cos_id_official = 142
        ORDER BY
            nod_id,
            coordinate_system
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_diff_sql,
        v_data_insert_sql
    );
    
    DROP TABLE IF EXISTS tmp_geodetic_vertical_mark;
    DROP TABLE IF EXISTS tmp_geo_nodes;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload, 
        '2',
        'Finished maintenance on geodetic simplified layers'
    );
    
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_MaintainSimplifiedGeodeticLayers(INTEGER) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION LDS_MaintainSimplifiedParcelLayers(
    p_upload INTEGER
)
RETURNS
    INTEGER AS $$
DECLARE
    v_table            REGCLASS;
    v_data_diff_sql    TEXT;
    v_data_insert_sql  TEXT;
BEGIN
    PERFORM bde_control.bde_WriteUploadLog(
        p_upload, 
        '2',
        'Starting maintenance on cadastral parcel simplified layers'
    );

    IF (
        NOT (
            SELECT bde_control.bde_TablesAffected(
                p_upload,
                ARRAY[
                    'crs_affected_parcl',
                    'crs_appellation',
                    'crs_estate_share',
                    'crs_legal_desc',
                    'crs_legal_desc_prl',
                    'crs_locality',
                    'crs_parcel',
                    'crs_proprietor',
                    'crs_stat_act_parcl',
                    'crs_statute',
                    'crs_statute_action',
                    'crs_survey',
                    'crs_sys_code',
                    'crs_title',
                    'crs_title_estate',
                    'crs_topology_class'
                ],
                'any affected'
            )
        )
        AND LDS.LDS_TableHasData('lds', 'primary_parcels')
        AND LDS.LDS_TableHasData('lds', 'land_parcels')
        AND LDS.LDS_TableHasData('lds', 'hydro_parcels')
        AND LDS.LDS_TableHasData('lds', 'non_primary_parcels')
        AND LDS.LDS_TableHasData('lds', 'non_primary_linear_parcels')
        AND LDS.LDS_TableHasData('lds', 'road_parcels')
        AND LDS.LDS_TableHasData('lds', 'strata_parcels')
        AND LDS.LDS_TableHasData('lds', 'titles')
        AND LDS.LDS_TableHasData('lds', 'titles_plus')
        AND LDS.LDS_TableHasData('lds', 'title_owners')
    )
    THEN
        PERFORM bde_control.bde_WriteUploadLog(
            p_upload, 
            '1',
            'Maintain cadastral parcel simplified layers has been skipped as no ' ||
            'relating tables were affected by the upload'
        );
        RETURN 1;
    END IF;

    
    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating temp table tmp_excluded_titles'
    );

    CREATE TEMP TABLE tmp_excluded_titles AS
    SELECT
        DISTINCT TTL.title_no
    FROM
        crs_title            TTL
    WHERE
    (
        title_no LIKE 'WNTRAIN%' OR
        title_no = 'WNTESTDATAONLY'
    ) OR
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
    );

    ALTER TABLE tmp_excluded_titles ADD PRIMARY KEY (title_no);
    ANALYSE tmp_excluded_titles;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating temp table tmp_parcel_titles'
    );

    CREATE TEMP TABLE tmp_parcel_titles AS
    SELECT
        PAR.id AS par_id,
        TTL.title_no
    FROM
        crs_parcel PAR
        JOIN crs_legal_desc_prl LGP ON PAR.id = LGP.par_id
        JOIN crs_legal_desc LGD ON LGP.lgd_id = LGD.id
        JOIN crs_title TTL ON LGD.ttl_title_no = TTL.title_no
    WHERE
        PAR.status = 'CURR' AND
        LGD.ttl_title_no IS NOT NULL AND
        TTL.status IN ('LIVE', 'PRTC') AND
        TTL.title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
    GROUP BY
        PAR.id,
        TTL.title_no;

    ALTER TABLE tmp_parcel_titles ADD PRIMARY KEY (par_id, title_no);

    ANALYSE tmp_parcel_titles;
    
    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating temp table tmp_par_stat_action'
    );
    
    CREATE TEMP TABLE tmp_par_stat_action AS
    SELECT
        SAP.par_id,
        string_agg(bde_get_par_stat_act(SAP.sta_id, SAP.par_id), E'\r\n') AS statutory_actions
    FROM
        crs_stat_act_parcl SAP
    WHERE
        SAP.status = 'CURR'
    GROUP BY
        SAP.par_id;

    ALTER TABLE tmp_par_stat_action ADD PRIMARY KEY (par_id);
    ANALYSE tmp_par_stat_action;
    
    -- make region data for determining how to calc areas
    CREATE TEMP TABLE tmp_world_regions (
        id INTEGER NOT NULL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        shape GEOMETRY NOT NULL
    );
    
    INSERT INTO tmp_world_regions (id, name, shape) VALUES
    ( 1, 'chathams',   'SRID=4167;POLYGON((183 -43.5,184 -43.5,184 -44.5,183 -44.5,183 -43.5))' ),
    ( 2, 'nz',         'SRID=4167;POLYGON((166 -34,179 -34,179 -47.5,166 -47.5,166 -34))' );
    
    CREATE INDEX tmp_world_regions_shpx ON tmp_world_regions USING gist (shape);
    ANALYSE tmp_world_regions;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating temp table tmp_current_parcels'
    );

    CREATE TEMP TABLE tmp_current_parcels AS
    SELECT
        PAR.id,
        bde_get_combined_appellation(PAR.id, 'N') AS appellation,
        string_agg(
            DISTINCT 
            CASE WHEN SUR.dataset_suffix IS NULL THEN
                SUR.dataset_series || ' ' || SUR.dataset_id
            ELSE
                SUR.dataset_series || ' ' || SUR.dataset_id || '/' || SUR.dataset_suffix
            END,
            ', ' 
        ) AS affected_surveys,
        PAR.parcel_intent,
        PAR.toc_code,
        PSA.statutory_actions,
        LOC.name AS land_district,
        string_agg(DISTINCT TTL.title_no, ', ' ) AS titles,
        COALESCE(PAR.total_area, PAR.area) AS survey_area,
        CASE WHEN WDR.name = 'chathams' THEN
            CAST(ST_Area(ST_Transform(PAR.shape, 3793)) AS NUMERIC(20, 4))
        ELSE
            CAST(ST_Area(ST_Transform(PAR.shape, 2193)) AS NUMERIC(20, 4))
        END AS calc_area,
        PAR.shape
    FROM
        tmp_world_regions WDR,
        crs_parcel PAR
        JOIN crs_locality LOC ON PAR.ldt_loc_id = LOC.id
        LEFT JOIN tmp_parcel_titles TTL ON PAR.id = TTL.par_id
        LEFT JOIN tmp_par_stat_action PSA ON PAR.id = PSA.par_id
        LEFT JOIN crs_affected_parcl AFP ON PAR.id = AFP.par_id
        LEFT JOIN crs_survey SUR ON AFP.sur_wrk_id = SUR.wrk_id
    WHERE
        PAR.status = 'CURR' AND
        ST_Contains(WDR.shape, PAR.shape)
    GROUP BY
        1, 2, 4, 5, 6, 7, 9, 10, 11
    ORDER BY
        PAR.id;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Finished creating temp table tmp_current_parcels'
    );
    
    DROP TABLE IF EXISTS tmp_world_regions;
    
    ----------------------------------------------------------------------------
    -- primary_parcels layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'primary_parcels');
    
    v_data_insert_sql := $sql$
    INSERT INTO %1% (
        id,
        appellation,
        affected_surveys,
        parcel_intent,
        topology_type,
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
    WHERE
        PAR.toc_code =  'PRIM' AND
        ST_GeometryType(PAR.shape) IN ('ST_MultiPolygon', 'ST_Polygon');
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- land_parcels layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'land_parcels');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            appellation,
            affected_surveys,
            parcel_intent,
            topology_type,
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
        WHERE
            PAR.toc_code =  'PRIM' AND
            PAR.parcel_intent NOT IN ('HYDR', 'ROAD') AND
            ST_GeometryType(PAR.shape) IN ('ST_MultiPolygon', 'ST_Polygon');
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- hydro_parcels layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'hydro_parcels');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            appellation,
            affected_surveys,
            parcel_intent,
            topology_type,
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
            SYSP.char_value AS parcel_intent,
            TOC.name AS topology_type,
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
        WHERE
            PAR.toc_code =  'PRIM' AND
            PAR.parcel_intent = 'HYDR' AND
            ST_GeometryType(PAR.shape) IN ('ST_MultiPolygon', 'ST_Polygon');
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- road_parcels layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'road_parcels');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            appellation,
            affected_surveys,
            parcel_intent,
            topology_type,
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
            SYSP.char_value AS parcel_intent,
            TOC.name AS topology_type,
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
        WHERE
            PAR.toc_code =  'PRIM' AND
            PAR.parcel_intent = 'ROAD' AND
            ST_GeometryType(PAR.shape) IN ('ST_MultiPolygon', 'ST_Polygon');
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- non_primary_parcels layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'non_primary_parcels');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            appellation,
            affected_surveys,
            parcel_intent,
            topology_type,
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
        WHERE
            PAR.toc_code IN ('SECO', 'TERT', 'STRA') AND
            ST_GeometryType(PAR.shape) IN ('ST_MultiPolygon', 'ST_Polygon');
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- non_primary_linear_parcels layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'non_primary_linear_parcels');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            appellation,
            affected_surveys,
            parcel_intent,
            topology_type,
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

    ----------------------------------------------------------------------------
    -- strata_parcels layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'strata_parcels');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            appellation,
            affected_surveys,
            parcel_intent,
            topology_type,
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
        WHERE
            PAR.toc_code IN ('STRA') AND
            ST_GeometryType(PAR.shape) IN ('ST_MultiPolygon', 'ST_Polygon');
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    -- These temp table not required from here on...
    DROP TABLE IF EXISTS tmp_par_stat_action;
    DROP TABLE IF EXISTS tmp_current_parcels;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating temp table tmp_titles'
    );
    
    CREATE TEMP TABLE tmp_titles AS
    SELECT
        TTL.audit_id AS id,
        TTL.title_no,
        TTL.status,
        TTLT.char_value AS type,
        LOC.name AS land_district,
        TTL.issue_date,
        TTLG.char_value AS guarantee_status,
        string_agg(
            DISTINCT(
                ETTT.char_value || ', ' || 
                ETT.share || COALESCE(', ' || LGD.legal_desc_text, '') ||
                COALESCE(', ' || to_char(ROUND(LGD.total_area, 0), 'FM9G999G999G999G999') || ' m�', '')
            ),
            E'\r\n'
        ) AS estate_description,
        string_agg(
            DISTINCT 
            CASE PRP.type
                WHEN 'CORP' THEN PRP.corporate_name
                WHEN 'PERS' THEN COALESCE(PRP.prime_other_names || ' ', '') || PRP.prime_surname
            END,
            ', '
        ) AS owners,
        count(
        DISTINCT
            CASE PRP.type
                WHEN 'CORP' THEN PRP.corporate_name
                WHEN 'PERS' THEN COALESCE(PRP.prime_other_names || ' ', '') || PRP.prime_surname
            END
        ) AS number_owners,
        TPA.title_no IS NOT NULL AS part_share,
        ST_Multi(ST_Collect(PAR.shape)) AS shape
    FROM
        crs_title TTL
        LEFT JOIN crs_title_estate ETT ON TTL.title_no = ETT.ttl_title_no AND ETT.status = 'REGD'
        LEFT JOIN crs_estate_share ETS ON ETT.id = ETS.ett_id AND ETT.status = 'REGD'
        LEFT JOIN crs_proprietor PRP ON ETS.id = PRP.ets_id AND PRP.status = 'REGD'
        LEFT JOIN crs_legal_desc LGD ON ETT.lgd_id = LGD.id AND LGD.type = 'ETT' AND LGD.status = 'REGD'
        LEFT JOIN crs_legal_desc_prl LGP ON LGD.id = LGP.lgd_id
        LEFT JOIN (
            SELECT
                title_no 
            FROM
                tmp_parcel_titles 
            GROUP BY
                title_no
            HAVING
                count(*) > 1
        ) TPA ON TTL.title_no = TPA.title_no
        LEFT JOIN (
            SELECT
                id,
                (ST_Dump(shape)).geom AS shape  
            FROM
                crs_parcel 
            WHERE
                status = 'CURR' AND 
                ST_GeometryType(shape) IN ('ST_MultiPolygon', 'ST_Polygon')
        ) PAR ON LGP.par_id = PAR.id 
        JOIN crs_locality LOC ON TTL.ldt_loc_id = LOC.id
        JOIN crs_sys_code TTLG ON TTL.guarantee_status = TTLG.code AND TTLG.scg_code = 'TTLG'
        JOIN crs_sys_code TTLT ON TTL.type = TTLT.code AND TTLT.scg_code = 'TTLT'
        LEFT JOIN crs_sys_code ETTT ON ETT.type = ETTT.code AND ETTT.scg_code = 'ETTT'
    WHERE
        TTL.status IN ('LIVE', 'PRTC') AND
        TTL.title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
    GROUP BY
        TTL.audit_id,
        TTL.title_no,
        TTL.status,
        TTLT.char_value,
        LOC.name,
        TTL.issue_date,
        TTLG.char_value,
        TPA.title_no;

    DROP TABLE IF EXISTS tmp_parcel_titles;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Finished creating temp table tmp_titles'
    );
    
    ----------------------------------------------------------------------------
    -- titles layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'titles');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            title_no,
            status,
            type,
            land_district,
            issue_date,
            guarantee_status,
            estate_description,
            number_owners,
            part_share,
            shape
        )
        SELECT
            id,
            title_no,
            status, 
            type,
            land_district,
            issue_date,
            guarantee_status,
            estate_description,
            number_owners,
            part_share,
            shape
        FROM
            tmp_titles;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- titles_plus layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'titles_plus');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            title_no,
            status,
            type,
            land_district,
            issue_date,
            guarantee_status,
            estate_description,
            owners,
            part_share,
            shape
        )
        SELECT
            id,
            title_no,
            status,
            type,
            land_district,
            issue_date,
            guarantee_status,
            estate_description,
            owners,
            part_share,
            shape
        FROM
            tmp_titles;
    $sql$;

    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    DROP TABLE IF EXISTS tmp_titles;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating temp table tmp_title_owners'
    );

    CREATE TEMP TABLE tmp_title_owners AS
    SELECT
        CASE PRP.type
            WHEN 'CORP' THEN PRP.corporate_name
            WHEN 'PERS' THEN COALESCE(PRP.prime_other_names || ' ', '') || PRP.prime_surname
        END AS owner,
        TTL.title_no,
        TTL.status AS title_status,
        ETS.share,
        LOC.name AS land_district,
        ST_Multi(ST_Collect(PAR.shape)) AS shape
    FROM
        crs_title TTL
        JOIN crs_title_estate ETT ON TTL.title_no = ETT.ttl_title_no AND ETT.status = 'REGD'
        JOIN crs_estate_share ETS ON ETT.id = ETS.ett_id AND ETT.status = 'REGD'
        JOIN crs_proprietor PRP ON ETS.id = PRP.ets_id AND PRP.status = 'REGD'
        LEFT JOIN crs_legal_desc LGD ON ETT.lgd_id = LGD.id AND LGD.type = 'ETT' AND LGD.status = 'REGD'
        LEFT JOIN crs_legal_desc_prl LGP ON LGD.id = LGP.lgd_id
        LEFT JOIN (
            SELECT
                id,
                (ST_Dump(shape)).geom AS shape  
            FROM
                crs_parcel 
            WHERE
                status = 'CURR' AND 
                ST_GeometryType(shape) IN ('ST_MultiPolygon', 'ST_Polygon')
        ) PAR ON LGP.par_id = PAR.id 
        JOIN crs_locality LOC ON TTL.ldt_loc_id = LOC.id
    WHERE
        TTL.status IN ('LIVE', 'PRTC') AND
        TTL.title_no NOT IN (SELECT title_no FROM tmp_excluded_titles)
    GROUP BY
        PRP.type,
        PRP.corporate_name,
        PRP.prime_other_names,
        PRP.prime_surname,
        TTL.title_no,
        TTL.status,
        ETS.share,
        LOC.name;
    
    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Finished creating temp table tmp_title_owners'
    );

    ----------------------------------------------------------------------------
    -- title_owners layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'title_owners');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            owner,
            title_no,
            title_status,
            land_district,
            share,
            shape
        )
        SELECT
            owner,
            title_no,
            title_status,
            land_district,
            share,
            shape
        FROM
            tmp_title_owners;
    $sql$;

    v_data_diff_sql := $sql$
        INSERT INTO %1% (
            id,
            owner,
            title_no,
            title_status,
            land_district,
            share,
            shape
        )
        SELECT
            COALESCE(ORG.id, nextval('lds.title_owners_id_seq')),
            TMP.owner,
            TMP.title_no,
            TMP.title_status,
            TMP.land_district,
            TMP.share,
            TMP.shape
        FROM
            tmp_title_owners AS TMP
            LEFT JOIN %2% AS ORG ON (ORG.owner = TMP.owner AND ORG.title_no = TMP.title_no)
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_diff_sql,
        v_data_insert_sql
    );

    DROP TABLE IF EXISTS tmp_title_owners;
    DROP TABLE IF EXISTS tmp_excluded_titles;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload, 
        '2',
        'Finished maintenance on cadastral parcel simplified layers'
    );
    
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_MaintainSimplifiedParcelLayers(INTEGER) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION LDS_MaintainSimplifiedElectoralLayers(
    p_upload INTEGER
)
RETURNS
    INTEGER AS $$
DECLARE
    v_table            REGCLASS;
    v_data_diff_sql    TEXT;
    v_data_insert_sql  TEXT;
BEGIN
    PERFORM bde_control.bde_WriteUploadLog(
        p_upload, 
        '2',
        'Starting maintenance on electoral simplified layers'
    );

    IF (
        NOT (
            SELECT bde_control.bde_TablesAffected(
                p_upload,
                ARRAY[
                    'crs_street_address',
                    'crs_road_name',
                    'crs_road_ctr_line',
                    'crs_road_name_asc'
                ],
                'any affected'
            )
        )
        AND LDS.LDS_TableHasData('lds', 'road_centre_line')
        AND LDS.LDS_TableHasData('lds', 'railway_centre_line')
        AND LDS.LDS_TableHasData('lds', 'street_address')
        AND LDS.LDS_TableHasData('lds', 'road_centre_line_subsection')
    )
    THEN
        PERFORM bde_control.bde_WriteUploadLog(
            p_upload, 
            '1',
            'Maintain electoral simplified layers has been skipped as no ' ||
            'relating tables were affected by the upload'
        );
        RETURN 1;
    END IF;

    ----------------------------------------------------------------------------
    -- road_centre_line layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'road_centre_line');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            "name",
            location,
            shape
        )
        SELECT
            RNA.id,
            RNA.name,
            RNA.location,
            ST_Collect(RCL.shape)
        FROM
            crs_road_ctr_line RCL,
            crs_road_name RNA,
            crs_road_name_asc RNS
        WHERE
            RCL.id = RNS.rcl_id AND
            RNA.id = RNS.rna_id AND
            RNA.status = 'CURR' AND
            RNA.type = 'ROAD'
        GROUP BY
            RNA.id,
            RNA.name,
            RNA.location;
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- road_centre_line_subsection layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'road_centre_line_subsection');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            road_id,
            "name",
            location,
            parcel_derived,
            shape
        )
        SELECT
            RCL.id,
            RNA.id AS road_id,
            RNA.name,
            RNA.location,
            CASE WHEN non_cadastral_rd = 'Y' THEN
                TRUE
            ELSE
                FALSE
            END AS parcel_derived,
            RCL.shape
        FROM
            crs_road_ctr_line RCL,
            crs_road_name RNA,
            crs_road_name_asc RNS
        WHERE
            RCL.id = RNS.rcl_id AND
            RNA.id = RNS.rna_id AND
            RNA.status = 'CURR' AND
            RNA.type = 'ROAD';
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    ----------------------------------------------------------------------------
    -- railway_centre_line layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'railway_centre_line');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id, 
            "name",
             shape
        )
        SELECT
            RNA.id,
            RNA.name,
            ST_Collect(RCL.shape)
        FROM
            crs_road_ctr_line RCL,
            crs_road_name RNA,
            crs_road_name_asc RNS
        WHERE
            RCL.id = RNS.rcl_id AND
            RNA.id = RNS.rna_id AND
            RNA.status = 'CURR' AND
            RNA.type = 'RLWY'
        GROUP BY
            RNA.id,
            RNA.name,
            RNA.location;
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- street_address layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'street_address');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            address_string,
            house_number,
            road_name,
            location,
            shape
        )
        SELECT
            SAD.id,
            SAD.house_number || ' ' || RNA.name,
            SAD.house_number,
            RNA.name,
            RNA.location,
            SAD.shape
        FROM
            crs_street_address SAD,
            crs_road_name RNA
        WHERE
            RNA.id = SAD.rna_id AND
            SAD.status = 'CURR'
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    PERFORM bde_control.bde_WriteUploadLog(
        p_upload, 
        '2',
        'Finished maintenance on electoral simplified layers'
    );
    
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_MaintainSimplifiedElectoralLayers(INTEGER) OWNER TO bde_dba;

CREATE OR REPLACE FUNCTION LDS_MaintainSimplifiedSurveyLayers(
    p_upload INTEGER
)
RETURNS
    INTEGER AS $$
DECLARE
    v_table            REGCLASS;
    v_data_diff_sql    TEXT;
    v_data_insert_sql  TEXT;
BEGIN
    PERFORM bde_control.bde_WriteUploadLog(
        p_upload, 
        '2',
        'Starting maintenance on cadastral survey simplified layers'
    );

    IF (
        NOT (
            SELECT bde_control.bde_TablesAffected(
                p_upload,
                ARRAY[
                    'crs_adjust_method',
                    'crs_adjustment_run',
                    'crs_coordinate',
                    'crs_coordinate_sys',
                    'crs_cord_order',
                    'crs_land_district',
                    'crs_line',
                    'crs_locality',
                    'crs_mark',
                    'crs_mark_name',
                    'crs_mrk_phys_state',
                    'crs_node',
                    'crs_obs_elem_type',
                    'crs_observation',
                    'crs_ordinate_adj',
                    'crs_parcel',
                    'crs_parcel_bndry',
                    'crs_parcel_dimen',
                    'crs_parcel_ring',
                    'crs_setup',
                    'crs_sur_plan_ref',
                    'crs_survey',
                    'crs_sys_code',
                    'crs_transact_type',
                    'crs_vector',
                    'crs_work'
                ],
                'any affected'
            )
        )
        AND LDS.LDS_TableHasData('lds', 'land_districts')
        AND LDS.LDS_TableHasData('lds', 'survey_plans')
        AND LDS.LDS_TableHasData('lds', 'cadastral_adjustments')
        AND LDS.LDS_TableHasData('lds', 'survey_observations')
        AND LDS.LDS_TableHasData('lds', 'survey_arc_observations')
        AND LDS.LDS_TableHasData('lds', 'parcel_vectors')
        AND LDS.LDS_TableHasData('lds', 'survey_network_marks')
        AND LDS.LDS_TableHasData('lds', 'survey_bdy_marks')
        AND LDS.LDS_TableHasData('lds', 'survey_non_bdy_marks')
    )
    THEN
        PERFORM bde_control.bde_WriteUploadLog(
            p_upload, 
            '1',
            'Maintain cadastral survey simplified layers has been skipped as no ' ||
            'relating tables were affected by the upload'
        );
        RETURN 1;
    END IF;

    ----------------------------------------------------------------------------
    -- land_districts layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'land_districts');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1%(
            id,
            name,
            shape
        )
        SELECT
            LOC.id,
            LOC.name,
            LDT.shape
        FROM
            crs_locality LOC
            JOIN crs_land_district LDT ON LOC.id = LDT.loc_id;
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    -- built mapping for datums

    CREATE TEMP TABLE tmp_proxy_datum AS
    SELECT id AS cos_id,
           'NZGD2000'::VARCHAR(10) AS name 
    FROM   crs_coordinate_sys
    WHERE  dtm_id = 19;

    INSERT INTO tmp_proxy_datum
    SELECT id AS cos_id,
           'NZGD1949'::VARCHAR(10) AS name 
    FROM   crs_coordinate_sys
    WHERE  dtm_id = 18;

    INSERT INTO tmp_proxy_datum
    SELECT id AS cos_id,
           'OCD'::VARCHAR(10) AS name 
    FROM   crs_coordinate_sys
    WHERE  cot_id = 65 
    AND    dtm_id <> 10;

    ALTER TABLE tmp_proxy_datum ADD CONSTRAINT
           pkey_proxy_datum PRIMARY KEY (cos_id);

    ANALYSE tmp_proxy_datum;
       
    ----------------------------------------------------------------------------
    -- survey_plans layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'survey_plans');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1%(
            id,
            survey_reference,
            land_district,
            description,
            status,
            survey_date,
            purpose,
            type,
            datum,
            shape
        )
        SELECT
            WRK.id,
            CASE WHEN SUR.dataset_suffix IS NULL THEN
                SUR.dataset_series || ' ' || SUR.dataset_id
            ELSE
                SUR.dataset_series || ' ' || SUR.dataset_id || '/' || SUR.dataset_suffix
            END AS survey_reference,
            LOC.name AS land_district,
            SUR.description,
            SCO2.char_value AS status,
            SUR.survey_date,
            SCO1.char_value AS purpose,
            TRT.description AS type,
            COALESCE(DTM.name, 'UNKNOWN') AS datum,
            ST_Collect(SPF.shape) AS shape
        FROM
            crs_survey SUR,
            crs_work WRK
                LEFT JOIN tmp_proxy_datum DTM ON (WRK.cos_id = DTM.cos_id),
            crs_sur_plan_ref SPF,
            crs_transact_type TRT,
            crs_sys_code SCO1,
            crs_sys_code SCO2,
            crs_locality LOC
        WHERE
            WRK.id = SUR.wrk_id AND
            SUR.wrk_id = SPF.wrk_id AND
            SUR.ldt_loc_id = LOC.id AND
            WRK.trt_grp = TRT.grp AND
            WRK.trt_type = TRT.type AND
            SCO1.scg_code = 'SURT' AND
            SCO1.code = SUR.type_of_dataset AND
            SCO2.scg_code = 'WRKC' AND
            SCO2.code = WRK.status AND
            SPF.shape IS NOT NULL
        GROUP BY
            1, 2, 3, 4, 5, 6, 7, 8, 9
        ORDER BY
            WRK.id;
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    DROP TABLE IF EXISTS tmp_proxy_datum;

    ----------------------------------------------------------------------------
    -- cadastral_adjustments layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'cadastral_adjustments');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1%(
            id,
            date_adjusted,
            survey_reference,
            adjusted_nodes,
            shape
        )
        SELECT
            ADJ.id,
            ADJ.adjust_datetime AS date_adjusted,
            CASE WHEN SUR.dataset_suffix IS NULL THEN
                SUR.dataset_series || ' ' || SUR.dataset_id
            ELSE
                SUR.dataset_series || ' ' || SUR.dataset_id || '/' || SUR.dataset_suffix
            END AS survey_reference,
            count(*) AS adjusted_nodes,
            CASE WHEN count(*) < 3 THEN
                ST_Buffer(St_ConvexHull(ST_Collect(NOD.shape)), 0.00001, 4)
            ELSE
                St_ConvexHull(ST_Collect(NOD.shape))
            END  AS shape
        FROM
            crs_adjustment_run ADJ
            JOIN crs_adjust_method ADM ON ADJ.adm_id = ADM.id
            LEFT JOIN crs_survey SUR ON ADJ.wrk_id = SUR.wrk_id
            JOIN crs_ordinate_adj ORJ ON ADJ.id = ORJ.adj_id
            JOIN crs_coordinate COO ON ORJ.coo_id_output = COO.id
            JOIN crs_node NOD ON COO.nod_id = NOD.id
        WHERE
            ADJ.status = 'AUTH' AND
            ADM.software_used = 'LNZC' AND
            NOD.cos_id_official = 109
        GROUP BY
            ADJ.id,
            ADJ.adjust_datetime,
            SUR.dataset_suffix,
            SUR.dataset_series,
            SUR.dataset_id
        ORDER BY
            ADJ.id;
    $sql$;
        
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- survey_observations layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'survey_observations');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1%(
            id,
            nod_id_start,
            nod_id_end,
            value,
            obs_type,
            surveyed_class,
            coordinate_system,
            ref_datetime,
            survey_reference,
            shape
        )
        SELECT
            OBN.id,
            VCT.nod_id_start,
            VCT.nod_id_end,
            OBN.value_1,
            OET.description,
            SCO.char_value,
            COS.name,
            OBN.ref_datetime,
            CASE WHEN SUR.dataset_suffix IS NULL THEN
                SUR.dataset_series || ' ' || SUR.dataset_id
            ELSE
                SUR.dataset_series || ' ' || SUR.dataset_id || '/' || SUR.dataset_suffix
            END AS survey_reference,
            VCT.shape
        FROM
            crs_observation OBN
            JOIN crs_obs_elem_type OET ON OBN.obt_sub_type = OET.type
            JOIN crs_setup STP ON OBN.stp_id_local = STP.id
            JOIN crs_vector VCT ON OBN.vct_id = VCT.id
            JOIN crs_coordinate_sys COS ON OBN.cos_id = COS.id
            JOIN crs_sys_code SCO ON OBN.surveyed_class = SCO.code AND SCO.scg_code = 'OBEC'
            LEFT JOIN crs_survey SUR ON STP.wrk_id = SUR.wrk_id
        WHERE
            OBN.rdn_id IS NULL AND
            OBN.obt_type = 'REDC' AND
            OBN.obt_sub_type IN ('SLDI', 'BEAR') AND
            OBN.status = 'AUTH' AND
            OBN.surveyed_class IN ('ADPT', 'CALC', 'MEAS') AND
            VCT.id = OBN.vct_id AND
            VCT.type = 'LINE';
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- survey_arc_observations layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'survey_arc_observations');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1%(
            id,
            nod_id_start,
            nod_id_end,
            chord_bearing,
            arc_length,
            arc_radius,
            arc_direction,
            surveyed_class,
            coordinate_system,
            ref_datetime,
            survey_reference,
            shape
        )
        SELECT
            OBN.id,
            VCT.nod_id_start,
            VCT.nod_id_end,
            OBN.value_1,
            OBN.value_2,
            OBN.arc_radius,
            OBN.arc_direction,
            SCO.char_value,
            COS.name,
            OBN.ref_datetime,
            CASE WHEN SUR.dataset_suffix IS NULL THEN
                SUR.dataset_series || ' ' || SUR.dataset_id
            ELSE
                SUR.dataset_series || ' ' || SUR.dataset_id || '/' || SUR.dataset_suffix
            END AS survey_reference,
            VCT.shape
        FROM
            crs_observation OBN
            JOIN crs_setup STP ON OBN.stp_id_local = STP.id
            JOIN crs_vector VCT ON OBN.vct_id = VCT.id
            JOIN crs_coordinate_sys COS ON OBN.cos_id = COS.id
            JOIN crs_sys_code SCO ON OBN.surveyed_class = SCO.code AND SCO.scg_code = 'OBEC'
            LEFT JOIN crs_survey SUR ON STP.wrk_id = SUR.wrk_id
        WHERE
            OBN.rdn_id IS NULL AND
            OBN.obt_type = 'REDC' AND
            OBN.obt_sub_type = 'ARCO' AND
            OBN.status = 'AUTH' AND
            OBN.surveyed_class IN ('ADPT', 'CALC', 'MEAS') AND
            VCT.id = OBN.vct_id AND
            VCT.type = 'LINE';
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- parcel_vectors layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'parcel_vectors');

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating temp table tmp_parcel_vectors'
    );

    CREATE TEMP TABLE tmp_parcel_vectors AS
    SELECT
        OBN.vct_id
    FROM
        crs_observation OBN
        JOIN crs_setup STP ON OBN.stp_id_local = STP.id
        JOIN crs_vector VCT ON VCT.id = OBN.vct_id
        LEFT JOIN crs_parcel_dimen PDI ON OBN.id = PDI.obn_id
        LEFT JOIN crs_parcel PAR ON PDI.par_id = PAR.id AND PAR.status = 'CURR' AND PAR.toc_code = 'PRIM'
    WHERE
        OBN.rdn_id IS NULL AND
        OBN.obt_type = 'REDC' AND
        OBN.obt_sub_type IN ('ARCO', 'SLDI', 'BEAR') AND
        OBN.status = 'AUTH' AND
        OBN.surveyed_class IN ('ADPT','CALC','MEAS') AND
        VCT.type = 'LINE' AND
        PAR.id IS NOT NULL
    GROUP BY
        OBN.vct_id;
    
    ANALYSE tmp_parcel_vectors;


    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating table tmp_parcel_vector_detail'
    );

    CREATE TEMP TABLE tmp_parcel_vector_detail AS
    WITH latest_vector (row_number, id, bearing, distance, type, shape) AS (
        SELECT
            row_number() OVER (PARTITION BY TPV.vct_id),
            VCT.id,
            first_value(OBN.value_1) OVER (PARTITION BY TPV.vct_id ORDER BY OBN.ref_datetime DESC),
            first_value(OBN.value_2) OVER (PARTITION BY TPV.vct_id ORDER BY OBN.ref_datetime DESC),
            'Arc'::TEXT,
            VCT.shape
        FROM
            tmp_parcel_vectors TPV
            JOIN crs_vector VCT ON TPV.vct_id = VCT.id
            JOIN crs_observation OBN ON TPV.vct_id = OBN.vct_id
        WHERE
            OBN.rdn_id IS NULL AND
            OBN.obt_type = 'REDC' AND
            OBN.obt_sub_type = 'ARCO' AND
            OBN.status = 'AUTH' AND
            OBN.surveyed_class IN ('ADPT', 'CALC', 'MEAS')
    )
    SELECT
        id,
        bearing,
        distance,
        type,
        shape
    FROM
        latest_vector
    WHERE
        row_number = 1;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started inserting vector rows into table tmp_parcel_vector_detail'
    );
    
    INSERT INTO tmp_parcel_vector_detail(
        id,
        bearing,
        distance,
        type,
        shape
    )
    WITH latest_vector (row_number, id, bearing, distance, type, shape) AS (
        SELECT
            row_number() OVER (PARTITION BY TPV.vct_id),
            VCT.id,
            first_value(OBN_B.value_1) OVER (PARTITION BY TPV.vct_id ORDER BY OBN_B.ref_datetime DESC),
            first_value(OBN_D.value_1) OVER (PARTITION BY TPV.vct_id ORDER BY OBN_D.ref_datetime DESC),
            'Vector'::TEXT,
            VCT.shape
        FROM
            tmp_parcel_vectors TPV
            JOIN crs_vector VCT ON TPV.vct_id = VCT.id
            JOIN crs_observation OBN_B ON TPV.vct_id = OBN_B.vct_id AND
                OBN_B.rdn_id IS NULL AND
                OBN_B.obt_type = 'REDC' AND
                OBN_B.obt_sub_type = 'BEAR' AND
                OBN_B.status = 'AUTH' AND
                OBN_B.surveyed_class IN ('ADPT', 'CALC', 'MEAS')
            LEFT JOIN crs_observation OBN_D ON TPV.vct_id = OBN_D.vct_id AND
                OBN_D.rdn_id IS NULL AND
                OBN_D.obt_type = 'REDC' AND
                OBN_D.obt_sub_type = 'SLDI' AND
                OBN_D.status = 'AUTH' AND
                OBN_D.surveyed_class IN ('ADPT', 'CALC', 'MEAS')
    )
    SELECT 
        LVT.id,
        LVT.bearing,
        LVT.distance,
        LVT.type,
        LVT.shape
    FROM
        latest_vector LVT
        LEFT JOIN tmp_parcel_vector_detail PVD ON LVT.id = PVD.id
    WHERE
        LVT.row_number = 1 AND
        PVD.id IS NULL;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Finished creating table tmp_parcel_vector_detail'
    );
    
    v_data_insert_sql := $sql$
        INSERT INTO %1%(
            id,
            bearing,
            distance,
            type,
            shape
        )
        SELECT
            id,
            bearing,
            distance,
            type,
            shape
        FROM
            tmp_parcel_vector_detail
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    DROP TABLE IF EXISTS tmp_parcel_vectors;
    DROP TABLE IF EXISTS tmp_parcel_vector_detail;

    ----------------------------------------------------------------------------
    -- survey_network_marks layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'survey_network_marks');
    
    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            geodetic_code, 
            current_mark_name, 
            description, 
            mark_type,
            mark_condition,
            "order",
            last_survey,
            shape
        )
        WITH t (
            row_number,
            id,
            geodetic_code,
            current_mark_name,
            description,
            mark_type,
            mark_condition,
            "order",
            last_survey,
            shape
        ) AS (
            SELECT
                row_number() OVER (PARTITION BY NOD.id ORDER BY MRK.status, MRK.replaced) AS row_number,
                NOD.id,
                GEO.name AS geodetic_code, 
                MKN.name AS current_mark_name,
                MRK.desc AS description, 
                RTRIM(mrk.type) AS mark_type,
                SCOC.char_value AS mark_condition,
                CAST(COR.display AS INTEGER) AS "order",
                CASE WHEN SUR.dataset_suffix IS NULL THEN
                    SUR.dataset_series || ' ' || SUR.dataset_id
                ELSE
                    SUR.dataset_series || ' ' || SUR.dataset_id || '/' || SUR.dataset_suffix
                END AS last_survey,
                NOD.shape
            FROM
                crs_node NOD
                JOIN crs_coordinate COO ON NOD.cos_id_official = COO.cos_id AND NOD.id = COO.nod_id AND COO.status = 'AUTH'
                JOIN crs_cord_order COR ON COO.cor_id = COR.id
                LEFT JOIN crs_mark MRK ON MRK.nod_id = NOD.id
                LEFT JOIN crs_mark_name MKN ON MRK.id = MKN.mrk_id AND MKN.type = 'CURR'
                LEFT JOIN crs_mark_name GEO ON MRK.id = GEO.mrk_id AND GEO.type = 'CODE'
                LEFT JOIN crs_mrk_phys_state MPS ON MPS.mrk_id = MRK.id AND MPS.type = 'MARK' AND MPS.status = 'CURR'
                LEFT JOIN crs_survey SUR ON MPS.wrk_id = SUR.wrk_id
                LEFT JOIN crs_sys_code SCOC ON MPS.condition = SCOC.code AND SCOC.scg_code = 'MPSC'
            WHERE
                COO.cor_id < 1908 AND
                NOD.cos_id_official = 109 AND
                NOD.status = 'AUTH'
        )
        SELECT
            id,
            geodetic_code,
            current_mark_name,
            description,
            mark_type,
            mark_condition,
            "order",
            last_survey,
            shape
        FROM
            t
        WHERE
            row_number = 1
        ORDER BY
            id;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    -- Temp tables required for survey_bdy_marks and survey_non_bdy_marks

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating table tmp_bdy_nodes'
    );
    
    CREATE TEMP TABLE tmp_bdy_nodes AS
    WITH bdy_nodes (nod_id_start, nod_id_end) AS (
        SELECT
            LIN.nod_id_start,
            LIN.nod_id_end
        FROM
            crs_parcel PAR,
            crs_parcel_ring PRI,
            crs_parcel_bndry PBD,
            crs_line LIN
        WHERE
            PAR.id = PRI.par_id AND
            PRI.id = PBD.pri_id AND
            PBD.lin_id = LIN.id AND
            PAR.status = 'CURR'
    )
    SELECT
        nod_id_start AS nod_id
    FROM
        bdy_nodes
    UNION
    SELECT
        nod_id_end AS nod_id
    FROM
        bdy_nodes;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating table tmp_node_last_adjusted'
    );

    CREATE TEMP TABLE tmp_node_last_adjusted AS
    SELECT
        NOD.id AS nod_id,
        MAX(ADJ.adjust_datetime) AS last_adjusted
    FROM
        crs_node NOD
        JOIN crs_coordinate COO ON NOD.id = COO.nod_id AND COO.cos_id = NOD.cos_id_official AND COO.status = 'AUTH'
        LEFT JOIN crs_ordinate_adj ORJ ON COO.id = ORJ.coo_id_output
        LEFT JOIN crs_adjustment_run ADJ ON ORJ.adj_id = ADJ.id
    WHERE
        NOD.status = 'AUTH' AND
        NOD.cos_id_official = 109 AND
        ADJ.status = 'AUTH'
    GROUP BY
        NOD.id;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Started creating table tmp_cadastral_marks'
    );

    CREATE TEMP TABLE tmp_cadastral_marks AS
    WITH t (
        row_number,
        id,
        name,
        "order",
        nominal_accuracy,
        last_adjusted,
        is_bdy,
        shape
    ) AS (
        SELECT
            row_number() OVER (PARTITION BY NOD.id ORDER BY MRK.status, MRK.replaced) AS row_number,
            NOD.id,
            MKN.name,
            CAST(COR.display AS INTEGER) AS order,
            COR.error AS nominal_accuracy,
            NAJ.last_adjusted,
            CASE WHEN BDY.nod_id IS NULL THEN
                FALSE
            ELSE
                TRUE
            END AS is_bdy,
            NOD.shape
        FROM
            crs_node NOD
            JOIN crs_coordinate COO ON NOD.id = COO.nod_id AND COO.cos_id = NOD.cos_id_official AND COO.status = 'AUTH'
            JOIN crs_cord_order COR ON COO.cor_id = COR.id
            LEFT JOIN tmp_node_last_adjusted NAJ ON NOD.id = NAJ.nod_id
            LEFT JOIN tmp_bdy_nodes BDY ON NOD.id = BDY.nod_id
            LEFT JOIN crs_mark MRK ON NOD.id = MRK.nod_id
            LEFT JOIN crs_mark_name MKN ON MRK.id = MKN.mrk_id AND MKN.type = 'CURR'
        WHERE
            NOD.status = 'AUTH' AND
            NOD.cos_id_official = 109
    )
    SELECT
        id,
        name,
        "order",
        nominal_accuracy,
        last_adjusted,
        is_bdy,
        shape
    FROM
        t
    WHERE
        row_number = 1;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload,
        '3',
        'Finished creating table tmp_cadastral_marks'
    );
    
    ----------------------------------------------------------------------------
    -- survey_bdy_marks layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'survey_bdy_marks');

    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            name,
            "order",
            nominal_accuracy,
            date_last_adjusted,
            shape
        )
        SELECT
            id,
            name,
            "order",
            nominal_accuracy,
            last_adjusted,
            shape
        FROM
            tmp_cadastral_marks
        WHERE
            is_bdy = TRUE;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );

    ----------------------------------------------------------------------------
    -- survey_non_bdy_marks layer
    ----------------------------------------------------------------------------
    v_table := LDS.LDS_GetTable('lds', 'survey_non_bdy_marks');

    v_data_insert_sql := $sql$
        INSERT INTO %1% (
            id,
            name,
            "order",
            nominal_accuracy,
            date_last_adjusted,
            shape
        )
        SELECT
            id,
            name,
            "order",
            nominal_accuracy,
            last_adjusted,
            shape
        FROM
            tmp_cadastral_marks
        WHERE
            is_bdy = FALSE;
    $sql$;
    
    PERFORM LDS.LDS_UpdateSimplifiedTable(
        p_upload,
        v_table,
        v_data_insert_sql,
        v_data_insert_sql
    );
    
    PERFORM bde_control.bde_WriteUploadLog(
        p_upload, 
        '2',
        'Finished maintenance on cadastral survey simplified layers'
    );
    
    DROP TABLE IF EXISTS tmp_bdy_nodes;
    DROP TABLE IF EXISTS tmp_node_last_adjusted;
    DROP TABLE IF EXISTS tmp_cadastral_marks;
    
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_MaintainSimplifiedSurveyLayers(INTEGER) OWNER TO bde_dba;