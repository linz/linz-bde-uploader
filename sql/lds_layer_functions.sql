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

CREATE OR REPLACE FUNCTION LDS_MaintainSimplifiedGeodeticLayers(
    p_upload INTEGER
)
RETURNS
    INTEGER AS $$
DECLARE
    v_created_geo_node BOOLEAN;
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
    THEN
        -- Upper is used on the mark type to
        -- force the planner to do a seq scan on crs_mark_name.
        CREATE TEMP TABLE tmp_geo_nodes AS
        SELECT DISTINCT
            MRK.nod_id,
            MKN.name as geodetic_code
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
        
        v_created_geo_node := TRUE;
    ELSE
        PERFORM bde_control.bde_WriteUploadLog(
            p_upload, 
            '1',
            'Maintain geodetic simplified layers has been skipped as no ' ||
            'relating tables were affected by the upload'
        );
        RETURN 1;
    END IF;
    
    ----------------------------------------------------------------------------
    -- geodetic_marks layer
    ----------------------------------------------------------------------------
    
    v_table := LDS.LDS_GetTable('lds', 'geodetic_marks');

    IF (
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
        ) OR NOT LDS.LDS_TableHasData(v_table)
    )
    THEN

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
            NOD.shape
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
            NOD.cos_id_official = 109 AND
            NOD.status = 'AUTH';
        
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
    END IF;
    
    ----------------------------------------------------------------------------
    -- geodetic_vertical_marks layer
    ----------------------------------------------------------------------------
    
    v_table := LDS.LDS_GetTable('lds', 'geodetic_vertical_marks');
    
    IF (
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
        ) OR NOT LDS.LDS_TableHasData(v_table)
    )
    THEN

        PERFORM bde_control.bde_WriteUploadLog(
            p_upload,
            '3',
            'Started creating temp tables for ' || v_table
        );
        
        -- The windowing partition will prioritise the commissioned, non-replaced marks for each node.
        CREATE TEMP TABLE tmp_geodetic_vertical_mark AS
        SELECT
            row_number() OVER (ORDER BY COS.name, GEO.geodetic_code, MRK.status, MRK.replaced) AS row_number,
            NOD.id as nod_id,
            GEO.geodetic_code,
            MKN.name AS current_mark_name,
            MRK.desc AS description,
            RTRIM(mrk.type) AS mark_type,
            SCOB.char_value AS beacon_type,
            SCOC.char_value AS mark_condition,
            COR.display AS "order",
            LOC.Name AS land_district,
            COO.value3 AS normal_orthometric_height,
            COS.name as coordinate_system,
            NOD.shape
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
            MRK.status = 'COMM';
        
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
    END IF;

    IF v_created_geo_node THEN
        DROP TABLE tmp_geo_nodes;
    END IF;

    PERFORM bde_control.bde_WriteUploadLog(
        p_upload, 
        '2',
        'Finished maintenance on geodetic simplified layers'
    );
    
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION LDS_MaintainSimplifiedGeodeticLayers(INTEGER) OWNER TO bde_dba;

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

ALTER FUNCTION LDS_TableHasData(REGCLASS) OWNER TO bde_dba;

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
 
    EXECUTE 'ALTER TABLE ' || p_table_dest || ' ADD PRIMARY KEY (' || v_key_column || ')';
    
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
