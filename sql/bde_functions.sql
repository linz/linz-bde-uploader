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
SET client_min_messages TO WARNING;
BEGIN;

SET search_path = bde, public;

DO $$
DECLARE
   v_pcid    TEXT;
   v_schema  TEXT = 'bde';
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

--
-- Name: bde_get_app_specific
--

CREATE OR REPLACE FUNCTION bde_get_app_specific(p_par_id INTEGER, p_context VARCHAR, p_long CHAR) RETURNS text
    AS $$
DECLARE
    v_app_text            TEXT;
    v_app_id              crs_appellation.id%TYPE;
    v_app_type            crs_appellation."type"%TYPE;
    v_part_indicator      crs_appellation.part_indicator%TYPE;
    v_count_app           INTEGER;
    v_surv                VARCHAR(4);
    v_title               VARCHAR(4);
    v_parcel_type         VARCHAR(255);
    v_parcel_value        TEXT;
    v_2_parcel_type       VARCHAR(255);
    v_2_parcel_value      crs_appellation.second_prcl_value%TYPE;
    v_block_number        crs_appellation.block_number%TYPE;
    v_app_val             crs_appellation.appellation_value%TYPE;
    v_sub_type            VARCHAR(255);
    v_sub_type_pos        crs_appellation.sub_type_position%TYPE;
    v_maori_name          crs_appellation.maori_name%TYPE;
    v_other_app           crs_appellation.other_appellation%TYPE;
    v_part                VARCHAR;
    v_share               CHAR(100);
    v_maori_parcel        crs_appellation.parcel_value%TYPE;
BEGIN
    v_surv := 'SURV';
    v_title := 'TITL';
    v_part := '';
    v_share := '1/1';

    SELECT
        APP.id,
        APP.type,
        APP.part_indicator,
        COALESCE( (CASE p_long
                    WHEN 'Y' THEN
                        bde_get_desccode( 'ASAP', APP.parcel_type)::TEXT
                    WHEN 'N' THEN
                        CASE WHEN APP.parcel_type in ( 'LOT', 'SECT', 'UNIT', 'FLAT', 'MARK' )
                            THEN
                               bde_get_charcode('ASAP',APP.parcel_type)::TEXT
                            ELSE
                               bde_get_desccode('ASAP',APP.parcel_type)::TEXT
                        END
                   END), ''),
        COALESCE( APP.parcel_value, ''),
        COALESCE( (CASE p_long
                    WHEN 'Y' THEN
                        bde_get_desccode('ASAP',APP.second_parcel_type)::TEXT
                    WHEN 'N' THEN
                         bde_get_charcode('ASAP',APP.second_parcel_type)::TEXT
                   END), ''),
        COALESCE( APP.second_prcl_value, ''),
        COALESCE( APP.block_number, ''),
        COALESCE( APP.appellation_value, ''),
        COALESCE( (CASE p_long
                    WHEN 'Y' THEN
                        bde_get_desccode('ASAU',APP.sub_type)::TEXT
                    WHEN 'N' THEN
                          CASE WHEN APP.sub_type in ( 'DP', 'LT', 'SO', 'ML', 'DPS' )
                            THEN
                                APP.sub_type::TEXT
                            ELSE
                                bde_get_charcode('ASAU',APP.sub_type)::TEXT
                          END
                   END), ''),
        COALESCE( APP.sub_type_position, ''),
        COALESCE( APP.maori_name, ''),
        COALESCE( APP.parcel_value, ''),
        COALESCE( APP.other_appellation, '')
    INTO
        v_app_id,
        v_app_type,
        v_part_indicator,
        v_parcel_type,
        v_parcel_value,
        v_2_parcel_type,
        v_2_parcel_value,
        v_block_number,
        v_app_val,
        v_sub_type,
        v_sub_type_pos,
        v_maori_name,
        v_maori_parcel,
        v_other_app
    FROM
        crs_appellation APP
    WHERE
        APP.par_id = p_par_id AND
        APP.status = 'CURR' AND
        ((p_context = v_title AND APP.title = 'Y' ) OR
         (p_context = v_surv AND APP.survey = 'Y' ));

    IF ( v_app_type = 'GNRL' ) THEN
        v_maori_name        := '';
        v_maori_parcel      := '';
        v_other_app         := '';

    ELSIF ( v_app_type = 'MAOR' ) THEN
        v_parcel_type      := '';
        v_parcel_value     := '';
        v_2_parcel_type    := '';
        v_2_parcel_value   := '';
        v_block_number     := '';
        v_sub_type_pos     := 'PRFX';
        v_other_app        := '';

    ELSIF ( v_app_type = 'OTHR' ) THEN
        v_parcel_type      := '';
        v_parcel_value     := '';
        v_2_parcel_type    := '';
        v_2_parcel_value   := '';
        v_block_number     := '';
        v_sub_type         := '';
        v_sub_type_pos     := '';
        v_app_val          := '';
        v_maori_name       := '';
        v_maori_parcel     := '';
    END IF;

    v_app_text := bde_write_appellation(
        TRIM(both FROM v_part_indicator),
        TRIM(both FROM v_part ),
        v_share,
        TRIM(both FROM v_parcel_type ),
        TRIM(both FROM v_parcel_value ),
        TRIM(both FROM v_2_parcel_type ),
        TRIM(both FROM v_2_parcel_value ),
        TRIM(both FROM v_block_number ),
        TRIM(both FROM v_sub_type_pos ),
        TRIM(both FROM v_sub_type ),
        TRIM(both FROM v_app_val ),
        TRIM(both FROM v_maori_parcel ),
        TRIM(both FROM v_maori_name ),
        TRIM(both FROM v_other_app )
    );

    RETURN v_app_text;
END;
    $$ LANGUAGE plpgsql STRICT;

ALTER FUNCTION bde_get_app_specific(INTEGER, VARCHAR, CHAR) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_app_specific(INTEGER, VARCHAR, CHAR) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_app_specific(INTEGER, VARCHAR, CHAR) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_app_specific(INTEGER, VARCHAR, CHAR) TO bde_user;


--
-- Name: bde_get_charcode(VARCHAR, VARCHAR)
--

CREATE OR REPLACE FUNCTION bde_get_charcode(p_code_group VARCHAR, p_code VARCHAR) RETURNS VARCHAR AS $$
    SELECT
        char_value
    FROM
        crs_sys_code
    WHERE
        scg_code = $1 AND
        code = $2;
$$ LANGUAGE sql STRICT;


ALTER FUNCTION bde_get_charcode(VARCHAR, VARCHAR) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_charcode(VARCHAR, VARCHAR) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_charcode(VARCHAR, VARCHAR) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_charcode(VARCHAR, VARCHAR) TO bde_user;

--
-- Name: bde_get_dataset(INTEGER)
--

CREATE OR REPLACE FUNCTION bde_get_dataset(p_wrk_id INTEGER) RETURNS VARCHAR
    AS $$
DECLARE
    v_survey         VARCHAR(50);
    v_dataset_series crs_survey.dataset_series%TYPE;
    v_dataset_id     crs_survey.dataset_id%TYPE;
    v_dataset_suffix crs_survey.dataset_suffix%TYPE;
BEGIN
    SELECT
        dataset_series,
        dataset_id,
        dataset_suffix
    INTO
        v_dataset_series,
        v_dataset_id,
        v_dataset_suffix
    FROM
        crs_survey
    WHERE
        wrk_id = p_wrk_id;

    v_survey := v_dataset_series || ' ' || v_dataset_id;
    
    IF v_dataset_suffix IS NOT NULL THEN
        v_survey := v_survey || '/' || v_dataset_suffix;
    END IF;
    
    RETURN v_survey;
END;
    $$ LANGUAGE plpgsql STRICT;


ALTER FUNCTION bde_get_dataset(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_dataset(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_dataset(INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_dataset(INTEGER) TO bde_user;

--
-- Name: bde_get_datasource(INTEGER)
--

CREATE OR REPLACE FUNCTION bde_get_datasource(p_wrk_id INTEGER) RETURNS CHAR(4) AS $$
    SELECT data_source
    FROM   crs_survey
    WHERE  wrk_id = $1;
$$ LANGUAGE sql STRICT;

ALTER FUNCTION bde_get_datasource(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_datasource(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_datasource(INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_datasource(INTEGER) TO bde_user;

--
-- Name: bde_get_desccode(VARCHAR, VARCHAR)
--

CREATE OR REPLACE FUNCTION bde_get_desccode(p_code_group VARCHAR, p_code VARCHAR) RETURNS TEXT AS $$
    SELECT SCO."desc"
    FROM   crs_sys_code SCO
    WHERE  SCO.scg_code = $1
    AND    SCO.code     = $2;
$$ LANGUAGE sql STRICT;

ALTER FUNCTION bde_get_desccode(VARCHAR, VARCHAR) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_desccode(VARCHAR, VARCHAR) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_desccode(VARCHAR, VARCHAR) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_desccode(VARCHAR, VARCHAR) TO bde_user;

--
-- Name: bde_get_district(INTEGER)
--

CREATE OR REPLACE FUNCTION bde_get_district(p_loc_id INTEGER) RETURNS VARCHAR AS $$
    SELECT name
    FROM   crs_locality
    WHERE  id = $1;
$$ LANGUAGE sql STRICT;

ALTER FUNCTION bde_get_district(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_district(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_district(INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_district(INTEGER) TO bde_user;

--
-- Name: bde_get_mrknodename(INTEGER, VARCHAR)
--

CREATE OR REPLACE FUNCTION bde_get_mrknodename(p_mrk_id INTEGER, p_type VARCHAR, OUT mrk_name VARCHAR(255), OUT mrk_name_type VARCHAR)
    AS $$
DECLARE
    v_tmp_nod_id INT4;
    v_tmp_mrk_id INT4;
    v_temp       VARCHAR;
BEGIN
    IF p_type = 'MARK' THEN
        SELECT
            MRK.nod_id,
            MRK.id
        INTO
            v_tmp_nod_id,
            v_tmp_mrk_id
        FROM
            crs_mark MRK
        WHERE
            MRK.id  = p_mrk_id;
    ELSE
        SELECT
            MRK.id,
            MRK.nod_id
        INTO
            v_tmp_mrk_id,
            v_tmp_nod_id 
        FROM
            crs_mark MRK
        WHERE
            MRK.nod_id  = p_mrk_id  AND
            MRK.replaced = 'N' AND
            MRK.status = 'COMM';
    END IF;

    IF v_tmp_mrk_id IS NOT NULL THEN
        SELECT
            CASE mkn.type
                WHEN 'CURR' THEN 7
                WHEN 'HIST' THEN 6
                WHEN 'ALTE' THEN 5
                WHEN 'CODE' THEN 4
                WHEN 'OTHR' THEN 3
                WHEN 'LABL' THEN 2
                ELSE 1
            END as order_type,
            mkn.name,
            bde_get_charcode('MKNT', mkn.type)
        INTO
            v_temp,
            mrk_name,
            mrk_name_type
        FROM
            crs_mark_name mkn
        WHERE
            mkn.mrk_id  = v_tmp_mrk_id
        ORDER BY
            order_type DESC;
    END IF;

    IF (p_type = 'MARK') AND (mrk_name IS NULL) AND (v_tmp_mrk_id IS NOT NULL) THEN
        mrk_name := 'Mark ID ' || v_tmp_mrk_id::VARCHAR;
        mrk_name_type := 'Mark ID';
    END IF;

    IF (p_type = 'NODE') AND (mrk_name IS NULL) AND (v_tmp_nod_id IS NOT NULL) THEN
        mrk_name := 'Node ID ' || v_tmp_nod_id::VARCHAR;
        mrk_name_type := 'Node ID';
    END IF;

    RETURN;
END;
    $$ LANGUAGE plpgsql STRICT;

ALTER FUNCTION bde_get_mrknodename(INTEGER, VARCHAR) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_mrknodename(INTEGER, VARCHAR) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_mrknodename(INTEGER, VARCHAR) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_mrknodename(INTEGER, VARCHAR) TO bde_user;

--
-- Name: bde_get_markname(INTEGER)
--

CREATE OR REPLACE FUNCTION bde_get_markname(p_mrk_id INTEGER) RETURNS VARCHAR AS $$
    SELECT mrk_name FROM bde_get_mrknodename($1, 'MARK')
$$ LANGUAGE sql STRICT;


ALTER FUNCTION bde_get_markname(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_markname(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_markname(INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_markname(INTEGER) TO bde_user;

--
-- Name: bde_get_nodename(INTEGER)
--

CREATE OR REPLACE FUNCTION bde_get_nodename(p_nod_id INTEGER) RETURNS VARCHAR AS $$
    SELECT mrk_name FROM bde_get_mrknodename($1, 'NODE')
$$ LANGUAGE sql STRICT;


ALTER FUNCTION bde_get_nodename(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_nodename(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_nodename(INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_nodename(INTEGER) TO bde_user;

--
-- Name: bde_get_nodename(INTEGER, VARCHAR)
--

CREATE OR REPLACE FUNCTION bde_get_nodename(p_nod_id INTEGER, p_type VARCHAR) RETURNS VARCHAR
    AS $$
DECLARE
    v_name varchar(255);
    v_nod_id int4;
    v_mrk_id int4;
BEGIN
    SELECT  max(MRK.id)
    INTO    v_mrk_id
    FROM    crs_mark MRK
    WHERE   MRK.nod_id  = p_nod_id;

    SELECT  MKN.name
    INTO    v_name
    FROM    crs_mark_name MKN
    WHERE   MKN.mrk_id  = v_mrk_id
    AND     MKN.type = p_type;

    RETURN v_name;
END;
    $$ LANGUAGE plpgsql STRICT;


ALTER FUNCTION bde_get_nodename(INTEGER, VARCHAR) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_nodename(INTEGER, VARCHAR) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_nodename(INTEGER, VARCHAR) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_nodename(INTEGER, VARCHAR) TO bde_user;

--
-- Name: bde_get_nodeorder(INTEGER)
--

CREATE OR REPLACE FUNCTION bde_get_nodeorder(p_node_id INTEGER) RETURNS VARCHAR AS $$
    SELECT  COR.display || ' order ' || DTM.name
    FROM    crs_cord_order      COR,
            crs_coordinate      COO,
            crs_datum           DTM,
            crs_coordinate_sys  COS
    WHERE   COO.nod_id = $1
    AND     COO.cor_id = COR.id
    AND     COO.cos_id = COS.id
    AND     COS.dtm_id = DTM.id;
$$ LANGUAGE sql STRICT;

ALTER FUNCTION bde_get_nodeorder(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_nodeorder(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_nodeorder(INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_nodeorder(INTEGER) TO bde_user;

--
-- Name: bde_get_office(INTEGER)
--

CREATE OR REPLACE FUNCTION bde_get_office(p_loc_id INTEGER) RETURNS VARCHAR AS $$
    SELECT  OFC.name
    FROM    crs_office OFC,
            crs_land_district LDT
    WHERE   LDT.loc_id = $1
    AND     LDT.off_code = OFC.code;
$$ LANGUAGE sql STRICT;


ALTER FUNCTION bde_get_office(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_office(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_office(INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_office(INTEGER) TO bde_user;

--
-- Name: bde_get_roadname(INTEGER, DOUBLE PRECISION);
--

CREATE OR REPLACE FUNCTION bde_get_roadname(p_rcl_id INTEGER, p_length DOUBLE PRECISION) RETURNS VARCHAR
    AS $$
DECLARE
    v_fullroadname            VARCHAR(255);
    v_display                 BOOLEAN;
    v_rcl_id_first            INTEGER;
    v_max_length              DOUBLE PRECISION;
    v_roadname                RECORD;
BEGIN
    v_fullroadname := NULL;

    IF p_length > 0.00040 THEN
       v_display := TRUE;
    ELSE
        SELECT
            min(RNS_OTHER.rcl_id),
            max(length(RCL_OTHER.shape))
        INTO
            v_rcl_id_first,
            v_max_length
        FROM
            crs_road_name_asc  RNS_THIS,
            crs_road_name_asc  RNS_OTHER,
            crs_road_ctr_line  RCL_OTHER
        WHERE
            RNS_THIS.rcl_id     = p_rcl_id AND
            RNS_THIS.priority   = 1 AND
            RNS_THIS.rna_id     = RNS_OTHER.rna_id AND
            RNS_OTHER.rcl_id    = RCL_OTHER.id AND
            RNS_OTHER.priority  = 1;

        IF v_max_length < 0.00040 AND v_rcl_id_first = p_rcl_id THEN
           v_display := TRUE;
        ELSE
            -- Another RCL will display the label so don't display it.
           v_display := FALSE;
        END IF;
    END IF;

    -- If roadname is to be displayed build into string
    IF v_display = TRUE THEN
        FOR v_roadname IN
            SELECT
                RNA."name",
                RNS.priority
            FROM
                crs_road_name       RNA,
                crs_road_name_asc   RNS
            WHERE
                RNS.rcl_id = p_rcl_id AND
                RNS.rna_id = RNA.id AND
                RNA.status = 'CURR'
            ORDER BY
                RNS.priority
        LOOP
            IF v_roadname."name" IS NOT NULL THEN
                IF v_fullroadname IS NULL THEN
                    v_fullroadname := v_roadname."name";
                ELSE
                    v_fullroadname := v_fullroadname || ' (' || v_roadname."name" || ')';
                END IF;
            END IF;
        END LOOP;
    ELSE
        v_fullroadname := '';
    END IF;
    
    RETURN v_fullroadname;
END;
    $$ LANGUAGE plpgsql STRICT;


ALTER FUNCTION bde_get_roadname(INTEGER, DOUBLE PRECISION) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_roadname(INTEGER, DOUBLE PRECISION) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_roadname(INTEGER, DOUBLE PRECISION) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_roadname(INTEGER, DOUBLE PRECISION) TO bde_user;

--
-- Name: bde_get_trtcode(VARCHAR, VARCHAR)
--

CREATE OR REPLACE FUNCTION bde_get_trtcode(p_code_group VARCHAR, p_type VARCHAR) RETURNS VARCHAR AS $$
   SELECT description
   FROM   crs_transact_type
   WHERE  grp  = $1
   AND    type = $2;
$$ LANGUAGE sql STRICT;

ALTER FUNCTION bde_get_trtcode(VARCHAR, VARCHAR) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_trtcode(VARCHAR, VARCHAR) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_trtcode(VARCHAR, VARCHAR) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_trtcode(VARCHAR, VARCHAR) TO bde_user;

--
-- Name: bde_get_username(VARCHAR)
--

CREATE OR REPLACE FUNCTION bde_get_username(p_user_id VARCHAR) RETURNS VARCHAR AS $$
    SELECT given_names  || ' ' || surname
    FROM   crs_user
    WHERE  id = $1;
$$ LANGUAGE sql STRICT;

ALTER FUNCTION bde_get_username(VARCHAR) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_username(VARCHAR) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_username(VARCHAR) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_username(VARCHAR) TO bde_user;

--
-- Name: bde_get_useroff(VARCHAR)
--

CREATE OR REPLACE FUNCTION bde_get_useroff(p_user_id VARCHAR) RETURNS VARCHAR AS $$
    SELECT off_code
    FROM   crs_user
    WHERE  id = $1;
$$ LANGUAGE sql STRICT;


ALTER FUNCTION bde_get_useroff(VARCHAR) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_useroff(VARCHAR) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_useroff(VARCHAR) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_useroff(VARCHAR) TO bde_user;

--
-- Name: bde_write_appellation(VARCHAR, VARCHAR, CHAR, VARCHAR, TEXT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT);
--

CREATE OR REPLACE FUNCTION bde_write_appellation(
    p_part_indicator VARCHAR,
    p_lgd_part VARCHAR,
    p_share CHAR,
    p_parcel_type VARCHAR,
    p_parcel_value TEXT,
    p_2_parcel_type VARCHAR,
    p_2_parcel_value VARCHAR,
    p_block_number VARCHAR,
    p_sub_type_pos VARCHAR,
    p_sub_type VARCHAR,
    p_app_val VARCHAR,
    p_maori_parcel VARCHAR,
    p_maori_name VARCHAR,
    p_other_app TEXT
)
RETURNS TEXT
    AS $$
DECLARE
    v_part               VARCHAR(5);
    v_share              VARCHAR(9);
    v_block              VARCHAR(7);
    v_output             TEXT;
BEGIN
    v_part          := ' Part';
    v_share         := ' share of';
    v_block         := ' Block';
    v_output        := '';

    IF ( p_part_indicator = 'PART' ) THEN
        v_output := v_output || v_part;
    END IF;

    IF ( p_lgd_part = 'PART' ) THEN
        v_output := v_output || v_part;
    END IF;

    IF ( p_share <> '1/1' ) THEN
        v_output := v_output || p_share::TEXT || v_share;
    END IF;

    IF ( char_length(p_maori_name) > 0 ) THEN
        v_output := v_output || ' ' || p_maori_name;
    END IF;

    IF ( char_length(p_maori_parcel) > 0 ) THEN
        v_output := v_output || ' ' || p_maori_parcel;
    END IF;

    IF ( char_length(p_maori_name) > 0 ) OR ( char_length(p_maori_parcel) > 0 ) THEN
        v_output := v_output || v_block;
    END IF;

    IF ( char_length(p_other_app) > 0 ) THEN
        v_output := v_output || ' ' || p_other_app;
    END IF;

    IF ( char_length(p_parcel_type) > 0 ) THEN
        v_output := v_output || ' ' || p_parcel_type;
    END IF;

    IF ( char_length(p_parcel_value) > 0 ) THEN
        v_output := v_output || ' ' || p_parcel_value;
    END IF;

    IF ( char_length(p_2_parcel_type) > 0 ) THEN
        v_output := v_output || ' ' || p_2_parcel_type;
    END IF;

    IF ( char_length(p_2_parcel_value) > 0 ) THEN
        v_output := v_output || ' ' || p_2_parcel_value;
    END IF;

    IF ( char_length(p_block_number) > 0 ) THEN
        v_output := v_output || v_block || ' ' || p_block_number;
    END IF;

    IF ( p_sub_type_pos = 'PRFX' ) THEN
        IF ( char_length(p_sub_type) > 0 ) THEN
            v_output := v_output || ' ' || p_sub_type;
        END IF;

        IF ( char_length(p_app_val) > 0 ) THEN
            v_output := v_output || ' ' || p_app_val;
        END IF;
    ELSE
        IF ( char_length(p_app_val) > 0 ) THEN
            v_output := v_output || ' ' || p_app_val;
        END IF;

        IF ( char_length(p_sub_type) > 0 ) THEN
            v_output := v_output || ' ' || p_sub_type;
        END IF;
    END IF;
    
    RETURN TRIM(both FROM v_output);
END;
    $$ LANGUAGE plpgsql;

ALTER FUNCTION bde_write_appellation(VARCHAR, VARCHAR, CHAR, VARCHAR, TEXT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_write_appellation(VARCHAR, VARCHAR, CHAR, VARCHAR, TEXT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_write_appellation(VARCHAR, VARCHAR, CHAR, VARCHAR, TEXT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_write_appellation(VARCHAR, VARCHAR, CHAR, VARCHAR, TEXT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT) TO bde_user;


CREATE OR REPLACE FUNCTION bde_get_combined_appellation(p_par_id INTEGER, p_long CHAR)
    RETURNS TEXT AS $$
DECLARE
    v_surv_app      CHAR(1);
    v_title_app     CHAR(1);
    v_app_count     BIGINT;
    v_temp_app      TEXT;
    v_appellation   TEXT;
BEGIN
    v_appellation := NULL;
    
    SELECT
        max(survey) as survey,
        max(title) as title,
        count(*)
    INTO
        v_surv_app,
        v_title_app,
        v_app_count
    FROM
        crs_appellation
    WHERE
        par_id = p_par_id
    GROUP BY
        par_id;

    IF FOUND THEN
        IF v_title_app = 'Y' AND v_surv_app = 'Y' THEN
            v_appellation := bde_get_app_specific(p_par_id, 'SURV', p_long);
            IF v_app_count > 1 THEN
                v_temp_app := bde_get_app_specific(p_par_id, 'TITL', p_long);
                IF v_temp_app <> v_appellation THEN
                    v_appellation := v_appellation || ' or ' || v_temp_app;
                END IF;
            END IF;
        ELSIF v_title_app = 'Y' THEN
            v_appellation := bde_get_app_specific(p_par_id, 'TITL', p_long);
        ELSE
            v_appellation := bde_get_app_specific(p_par_id, 'SURV', p_long);
        END IF;
    END IF;

    RETURN v_appellation;
END;
    $$ LANGUAGE plpgsql VOLATILE STRICT;

ALTER FUNCTION bde_get_combined_appellation(INTEGER, CHAR) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_combined_appellation(INTEGER, CHAR) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_combined_appellation(INTEGER, CHAR) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_combined_appellation(INTEGER, CHAR) TO bde_user;

CREATE OR REPLACE FUNCTION bde_get_par_stat_act(p_sta_id INTEGER, p_par_id INTEGER)
    RETURNS TEXT AS $$
DECLARE
    v_sap_action      TEXT;
    v_sap_purpose     crs_stat_act_parcl.purpose%TYPE;
    v_sap_name        crs_stat_act_parcl.name%TYPE;
    v_comments        crs_stat_act_parcl.comments%TYPE;
    v_par_act_desc    TEXT;
    v_stat_act_type   crs_statute_action.type%TYPE;
    v_ste_id          crs_statute.id%TYPE;
    v_sur_wrk_id_vest crs_statute_action.sur_wrk_id_vesting%TYPE;
    v_gazette         crs_statute_action.gazette_type%TYPE;
    v_gaz_year        crs_statute_action.gazette_year%TYPE;
    v_gaz_page        crs_statute_action.gazette_page%TYPE;
    v_page            VARCHAR(10);
    v_other_legality  VARCHAR(255);
    v_name_and_date   VARCHAR(255);
    v_section         VARCHAR(255);
    v_stat_act_desc   TEXT;
    v_description     TEXT;
BEGIN
    SELECT
        SCO.char_value,
        regexp_replace(SAP.purpose, E'(\r\n)|(\n)', '', 'g'),
        SAP.name,
        regexp_replace(SAP.comments, E'(\r\n)|(\n)', '', 'g')
    INTO
        v_sap_action,
        v_sap_purpose,
        v_sap_name,
        v_comments
    FROM
        crs_stat_act_parcl SAP,
        crs_sys_code SCO
    WHERE
        SCO.scg_code = 'SAPA' AND
        SCO.code = SAP.action AND
        SAP.sta_id = p_sta_id AND
        SAP.par_id = p_par_id;

    IF FOUND THEN
        v_par_act_desc := '[' ||  v_sap_action || ']';
        IF v_sap_purpose IS NOT NULL THEN
             v_par_act_desc := v_par_act_desc || ' ' || v_sap_purpose;
        END IF;
        IF v_sap_name IS NOT NULL THEN
             v_par_act_desc := v_par_act_desc || ' [' || v_sap_name || ']';
        END IF;
    END IF;
    
    SELECT
        STA.type,
        STA.ste_id,
        STA.sur_wrk_id_vesting,
        STA.gazette_type,
        STA.gazette_year,
        STA.gazette_page,
        STA.other_legality
    INTO
        v_stat_act_type,
        v_ste_id,
        v_sur_wrk_id_vest,
        v_gazette,
        v_gaz_year,
        v_gaz_page,
        v_other_legality
    FROM
        crs_statute_action STA,
        crs_sys_code SCO
    WHERE
        SCO.scg_code = 'SAPA' AND
        STA.id = p_sta_id;
    
    IF v_stat_act_type = 'GNOT' THEN
        IF v_gaz_page IS NULL THEN
            v_page :=  ' ';
        ELSE 
            v_page := ' p ';
        END IF; 
        
        v_stat_act_desc := bde_get_charcode('STAG', v_gazette) || ' ' || CAST(v_gaz_year AS TEXT) || v_page || CAST(v_gaz_page AS TEXT);
        
        IF v_other_legality IS NOT NULL THEN
            v_stat_act_desc := v_stat_act_desc || ', ' || v_other_legality;
        END IF;
    ELSIF v_stat_act_type = 'VSDP' THEN
        v_stat_act_desc  = 'Vested on ' || bde_get_dataset(v_sur_wrk_id_vest);
    ELSIF v_stat_act_type = 'STVE' THEN
        SELECT
            STE.name_and_date,
            STE.section
        INTO
            v_name_and_date,
            v_section
        FROM
            crs_statute STE
        WHERE
            STE.id   = v_ste_id;
        v_stat_act_desc := v_section || ', ' || v_name_and_date;
    ELSIF v_stat_act_type IS NOT NULL THEN
        v_stat_act_desc := v_other_legality;
    END IF;
    
    IF v_stat_act_type IS NOT NULL AND v_stat_act_desc IS NULL THEN
        v_stat_act_desc := v_other_legality;
    END IF;

    IF v_par_act_desc IS NULL THEN
        v_description := v_stat_act_desc;
    ELSE
        v_description := v_par_act_desc;
        IF v_stat_act_desc IS NOT NULL THEN
           v_description := v_description || ' ' || v_stat_act_desc;
        END IF;
        IF v_comments IS NOT NULL THEN
            v_description := v_description || ' ' || v_comments;
        END IF;
    END IF;
    RETURN v_description;
END;
    $$ LANGUAGE plpgsql VOLATILE STRICT;

ALTER FUNCTION bde_get_par_stat_act(INTEGER, INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_get_par_stat_act(INTEGER, INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_get_par_stat_act(INTEGER, INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_get_par_stat_act(INTEGER, INTEGER) TO bde_user;

CREATE OR REPLACE FUNCTION bde_getnearnodes (
    p_latitude  DOUBLE PRECISION,
    p_longitude DOUBLE PRECISION,
    p_tolerance DOUBLE PRECISION
    )
RETURNS TABLE(nod_id INTEGER, distance DOUBLE PRECISION) 
AS 
$body$ 
DECLARE
    v_point    GEOMETRY;
    v_lat      DOUBLE PRECISION;
    v_lon      DOUBLE PRECISION;
    v_lat0     DOUBLE PRECISION;
    v_lat1     DOUBLE PRECISION;
    v_lon0     DOUBLE PRECISION;
    v_lon1     DOUBLE PRECISION;
    v_degoff   DOUBLE PRECISION;
    v_spheroid SPHEROID  := 'SPHEROID["GRS_1980",6378137,298.257222101]';
    v_srid     INTEGER   := 4167;
BEGIN
    v_point := ST_SetSRID(ST_MakePoint(p_longitude, p_latitude), v_srid);
    v_lat    := p_latitude;
    v_lon    := p_longitude;
    v_degoff := degrees(p_tolerance/6400000.0);
    v_lat0   := v_lat-v_degoff;
    v_lat1   := v_lat+v_degoff;
    v_degoff := v_degoff/cos(radians(v_lat));
    v_lon0   := v_lon-v_degoff;
    v_lon1   := v_lon+v_degoff;

    -- RAISE INFO 'lat0 % lat1 % lon0 % lon1 % off %',lat0,lat1,lon0,lon1,degoff;

    RETURN QUERY
        SELECT 
            coo.nod_id,
            ST_Distance_Spheroid(nod.shape, v_point, v_spheroid)
        FROM 
            crs_coordinate coo
            JOIN crs_node nod ON coo.nod_id = nod.id AND coo.cos_id = nod.cos_id_official
        WHERE
            coo.status = 'AUTH' AND
            nod.shape && 
            ST_SetSRID(
                ST_MakeBox2D(ST_Point(v_lon0,v_lat0),ST_Point(v_lon1,v_lat1)),
                v_srid
            ) AND
            ST_Distance_Spheroid(nod.shape, v_point, v_spheroid) < p_tolerance
        ORDER BY
            2;
    END;
$body$ LANGUAGE plpgsql;

ALTER FUNCTION bde_getnearnodes(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION bde_getnearnodes(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_getnearnodes(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION) TO bde_admin;
GRANT EXECUTE ON FUNCTION bde_getnearnodes(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION) TO bde_user;

--  Drop connnections to the database more than a specific age
-- 
-- This is to prevent Quantum edit sessions left open overnight from
-- blocking the parcel layer maintenance.

CREATE OR REPLACE FUNCTION bde_drop_idle_connections( p_upload_id INT )
RETURNS INT
AS $$
DECLARE 
    v_pid integer;
    v_usename name;
    v_ndrop integer;
BEGIN
    v_ndrop := 0;
    FOR v_pid, v_usename IN
       SELECT 
            procpid,
            usename
        FROM 
            pg_stat_activity a
        WHERE
            a.datname = current_database() AND 
            a.current_query = '<IDLE> in transaction' AND 
            (clock_timestamp()-query_start) > '2 HOURS' 
    LOOP
        BEGIN
            RAISE WARNING 'Dropping idle connection held by %', v_usename;
            PERFORM pg_terminate_backend( v_pid );
            v_ndrop := v_ndrop + 1;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to drop idle connection:  %', SQLERRM;
        END;
    END LOOP;
    RETURN v_ndrop;
END
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION bde_drop_idle_connections(INTEGER) OWNER TO bde_dba;
REVOKE ALL PRIVILEGES ON FUNCTION bde_drop_idle_connections(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bde_drop_idle_connections(INTEGER) TO bde_admin;

DO $$
DECLARE
    v_comment TEXT;
    v_pcid    TEXT;
    v_schema  TEXT = 'bde';
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
       
        v_comment := '$Id$'
                    || E'\n' || 'Installed: ' ||
                    to_char(current_timestamp,'YYYY-MM-DD HH:MI') || v_comment;
       
        EXECUTE 'COMMENT ON FUNCTION ' || v_schema || '.' || v_pcid || ' IS '
            || quote_literal( v_comment );
    END LOOP;
END
$$;

COMMIT;
