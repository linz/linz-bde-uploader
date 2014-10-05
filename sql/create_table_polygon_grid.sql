--------------------------------------------------------------------------------
--
-- $Id$
--
-- Copyright 2014 Crown copyright (c)
-- Land Information New Zealand and the New Zealand Government.
-- All rights reserved
--
-- This software is released under the terms of the new BSD license. See the 
-- LICENSE file for more information.
--
--------------------------------------------------------------------------------
SET SEARCH_PATH = public;

--------------------------------------------------------------------------
-- create_table_polygon_grid(<schema>, <table>, <column>, <res_x>, <res_y>)
--------------------------------------------------------------------------
-- This postgis function splits a table of polygons into a grid. It is
-- designed to deal with small table of very large polygons which are slow
-- to execute spatial operations on (such as intersects). For each table
-- polygon row the function will split it into many rows each with a 'grid 
-- cell' of that polygon. The res_x and res_y parameters define the resolution
-- of cells.
--
-- REQUIRES PostGIS > 1.5
--------------------------------------------------------------------------

-- DROP FUNCTION create_table_polygon_grid(NAME, NAME, NAME, FLOAT8, FLOAT8);

CREATE OR REPLACE FUNCTION create_table_polygon_grid(
    p_schema_name NAME,
    p_table_name NAME,
    p_column_name NAME,
    p_res_x FLOAT8,
    p_res_y FLOAT8
)
RETURNS REGCLASS AS
$$
DECLARE
    v_table_oid OID;
    v_table_cur RECORD;
    v_sql TEXT;
    v_sql_filter TEXT;
    v_count BIGINT;
    v_table_key_column NAME;
    v_table_key_type TEXT;
    v_srid INT;
    v_srid_count INT;
    v_xmax FLOAT8;
    v_xmin FLOAT8;
    v_ymax FLOAT8;
    v_ymin FLOAT8;
    v_output_table TEXT;
    v_full_output_table TEXT;
    v_output_fkey TEXT;
    v_rights VARCHAR[];
    v_right VARCHAR;
    v_grant VARCHAR;
    v_rolename NAME;
BEGIN
    SELECT
        CLS.oid
    INTO
        v_table_oid
    FROM
        pg_namespace NSP,
        pg_class CLS
    WHERE
        NSP.nspname = p_schema_name AND
        CLS.relname = p_table_name AND
        NSP.oid     = CLS.relnamespace;
    
    IF v_table_oid IS NULL THEN
        RAISE EXCEPTION 'Table %.% does not exists',
            quote_ident(p_schema_name), quote_ident(p_table_name);
    END IF;

    IF NOT EXISTS (
        SELECT
            *
        FROM
            pg_class c,
            pg_attribute a,
            pg_type t,
            pg_namespace n
        WHERE
            c.relkind = 'r' AND 
            t.typname = 'geometry' AND
            a.attisdropped = false AND
            a.atttypid = t.oid AND
            a.attrelid = c.oid AND
            c.relnamespace = n.oid AND
            n.nspname NOT ILIKE 'pg_temp%' AND
            c.oid = v_table_oid AND
            a.attname = p_column_name
    ) THEN
        RAISE EXCEPTION 'Table %.% geometry column % does not exists',
            quote_ident(p_schema_name), quote_ident(p_table_name), quote_ident(p_column_name);
    END IF;

    v_sql_filter := ' WHERE ST_GeometryType(SRC.' || quote_ident(p_column_name) || ')'
        || ' IN (''ST_Polygon'', ''ST_MultiPolygon'')';

    -- check that the table is polygon table
    v_sql := 'SELECT count(*) FROM ' || quote_ident(p_schema_name) || '.'
        || quote_ident(p_table_name) || ' AS SRC ' || v_sql_filter;
    EXECUTE v_sql INTO v_count;
    IF NOT v_count > 0 THEN
        RAISE EXCEPTION 'Table %.% column & does not contain any polygons',
            quote_literal(p_schema_name), quote_ident(p_table_name), quote_ident(p_column_name);
    END IF;

    -- Table table primary key. Must be unique not null, non-composite column key.
    SELECT
        ATT.attname,
        format_type(ATT.atttypid, ATT.atttypmod)
    INTO
        v_table_key_column,
        v_table_key_type
    FROM
        pg_index IDX,
        pg_attribute ATT
    WHERE
        IDX.indrelid = v_table_oid AND
        ATT.attrelid = IDX.indrelid AND
        ATT.attnum = ANY(IDX.indkey) AND
        ATT.attnotnull = TRUE AND
        IDX.indisunique = TRUE AND
        IDX.indexprs IS NULL AND
        IDX.indpred IS NULL AND
        array_length(IDX.indkey::INTEGER[], 1) = 1
    ORDER BY
        IDX.indisprimary DESC;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% does not have a unique non-composite, non-null primary key',
            quote_literal(p_schema_name), quote_ident(p_table_name);
    END IF;
    
    -- determine table SRID
    EXECUTE 'SELECT DISTINCT ST_Srid(' || quote_ident(p_column_name) || ') AS extents FROM '
        || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name)
        || ' AS SRC ' || v_sql_filter
    INTO v_srid;

    GET DIAGNOSTICS v_srid_count = ROW_COUNT;
    IF v_srid_count > 1 THEN
        RAISE EXCEPTION 'Table %.% column % SRID is not consistent',
            quote_literal(p_schema_name), quote_ident(p_table_name), quote_ident(p_column_name);
    END IF;

    --determine extents and then calculate grid from user supplied x and y grid resolution
    v_sql := 'SELECT ST_XMax(extents) + 0.001 AS xmax, ST_XMin(extents) - 0.001'
        || ' AS xmin, ST_YMax(extents) + 0.001 AS ymax, ST_YMin(extents) - 0.001 AS ymin'
        || ' FROM (SELECT ST_Extent(' || quote_ident(p_column_name) || ') AS extents FROM ' 
        || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name) || ' AS SRC '
        || v_sql_filter || ') AS t';
    EXECUTE v_sql INTO
        v_xmax,
        v_xmin,
        v_ymax,
        v_ymin;

    IF (v_ymax-v_ymin = 0 OR v_xmax-v_xmin=0) THEN
        RAISE EXCEPTION 'Table extents of xmin=% xmax=% ymin=% ymax=% are not valid',
            v_xmin, v_xmax, v_ymin,  v_ymax;
    END IF;
    
    CREATE TEMP TABLE tmp_grid AS
    SELECT
        i + 1 AS row, 
        j + 1 AS col, 
        ST_SetSrid(
            ST_Translate(cell, j * p_res_x + v_xmin, i * p_res_y + v_ymin),
            v_srid
        ) AS cell
    FROM
        generate_series(0, ceil((v_ymax-v_ymin)/p_res_y)::INTEGER - 1) AS i,
        generate_series(0, ceil((v_xmax-v_xmin)/p_res_x)::INTEGER - 1) AS j,
    (
        SELECT ('POLYGON((0 0, 0 ' || p_res_y || ', ' || p_res_x || ' '
            || p_res_y || ', ' || p_res_x || ' 0,0 0))')::geometry AS cell
    ) AS t;

    CREATE INDEX idx_tmp_grid_cell ON tmp_grid USING GIST (cell);
    ANALYSE tmp_grid;

    v_output_table := quote_ident(p_table_name) || '_grid';
    v_full_output_table := quote_ident(p_schema_name) || '.' || v_output_table;
    v_output_fkey := quote_ident(p_table_name) || '_' || v_table_key_column;
    
    -- create grid table
    EXECUTE 'DROP TABLE IF EXISTS ' || v_output_table;
    
    v_sql := 'CREATE TABLE ' || v_output_table || ' ('
        || 'id INTEGER NOT NULL, '
        || v_output_fkey || ' ' || v_table_key_type || ' NOT NULL )';
    EXECUTE v_sql;
    
    PERFORM AddGeometryColumn(p_schema_name, v_output_table ,'geom', v_srid, 'MULTIPOLYGON', 2);

    -- set table permissions
    v_sql := 'ALTER TABLE ' || v_output_table || ' OWNER TO ' || 
        quote_ident((
            SELECT rolname 
            FROM pg_authid
            WHERE oid = (
                SELECT refobjid 
                FROM pg_shdepend 
                WHERE objid=v_table_oid
                AND deptype='o'
                )
            ));
    EXECUTE v_sql;

    v_rights := ARRAY[
        'SELECT',
        'INSERT',
        'UPDATE',
        'DELETE',
        'TRUNCATE',
        'REFERENCES',
        'TRIGGER'
    ];

    FOR v_rolename IN
        SELECT AUTH.rolname
        FROM   pg_shdepend DEP,
               pg_authid AUTH
        WHERE  DEP.objid = v_table_oid
        AND    AUTH.oid = DEP.refobjid
        AND    DEP.deptype='a'
        UNION
        SELECT 'public'
    LOOP
        FOR v_right IN SELECT * FROM unnest(v_rights)
        LOOP
            v_sql := '';
            v_grant := '';
            IF has_table_privilege(
                v_rolename, v_table_oid, v_right || ' WITH GRANT OPTION'
            ) THEN
                v_sql := v_right;
                v_grant := ' WITH GRANT OPTION';
            ELSIF has_table_privilege(v_rolename, v_table_oid, v_right) THEN
                v_sql := v_right;
            END IF;
            IF v_sql <> '' THEN
                v_sql := 'GRANT ' || v_sql || ' ON TABLE ' || v_output_table || 
                    ' TO ' || v_rolename || v_grant;
                EXECUTE v_sql;
            END IF;
        END LOOP;
    END LOOP;
    
    -- insert the gridded data
    v_sql := 'INSERT INTO ' || v_output_table || '(id, ' || v_output_fkey || ', geom) ' 
        || 'SELECT row_number() OVER () AS id, t.fkey, t.geom FROM '
        || '(SELECT SRC.' || v_table_key_column || ' AS fkey, '
        || 'ST_Multi((ST_Dump(ST_Intersection(' || quote_ident(p_column_name)
        || ', GRID.cell))).geom) AS geom '
        || 'FROM ' || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name)
        || ' AS SRC, tmp_grid AS GRID'
        || v_sql_filter || ' AND GRID.cell && SRC.' || quote_ident(p_column_name) || ' AND '
        || 'ST_Intersects(SRC.' || quote_ident(p_column_name) || ', GRID.cell) ) AS t';
    EXECUTE v_sql;

    EXECUTE 'ALTER TABLE ' || v_output_table || ' ADD PRIMARY KEY (id)';
    EXECUTE 'CREATE INDEX idx_' || p_table_name || '_' || quote_ident(p_column_name)
        || ' ON ' || v_output_table || ' USING GIST (geom)';
    
    EXECUTE 'ANALYSE ' || v_output_table;

    DROP TABLE tmp_grid;

    RETURN v_output_table::REGCLASS;
END;
$$
    LANGUAGE plpgsql;

