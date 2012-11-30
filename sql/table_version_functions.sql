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
-- Creates a PostgreSQL 9.0+ table version management system.
--------------------------------------------------------------------------------
SET client_min_messages TO WARNING;
BEGIN;

SET search_path = table_version, public;

DROP FUNCTION IF EXISTS ver_enable_versioning(NAME, NAME);
/**
* Enable versioning for a table. Versioning a table will do the following things:
*   1. A revision table with the schema_name_revision naming convention will be
*        created in the table_version schema.
*   2. Any data in the table will be inserted into the revision data table. If
*      SQL session is not currently in an active revision, a revision will be
*      will be automatically created, then completed once the data has been
*      inserted.
*   3. A trigger will be created on the versioned table that will maintain the changes
*      in the revision table.
*   4. A function will be created with the ver_schema_name_revision_diff naming 
*      convention in the table_version schema that allow you to get changeset data
*      for a range of revisions.
*   5. A function will be created with the ver_schema_name_revision_revision naming 
*      convention in the table_version schema that allow you to get a specific revision
*      of the table.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 If versioning the table was successful.
* @throws RAISE_EXCEPTION If the table does not exist
* @throws RAISE_EXCEPTION If the table is already versioned
* @throws RAISE_EXCEPTION If the table does not have a unique non-compostite integer column
*/
CREATE OR REPLACE FUNCTION ver_enable_versioning(
    p_schema NAME,
    p_table  NAME
) 
RETURNS BOOLEAN AS
$$
DECLARE
    v_table_oid       REGCLASS;
    v_key_col         NAME;
    v_revision_table  TEXT;
    v_sql             TEXT;
    v_table_id        table_version.versioned_tables.id%TYPE;
    v_revision        table_version.revision.id%TYPE;
    v_revision_exists BOOLEAN;
    v_table_has_data  BOOLEAN;
BEGIN
    IF NOT EXISTS (SELECT * FROM pg_tables WHERE tablename = p_table AND schemaname = p_schema) THEN
        RAISE EXCEPTION 'Table %.% does not exists', quote_ident(p_schema), quote_ident(p_table);
    END IF;

    IF table_version.ver_is_table_versioned(p_schema, p_table) THEN
        RAISE EXCEPTION 'Table %.% is already versioned', quote_ident(p_schema), quote_ident(p_table);
    END IF;

    SELECT
        CLS.oid
    INTO
        v_table_oid
    FROM
        pg_namespace NSP,
        pg_class CLS
    WHERE
        NSP.nspname = p_schema AND
        CLS.relname = p_table AND
        NSP.oid     = CLS.relnamespace;

    SELECT
        ATT.attname as col
    INTO
        v_key_col
    FROM
        pg_index IDX,
        pg_attribute ATT
    WHERE
        IDX.indrelid = v_table_oid AND
        ATT.attrelid = v_table_oid AND
        ATT.attnum = ANY(IDX.indkey) AND
        ATT.attnotnull = TRUE AND
        IDX.indisunique = TRUE AND
        IDX.indexprs IS NULL AND
        IDX.indpred IS NULL AND
        format_type(ATT.atttypid, ATT.atttypmod) IN ('integer', 'bigint') AND
        array_length(IDX.indkey::INTEGER[], 1) = 1
    ORDER BY
        IDX.indisprimary DESC
    LIMIT 1;

    IF v_key_col IS NULL THEN
        RAISE EXCEPTION 'Table %.% does not have a unique non-compostite integer column', quote_ident(p_schema), quote_ident(p_table);
    END IF;

    v_revision_table := table_version.ver_get_version_table_full(p_schema, p_table);
    
    v_sql :=
    'CREATE TABLE ' || v_revision_table || '(' ||
        '_revision_created INTEGER NOT NULL REFERENCES table_version.revision,' ||
        '_revision_expired INTEGER REFERENCES table_version.revision,' ||
        'LIKE ' || quote_ident(p_schema) || '.' || quote_ident(p_table) ||
    ');';
    EXECUTE v_sql;


    v_sql := (
        SELECT
            'ALTER TABLE  ' || v_revision_table || ' ALTER COLUMN ' || attname || ' SET STATISTICS ' ||  attstattarget
        FROM
            pg_attribute 
        WHERE
            attrelid = v_table_oid AND
            attname = v_key_col AND
            attisdropped IS FALSE AND
            attnum > 0 AND
            attstattarget > 0
    );
    IF v_sql IS NOT NULL THEN
        EXECUTE v_sql;
    END IF;
    
    -- insert base data into table using a revision that is currently in
    -- progress, or if one does not exist create one.
    
    v_revision_exists := FALSE;
    
    EXECUTE 'SELECT EXISTS (SELECT * FROM ' || CAST(v_table_oid AS TEXT) || ' LIMIT 1)'
    INTO v_table_has_data;
    
    IF v_table_has_data THEN
        IF table_version._ver_get_reversion_temp_table('_changeset_revision') THEN
            SELECT
                max(VER.revision)
            INTO
                v_revision
            FROM
                _changeset_revision VER;
            
            v_revision_exists := TRUE;
        ELSE
            SELECT table_version.ver_create_revision(
                'Initial revisioning of ' || CAST(v_table_oid AS TEXT)
            )
            INTO  v_revision;
        END IF;
    
        v_sql :=
            'INSERT INTO ' || v_revision_table ||
            ' SELECT ' || v_revision || ', NULL, * FROM ' || CAST(v_table_oid AS TEXT);
        EXECUTE v_sql;
        
        IF NOT v_revision_exists THEN
            PERFORM table_version.ver_complete_revision();
        END IF;
    
    END IF;

    v_sql := 'ALTER TABLE  ' || v_revision_table || ' ADD CONSTRAINT ' ||
        quote_ident('pkey_' || v_revision_table) || ' PRIMARY KEY(_revision_created, ' ||
        quote_ident(v_key_col) || ')';
    EXECUTE v_sql;
    
    v_sql := 'CREATE INDEX ' || quote_ident('idx_' || p_table) || '_' || quote_ident(v_key_col) || ' ON ' || v_revision_table ||
        '(' || quote_ident(v_key_col) || ')';
    EXECUTE v_sql;

    v_sql := 'CREATE INDEX ' || quote_ident('fk_' || p_table) || '_expired ON ' || v_revision_table ||
        '(_revision_expired)';
    EXECUTE v_sql;

    v_sql := 'CREATE INDEX ' || quote_ident('fk_' || p_table) || '_created ON ' || v_revision_table ||
        '(_revision_created)';
    EXECUTE v_sql;
    
    EXECUTE 'ANALYSE ' || v_revision_table;

    -- Add dependency of the revision table on the newly versioned table 
    -- to avoid simple drop. Some people might forget that the table is
    -- versioned!
    
    INSERT INTO pg_catalog.pg_depend(
        classid,
        objid,
        objsubid,
        refclassid,
        refobjid,
        refobjsubid,
        deptype
    )
    SELECT
        cat.oid,
        fobj.oid,
        0,
        cat.oid,
        tobj.oid,
        0,
        'n'
    FROM
        pg_class cat, 
        pg_namespace fnsp, 
        pg_class fobj,
        pg_namespace tnsp,
        pg_class tobj
    WHERE
        cat.relname = 'pg_class' AND
        fnsp.nspname = 'table_version' AND
        fnsp.oid = fobj.relnamespace AND
        fobj.relname = table_version.ver_get_version_table(p_schema, p_table) AND
        tnsp.nspname = p_schema AND
        tnsp.oid = tobj.relnamespace AND
        tobj.relname   = p_table;

    SELECT
        id
    INTO
        v_table_id
    FROM
        table_version.versioned_tables
    WHERE
        schema_name = p_schema AND
        table_name = p_table;
    
    IF v_table_id IS NOT NULL THEN
        UPDATE table_version.versioned_tables
        SET    versioned = TRUE
        WHERE  schema_name = p_schema
        AND    table_name = p_table;
    ELSE
        INSERT INTO table_version.versioned_tables(schema_name, table_name, key_column, versioned)
        VALUES (p_schema, p_table, v_key_col, TRUE)
        RETURNING id INTO v_table_id;
    END IF;
    
    IF v_table_id IS NOT NULL AND v_table_has_data THEN
        INSERT INTO table_version.tables_changed(
            revision,
            table_id
        )
        SELECT
            v_revision,
            v_table_id
        WHERE
            NOT EXISTS (
                SELECT *
                FROM   table_version.tables_changed
                WHERE  table_id = v_table_id
                AND    revision = v_revision
        );
    END IF;

    PERFORM table_version.ver_create_table_functions(p_schema, p_table, v_key_col);
    PERFORM table_version.ver_create_version_trigger(p_schema, p_table, v_key_col);
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_enable_versioning(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_enable_versioning(NAME, NAME) FROM PUBLIC;

DROP FUNCTION IF EXISTS ver_disable_versioning(NAME, NAME);
/**
* Disables versioning on a table. All assoicated objects created for the versioning
* will be dropped.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 If disabling versioning on the table was successful.
* @throws RAISE_EXCEPTION If the table is not versioned
*/
CREATE OR REPLACE FUNCTION ver_disable_versioning(
    p_schema NAME, 
    p_table  NAME
) 
RETURNS BOOLEAN AS
$$
BEGIN
    IF NOT (SELECT table_version.ver_is_table_versioned(p_schema, p_table)) THEN
        RAISE EXCEPTION 'Table %.% is not versioned', quote_ident(p_schema), quote_ident(p_table);
    END IF;
    
    UPDATE table_version.versioned_tables
    SET    versioned = FALSE
    WHERE  schema_name = p_schema
    AND    table_name = p_table;

    EXECUTE 'DROP TRIGGER IF EXISTS '  || table_version._ver_get_version_trigger(p_schema, p_table) || ' ON ' ||  
        quote_ident(p_schema) || '.' || quote_ident(p_table);
    EXECUTE 'DROP FUNCTION IF EXISTS ' || table_version.ver_get_version_table_full(p_schema, p_table) || '()';
    EXECUTE 'DROP FUNCTION IF EXISTS ' || table_version._ver_get_diff_function(p_schema, p_table);
    EXECUTE 'DROP FUNCTION IF EXISTS ' || table_version._ver_get_revision_function(p_schema, p_table);
    EXECUTE 'DROP TABLE IF EXISTS '    || table_version.ver_get_version_table_full(p_schema, p_table) || ' CASCADE';    
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_disable_versioning(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_disable_versioning(NAME, NAME) FROM PUBLIC;

DROP FUNCTION IF EXISTS ver_create_revision(TEXT, TIMESTAMP, BOOLEAN);
/**
* Create a new revision within the curernt SQL session. This must be called before INSERTS, UPDATES OR DELETES
* can occur on a versioned table.
*
* @param p_comment        A comment for revision.
* @param p_revision_time  The the datetime of the revision in terms of a business context.
* @param p_schema_change  Does this revision implement a schema change.
* @return                 The identifier for the new revision.
* @throws RAISE_EXCEPTION If a revision is still in progress within the current SQL session.
*/
CREATE OR REPLACE FUNCTION ver_create_revision(
    p_comment       TEXT, 
    p_revision_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    p_schema_change BOOLEAN DEFAULT FALSE
) 
RETURNS INTEGER AS
$$
DECLARE
    v_revision table_version.revision.id%TYPE;
BEGIN
    IF table_version._ver_get_reversion_temp_table('_changeset_revision') THEN
        RAISE EXCEPTION 'A revision changeset is still in progress. Please complete the changeset before starting a new one';
    END IF;

    INSERT INTO table_version.revision (revision_time, schema_change, comment)
    VALUES (p_revision_time, p_schema_change, p_comment)
    RETURNING id INTO v_revision;
    
    CREATE TEMP TABLE _changeset_revision(
        revision INTEGER NOT NULL PRIMARY KEY
    );
    INSERT INTO _changeset_revision(revision) VALUES (v_revision);
    ANALYSE _changeset_revision;
    
    RETURN v_revision;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_create_revision(TEXT, TIMESTAMP, BOOLEAN) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_create_revision(TEXT, TIMESTAMP, BOOLEAN) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_create_revision(TEXT, TIMESTAMP, BOOLEAN) TO bde_admin;

DROP FUNCTION IF EXISTS ver_complete_revision();
/**
* Completed a revision. This must be called after a revision is created.
*
* @return                 Return if the revision was sucessfully completed.
*/
CREATE OR REPLACE FUNCTION ver_complete_revision() RETURNS BOOLEAN AS
$$
BEGIN
    IF NOT table_version._ver_get_reversion_temp_table('_changeset_revision') THEN
        RETURN FALSE;
    END IF;

    DROP TABLE _changeset_revision;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_complete_revision() OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_complete_revision() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_complete_revision() TO bde_admin;

DROP FUNCTION IF EXISTS ver_delete_revision(INTEGER);
/**
* Delete a revision. This is useful if the revision was allocated, but was not
* used for any table updates.
*
* @param p_revision       The revision ID
* @return                 Returns true if the revision was successfully deleted.
*/
CREATE OR REPLACE FUNCTION ver_delete_revision(
    p_revision INTEGER
) 
RETURNS BOOLEAN AS
$$
DECLARE
    v_status BOOLEAN;
BEGIN
    BEGIN
        DELETE FROM table_version.revision
        WHERE id = p_revision;
        v_status := FOUND;
    EXCEPTION
        WHEN foreign_key_violation THEN
            RAISE WARNING 'Can not delete revision % as it is referenced by other tables', p_revision;
            v_status := FALSE;
    END;
    RETURN v_status;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_delete_revision(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_delete_revision(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_delete_revision(INTEGER) TO bde_admin;

DROP FUNCTION IF EXISTS ver_get_revision(INTEGER);
/**
* Get the revision information for the given revision ID.
*
* @param p_revision       The revision ID
* @param id               The returned revision id
* @param revision_time    The returned revision datetime
* @param start_time       The returned start time of when revision record was created
* @param schema_change    The returned flag if the revision had a schema change
* @param comment          The returned revision comment
*/
CREATE OR REPLACE FUNCTION ver_get_revision(
    p_revision        INTEGER, 
    OUT id            INTEGER, 
    OUT revision_time TIMESTAMP,
    OUT start_time    TIMESTAMP,
    OUT schema_change BOOLEAN,
    OUT comment       TEXT
) AS $$
    SELECT
        id,
        revision_time,
        start_time,
        schema_change,
        comment
    FROM
        table_version.revision
    WHERE
        id = $1
$$ LANGUAGE sql SECURITY DEFINER;

ALTER FUNCTION ver_get_revision(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_revision(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_revision(INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_revision(INTEGER) TO bde_user;

DROP FUNCTION IF EXISTS ver_get_revisions(INTEGER[]);
/**
* Get all revisions.
* 
* @param p_revisions      An array of revision ids
* @return                 A tableset of revisions records.
*/
CREATE OR REPLACE FUNCTION ver_get_revisions(p_revisions INTEGER[]) 
RETURNS TABLE(
    id             INTEGER,
    revision_time  TIMESTAMP,
    start_time     TIMESTAMP,
    schema_change  BOOLEAN,
    comment        TEXT
) AS $$
    SELECT
        id,
        revision_time,
        start_time,
        schema_change,
        comment
    FROM
        table_version.revision
    WHERE
        id = ANY($1)
    ORDER BY
        revision DESC;
$$ LANGUAGE sql SECURITY DEFINER;

ALTER FUNCTION ver_get_revisions(INTEGER[]) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_revisions(INTEGER[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_revisions(INTEGER[]) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_revisions(INTEGER[]) TO bde_user;

DROP FUNCTION IF EXISTS ver_get_revisions(TIMESTAMP, TIMESTAMP);
/**
* Get revisions for a given date range
*
* @param p_start_date     The start datetime for the range of revisions
* @param p_end_date       The end datetime for the range of revisions
* @return                 A tableset of revision records
*/
CREATE OR REPLACE FUNCTION ver_get_revisions(
    p_start_date TIMESTAMP,
    p_end_date   TIMESTAMP
)
RETURNS TABLE(
    id             INTEGER
) AS $$
    SELECT
        id
    FROM
        table_version.revision
    WHERE
        revision_time >= $1 AND
        revision_time <= $2
    ORDER BY
        revision DESC;
$$ LANGUAGE sql SECURITY DEFINER;

ALTER FUNCTION ver_get_revisions(TIMESTAMP, TIMESTAMP) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_revisions(TIMESTAMP, TIMESTAMP) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_revisions(TIMESTAMP, TIMESTAMP) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_revisions(TIMESTAMP, TIMESTAMP) TO bde_user;

DROP FUNCTION IF EXISTS ver_get_revision(TIMESTAMP);
/**
* Get the last revision for the given datetime. If no revision is recorded at
* the datetime, then the next oldest revision is returned.
*
* @param p_date_time      The datetime for the revision required.
* @return                 The revision id
*/
CREATE OR REPLACE FUNCTION ver_get_revision(
    p_date_time       TIMESTAMP
) 
RETURNS INTEGER AS 
$$
    SELECT
        id
    FROM
        table_version.revision
    WHERE
        id IN (
            SELECT max(id) 
            FROM   table_version.revision
            WHERE  revision_time <= $1
        );
$$ LANGUAGE sql SECURITY DEFINER;

ALTER FUNCTION ver_get_revision(TIMESTAMP) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_revision(TIMESTAMP) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_revision(TIMESTAMP) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_revision(TIMESTAMP) TO bde_user;

DROP FUNCTION IF EXISTS ver_get_last_revision();
/**
* Get the last revision.
*
* @return               The revision id
*/
CREATE OR REPLACE FUNCTION ver_get_last_revision() 
RETURNS INTEGER AS 
$$
    SELECT
        id
    FROM
        table_version.revision
    WHERE
        id IN (
            SELECT max(id) 
            FROM   table_version.revision
        );
$$ LANGUAGE sql SECURITY DEFINER;

ALTER FUNCTION ver_get_last_revision() OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_last_revision() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_last_revision() TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_last_revision() TO bde_user;

DROP FUNCTION IF EXISTS ver_get_table_base_revision(NAME, NAME);
/**
* Get the base revision for a given table.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 The revision id
*/
CREATE OR REPLACE FUNCTION ver_get_table_base_revision(
    p_schema          NAME,
    p_table           NAME
)
RETURNS INTEGER AS
$$
DECLARE
    v_id INTEGER;
BEGIN
    IF NOT table_version.ver_is_table_versioned(p_schema, p_table) THEN
        RAISE EXCEPTION 'Table %.% is not versioned', quote_ident(p_schema), quote_ident(p_table);
    END IF;
    
    SELECT
        VER.id
    INTO
        v_id
    FROM
        table_version.revision VER
    WHERE
        VER.id IN (
            SELECT min(TBC.revision)
            FROM   table_version.versioned_tables VTB,
                   table_version.tables_changed TBC
            WHERE  VTB.schema_name = p_schema
            AND    VTB.table_name = p_table
            AND    VTB.id = TBC.table_id
        );
    RETURN v_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_get_table_base_revision(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_table_base_revision(NAME, NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_table_base_revision(NAME, NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_table_base_revision(NAME, NAME) TO bde_user;

DROP FUNCTION IF EXISTS ver_get_table_last_revision(NAME, NAME);
/**
* Get the last revision for a given table.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 The revision id
* @throws RAISE_EXCEPTION If the table is not versioned
*/
CREATE OR REPLACE FUNCTION ver_get_table_last_revision(
    p_schema          NAME,
    p_table           NAME
) 
RETURNS INTEGER AS
$$
DECLARE
    v_id INTEGER;
BEGIN
    IF NOT table_version.ver_is_table_versioned(p_schema, p_table) THEN
        RAISE EXCEPTION 'Table %.% is not versioned', quote_ident(p_schema), quote_ident(p_table);
    END IF;
    
    SELECT
        VER.id
    INTO
        v_id
    FROM
        table_version.revision VER
    WHERE
        VER.id IN (
            SELECT max(TBC.revision)
            FROM   table_version.versioned_tables VTB,
                   table_version.tables_changed TBC
            WHERE  VTB.schema_name = p_schema
            AND    VTB.table_name = p_table
            AND    VTB.id = TBC.table_id
        );
    RETURN v_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_get_table_last_revision(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_table_last_revision(NAME, NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_table_last_revision(NAME, NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_table_last_revision(NAME, NAME) TO bde_user;

DROP FUNCTION IF EXISTS ver_get_modified_tables(INTEGER);
/**
* Get all tables that are modified by a revision.
*
* @param p_revision       The revision
* @return                 A tableset of modified table records
* @throws RAISE_EXCEPTION If the provided revision does not exist
*/
CREATE OR REPLACE FUNCTION ver_get_modified_tables(
    p_revision  INTEGER
)
RETURNS TABLE(
    schema_name NAME,
    table_name  NAME
) 
AS $$
BEGIN
    IF NOT EXISTS(SELECT * FROM table_version.revision WHERE id = p_revision) THEN
        RAISE EXCEPTION 'Revision % does not exist', p_revision;
    END IF;
            
    RETURN QUERY
        SELECT
            VTB.schema_name,
            VTB.table_name
        FROM
            table_version.versioned_tables VTB,
            table_version.tables_changed TBC
        WHERE
            VTB.id = TBC.table_id AND
            TBC.revision = p_revision
        ORDER BY
            VTB.schema_name,
            VTB.table_name;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_get_modified_tables(INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_modified_tables(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_modified_tables(INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_modified_tables(INTEGER) TO bde_user;

DROP FUNCTION IF EXISTS ver_get_modified_tables(INTEGER, INTEGER);
/**
* Get tables that are modified for a given revision range.
*
* @param p_revision1      The start revision for the range
* @param p_revision2      The end revision for the range
* @return                 A tableset of records modified tables and revision when the change occured.
*/
CREATE OR REPLACE FUNCTION ver_get_modified_tables(
    p_revision1 INTEGER,
    p_revision2 INTEGER
) 
RETURNS TABLE(
    revision    INTEGER,
    schema_name NAME,
    table_name  NAME
) AS
$$
DECLARE
    v_revision1 INTEGER;
    v_revision2 INTEGER;
    v_temp      INTEGER;
BEGIN
    v_revision1 := p_revision1;
    v_revision2 := p_revision2;

    IF v_revision1 > v_revision2 THEN
        v_temp      := v_revision1;
        v_revision1 := v_revision2;
        v_revision2 := v_temp;
    END IF;
    
    RETURN QUERY
        SELECT
            TBC.revision,
            VTB.schema_name,
            VTB.table_name
        FROM
            table_version.versioned_tables VTB,
            table_version.tables_changed TBC
        WHERE
            VTB.id = TBC.table_id AND
            TBC.revision > v_revision1 AND
            TBC.revision <= v_revision2
        ORDER BY
            TBC.revision,
            VTB.schema_name,
            VTB.table_name;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_get_modified_tables(INTEGER, INTEGER) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_modified_tables(INTEGER, INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_modified_tables(INTEGER, INTEGER) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_modified_tables(INTEGER, INTEGER) TO bde_user;

DROP FUNCTION IF EXISTS ver_is_table_versioned(NAME, NAME);
/**
* Check if table is versioned
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 If the table is versioned
*/
CREATE OR REPLACE FUNCTION ver_is_table_versioned(
    p_schema NAME,
    p_table  NAME
)
RETURNS BOOLEAN AS 
$$
DECLARE
    v_is_versioned BOOLEAN;
BEGIN
    SELECT
        versioned
    INTO
        v_is_versioned
    FROM 
        table_version.versioned_tables 
    WHERE
        schema_name = p_schema AND
        table_name = p_table;

    IF v_is_versioned IS NULL THEN
        v_is_versioned := FALSE;
    END IF;

    RETURN v_is_versioned;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_is_table_versioned(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_is_table_versioned(NAME, NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_is_table_versioned(NAME, NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_is_table_versioned(NAME, NAME) TO bde_user;

DROP FUNCTION IF EXISTS ver_get_versioned_tables();
/**
* Get all versioned tables
*
* @return       A tableset of modified table records
*/
CREATE OR REPLACE FUNCTION ver_get_versioned_tables()
RETURNS TABLE(
    schema_name NAME,
    table_name  NAME,
    key_column  VARCHAR(64)
) AS $$
    SELECT
        schema_name,
        table_name,
        key_column
    FROM 
        table_version.versioned_tables
    WHERE
        versioned = TRUE;
$$ LANGUAGE sql SECURITY DEFINER;

ALTER FUNCTION ver_get_versioned_tables() OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_versioned_tables() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_versioned_tables() TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_versioned_tables() TO bde_user;

DROP FUNCTION IF EXISTS ver_get_versioned_table_key(NAME, NAME);
/**
* Get the versioned table key
*
* @return       The versioned table key
*/
CREATE OR REPLACE FUNCTION ver_get_versioned_table_key(
    p_schema_name NAME,
    p_table_name  NAME
)
RETURNS VARCHAR(64)
AS $$
    SELECT
        key_column
    FROM 
        table_version.versioned_tables
    WHERE
        versioned = TRUE AND
        schema_name = $1 AND
        table_name = $2;
$$ LANGUAGE sql SECURITY DEFINER;

ALTER FUNCTION ver_get_versioned_table_key(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_versioned_table_key(NAME, NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_versioned_table_key(NAME, NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_versioned_table_key(NAME, NAME) TO bde_user;

DROP FUNCTION IF EXISTS ver_create_table_functions(NAME, NAME, NAME);
/**
* Creates functions required for versioning the table.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @param p_key_col        The unique non-compostite integer column key.
* @return                 If creating the functions was successful.
* @throws RAISE_EXCEPTION If the table is not versioned
* @throws RAISE_EXCEPTION If the table column definition could not be found
*/
CREATE OR REPLACE FUNCTION ver_create_table_functions(
    p_schema  NAME, 
    p_table   NAME, 
    p_key_col NAME
) 
RETURNS BOOLEAN AS
$$
DECLARE
    v_revision_table      TEXT;
    v_sql                 TEXT;
    v_col_cur             refcursor;
    v_column_name         NAME;
    v_column_type         TEXT;
    v_table_columns       TEXT;
    v_select_columns_diff TEXT;
    v_select_columns_rev  TEXT;
BEGIN
    IF NOT table_version.ver_is_table_versioned(p_schema, p_table) THEN
        RAISE EXCEPTION 'Table %.% is not versioned', quote_ident(p_schema), quote_ident(p_table);
    END IF;
    
    v_revision_table := table_version.ver_get_version_table_full(p_schema, p_table);
    v_table_columns := '';
    v_select_columns_diff := '';
    v_select_columns_rev := '';
    
    OPEN v_col_cur FOR
    SELECT column_name, column_type
    FROM table_version._ver_get_table_cols(p_schema, p_table);

    FETCH FIRST IN v_col_cur INTO v_column_name, v_column_type;
    LOOP
        v_select_columns_rev := v_select_columns_rev || REPEAT(' ', 16) || 'T.' || quote_ident(v_column_name);
        v_select_columns_diff := v_select_columns_diff || REPEAT(' ', 16) || 'LVC.' || quote_ident(v_column_name);
        v_table_columns := v_table_columns || '    ' || quote_ident(v_column_name) || ' ' || v_column_type;
        FETCH v_col_cur INTO v_column_name, v_column_type;
        IF FOUND THEN
            v_select_columns_rev :=  v_select_columns_rev || ', ' || E'\n';
            v_select_columns_diff :=  v_select_columns_diff || ', ' || E'\n';
            v_table_columns :=   v_table_columns  || ', ' || E'\n';
        ELSE
            v_table_columns  :=  v_table_columns  || E'\n';
            EXIT;
        END IF;
    END LOOP;
    
    CLOSE v_col_cur;

    -- Create difference function for table called:
    -- ver_get_$schema$_$table$_diff(p_revision1 integer, p_revision2 integer)
    v_sql := $template$
    
CREATE OR REPLACE FUNCTION %func_sig%
RETURNS TABLE(
    _diff_action CHAR(1),
    %table_columns%
) 
AS $FUNC$
    DECLARE
        v_revision1      INTEGER;
        v_revision2      INTEGER;
        v_temp           INTEGER;
        v_base_version   INTEGER;
        v_revision_table TEXT;
    BEGIN
        IF NOT table_version.ver_is_table_versioned(%schema_name%, %table_name%) THEN
            RAISE EXCEPTION 'Table %full_table_name% is not versioned';
        END IF;
        
        v_revision1 := p_revision1;
        v_revision2 := p_revision2;
        IF v_revision1 = v_revision2 THEN
            RETURN;
        END IF;
        
        IF v_revision1 > v_revision2 THEN
            RAISE EXCEPTION 'Revision 1 (%) is greater than revision 2 (%)', v_revision1, v_revision2;
        END IF;
        
        SELECT table_version.ver_get_table_base_revision(%schema_name%, %table_name%)
        INTO   v_base_version;
        IF v_base_version > v_revision2 THEN
            RETURN;
        END IF;
        IF v_base_version > v_revision1 THEN
            v_revision1 := v_base_version;
        END IF;
        
        RETURN QUERY EXECUTE
        table_version.ver_ExpandTemplate(
            $sql$
            WITH last_value_changed AS (
                SELECT DISTINCT ON (T.%key_col%)
                    T.*
                FROM
                    %revision_table% AS T
                WHERE (
                    (T._revision_created <= %1% AND T._revision_expired > %1% AND T._revision_expired <= %2%) OR
                    (T._revision_created > %1%  AND T._revision_created <= %2% AND T._revision_expired > %2%)
                )
                ORDER BY
                    T.%key_col%, 
                    T._revision_created DESC
            ),
            old_state_changed AS(
                SELECT DISTINCT
                    T.%key_col%
                FROM
                    %revision_table% AS T
                WHERE
                     T._revision_created <= %1% AND T._revision_expired > %1% AND
                     T.%key_col% IN (SELECT last_value_changed.%key_col% FROM last_value_changed)
            )
            SELECT
                CASE WHEN LVC._revision_expired <= %2% THEN
                    'D'::CHAR(1)
                WHEN OSC.%key_col% IS NULL THEN
                    'I'::CHAR(1)
                ELSE
                    'U'::CHAR(1)
                END AS diff_action,
%select_columns%
            FROM
                last_value_changed AS LVC
                LEFT JOIN old_state_changed AS OSC ON LVC.%key_col% = OSC.%key_col%;
            $sql$,
            ARRAY[
                v_revision1::TEXT,
                v_revision2::TEXT
            ]
        );
        RETURN;
    END;
$FUNC$ LANGUAGE plpgsql SECURITY DEFINER;

    $template$;
    
    v_sql := REPLACE(v_sql, '%func_sig%',       table_version._ver_get_diff_function(p_schema, p_table));
    v_sql := REPLACE(v_sql, '%table_columns%',  v_table_columns);
    v_sql := REPLACE(v_sql, '%schema_name%',    quote_literal(p_schema));
    v_sql := REPLACE(v_sql, '%table_name%',     quote_literal(p_table));
    v_sql := REPLACE(v_sql, '%full_table_name%', quote_ident(p_schema) || '.' || quote_ident(p_table));
    v_sql := REPLACE(v_sql, '%select_columns%', v_select_columns_diff);
    v_sql := REPLACE(v_sql, '%key_col%',        quote_ident(p_key_col));
    v_sql := REPLACE(v_sql, '%revision_table%', v_revision_table);
    EXECUTE v_sql;
    
    EXECUTE 'REVOKE ALL ON FUNCTION '||table_version._ver_get_diff_function(p_schema, p_table)||' FROM PUBLIC;';
	EXECUTE 'GRANT EXECUTE ON FUNCTION '||table_version._ver_get_diff_function(p_schema, p_table)||' TO bde_admin;';
	EXECUTE 'GRANT EXECUTE ON FUNCTION '||table_version._ver_get_diff_function(p_schema, p_table)||' TO bde_user;';

    -- Create get version function for table called: 
    -- ver_get_$schema$_$table$_revision(p_revision integer)
    v_sql := $template$
    
CREATE OR REPLACE FUNCTION %func_sig%
RETURNS TABLE(
    %table_columns%
) AS
$FUNC$
BEGIN
    RETURN QUERY EXECUTE
    table_version.ver_ExpandTemplate(
        $sql$
            SELECT
%select_columns%
            FROM
                %revision_table% AS T
            WHERE
                _revision_created <= %1% AND
                (_revision_expired > %1% OR _revision_expired IS NULL)
        $sql$,
        ARRAY[
            p_revision::TEXT
        ]
    );
END;
$FUNC$ LANGUAGE plpgsql SECURITY DEFINER;

    $template$;
    
    v_sql := REPLACE(v_sql, '%func_sig%', table_version._ver_get_revision_function(p_schema, p_table));
    v_sql := REPLACE(v_sql, '%table_columns%', v_table_columns);
    v_sql := REPLACE(v_sql, '%select_columns%', v_select_columns_rev);
    v_sql := REPLACE(v_sql, '%revision_table%', v_revision_table);
    EXECUTE v_sql;
    
	EXECUTE 'REVOKE ALL ON FUNCTION '||table_version._ver_get_revision_function(p_schema, p_table)||' FROM PUBLIC;';
	EXECUTE 'GRANT EXECUTE ON FUNCTION '||table_version._ver_get_revision_function(p_schema, p_table)||' TO bde_admin;';
	EXECUTE 'GRANT EXECUTE ON FUNCTION '||table_version._ver_get_revision_function(p_schema, p_table)||' TO bde_user;';

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


ALTER FUNCTION ver_create_table_functions(NAME, NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_create_table_functions(NAME, NAME, NAME) FROM PUBLIC;


DROP FUNCTION IF EXISTS ver_create_version_trigger(NAME, NAME, NAME);
/**
* Creates trigger and trigger function required for versioning the table.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @param p_key_col        The unique non-compostite integer column key.
* @return                 If creating the functions was successful.
* @throws RAISE_EXCEPTION If the table is not versioned
*/
CREATE OR REPLACE FUNCTION ver_create_version_trigger(
    p_schema  NAME,
    p_table   NAME,
    p_key_col NAME
) 
RETURNS BOOLEAN AS
$$
DECLARE
    v_revision_table TEXT;
    v_sql            TEXT;
    v_trigger_name   VARCHAR;
    v_column_name    NAME;
    v_column_update  TEXT;
BEGIN
    IF NOT table_version.ver_is_table_versioned(p_schema, p_table) THEN
        RAISE EXCEPTION 'Table %.% is not versioned', quote_ident(p_schema), quote_ident(p_table);
    END IF;
    
    v_revision_table := table_version.ver_get_version_table_full(p_schema, p_table);
    
    v_column_update := '';
    FOR v_column_name IN
        SELECT column_name
        FROM table_version._ver_get_table_cols(p_schema, p_table)
    LOOP
        IF v_column_name = p_key_col THEN
            CONTINUE;
        END IF;
        IF v_column_update != '' THEN
            v_column_update := v_column_update || E',\n                        ';
        END IF;
        
        v_column_update := v_column_update || quote_ident(v_column_name) || ' = NEW.' 
            || quote_ident(v_column_name);
    END LOOP;
    
    v_sql := $template$

CREATE OR REPLACE FUNCTION %revision_table%() RETURNS trigger AS $TRIGGER$
    DECLARE
       v_revision      table_version.revision.id%TYPE;
       v_last_revision table_version.revision.id%TYPE;
       v_table_id      table_version.versioned_tables.id%TYPE;
    BEGIN
        BEGIN
            SELECT
                max(VER.revision)
            INTO
                v_revision
            FROM
                _changeset_revision VER;
                
            IF v_revision IS NULL THEN
                RAISE EXCEPTION 'Versioning system information is missing';
            END IF;
        EXCEPTION
            WHEN undefined_table THEN
                RAISE EXCEPTION 'To begin editing %full_table_name% you need to create a revision';
        END;

        SELECT
            VTB.id
        INTO
            v_table_id
        FROM
            table_version.versioned_tables VTB
        WHERE
            VTB.table_name = %table_name% AND
            VTB.schema_name = %schema_name%;
        
        IF v_table_id IS NULL THEN
            RAISE EXCEPTION 'Table versioning system information is missing for %full_table_name%';
        END IF;

        IF NOT EXISTS (
            SELECT TRUE
            FROM   table_version.tables_changed
            WHERE  table_id = v_table_id
            AND    revision = v_revision
        )
        THEN
            INSERT INTO table_version.tables_changed(revision, table_id)
            VALUES (v_revision, v_table_id);
        END IF;

        
        IF (TG_OP <> 'INSERT') THEN
            SELECT 
                _revision_created INTO v_last_revision
            FROM 
                %revision_table%
            WHERE 
                %key_col% = OLD.%key_col% AND
                _revision_expired IS NULL;

            IF v_last_revision = v_revision THEN
                IF TG_OP = 'UPDATE' AND OLD.%key_col% = NEW.%key_col% THEN
                    UPDATE
                        %revision_table%
                    SET
                        %revision_update_cols%
                    WHERE
                        %key_col% = NEW.%key_col% AND
                        _revision_created = v_revision AND
                        _revision_expired IS NULL;
                    RETURN NEW;
                ELSE
                    DELETE FROM 
                        %revision_table%
                    WHERE
                        %key_col% = OLD.%key_col% AND
                        _revision_created = v_last_revision;
                END IF;
            ELSE
                UPDATE
                    %revision_table%
                SET
                    _revision_expired = v_revision
                WHERE
                    %key_col% = OLD.%key_col% AND
                    _revision_created = v_last_revision;
            END IF;
        END IF;

        IF( TG_OP <> 'DELETE') THEN
            INSERT INTO %revision_table%
            SELECT v_revision, NULL, NEW.*;
            RETURN NEW;
        END IF;
        
        RETURN NULL;
    END;
$TRIGGER$ LANGUAGE plpgsql SECURITY DEFINER;

    $template$;

    v_sql := REPLACE(v_sql, '%schema_name%',    quote_literal(p_schema));
    v_sql := REPLACE(v_sql, '%table_name%',     quote_literal(p_table));
    v_sql := REPLACE(v_sql, '%full_table_name%', quote_ident(p_schema) || '.' || quote_ident(p_table));
    v_sql := REPLACE(v_sql, '%key_col%',        quote_ident(p_key_col));
    v_sql := REPLACE(v_sql, '%revision_table%', v_revision_table);
    v_sql := REPLACE(v_sql, '%revision_update_cols%', v_column_update);
    EXECUTE v_sql;

    SELECT table_version._ver_get_version_trigger(p_schema, p_table)
    INTO v_trigger_name;

    EXECUTE 'DROP TRIGGER IF EXISTS '  || v_trigger_name|| ' ON ' ||  
        quote_ident(p_schema) || '.' || quote_ident(p_table);

    EXECUTE 'CREATE TRIGGER '  || v_trigger_name || ' AFTER INSERT OR UPDATE OR DELETE ON ' ||  
        quote_ident(p_schema) || '.' || quote_ident(p_table) ||
        ' FOR EACH ROW EXECUTE PROCEDURE ' || v_revision_table || '()';
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION ver_create_version_trigger(NAME, NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_create_version_trigger(NAME, NAME, NAME) FROM PUBLIC;

DROP FUNCTION IF EXISTS ver_ExpandTemplate(TEXT, TEXT[]);
/**
* Processes a text template given a set of input template parameters. Template 
* parameters within the text are substituted content must be written as '%1%' 
* to '%n%' where n is the number of text parameters.
*
* @param p_template       The template text
* @param p_params         The template parameters
* @return                 The expanded template text
*/
CREATE OR REPLACE FUNCTION ver_ExpandTemplate (
    p_template TEXT,
    p_params TEXT[]
)
RETURNS
    TEXT AS
$$
DECLARE 
    v_expanded TEXT;
BEGIN
    v_expanded := p_template;
    FOR i IN 1 .. array_length(p_params,1) LOOP
        v_expanded := REPLACE( v_expanded, '%' || i || '%', p_params[i]);
    END LOOP;
    RETURN v_expanded;
END;
$$
LANGUAGE plpgsql;

ALTER FUNCTION ver_ExpandTemplate(TEXT, TEXT[]) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_ExpandTemplate(TEXT, TEXT[]) FROM PUBLIC;

DROP FUNCTION IF EXISTS _ver_get_table_cols(NAME, NAME);
/**
* Gets columns for a given table
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 The revision data table name
*/
CREATE OR REPLACE FUNCTION _ver_get_table_cols(
    p_schema NAME,
    p_table NAME
) 
RETURNS TABLE(
    column_name NAME,
    column_type TEXT
) AS $$
    SELECT
        ATT.attname,
        format_type(ATT.atttypid, ATT.atttypmod)
    FROM
        pg_attribute ATT
    WHERE
        ATT.attnum > 0 AND
        NOT ATT.attisdropped AND
        ATT.attrelid = (
            SELECT
                CLS.oid
            FROM
                pg_class CLS
                JOIN pg_namespace NSP ON NSP.oid = CLS.relnamespace
            WHERE
                NSP.nspname = $1 AND
                CLS.relname = $2
        );
$$ LANGUAGE sql;

ALTER FUNCTION _ver_get_table_cols(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION _ver_get_table_cols(NAME, NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION _ver_get_table_cols(NAME, NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION _ver_get_table_cols(NAME, NAME) TO bde_user;


DROP FUNCTION IF EXISTS ver_get_version_table(NAME, NAME);
/**
* Gets the tablename for the tables revision data.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 The revision data table name
*/
CREATE OR REPLACE FUNCTION ver_get_version_table(
    p_schema NAME,
    p_table NAME
) 
RETURNS VARCHAR AS $$
    SELECT quote_ident($1 || '_' || $2 || '_revision');
$$ LANGUAGE sql IMMUTABLE;

ALTER FUNCTION ver_get_version_table(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_version_table(NAME, NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_version_table(NAME, NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_version_table(NAME, NAME) TO bde_user;

DROP FUNCTION IF EXISTS ver_get_version_table_full(NAME, NAME);
/**
* Gets the fully qualified tablename for the tables revision data.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 The revision data fully qualified table name
*/
CREATE OR REPLACE FUNCTION ver_get_version_table_full(
    p_schema NAME,
    p_table NAME
) 
RETURNS VARCHAR AS $$
    SELECT 'table_version.' || table_version.ver_get_version_table($1, $2);
$$ LANGUAGE sql IMMUTABLE;

ALTER FUNCTION ver_get_version_table_full(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION ver_get_version_table_full(NAME, NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ver_get_version_table_full(NAME, NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION ver_get_version_table_full(NAME, NAME) TO bde_user;

DROP FUNCTION IF EXISTS _ver_get_version_trigger(NAME, NAME);
/**
* Gets the trigger name that is created on the versioned table.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 The trigger name
*/
CREATE OR REPLACE FUNCTION _ver_get_version_trigger(
    p_schema NAME,
    p_table NAME
)
RETURNS VARCHAR AS $$
    SELECT quote_ident($1 || '_' || $2 || '_revision_trg');
$$ LANGUAGE sql IMMUTABLE;

ALTER FUNCTION _ver_get_version_trigger(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION _ver_get_version_trigger(NAME, NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION _ver_get_version_trigger(NAME, NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION _ver_get_version_trigger(NAME, NAME) TO bde_user;

DROP FUNCTION IF EXISTS _ver_get_diff_function(NAME, NAME);
/**
* Gets the changset difference function name and signature that is created for the versioned table.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 The function name and signature
*/
CREATE OR REPLACE FUNCTION _ver_get_diff_function(
    p_schema NAME,
    p_table NAME
)
RETURNS VARCHAR AS $$
    SELECT ('table_version.' || quote_ident('ver_get_' || $1 || '_' || $2 || '_diff') || '(p_revision1 INTEGER, p_revision2 INTEGER)');
$$ LANGUAGE sql IMMUTABLE;

ALTER FUNCTION _ver_get_diff_function(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION _ver_get_diff_function(NAME, NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION _ver_get_diff_function(NAME, NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION _ver_get_diff_function(NAME, NAME) TO bde_user;

DROP FUNCTION IF EXISTS _ver_get_revision_function(NAME, NAME);
/**
* Gets the revision function name and signature that is created for the versioned table.
*
* @param p_schema         The table schema
* @param p_table          The table name
* @return                 The function name and signature
*/
CREATE OR REPLACE FUNCTION _ver_get_revision_function(
    p_schema NAME,
    p_table NAME
)
RETURNS VARCHAR AS $$
    SELECT ('table_version.' || quote_ident('ver_get_' || $1 || '_' || $2 || '_revision') || '(p_revision INTEGER)');
$$ LANGUAGE sql IMMUTABLE;

ALTER FUNCTION _ver_get_revision_function(NAME, NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION _ver_get_revision_function(NAME, NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION _ver_get_revision_function(NAME, NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION _ver_get_revision_function(NAME, NAME) TO bde_user;


DROP FUNCTION IF EXISTS _ver_get_reversion_temp_table(NAME);
/**
* Determine if a temp table exists within the current SQL session.
*
* @param p_table_name     The name of the temp table
* @return                 If true if the table exists.
*/
CREATE OR REPLACE FUNCTION _ver_get_reversion_temp_table(
    p_table_name NAME
)
RETURNS BOOLEAN AS
$$
DECLARE
    v_exists BOOLEAN;
BEGIN
    SELECT
        TRUE
    INTO
        v_exists
    FROM
        pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE
        n.nspname LIKE 'pg_temp_%' AND
        pg_catalog.pg_table_is_visible(c.oid) AND
        c.relkind = 'r' AND
        c.relname = p_table_name;

    IF v_exists IS NULL THEN
        v_exists := FALSE;
    END IF;

    RETURN v_exists;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION _ver_get_reversion_temp_table(NAME) OWNER TO bde_dba;
REVOKE ALL ON FUNCTION _ver_get_reversion_temp_table(NAME) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION _ver_get_reversion_temp_table(NAME) TO bde_admin;
GRANT EXECUTE ON FUNCTION _ver_get_reversion_temp_table(NAME) TO bde_user;

COMMIT;
