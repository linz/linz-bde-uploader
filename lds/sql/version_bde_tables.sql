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
-- Creates system tables required for table versioning support
--------------------------------------------------------------------------------
BEGIN;

SELECT table_version.ver_create_revision('Initial revisioning for BDE tables');

SELECT
    'Enable versioning on table ' || CLS.relname,
    'OK: ' || table_version.ver_enable_versioning(NSP.nspname, CLS.relname)
FROM
    pg_catalog.pg_class CLS
    JOIN pg_catalog.pg_namespace NSP ON NSP.oid = CLS.relnamespace
WHERE
    NSP.nspname = 'bde' AND
    CLS.relkind = 'r'
ORDER BY
    NSP.nspname,
    CLS.relname;

SELECT table_version.ver_complete_revision();

COMMIT;
