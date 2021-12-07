--------------------------------------------------------------------------------
--
-- linz_bde_uploader - LINZ BDE uploader for PostgreSQL
--
-- Copyright 2016 Crown copyright (c)
-- Land Information New Zealand and the New Zealand Government.
-- All rights reserved
--
-- This software is released under the terms of the new BSD license. See the
-- LICENSE file for more information.
--
--------------------------------------------------------------------------------
-- Patches to apply to BDE control system. Note that the order of patches listed
-- in this file should be done sequentially i.e Newest patches go at the bottom
-- of the file.
--------------------------------------------------------------------------------

DO $PATCHES$
BEGIN

IF NOT EXISTS (
    SELECT *
    FROM   pg_class CLS,
           pg_namespace NSP
    WHERE  CLS.relname = 'applied_patches'
    AND    NSP.oid = CLS.relnamespace
    AND    NSP.nspname = '_patches'
) THEN
    RAISE EXCEPTION 'dbpatch extension is not installed correctly';
END IF;

-- Patches start from here

-------------------------------------------------------------------------------
-- 2.5.0 Fix swapped ninsert/ndelete in bde_control.upload_stat
-------------------------------------------------------------------------------

-- Fix swapped ninsert/ndelete in bde_control.upload_stat
-- if coming from version < 2.5.0
PERFORM _patches.apply_patch(
    'linz-bde-uploader 2.5.0: '
    'Fix swapped ninsert/ndelete in bde_control.upload_stat',
    $P$
        UPDATE bde_control.upload_stats
        SET ninsert = ndelete, ndelete = ninsert
        WHERE incremental IS TRUE;
    $P$
);



-------------------------------------------------------------------------------
-- 2.6.0 Drop deprecated bde_control.bde_CheckTableCount
-------------------------------------------------------------------------------

PERFORM _patches.apply_patch(
    'linz-bde-uploader 2.6.0: '
    'Drop deprecated bde_control.bde_CheckTableCount',
    $P$
        DROP FUNCTION IF EXISTS bde_control.bde_CheckTableCount(INTEGER, NAME);
    $P$
);

-------------------------------------------------------------------------------
-- 2.7.0 Drop deprecated bde_control._bde_GetDependentObjectSql
-------------------------------------------------------------------------------

PERFORM _patches.apply_patch(
    'linz-bde-uploader 2.7.0: '
    'Drop deprecated bde_control._bde_GetDependentObjectSql',
    $P$
        DROP FUNCTION IF EXISTS bde_control._bde_GetDependentObjectSql(INTEGER, regclass);
    $P$
);


END;
$PATCHES$;
