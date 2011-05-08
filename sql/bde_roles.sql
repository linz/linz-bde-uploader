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

DO $ROLES$
BEGIN

IF NOT EXISTS (SELECT * FROM pg_roles where rolname = 'bde_dba') THEN
    CREATE ROLE bde_dba
        SUPERUSER INHERIT CREATEDB CREATEROLE;
    ALTER ROLE bde_dba SET search_path=bde, bde_control, public;
END IF;

IF NOT EXISTS (SELECT * FROM pg_roles where rolname = 'bde_admin') THEN
    CREATE ROLE bde_admin
        NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE;
    ALTER ROLE bde_admin SET search_path=bde, public;
END IF;

IF NOT EXISTS (SELECT * FROM pg_roles where rolname = 'bde_user') THEN
    CREATE ROLE bde_user
        NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE;
    ALTER ROLE bde_user SET search_path=bde, public;
END IF;

END;
$ROLES$;
