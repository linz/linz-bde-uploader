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
CREATE ROLE bde_admin
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE;
ALTER ROLE bde_admin SET search_path=bde, public;

CREATE ROLE bde_dba
  SUPERUSER INHERIT CREATEDB CREATEROLE;
ALTER ROLE bde_dba SET search_path=bde, bde_control, public;

CREATE ROLE bde_user
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE;
ALTER ROLE bde_user SET search_path=bde, public;

