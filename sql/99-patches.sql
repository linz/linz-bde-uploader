--------------------------------------------------------------------------------
--
--
-- linz_bde_loader - LINZ BDE loader for PostgreSQL
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
SET client_min_messages TO WARNING;

SET search_path = bde_control, bde, public;

