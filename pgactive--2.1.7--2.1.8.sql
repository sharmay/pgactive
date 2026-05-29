/* pgactive--2.1.7--2.1.8.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pgactive UPDATE TO '2.1.8'" to load this file. \quit

SET pgactive.skip_ddl_replication = true;
SET LOCAL search_path = pgactive;
-- Start Upgrade SQLs/Functions/Procedures



REVOKE ALL ON FUNCTION _pgactive_node_name_present_private(text, text) FROM PUBLIC;

-- Finish Upgrade SQLs/Functions/Procedures
RESET pgactive.skip_ddl_replication;
RESET search_path;
