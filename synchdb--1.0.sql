--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION synchdb" to load this file. \quit
 
CREATE OR REPLACE FUNCTION synchdb_start_engine(text, int, text, text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_get_changes() RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_stop_engine() RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;
