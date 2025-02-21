--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION synchdb" to load this file. \quit
 
CREATE OR REPLACE FUNCTION synchdb_start_engine_bgw(text) RETURNS int
AS '$libdir/synchdb', 'synchdb_start_engine_bgw'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_start_engine_bgw(text, text) RETURNS int
AS '$libdir/synchdb', 'synchdb_start_engine_bgw_snapshot_mode'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_stop_engine_bgw(text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_get_state() RETURNS SETOF record
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE VIEW synchdb_state_view AS SELECT * FROM synchdb_get_state() AS (name text, connector_type text, pid int, stage text, state text, err text, last_dbz_offset text);

CREATE OR REPLACE FUNCTION synchdb_pause_engine(text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_resume_engine(text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_set_offset(text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_add_conninfo(text, text, int, text, text, text, text, text, text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_restart_connector(text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_log_jvm_meminfo(text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_get_stats() RETURNS SETOF record
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_reset_stats(text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE VIEW synchdb_stats_view AS SELECT * FROM synchdb_get_stats() AS (name text, ddls bigint, dmls bigint, reads bigint, creates bigint, updates bigint, deletes bigint, bad_events bigint, total_events bigint, batches_done bigint, avg_batch_size bigint);

CREATE TABLE IF NOT EXISTS synchdb_conninfo(name TEXT PRIMARY KEY, isactive BOOL, data JSONB);
