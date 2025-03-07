\echo Use "CREATE EXTENSION synchdb" to load this file. \quit

CREATE TABLE IF NOT EXISTS synchdb_attribute (
    name name,
    type name,
    attrelid oid,
    attnum smallint,
    ext_tbname name,
    ext_attname name,
    ext_atttypename name,
    PRIMARY KEY (name, type, attrelid, attnum)
);

CREATE OR REPLACE FUNCTION synchdb_add_objmap(name, name, name, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_reload_objmap(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE TABLE IF NOT EXISTS synchdb_objmap (
    name name,
    objtype name,
    enabled bool,
    srcobj name,
    dstobj text,
    PRIMARY KEY (name, objtype, srcobj)
);

CREATE VIEW synchdb_att_view AS
	SELECT
		name,
		type,
		synchdb_attribute.attnum,
		ext_tbname,
		(SELECT n.nspname || '.' || c.relname AS table_full_name FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.oid=pg_attribute.attrelid) AS pg_tbname,
		synchdb_attribute.ext_attname,
		pg_attribute.attname AS pg_attname,
		synchdb_attribute.ext_atttypename,
		format_type(pg_attribute.atttypid, NULL) AS pg_atttypename,
		(SELECT dstobj FROM synchdb_objmap WHERE synchdb_objmap.objtype='transform' AND synchdb_objmap.enabled=true AND synchdb_objmap.srcobj = synchdb_attribute.ext_tbname || '.' || synchdb_attribute.ext_attname) AS transform
	FROM synchdb_attribute
	LEFT JOIN pg_attribute
	ON synchdb_attribute.attrelid = pg_attribute.attrelid
	AND synchdb_attribute.attnum = pg_attribute.attnum
	ORDER BY (name, type, ext_tbname, synchdb_attribute.attnum);

CREATE OR REPLACE FUNCTION synchdb_add_extra_conninfo(name, name, text, text, text, text) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_extra_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_conninfo(name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION synchdb_del_objmap(name, name, name) RETURNS int
AS '$libdir/synchdb'
LANGUAGE C IMMUTABLE STRICT;
