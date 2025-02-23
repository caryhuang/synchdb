\echo Use "CREATE EXTENSION synchdb" to load this file. \quit

CREATE TABLE IF NOT EXISTS synchdb_attribute (
    connector name,
    attrelid oid,
    attnum smallint,
    ext_tbname name,
    ext_attname name,
    ext_atttypename name,
    PRIMARY KEY (connector, attrelid, attnum)
);

CREATE VIEW synchdb_att_view AS
	SELECT
		connector,
		synchdb_attribute.attnum,
		ext_tbname,
		(SELECT n.nspname || '.' || c.relname AS table_full_name FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.oid=pg_attribute.attrelid) AS pg_tbname,
		synchdb_attribute.ext_attname,
		pg_attribute.attname AS pg_attname,
		synchdb_attribute.ext_atttypename,
		(SELECT typname FROM pg_type WHERE pg_type.oid = pg_attribute.atttypid) AS pg_atttypename
	FROM synchdb_attribute
	LEFT JOIN pg_attribute
	ON synchdb_attribute.attrelid = pg_attribute.attrelid
	AND synchdb_attribute.attnum = pg_attribute.attnum
	ORDER BY (connector, ext_tbname, synchdb_attribute.attnum);

