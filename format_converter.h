/*
 * format_converter.h
 *
 *  Created on: Jul. 11, 2024
 *      Author: caryh
 */

#ifndef SYNCHDB_FORMAT_CONVERTER_H_
#define SYNCHDB_FORMAT_CONVERTER_H_

#include "nodes/pg_list.h"
#include "replication_agent.h"
#include "synchdb.h"

/* data structures representing DBZ change events */
typedef struct dbz_ddl_column
{
	char * name;
	int length;
	bool optional;
	int position;
	char * typeName;
	char * enumValues;
	char * charsetName;
	bool autoIncremented;
	char * defaultValueExpression;
} DBZ_DDL_COLUMN;

typedef struct dbz_ddl
{
	char * id;
	char * type;
	char * primaryKeyColumnNames;
	List * columns;		/* list of DBZ_DDL_COLUMN */
} DBZ_DDL;

typedef struct
{
	char name[NAMEDATALEN];
	Oid oid;
	int position;
} NameOidEntry;

typedef struct dbz_ddl_column_value
{
	char * name;
	char * value;	/* expressed as string as taken from json */
	Oid datatype;	/* data type Oid as defined by PostgreSQL */
	int position;	/* position of this column value, start from 1 */
} DBZ_DML_COLUMN_VALUE;

typedef struct dbz_dml
{
	char op;
	char * db;
	char * table;
	Oid tableoid;
	List * columnValuesBefore;	/* list of DBZ_DML_COLUMN_VALUE */
	List * columnValuesAfter;	/* list of DBZ_DML_COLUMN_VALUE */
} DBZ_DML;

int fc_processDBZChangeEvent(const char * event);
ConnectorType fc_get_connector_type(const char * connector);

#endif /* SYNCHDB_FORMAT_CONVERTER_H_ */
