/*
 * format_converter.h
 *
 * Header file for the SynchDB format converter module
 *
 * This module provides structures and functions for processing
 * database change events in Debezium (DBZ) format and converting
 * them to a format suitable for SynchDB.
 *
 * Key components:
 * - Structures for representing DDL (Data Definition Language) events
 * - Structures for representing DML (Data Manipulation Language) events
 * - Functions for processing and converting DBZ change events
 *
 * Copyright (c) Hornetlabs Technology, Inc.
 * 
 */

#ifndef SYNCHDB_FORMAT_CONVERTER_H_
#define SYNCHDB_FORMAT_CONVERTER_H_

#include "nodes/pg_list.h"
#include "replication_agent.h"
#include "synchdb.h"

/* Structure to represent a column in a DDL event */
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

/* Structure to represent a DDL event */
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

/* Structure to represent a column value in a DML event */
typedef struct dbz_dml_column_value
{
	char * name;
	char * value;	/* expressed as string as taken from json */
	Oid datatype;	/* data type Oid as defined by PostgreSQL */
	int position;	/* position of this column value, start from 1 */
} DBZ_DML_COLUMN_VALUE;

/* Structure to represent a DML event */
typedef struct dbz_dml
{
	char op;
	char * db;
	char * table;
	Oid tableoid;
	List * columnValuesBefore;	/* list of DBZ_DML_COLUMN_VALUE */
	List * columnValuesAfter;	/* list of DBZ_DML_COLUMN_VALUE */
} DBZ_DML;

/* Function prototypes */
int fc_processDBZChangeEvent(const char * event);
ConnectorType fc_get_connector_type(const char * connector);

#endif /* SYNCHDB_FORMAT_CONVERTER_H_ */
