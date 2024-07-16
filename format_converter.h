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
	List * columns;	/* list of DDL_COLUMN */
} DBZ_DDL;

int fc_processDBZChangeEvent(const char * event);

#endif /* SYNCHDB_FORMAT_CONVERTER_H_ */
