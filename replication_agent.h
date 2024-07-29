/*
 * replication_agent.h
 *
 *  Created on: Jul. 16, 2024
 *      Author: caryh
 */

#ifndef SYNCHDB_REPLICATION_AGENT_H_
#define SYNCHDB_REPLICATION_AGENT_H_

#include "executor/tuptable.h"

/* data structures representing PostgreSQL data formats */
typedef struct pg_ddl
{
	char * ddlquery;	/* to be fed into SPI*/
} PG_DDL;

typedef struct pg_ddl_column_value
{
	char * value;	/* string representation of column values that
					 * is processed and ready to be used to built
					 * into TupleTableSlot.
					 */
	Oid datatype;
} PG_DML_COLUMN_VALUE;

typedef struct pg_dml
{
	char * dmlquery;	/* to be fed into SPI */

	char op;
	Oid tableoid;
	List * columnValuesBefore;	/* list of PG_DML_COLUMN_VALUE */
	List * columnValuesAfter;	/* list of PG_DML_COLUMN_VALUE */
} PG_DML;

int ra_executePGDDL(PG_DDL * pgddl);
int ra_executePGDML(PG_DML * pgdml);

#endif /* SYNCHDB_REPLICATION_AGENT_H_ */
