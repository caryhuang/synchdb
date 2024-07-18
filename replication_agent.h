/*
 * replication_agent.h
 *
 *  Created on: Jul. 16, 2024
 *      Author: caryh
 */

#ifndef SYNCHDB_REPLICATION_AGENT_H_
#define SYNCHDB_REPLICATION_AGENT_H_

/* data structures representing PostgreSQL data formats */
typedef struct pg_ddl
{
	char * ddlquery;	/* to be fed into SPI interface for fast execution*/
} PG_DDL;

typedef struct pg_dml
{
	char * dmlquery;
	/* todo: express dml in HeapTuple or TableTupleSlot and invoke
	 * Heap Access Method for direct handling of insert, update,
	 * deletem, truncate...etc
	 *
	 * refer to src/backend/replication/logical/worker.c function
	 * apply_handle_insert() about how to read from a change stream
	 * and directly invoke Heap Access Method via the executor
	 */
} PG_DML;

int ra_executePGDDL(PG_DDL * pgddl);
int ra_executePGDML(PG_DML * pgdml);

#endif /* SYNCHDB_REPLICATION_AGENT_H_ */
