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

int ra_executePGDDL(PG_DDL * pgddl);

#endif /* SYNCHDB_REPLICATION_AGENT_H_ */
