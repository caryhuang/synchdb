/*
 * synchdb.h
 *
 *  Created on: Jul. 31, 2024
 *      Author: caryh
 */

#ifndef SYNCHDB_SYNCHDB_H_
#define SYNCHDB_SYNCHDB_H_

#include "storage/lwlock.h"

#define SYNCHDB_ERRMSG_SIZE 128

typedef enum _connectorType
{
	TYPE_UNDEF = 0,
	TYPE_MYSQL,
	TYPE_ORACLE,
	TYPE_SQLSERVER,
} ConnectorType;

typedef enum _connectorState
{
	STATE_UNDEF = 0,
	STATE_STOPPED,		/* connector is stopped */
	STATE_INITIALIZING,	/* connector is initializing java dbz runner engine */
	STATE_PAUSED,		/* connector paused until commanded to resume */
	STATE_SYNCING,		/* connector is polling changes from dbz engine */
	STATE_PARSING,		/* got a change event, try to parse it */
	STATE_CONVERTING,	/* parsing done, try to convert it to pg */
	STATE_EXECUTING,	/* conversion done, try to execute it on pg */
} ConnectorState;

typedef struct _MysqlStateInfo
{
	/* todo */
	pid_t pid;
	ConnectorState state;
	char errmsg[SYNCHDB_ERRMSG_SIZE];
} MysqlStateInfo;

typedef struct _OracleStateInfo
{
	/* todo */
	pid_t pid;
	ConnectorState state;
	char errmsg[SYNCHDB_ERRMSG_SIZE];
} OracleStateInfo;

typedef struct _SqlserverStateInfo
{
	/* todo */
	pid_t pid;
	ConnectorState state;
	char errmsg[SYNCHDB_ERRMSG_SIZE];
} SqlserverStateInfo;

/* Shared state information for synchdb bgworker. */
typedef struct _SynchdbSharedState
{
	LWLock		lock;		/* mutual exclusion */
	MysqlStateInfo mysqlinfo;
	OracleStateInfo oracleinfo;
	SqlserverStateInfo sqlserverinfo;

} SynchdbSharedState;

const char * get_shm_connector_name(ConnectorType type);
pid_t get_shm_connector_pid(ConnectorType type);
void set_shm_connector_pid(ConnectorType type, pid_t pid);
void set_shm_connector_errmsg(ConnectorType type, char * err);
const char * get_shm_connector_errmsg(ConnectorType type);
void set_shm_connector_state(ConnectorType type, ConnectorState state);
const char * get_shm_connector_state(ConnectorType type);


#endif /* SYNCHDB_SYNCHDB_H_ */