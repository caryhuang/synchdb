/*
 * synchdb.h
 *
 * Header file for the SynchDB synchronization system
 *
 * This file defines the core structures and functions used by SynchDB
 * to manage database synchronization across different connector types.
 *
 * Key components:
 * - Connector types and states
 * - Shared state structures for different database connectors
 * - Function prototypes for shared memory operations
 * 
 * Copyright (c) 2024 Hornetlabs Technology, Inc.
 *
 */

#ifndef SYNCHDB_SYNCHDB_H_
#define SYNCHDB_SYNCHDB_H_

#include "storage/lwlock.h"

/* Constants */
#define SYNCHDB_ERRMSG_SIZE 128
#define SYNCHDB_MAX_DB_NAME_SIZE 64
#define SYNCHDB_DATATYPE_NAME_SIZE 64

#define SYNCHDB_MYSQL_OFFSET_FILE "pg_synchdb/mysql_offsets.dat"
#define SYNCHDB_ORACLE_OFFSET_FILE "pg_synchdb/oracle_offsets.dat"
#define SYNCHDB_SQLSERVER_OFFSET_FILE "pg_synchdb/sqlserver_offsets.dat"

/* Enumerations */

/**
 * ConnectorType - Enum representing different types of database connectors
 */
typedef enum _connectorType
{
	TYPE_UNDEF = 0,
	TYPE_MYSQL,
	TYPE_ORACLE,
	TYPE_SQLSERVER,
} ConnectorType;

/**
 * ConnectorState - Enum representing different states of a connector
 */
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
	STATE_OFFSET_UPDATE,/* in this state when user requests offset update */
} ConnectorState;

/**
 * SynchdbRequest - Structure representing a request to change connector state
 */
typedef struct _SynchdbRequest
{
	ConnectorState reqstate;
	char reqdata[SYNCHDB_ERRMSG_SIZE];
} SynchdbRequest;

/**
 *  Structure holding state information for connectors
 */
typedef struct _MysqlStateInfo
{
	/* todo */
	pid_t pid;
	ConnectorState state;
	SynchdbRequest req;
	char errmsg[SYNCHDB_ERRMSG_SIZE];
	char dbzoffset[SYNCHDB_ERRMSG_SIZE];
	char srcdb[SYNCHDB_MAX_DB_NAME_SIZE];
	char dstdb[SYNCHDB_MAX_DB_NAME_SIZE];
} MysqlStateInfo;

typedef struct _OracleStateInfo
{
	/* todo */
	pid_t pid;
	ConnectorState state;
	SynchdbRequest req;
	char errmsg[SYNCHDB_ERRMSG_SIZE];
	char dbzoffset[SYNCHDB_ERRMSG_SIZE];
	char srcdb[SYNCHDB_MAX_DB_NAME_SIZE];
	char dstdb[SYNCHDB_MAX_DB_NAME_SIZE];
} OracleStateInfo;

typedef struct _SqlserverStateInfo
{
	/* todo */
	pid_t pid;
	ConnectorState state;
	SynchdbRequest req;
	char errmsg[SYNCHDB_ERRMSG_SIZE];
	char dbzoffset[SYNCHDB_ERRMSG_SIZE];
	char srcdb[SYNCHDB_MAX_DB_NAME_SIZE];
	char dstdb[SYNCHDB_MAX_DB_NAME_SIZE];
} SqlserverStateInfo;

/**
 * SynchdbSharedState - Shared state information for synchdb background worker
 */
typedef struct _SynchdbSharedState
{
	LWLock		lock;		/* mutual exclusion */
	MysqlStateInfo mysqlinfo;
	OracleStateInfo oracleinfo;
	SqlserverStateInfo sqlserverinfo;
} SynchdbSharedState;

/**
 * ConnectionInfo - DBZ Connection info
 */
typedef struct _ConnectionInfo
{
    char *hostname;
    unsigned int port;
    char *user;
    char *pwd;
    char *src_db;
    char *dst_db;
    char *table;
} ConnectionInfo;

/* Function prototypes */

const char * get_shm_connector_name(ConnectorType type);
pid_t get_shm_connector_pid(ConnectorType type);
void set_shm_connector_pid(ConnectorType type, pid_t pid);
void set_shm_connector_errmsg(ConnectorType type, const char * err);
const char * get_shm_connector_errmsg(ConnectorType type);
void set_shm_connector_state(ConnectorType type, ConnectorState state);
const char * get_shm_connector_state(ConnectorType type);
void set_shm_connector_dbs(ConnectorType type, char * srcdb, char * dstdb);
void set_shm_dbz_offset(ConnectorType type);
const char * get_shm_dbz_offset(ConnectorType type);
ConnectorState get_shm_connector_state_enum(ConnectorType type);
const char* connectorTypeToString(ConnectorType type);

#endif /* SYNCHDB_SYNCHDB_H_ */
