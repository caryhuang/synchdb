/*
 * synchdb.c
 *
 * Implementation of SynchDB functionality for PostgreSQL
 *
 * This file contains the core functionality for the SynchDB extension,
 * including background worker management, JNI interactions with the
 * Debezium engine, and shared memory state management.
 *
 * Key components:
 * - JNI setup and interaction with Debezium
 * - Background worker management
 * - Shared memory state management
 * - User-facing functions for controlling SynchDB
 *
 * Copyright (c) 2024 Hornetlabs Technology, Inc.
 *
 */

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include <jni.h>
#include <unistd.h>
#include "format_converter.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/procsignal.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "storage/fd.h"
#include "miscadmin.h"
#include "utils/wait_event.h"
#include "utils/guc.h"
#include "varatt.h"
#include "funcapi.h"
#include "synchdb.h"
#include "replication_agent.h"

PG_MODULE_MAGIC;

/* Function declarations for user-facing functions */
PG_FUNCTION_INFO_V1(synchdb_stop_engine_bgw);
PG_FUNCTION_INFO_V1(synchdb_start_engine_bgw);
PG_FUNCTION_INFO_V1(synchdb_get_state);
PG_FUNCTION_INFO_V1(synchdb_pause_engine);
PG_FUNCTION_INFO_V1(synchdb_resume_engine);
PG_FUNCTION_INFO_V1(synchdb_set_offset);
PG_FUNCTION_INFO_V1(synchdb_add_conninfo);

/* Constants */
#define SYNCHDB_METADATA_DIR "pg_synchdb"
#define DBZ_ENGINE_JAR_FILE "dbz-engine-1.0.0.jar"
#define MAX_PATH_LENGTH 1024
#define MAX_JAVA_OPTION_LENGTH 512

/* Global variables */
SynchdbSharedState *sdb_state = NULL; /* Pointer to shared-memory state. */
int myConnectorId = -1;	/* Global index number to SynchdbSharedState in shared memory - global per worker */

/* GUC variables */
int synchdb_worker_naptime = 5;
bool synchdb_dml_use_spi = false;

/* JNI-related variables */
static JavaVM *jvm = NULL; /* represents java vm instance */
static JNIEnv *env = NULL; /* represents JNI run-time environment */
static jclass cls;		   /* represents debezium runner java class */
static jobject obj;		   /* represents debezium runner java class object */

/* Function declarations */
PGDLLEXPORT void synchdb_engine_main(Datum main_arg);

/* Static function prototypes */
static int dbz_engine_stop(void);
static int dbz_engine_init(JNIEnv *env, jclass *cls, jobject *obj);
static int dbz_engine_get_change(JavaVM *jvm, JNIEnv *env, jclass *cls, jobject *obj);
static int dbz_engine_start(const ConnectionInfo *connInfo, ConnectorType connectorType);
static char *dbz_engine_get_offset(int connectorId);
static TupleDesc synchdb_state_tupdesc(void);
static void synchdb_init_shmem(void);
static void synchdb_detach_shmem(int code, Datum arg);
static void prepare_bgw(BackgroundWorker *worker, const ConnectionInfo *connInfo, const char *connector, int connectorid);
static const char *connectorStateAsString(ConnectorState state);
static void reset_shm_request_state(int connectorId);
static int dbz_engine_set_offset(ConnectorType connectorType, char *db, char *offset, char *file);
static void processRequestInterrupt(const ConnectionInfo *connInfo, ConnectorType type, int connectorId);
static void parse_arguments(Datum main_arg, ConnectorType *connectorType, ConnectionInfo *connInfo);
static void setup_environment(ConnectorType connectorType, const char *dst_db);
static void initialize_jvm(void);
static void start_debezium_engine(ConnectorType connectorType, const ConnectionInfo *connInfo);
static void main_loop(ConnectorType connectorType, const ConnectionInfo *connInfo);
static void cleanup(ConnectorType connectorType);

/*
 * dbz_engine_stop - Stop the Debezium engine
 *
 * This function stops the Debezium engine by calling the stopEngine method
 * on the DebeziumRunner object.
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_stop(void)
{
	jmethodID stopEngine;
	jthrowable exception;

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		return -1;
	}
	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		return -1;
	}

	/* Find the stopEngine method */
	stopEngine = (*env)->GetMethodID(env, cls, "stopEngine", "()V");
	if (stopEngine == NULL)
	{
		elog(WARNING, "Failed to find stopEngine method");
		return -1;
	}

	(*env)->CallVoidMethod(env, obj, stopEngine);

	/* Check for exceptions */
	exception = (*env)->ExceptionOccurred(env);
	if (exception)
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while stopping Debezium engine");
		return -1;
	}

	return 0;
}

/*
 * dbz_engine_init - Initialize the Debezium engine
 *
 * This function initializes the Debezium engine by finding the DebeziumRunner
 * class and allocating an instance of it. It handles JNI interactions and
 * exception checking.
 *
 * @param env: JNI environment pointer
 * @param cls: Pointer to store the found Java class
 * @param obj: Pointer to store the allocated Java object
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_init(JNIEnv *env, jclass *cls, jobject *obj)
{
	elog(DEBUG1, "dbz_engine_init - Starting initialization");

	/* Find the DebeziumRunner class */
	*cls = (*env)->FindClass(env, "com/example/DebeziumRunner");
	if (*cls == NULL)
	{
		if ((*env)->ExceptionCheck(env))
		{
			(*env)->ExceptionDescribe(env);
			(*env)->ExceptionClear(env);
		}
		elog(WARNING, "Failed to find com.example.DebeziumRunner class");
		return -1;
	}

	elog(DEBUG1, "dbz_engine_init - Class found, allocating object");

	/* Allocate an instance of the DebeziumRunner class */
	*obj = (*env)->AllocObject(env, *cls);
	if (*obj == NULL)
	{
		if ((*env)->ExceptionCheck(env))
		{
			(*env)->ExceptionDescribe(env);
			(*env)->ExceptionClear(env);
		}
		elog(WARNING, "Failed to allocate DBZ Runner object");
		return -1;
	}

	elog(DEBUG1, "dbz_engine_init - Object allocated successfully");

	return 0;
}

/*
 * dbz_engine_get_change - Retrieve and process change events from the Debezium engine
 *
 * This function retrieves change events from the Debezium engine and processes them.
 *
 * @param jvm: Pointer to the Java VM
 * @param env: Pointer to the JNI environment
 * @param cls: Pointer to the DebeziumRunner class
 * @param obj: Pointer to the DebeziumRunner object
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_get_change(JavaVM *jvm, JNIEnv *env, jclass *cls, jobject *obj)
{
	jmethodID getChangeEvents, sizeMethod, getMethod;
	jobject changeEventsList;
	jint size;
	jclass listClass;
	jobject event;
	const char *eventStr;

	/* Validate input parameters */
	if (!jvm || !env || !cls || !obj)
	{
		elog(WARNING, "dbz_engine_get_change: Invalid input parameters");
		return -1;
	}

	/* Get the getChangeEvents method */
	getChangeEvents = (*env)->GetMethodID(env, *cls, "getChangeEvents", "()Ljava/util/List;");
	if (getChangeEvents == NULL)
	{
		elog(WARNING, "Failed to find getChangeEvents method");
		return -1;
	}

	/* Call getChangeEvents method */
	changeEventsList = (*env)->CallObjectMethod(env, *obj, getChangeEvents);

	if ((*env)->ExceptionCheck(env))
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while calling getChangeEvents");
		return -1;
	}

	if (changeEventsList == NULL)
	{
		elog(WARNING, "dbz_engine_get_change: getChangeEvents returned null");
		return -1;
	}

	/* Get List class and methods */
	listClass = (*env)->FindClass(env, "java/util/List");
	if (listClass == NULL)
	{
		elog(WARNING, "Failed to find java list class");
		return -1;
	}

	sizeMethod = (*env)->GetMethodID(env, listClass, "size", "()I");
	if (sizeMethod == NULL)
	{
		elog(WARNING, "Failed to find java list.size method");
		return -1;
	}

	getMethod = (*env)->GetMethodID(env, listClass, "get", "(I)Ljava/lang/Object;");
	if (getMethod == NULL)
	{
		elog(WARNING, "Failed to find java list.get method");
		return -1;
	}

	/* Process change events */
	size = (*env)->CallIntMethod(env, changeEventsList, sizeMethod);
	elog(DEBUG1, "dbz_engine_get_change: Retrieved %d change events", size);

	for (int i = 0; i < size; i++)
	{
		event = (*env)->CallObjectMethod(env, changeEventsList, getMethod, i);
		if (event == NULL)
		{
			elog(WARNING, "dbz_engine_get_change: Received NULL event at index %d", i);
			continue;
		}

		eventStr = (*env)->GetStringUTFChars(env, (jstring)event, 0);
		if (eventStr == NULL)
		{
			elog(WARNING, "dbz_engine_get_change: Failed to get string for event at index %d", i);
			(*env)->DeleteLocalRef(env, event);
			continue;
		}

		elog(WARNING, "Processing DBZ Event: %s", eventStr);

		if (fc_processDBZChangeEvent(eventStr) != 0)
		{
			elog(WARNING, "dbz_engine_get_change: Failed to process event at index %d", i);
		}

		(*env)->ReleaseStringUTFChars(env, (jstring)event, eventStr);
		(*env)->DeleteLocalRef(env, event);
	}

	(*env)->DeleteLocalRef(env, changeEventsList);
	(*env)->DeleteLocalRef(env, listClass);
	return 0;
}

/*
 * dbz_engine_start - Start the Debezium engine
 *
 * This function starts the Debezium engine with the provided connection information.
 *
 * @param connInfo: Pointer to the ConnectionInfo structure containing connection details
 * @param connectorType: The type of connector to start
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_start(const ConnectionInfo *connInfo, ConnectorType connectorType)
{
	jmethodID mid;
	jstring jHostname, jUser, jPassword, jDatabase, jTable, jName;
	jthrowable exception;

	elog(LOG, "dbz_engine_start: Starting dbz engine %s:%d ", connInfo->hostname, connInfo->port);
	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		return -1;
	}

	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		return -1;
	}

	/* Find the startEngine method */
	mid = (*env)->GetMethodID(env, cls, "startEngine",
							  "(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;ILjava/lang/String;)V");
	if (mid == NULL)
	{
		elog(WARNING, "Failed to find startEngine method");
		return -1;
	}

	/* Create Java strings from C strings */
	jHostname = (*env)->NewStringUTF(env, connInfo->hostname);
	jUser = (*env)->NewStringUTF(env, connInfo->user);
	jPassword = (*env)->NewStringUTF(env, connInfo->pwd);
	jDatabase = (*env)->NewStringUTF(env, connInfo->src_db);
	jTable = (*env)->NewStringUTF(env, connInfo->table);
	jName = (*env)->NewStringUTF(env, connInfo->name);

	/* Call the Java method */
	(*env)->CallVoidMethod(env, obj, mid, jHostname, connInfo->port, jUser, jPassword, jDatabase, jTable, connectorType, jName);

	/* Check for exceptions */
	exception = (*env)->ExceptionOccurred(env);
	if (exception)
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while starting Debezium engine");
		goto cleanup;
	}

	elog(LOG, "Debezium engine started successfully for %s connector", connectorTypeToString(connectorType));

cleanup:
	/* Clean up local references */
	if (jHostname)
		(*env)->DeleteLocalRef(env, jHostname);
	if (jUser)
		(*env)->DeleteLocalRef(env, jUser);
	if (jPassword)
		(*env)->DeleteLocalRef(env, jPassword);
	if (jDatabase)
		(*env)->DeleteLocalRef(env, jDatabase);
	if (jTable)
		(*env)->DeleteLocalRef(env, jTable);
	if (jName)
		(*env)->DeleteLocalRef(env, jName);

	return exception ? -1 : 0;
}

/*
 * dbz_engine_get_offset - Get the current offset from the Debezium engine
 *
 * This function retrieves the current offset for a specific connector type
 * from the Debezium engine.
 *
 * @param connectorType: The type of connector to get the offset for
 *
 * @return: The offset as a string (caller must free), or NULL on failure
 */
static char *
dbz_engine_get_offset(int connectorId)
{
	jmethodID getoffsets;
	jstring jdb, result, jName;
	char *resultStr = NULL;
	char *db = NULL, *name = NULL;
	const char *tmp;
	jthrowable exception;

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		return NULL;
	}

	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		return NULL;
	}

	/* Get the source database name based on connector type */
	db = sdb_state->connectors[connectorId].srcdb;
	if (!db)
	{
		elog(WARNING, "Source database name not set for connector type: %d",
				sdb_state->connectors[connectorId].type);
		return NULL;
	}

	/* Get the unique name */
	name = sdb_state->connectors[connectorId].name;
	if (!name)
	{
		elog(WARNING, "Unique name not set for connector type: %d",
				sdb_state->connectors[connectorId].type);
		return NULL;
	}

	getoffsets = (*env)->GetMethodID(env, cls, "getConnectorOffset",
									 "(ILjava/lang/String;Ljava/lang/String;)Ljava/lang/String;");
	if (getoffsets == NULL)
	{
		elog(WARNING, "Failed to find getConnectorOffset method");
		return NULL;
	}

	jdb = (*env)->NewStringUTF(env, db);
	jName = (*env)->NewStringUTF(env, name);

	result = (jstring)(*env)->CallObjectMethod(env, obj, getoffsets,
			(int)sdb_state->connectors[connectorId].type, jdb, jName);
	/* Check for exceptions */
	exception = (*env)->ExceptionOccurred(env);
	if (exception)
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while getting connector offset");
		(*env)->DeleteLocalRef(env, jdb);
		(*env)->DeleteLocalRef(env, jName);
		return NULL;
	}

	/* Convert Java string to C string */
	tmp = (*env)->GetStringUTFChars(env, result, NULL);
	if (tmp && strlen(tmp) > 0)
		resultStr = pstrdup(tmp);
	else
		resultStr = pstrdup("no offset");

	/* Clean up */
	(*env)->ReleaseStringUTFChars(env, result, tmp);
	(*env)->DeleteLocalRef(env, jdb);
	(*env)->DeleteLocalRef(env, result);
	(*env)->DeleteLocalRef(env, jName);

	elog(DEBUG1, "Retrieved offset for %s connector: %s",
			connectorTypeToString(sdb_state->connectors[connectorId].type), resultStr);

	return resultStr;
}

/*
 * synchdb_state_tupdesc - Create a TupleDesc for SynchDB state information
 *
 * This function constructs a TupleDesc that describes the structure of
 * the tuple returned by SynchDB state queries. It defines the columns
 * that will be present in the result set.
 *
 * Returns: A blessed TupleDesc, or NULL on failure
 */
static TupleDesc
synchdb_state_tupdesc(void)
{
	TupleDesc tupdesc;
	AttrNumber attrnum = 7;
	AttrNumber a = 0;

	tupdesc = CreateTemplateTupleDesc(attrnum);

	/* todo: add more columns here per connector if needed */
	TupleDescInitEntry(tupdesc, ++a, "id", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "connector", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "conninfo_name", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "pid", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "state", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "err", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "last_dbz_offset", TEXTOID, -1, 0);

	Assert(a == maxattr);
	return BlessTupleDesc(tupdesc);
}

/*
 * synchdb_init_shmem - Initialize or attach to synchdb shared memory
 *
 * Allocate and initialize synchdb related shared memory, if not already
 * done, and set up backend-local pointer to that state.
 *
 */
static void
synchdb_init_shmem(void)
{
	bool found;
	int i = 0;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	sdb_state = ShmemInitStruct("synchdb",
								sizeof(SynchdbSharedState),
								&found);
	if (!found)
	{
		/* First time through ... */
		LWLockInitialize(&sdb_state->lock, LWLockNewTrancheId());
		for (i = 0; i < SYNCHDB_MAX_ACTIVE_CONNECTORS; i++)
		{
			sdb_state->connectors[i].pid = InvalidPid;
			sdb_state->connectors[i].state = STATE_UNDEF;
			sdb_state->connectors[i].type = TYPE_UNDEF;
		}
	}
	LWLockRelease(AddinShmemInitLock);
	LWLockRegisterTranche(sdb_state->lock.tranche, "synchdb");
}

/*
 * synchdb_detach_shmem - Detach from synchdb shared memory
 *
 * This function is responsible for detaching a process from the synchdb shared memory.
 * It updates the shared memory state if the current process is the one registered for
 * the specified connector type.
 *
 * @param code: An integer representing the exit code or reason for detachment
 * @param arg: A Datum containing the connector type as an unsigned integer
 */
static void
synchdb_detach_shmem(int code, Datum arg)
{
	pid_t enginepid;

	elog(LOG, "synchdb detach shm ... myConnectorId %d, code %d",
		 DatumGetUInt32(arg), code);

	enginepid = get_shm_connector_pid(DatumGetUInt32(arg));
	if (enginepid == MyProcPid)
	{
		set_shm_connector_pid(DatumGetUInt32(arg), InvalidPid);
		set_shm_connector_state(DatumGetUInt32(arg), STATE_UNDEF);
	}
}

/*
 * prepare_bgw - Prepare a background worker for synchdb
 *
 * This function sets up a BackgroundWorker structure with the appropriate
 * information based on the connector type and connection details.
 *
 * @param worker: Pointer to the BackgroundWorker structure to be prepared
 * @param connInfo: Pointer to the ConnectionInfo structure containing connection details
 * @param connector: String representing the connector type
 */
static void
prepare_bgw(BackgroundWorker *worker, const ConnectionInfo *connInfo, const char *connector, int connectorid)

{
	ConnectorType type = fc_get_connector_type(connector);

	worker->bgw_main_arg = UInt32GetDatum(connectorid);
	snprintf(worker->bgw_name, BGW_MAXLEN, "synchdb engine: %s@%s:%u", connector, connInfo->hostname, connInfo->port);
	snprintf(worker->bgw_type, BGW_MAXLEN, "synchdb engine: %s", connector);

	/* append destination database to worker->bgw_name for clarity */
	strcat(worker->bgw_name, " -> ");
	strcat(worker->bgw_name, connInfo->dst_db);

	/*
	 * Format the extra args into the bgw_extra field
	 * todo: BGW_EXTRALEN is only 128 bytes, so the formatted string will be
	 * cut off here if exceeding this length. Maybe there is a better way to
	 * pass these args to background worker?
	 */
	snprintf(worker->bgw_extra, BGW_EXTRALEN, "%s:%u:%s:%s:%s:%s:%s:%d:%s",
			 connInfo->hostname, connInfo->port, connInfo->user, connInfo->pwd,
			 connInfo->src_db, connInfo->dst_db, connInfo->table, type, connInfo->name);
}

/*
 * connectorStateAsString - Convert ConnectorState to string representation
 */
static const char *
connectorStateAsString(ConnectorState state)
{
	switch (state)
	{
	case STATE_UNDEF:
	case STATE_STOPPED:
		return "stopped";
	case STATE_INITIALIZING:
		return "initializing";
	case STATE_PAUSED:
		return "paused";
	case STATE_SYNCING:
		return "syncing";
	case STATE_PARSING:
		return "parsing";
	case STATE_CONVERTING:
		return "converting";
	case STATE_EXECUTING:
		return "executing";
	case STATE_OFFSET_UPDATE:
		return "updating offset";
	}
	return "UNKNOWN";
}

/*
 * reset_shm_request_state - Reset the shared memory request state for a connector
 *
 * This function resets the request state and clears the request data for a
 * specific connector type in shared memory.
 *
 * @param type: The type of connector whose request state should be reset
 */
static void
reset_shm_request_state(int connectorId)
{
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	sdb_state->connectors[connectorId].req.reqstate = STATE_UNDEF;
	memset(sdb_state->connectors[connectorId].req.reqdata, 0, SYNCHDB_ERRMSG_SIZE);
	LWLockRelease(&sdb_state->lock);
}

/*
 * dbz_engine_set_offset - Set the offset for the Debezium engine
 *
 * This function interacts with the Java VM to set the offset for a specific
 * Debezium connector.
 *
 * @param connectorType: The type of connector
 * @param db: The database name
 * @param offset: The offset to set
 * @param file: The file to store the offset
 *
 * @return: 0 on success, -1 on failure
 */
static int
dbz_engine_set_offset(ConnectorType connectorType, char *db, char *offset, char *file)
{
	jmethodID setoffsets;
	jstring joffsetstr, jdb, jfile;
	jthrowable exception;

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
		return -1;
	}

	if (!env)
	{
		elog(WARNING, "jvm env not initialized");
		return -1;
	}

	/* Find the setConnectorOffset method */
	setoffsets = (*env)->GetMethodID(env, cls, "setConnectorOffset",
									 "(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;)V");
	if (setoffsets == NULL)
	{
		elog(WARNING, "Failed to find setConnectorOffset method");
		return -1;
	}

	/* Create Java strings from C strings */
	joffsetstr = (*env)->NewStringUTF(env, offset);
	jdb = (*env)->NewStringUTF(env, db);
	jfile = (*env)->NewStringUTF(env, file);

	/* Call the Java method */
	(*env)->CallVoidMethod(env, obj, setoffsets, jfile, (int)connectorType, jdb, joffsetstr);

	/* Check for exceptions */
	exception = (*env)->ExceptionOccurred(env);
	if (exception)
	{
		(*env)->ExceptionDescribe(env);
		(*env)->ExceptionClear(env);
		elog(WARNING, "Exception occurred while setting connector offset");
		return -1;
	}

	/* Clean up local references */
	(*env)->DeleteLocalRef(env, joffsetstr);
	(*env)->DeleteLocalRef(env, jdb);
	(*env)->DeleteLocalRef(env, jfile);

	elog(LOG, "Successfully set offset for %s connector", connectorTypeToString(connectorType));
	return 0;
}

/**
 * processRequestInterrupt - Handles state transition requests for SynchDB connectors
 *
 * This function processes requests to change the state of a SynchDB connector,
 * such as pausing, resuming, or updating offsets.
 *
 * @param connInfo: Pointer to the connection information
 * @param type: The type of connector being processed
 */
static void
processRequestInterrupt(const ConnectionInfo *connInfo, ConnectorType type, int connectorId)
{
	SynchdbRequest *req, *reqcopy;
	ConnectorState *currstate, *currstatecopy;
	char offsetfile[SYNCHDB_JSON_PATH_SIZE] = {0};
	char *srcdb;
	int ret;

	if (!sdb_state)
		return;

	req = &(sdb_state->connectors[connectorId].req);
	currstate = &(sdb_state->connectors[connectorId].state);
	srcdb = sdb_state->connectors[connectorId].srcdb;

	/*
	 * make a copy of requested state, its data and curr state to avoid holding locks
	 * for too long. Currently we support 1 request at any one time.
	 */
	reqcopy = palloc0(sizeof(SynchdbRequest));
	currstatecopy = palloc0(sizeof(ConnectorState));

	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	memcpy(reqcopy, req, sizeof(SynchdbRequest));
	memcpy(currstatecopy, currstate, sizeof(ConnectorState));
	LWLockRelease(&sdb_state->lock);

	/* Process the request based on current and requested states */
	if (reqcopy->reqstate == STATE_UNDEF)
	{
		/* no requests, do nothing */
	}
	else if (reqcopy->reqstate == STATE_PAUSED && *currstatecopy == STATE_SYNCING)
	{
		/* we can only transition to STATE_PAUSED from STATE_SYNCING */
		elog(LOG, "Pausing %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));

		elog(WARNING, "shut down dbz engine...");
		ret = dbz_engine_stop();
		if (ret)
		{
			elog(WARNING, "failed to stop dbz engine...");
			reset_shm_request_state(connectorId);
			pfree(reqcopy);
			pfree(currstatecopy);
			return;
		}
		set_shm_connector_state(connectorId, STATE_PAUSED);
	}
	else if (reqcopy->reqstate == STATE_SYNCING && *currstatecopy == STATE_PAUSED)
	{
		/* Handle resume request, we can only transition to STATE_SYNCING from STATE_PAUSED */
		elog(LOG, "Resuming %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));

		/* restart dbz engine */
		elog(WARNING, "restart dbz engine...");

		ret = dbz_engine_start(connInfo, type);
		if (ret < 0)
		{
			elog(WARNING, "Failed to restart dbz engine");
			reset_shm_request_state(connectorId);
			pfree(reqcopy);
			pfree(currstatecopy);
			return;
		}
		set_shm_connector_state(connectorId, STATE_SYNCING);
	}
	else if (reqcopy->reqstate == STATE_OFFSET_UPDATE && *currstatecopy == STATE_PAUSED)
	{
		/* Handle offset update request */
		elog(LOG, "Updating offset for %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));

		/* derive offset file*/
		snprintf(offsetfile, SYNCHDB_JSON_PATH_SIZE, SYNCHDB_OFFSET_FILE_PATTERN,
				get_shm_connector_name(type), connInfo->name);

		set_shm_connector_state(connectorId, STATE_OFFSET_UPDATE);
		ret = dbz_engine_set_offset(type, srcdb, reqcopy->reqdata, offsetfile);
		if (ret < 0)
		{
			elog(WARNING, "Failed to set offset for %s connector", connectorTypeToString(type));
			reset_shm_request_state(connectorId);
			set_shm_connector_state(connectorId, STATE_PAUSED);
			pfree(reqcopy);
			pfree(currstatecopy);
			return;
		}

		/* after new offset is set, change state back to STATE_PAUSED */
		set_shm_connector_state(connectorId, STATE_PAUSED);

		/* and also update this worker's shm offset */
		set_shm_dbz_offset(connectorId);
	}
	else
	{
		/* unsupported request state combinations */
		elog(WARNING, "Invalid state transition requested for %s connector. Current state: %s, Requested state: %s",
			 connectorTypeToString(type),
			 connectorStateAsString(*currstatecopy),
			 connectorStateAsString(reqcopy->reqstate));
	}

	/* reset request state so we can receive more requests to process */
	reset_shm_request_state(connectorId);
	pfree(reqcopy);
	pfree(currstatecopy);
}

/**
 * parse_arguments - Parses and validates arguments for the SynchDB engine
 *
 * This function extracts connection information from the background worker's
 * extra data and validates the required fields.
 *
 * @param main_arg: The main argument containing the connector type
 * @param connectorType: Pointer to store the parsed connector type
 * @param connInfo: Pointer to store the parsed connection information
 */
static void
parse_arguments(Datum main_arg, ConnectorType *connectorType, ConnectionInfo *connInfo)
{
	char *args, *tmp;

	/* Extract connector type from main argument */
	myConnectorId = DatumGetUInt32(main_arg);

	/* Copy and parse the extra arguments */
	args = pstrdup(MyBgworkerEntry->bgw_extra);

	/* Parse individual fields */
	tmp = strtok(args, ":");
	if (tmp)
		connInfo->hostname = pstrdup(tmp);
	tmp = strtok(NULL, ":");
	if (tmp)
		connInfo->port = atoi(tmp);
	tmp = strtok(NULL, ":");
	if (tmp)
		connInfo->user = pstrdup(tmp);
	tmp = strtok(NULL, ":");
	if (tmp)
		connInfo->pwd = pstrdup(tmp);
	tmp = strtok(NULL, ":");
	if (tmp)
		connInfo->src_db = pstrdup(tmp);
	tmp = strtok(NULL, ":");
	if (tmp)
		connInfo->dst_db = pstrdup(tmp);
	tmp = strtok(NULL, ":");
	if (tmp)
		connInfo->table = pstrdup(tmp);
	tmp = strtok(NULL, ":");
	if (tmp)
		*connectorType = atoi(tmp);
	tmp = strtok(NULL, ":");
	if (tmp)
		connInfo->name = pstrdup(tmp);

	pfree(args);

	/* Validate required fields */
	if (!connInfo->hostname || !connInfo->user || !connInfo->pwd || !connInfo->dst_db)
	{
		set_shm_connector_errmsg(myConnectorId, "Missing required arguments for SynchDB engine initialization");
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("missing required arguments for SynchDB engine initialization"),
				 errhint("hostname, user, password and destination database are required")));
	}

	/* Log parsed arguments (TODO: consider removing or obfuscating sensitive data in production) */
	elog(LOG, "SynchDB engine initialized with: myConnectorId %d, host %s, port %u, user %s, src_db %s, dst_db %s, table %s, connectorType %u (%s), conninfo_name %s",
			myConnectorId,
			connInfo->hostname, connInfo->port, connInfo->user,
			connInfo->src_db ? connInfo->src_db : "N/A",
			connInfo->dst_db,
			connInfo->table ? connInfo->table : "N/A",
			*connectorType, connectorTypeToString(*connectorType),
			connInfo->name);
}

/**
 * setup_environment - Prepares the environment for the SynchDB background worker
 *
 * This function sets up signal handlers, initializes the database connection,
 * sets up shared memory, and checks for existing worker processes.
 *
 * @param connectorType: The type of connector being set up
 * @param dst_db: The name of the destination database to connect to
 */
static void
setup_environment(ConnectorType connectorType, const char *dst_db)
{
	pid_t enginepid;

	/* Establish signal handlers */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);

	/* Unblock signals to allow handling */
	BackgroundWorkerUnblockSignals();

	/* Connect to current database: NULL user - bootstrap superuser is used */
	BackgroundWorkerInitializeConnection(dst_db, NULL, 0);

	/* Initialize or attach to SynchDB shared memory */
	synchdb_init_shmem();

	/* Set up cleanup handler for shared memory */
	on_shmem_exit(synchdb_detach_shmem, UInt32GetDatum(myConnectorId));

	/* Check if the worker is already running */
	enginepid = get_shm_connector_pid(myConnectorId);
	if (enginepid != InvalidPid)
		ereport(ERROR,
				(errmsg("synchdb %s worker (%u) is already running under PID %d",
						connectorTypeToString(connectorType),
						connectorType,
						(int)enginepid)));

	/* Register this process as the worker for this connector type */
	set_shm_connector_pid(myConnectorId, MyProcPid);
	sdb_state->connectors[myConnectorId].type = connectorType;

	elog(LOG, "Environment setup completed for SynchDB %s worker (type %u)",
		 connectorTypeToString(connectorType), connectorType);
}

/**
 * initialize_jvm - Initialize the Java Virtual Machine and Debezium engine
 *
 * This function sets up the Java environment, locates the Debezium engine JAR file,
 * creates a Java VM, and initializes the Debezium engine.
 *
 * @param connectorType: The type of connector being initialized
 */
static void
initialize_jvm(void)
{
	JavaVMInitArgs vm_args;
	JavaVMOption options[2];
	char javaopt[MAX_JAVA_OPTION_LENGTH] = {0};
	char jar_path[MAX_PATH_LENGTH] = {0};
	const char *dbzpath = getenv("DBZ_ENGINE_DIR");
	int ret;

	/* Determine the path to the Debezium engine JAR file */
	if (dbzpath)
	{
		snprintf(jar_path, sizeof(jar_path), "%s/%s", dbzpath, DBZ_ENGINE_JAR_FILE);
	}
	else
	{
		snprintf(jar_path, sizeof(jar_path), "%s/dbz_engine/%s", pkglib_path, DBZ_ENGINE_JAR_FILE);
	}

	/* Check if the JAR file exists */
	if (access(jar_path, F_OK) == -1)
	{
		set_shm_connector_errmsg(myConnectorId, "Cannot find DBZ engine jar file");
		elog(ERROR, "Cannot find DBZ engine jar file at %s", jar_path);
	}

	/* Set up Java classpath */
	snprintf(javaopt, sizeof(javaopt), "-Djava.class.path=%s", jar_path);
	elog(INFO, "Initializing DBZ engine with JAR file: %s", jar_path);

	/* Configure JVM options */
	options[0].optionString = javaopt;
	options[1].optionString = "-Xrs"; // Reduce use of OS signals by JVM
	vm_args.version = JNI_VERSION_21;
	vm_args.nOptions = 2;
	vm_args.options = options;
	vm_args.ignoreUnrecognized = JNI_FALSE;

	/* Create the Java VM */
	ret = JNI_CreateJavaVM(&jvm, (void **)&env, &vm_args);
	if (ret < 0 || !env)
	{
		set_shm_connector_errmsg(myConnectorId, "Unable to Launch JVM");
		elog(ERROR, "Failed to create Java VM (return code: %d)", ret);
	}

	elog(INFO, "Java VM created successfully");

	/* Initialize the Debezium engine */
	ret = dbz_engine_init(env, &cls, &obj);
	if (ret < 0)
	{
		set_shm_connector_errmsg(myConnectorId, "Failed to initialize Debezium engine");
		elog(ERROR, "Failed to initialize Debezium engine");
	}

	elog(INFO, "Debezium engine initialized successfully");
}

/**
 * start_debezium_engine - Starts the Debezium engine for a given connector
 *
 * This function initiates the Debezium engine using the provided connection
 * information and sets the connector state to SYNCING upon successful start.
 *
 * @param connectorType: The type of connector being started
 * @param connInfo: Pointer to the ConnectionInfo structure containing connection details
 */
static void
start_debezium_engine(ConnectorType connectorType, const ConnectionInfo *connInfo)
{
	int ret = dbz_engine_start(connInfo, connectorType);
	if (ret < 0)
	{
		set_shm_connector_errmsg(myConnectorId, "Failed to start dbz engine");
		elog(ERROR, "Failed to start Debezium engine for connector type %d", connectorType);
	}

	set_shm_connector_state(myConnectorId, STATE_SYNCING);

	elog(LOG, "Debezium engine started successfully for %s:%d (connector type %d)",
		 connInfo->hostname, connInfo->port, connectorType);
}

static void
main_loop(ConnectorType connectorType, const ConnectionInfo *connInfo)
{
	ConnectorState currstate;
	elog(LOG, "Main LOOP ENTER ");
	while (!ShutdownRequestPending)
	{
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		processRequestInterrupt(connInfo, connectorType, myConnectorId);

		currstate = get_shm_connector_state_enum(myConnectorId);
		switch (currstate)
		{
		case STATE_SYNCING:
			dbz_engine_get_change(jvm, env, &cls, &obj);
			break;
		case STATE_PAUSED:
			/* Do nothing when paused */
			break;
		default:
			/* Handle other states if necessary */
			break;
		}

		(void)WaitLatch(MyLatch,
						WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						synchdb_worker_naptime * 1000,
						PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);
	}
	elog(LOG, "Main LOOP QUIT ");
}

static void
cleanup(ConnectorType connectorType)
{
	int ret;

	elog(WARNING, "synchdb_engine_main shutting down");

	ret = dbz_engine_stop();
	if (ret)
	{
		elog(WARNING, "Failed to call dbz engine stop method");
	}

	if (jvm != NULL)
	{
		(*jvm)->DestroyJavaVM(jvm);
		jvm = NULL;
		env = NULL;
	}

	fc_deinitFormatConverter(connectorType);
}

static int
assign_connector_id(char * name)
{
	int i = 0;

	/*
	 * first, check if "name" has been used in one of the connector slots.
	 * If yes, return its index
	 */
	for (i = 0; i < SYNCHDB_MAX_ACTIVE_CONNECTORS; i++)
	{
		if (!strcasecmp(sdb_state->connectors[i].name, name))
		{
			return i;
		}
	}

	/* if not, find the next unnamed free slot */
	for (i = 0; i < SYNCHDB_MAX_ACTIVE_CONNECTORS; i++)
	{
		if (sdb_state->connectors[i].state == STATE_UNDEF &&
				strlen(sdb_state->connectors[i].name) == 0)
		{
			return i;
		}
	}

	/* if not, find the next free slot */
	for (i = 0; i < SYNCHDB_MAX_ACTIVE_CONNECTORS; i++)
	{
		if (sdb_state->connectors[i].state == STATE_UNDEF)
		{
			return i;
		}
	}
	return -1;
}

const char *
connectorTypeToString(ConnectorType type)
{
	switch (type)
	{
	case TYPE_UNDEF:
		return "UNDEFINED";
	case TYPE_MYSQL:
		return "MYSQL";
	case TYPE_ORACLE:
		return "ORACLE";
	case TYPE_SQLSERVER:
		return "SQLSERVER";
	default:
		return "UNKNOWN";
	}
}

/* public functions to access / change synchdb parameters in shared memory */
const char *
get_shm_connector_name(ConnectorType type)
{
	switch (type)
	{
	case TYPE_MYSQL:
		return "mysql";
	case TYPE_ORACLE:
		return "oracle";
	case TYPE_SQLSERVER:
		return "sqlserver";
	/* todo: support more dbz connector types here */
	default:
		return "null";
	}
}

pid_t
get_shm_connector_pid(int connectorId)
{
	if (!sdb_state)
		return InvalidPid;

	return sdb_state->connectors[connectorId].pid;
}

void
set_shm_connector_pid(int connectorId, pid_t pid)
{
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	sdb_state->connectors[connectorId].pid = pid;
	LWLockRelease(&sdb_state->lock);
}

void
set_shm_connector_dbs(int connectorId, char *srcdb, char *dstdb)
{
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	strlcpy(sdb_state->connectors[connectorId].srcdb, srcdb, SYNCHDB_MAX_DB_NAME_SIZE);
	strlcpy(sdb_state->connectors[connectorId].dstdb, dstdb, SYNCHDB_MAX_DB_NAME_SIZE);
	LWLockRelease(&sdb_state->lock);
}

const char *
get_shm_connector_errmsg(int connectorId)
{
	if (!sdb_state)
		return "no error";

	return (sdb_state->connectors[connectorId].errmsg[0] != '\0') ?
			sdb_state->connectors[connectorId].errmsg : "no error";
}

/*
 * set_shm_connector_errmsg - Set the error message for a specific connector in shared memory
 *
 * This function sets the error message for a given connector type in the shared
 * memory state. It ensures thread-safety by using a lock when accessing shared memory.
 *
 * @param type: The type of connector for which to set the error message
 * @param err: The error message to set. If NULL, an empty string will be set.
 */
void
set_shm_connector_errmsg(int connectorId, const char *err)
{
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	strlcpy(sdb_state->connectors[connectorId].errmsg, err ? err : "", SYNCHDB_ERRMSG_SIZE);
	LWLockRelease(&sdb_state->lock);
}

/*
 * get_shm_connector_state - Get the current state of a specific connector from shared memory
 *
 * This function retrieves the current state of a given connector type from the shared
 * memory state. It returns the state as a string representation.
 *
 * @param type: The type of connector for which to get the state
 *
 * @return: A string representation of the connector's state. If the shared memory
 *          is not initialized or the connector type is unknown, it returns "stopped"
 *          or "undefined" respectively.
 */
const char *
get_shm_connector_state(int connectorId)
{
	ConnectorState state;

	if (!sdb_state)
		return "stopped";

	/*
	 * We're only reading, so shared lock is sufficient.
	 * This ensures thread-safety without blocking other readers.
	 */
	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	state = sdb_state->connectors[connectorId].state;
	LWLockRelease(&sdb_state->lock);

	return connectorStateAsString(state);
}

/*
 * get_shm_connector_state_enum - Get the current state enum of a specific connector from shared memory
 *
 * This function retrieves the current state of a given connector type from the shared
 * memory state as a ConnectorState enum value.
 *
 * @param type: The type of connector for which to get the state
 *
 * @return: A ConnectorState enum representing the connector's state. If the shared memory
 *          is not initialized or the connector type is unknown, it returns STATE_UNDEF.
 */
ConnectorState
get_shm_connector_state_enum(int connectorId)
{
	ConnectorState state;

	if (!sdb_state)
		return STATE_UNDEF;

	/*
	 * We're only reading, so shared lock is sufficient.
	 * This ensures thread-safety without blocking other readers.
	 */
	LWLockAcquire(&sdb_state->lock, LW_SHARED);
	state = sdb_state->connectors[connectorId].state;
	LWLockRelease(&sdb_state->lock);

	return state;
}

/*
 * set_shm_connector_state - Set the state of a specific connector in shared memory
 *
 * This function sets the state of a given connector type in the shared memory.
 * It ensures thread-safety by using an exclusive lock when accessing shared memory.
 *
 * @param type: The type of connector for which to set the state
 * @param state: The new state to set for the connector
 */
void
set_shm_connector_state(int connectorId, ConnectorState state)
{
	if (!sdb_state)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	sdb_state->connectors[connectorId].state = state;
	LWLockRelease(&sdb_state->lock);
}

/* TODO: set_shm_dbz_offset:
 * This method reads from dbz's offset file per connector type, which does not
 * reflect the real-time offset of dbz engine. If we were to resume from this point
 * due to an error, there may be duplicate values after the resume in which we must
 * handle. In the future, we will need to explore a more accurate way to find out
 * the offset managed within dbz so we could freely resume from any reference not
 * just at the flushed locations
 */
void
set_shm_dbz_offset(int connectorId)
{
	char *offset;

	if (!sdb_state)
		return;

	offset = dbz_engine_get_offset(connectorId);
	if (!offset)
		return;

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	strlcpy(sdb_state->connectors[connectorId].dbzoffset, offset, SYNCHDB_ERRMSG_SIZE);
	LWLockRelease(&sdb_state->lock);

	pfree(offset);
}

const char *
get_shm_dbz_offset(int connectorId)
{
	if (!sdb_state)
		return "n/a";

	return (sdb_state->connectors[connectorId].dbzoffset[0] != '\0') ?
			sdb_state->connectors[connectorId].dbzoffset : "no offset";
}

/*
 * _PG_init - Initialize the SynchDB extension
 */
void
_PG_init(void)
{
	DefineCustomIntVariable("synchdb.naptime",
							"Duration between each data polling (in seconds).",
							NULL,
							&synchdb_worker_naptime,
							5,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("synchdb.dml_use_spi",
							 "switch to use SPI to handle DML operations. Default false",
							 NULL,
							 &synchdb_dml_use_spi,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/* create a pg_synchdb directory under $PGDATA to store connector meta data */
	if (MakePGDirectory(SYNCHDB_METADATA_DIR) < 0)
	{
		if (errno != EEXIST)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create directory \"%s\": %m",
							SYNCHDB_METADATA_DIR)));
		}
	}
	else
	{
		fsync_fname(SYNCHDB_METADATA_DIR, true);
	}
}

/*
 * Finalization function
 */
void
_PG_fini(void)
{
	elog(WARNING," shutdown synchdb");
}

/*
 * synchdb_engine_main - Main entry point for the SynchDB background worker
 */
void
synchdb_engine_main(Datum main_arg)
{
	ConnectorType connectorType;
	ConnectionInfo connInfo = {0};

	/* Parse arguments and initialize connection info */
	parse_arguments(main_arg, &connectorType, &connInfo);

	/* Set up signal handlers and initialize shared memory */
	setup_environment(connectorType, connInfo.dst_db);

	/* Initialize the connector state */
	set_shm_connector_state(myConnectorId, STATE_INITIALIZING);
	set_shm_connector_errmsg(myConnectorId, NULL);
	set_shm_connector_dbs(myConnectorId, connInfo.src_db, connInfo.dst_db);
	memset(sdb_state->connectors[myConnectorId].name, 0, SYNCHDB_MAX_DB_NAME_SIZE);
	strlcpy(sdb_state->connectors[myConnectorId].name, connInfo.name, SYNCHDB_MAX_DB_NAME_SIZE);

	/* initialize format converter */
	fc_initFormatConverter(connectorType);

	/* Initialize JVM */
	initialize_jvm();

	/* read current offset and update shm */
	memset(sdb_state->connectors[myConnectorId].dbzoffset, 0, SYNCHDB_ERRMSG_SIZE);
	set_shm_dbz_offset(myConnectorId);

	/* start Debezium engine */
	start_debezium_engine(connectorType, &connInfo);

	elog(LOG, "Going to main loop .... ");
	/* Main processing loop */
	main_loop(connectorType, &connInfo);

	/* Cleanup */
	cleanup(connectorType);

	/* Free allocated memory */
	if (connInfo.hostname)
		pfree(connInfo.hostname);
	if (connInfo.user)
		pfree(connInfo.user);
	if (connInfo.pwd)
		pfree(connInfo.pwd);
	if (connInfo.src_db)
		pfree(connInfo.src_db);
	if (connInfo.dst_db)
		pfree(connInfo.dst_db);
	if (connInfo.table)
		pfree(connInfo.table);

	proc_exit(0);
}

Datum
synchdb_start_engine_bgw(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t pid;
	ConnectionInfo connInfo;
	char *connector = NULL;
	int ret = -1, connectorid = -1;

	/* Parse input arguments */
	text *name_text = PG_GETARG_TEXT_PP(0);

	/* Sanity check on input arguments */
	if (VARSIZE(name_text) - VARHDRSZ == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connection name cannot be empty")));
	}

	ret = ra_getConninfoByName(text_to_cstring(name_text), &connInfo, &connector);
	if (ret)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connection name does not exist"),
				 errhint("use synchdb_add_conninfo to add one first")));

	/*
	 * attach or initialize synchdb shared memory area so we can assign
	 * a connector ID for this worker
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	connectorid = assign_connector_id(connInfo.name);
	if (connectorid == -1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max number of connectors reached"),
				 errhint("use synchdb_stop_engine_bgw to stop some active connectors")));

	/* prepare background worker */
	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_notify_pid = MyProcPid;

	strcpy(worker.bgw_library_name, "synchdb");
	strcpy(worker.bgw_function_name, "synchdb_engine_main");

	prepare_bgw(&worker, &connInfo, connector, connectorid);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));

	PG_RETURN_INT32(0);
}

Datum
synchdb_stop_engine_bgw(PG_FUNCTION_ARGS)
{
	int connectorId = PG_GETARG_INT32(0);
	pid_t pid;

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	pid = get_shm_connector_pid(connectorId);
	if (pid != InvalidPid)
	{
		elog(WARNING, "terminating dbz connector id (%d) with pid %d", connectorId, (int)pid);
		DirectFunctionCall2(pg_terminate_backend, UInt32GetDatum(pid), Int64GetDatum(5000));
		set_shm_connector_pid(connectorId, InvalidPid);
	}
	else
	{
		ereport(ERROR,
				(errmsg("dbz connector id (%d) is not running", connectorId),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));
	}
	PG_RETURN_INT32(0);
}

Datum
synchdb_get_state(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int *idx = NULL;

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		funcctx->tuple_desc = synchdb_state_tupdesc();
		funcctx->user_fctx = palloc0(sizeof(int));
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	idx = (int *)funcctx->user_fctx;

	/*
	 * todo: put 3 to a dynamic MACRO as the number of connector synchdb currently
	 * can support
	 */
	if (*idx < SYNCHDB_MAX_ACTIVE_CONNECTORS)
	{
		Datum values[7];
		bool nulls[7] = {0};
		HeapTuple tuple;

		LWLockAcquire(&sdb_state->lock, LW_SHARED);
		values[0] = Int32GetDatum(*idx);
		values[1] = CStringGetTextDatum(get_shm_connector_name(sdb_state->connectors[*idx].type));
		values[2] = CStringGetTextDatum(sdb_state->connectors[*idx].name);
		values[3] = Int32GetDatum((int)get_shm_connector_pid(*idx));
		values[4] = CStringGetTextDatum(get_shm_connector_state(*idx));
		values[5] = CStringGetTextDatum(get_shm_connector_errmsg(*idx));
		values[6] = CStringGetTextDatum(get_shm_dbz_offset(*idx));
		LWLockRelease(&sdb_state->lock);

		*idx += 1;

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	SRF_RETURN_DONE(funcctx);
}

Datum
synchdb_pause_engine(PG_FUNCTION_ARGS)
{
	pid_t pid;
	int connectorId = PG_GETARG_INT32(0);
	SynchdbRequest *req;

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector id (%d) is not running", connectorId),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector id %d", connectorId),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_PAUSED;
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent pause request interrupt to dbz connector (%d)", connectorId);
	PG_RETURN_INT32(0);
}

Datum
synchdb_resume_engine(PG_FUNCTION_ARGS)
{
	pid_t pid;
	SynchdbRequest *req;
	int connectorId = PG_GETARG_INT32(0);

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector id (%d) is not running", connectorId),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector id %d", connectorId),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_SYNCING;
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent resume request interrupt to dbz connector id (%d)", connectorId);
	PG_RETURN_INT32(0);
}

Datum
synchdb_set_offset(PG_FUNCTION_ARGS)
{
	pid_t pid;
	int connectorId;
	char *offsetstr;
	SynchdbRequest *req;
	ConnectorState currstate;
	connectorId = PG_GETARG_INT32(0);
	offsetstr = text_to_cstring(PG_GETARG_TEXT_P(1));

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to init or attach to synchdb shared memory")));

	pid = get_shm_connector_pid(connectorId);
	if (pid == InvalidPid)
		ereport(ERROR,
				(errmsg("dbz connector id (%d) is not running", connectorId),
				 errhint("use synchdb_start_engine_bgw() to start a worker first")));

	currstate = get_shm_connector_state_enum(connectorId);
	if (currstate != STATE_PAUSED)
		ereport(ERROR,
				(errmsg("dbz connector id (%d) is not in paused state.", connectorId),
				 errhint("use synchdb_pause_engine() to pause the worker first")));

	/* point to the right construct based on type */
	req = &(sdb_state->connectors[connectorId].req);

	/* an active state change request is currently in progress */
	if (req->reqstate != STATE_UNDEF)
		ereport(ERROR,
				(errmsg("an active request is currently active for connector id %d", connectorId),
				 errhint("wait for it to finish and try again later")));

	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	req->reqstate = STATE_OFFSET_UPDATE;
	strncpy(req->reqdata, offsetstr, SYNCHDB_ERRMSG_SIZE);
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "sent update offset request interrupt to dbz connector id (%d)", connectorId);
	PG_RETURN_INT32(0);
}

Datum
synchdb_add_conninfo(PG_FUNCTION_ARGS)
{
	text *name_text = PG_GETARG_TEXT_PP(0);
	text *hostname_text = PG_GETARG_TEXT_PP(1);
	unsigned int port = PG_GETARG_UINT32(2);
	text *user_text = PG_GETARG_TEXT_PP(3);
	text *pwd_text = PG_GETARG_TEXT_PP(4);
	text *src_db_text = PG_GETARG_TEXT_PP(5);
	text *dst_db_text = PG_GETARG_TEXT_PP(6);
	text *table_text = PG_GETARG_TEXT_PP(7);
	text *connector_text = PG_GETARG_TEXT_PP(8);
	char *connector;

	ConnectionInfo connInfo;
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	/* Sanity check on input arguments */
	if (VARSIZE(name_text) - VARHDRSZ == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("name cannot be empty")));
	}
	connInfo.name = text_to_cstring(name_text);

	/* Sanity check on input arguments */
	if (VARSIZE(hostname_text) - VARHDRSZ == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("hostname cannot be empty")));
	}
	connInfo.hostname = text_to_cstring(hostname_text);

	connInfo.port = port;
	if (connInfo.port == 0 || connInfo.port > 65535)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid port number")));
	}

	if (VARSIZE(user_text) - VARHDRSZ == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("username cannot be empty")));
	}
	connInfo.user = text_to_cstring(user_text);

	if (VARSIZE(pwd_text) - VARHDRSZ == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("password cannot be empty")));
	}
	connInfo.pwd = text_to_cstring(pwd_text);

	/* source database can be empty or NULL */
	if (VARSIZE(src_db_text) - VARHDRSZ == 0)
		connInfo.src_db = "null";
	else
		connInfo.src_db = text_to_cstring(src_db_text);

	if (VARSIZE(dst_db_text) - VARHDRSZ == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("destination database cannot be empty")));
	}
	connInfo.dst_db = text_to_cstring(dst_db_text);

	/* table can be empty or NULL */
	if (VARSIZE(table_text) - VARHDRSZ == 0)
		connInfo.table = "null";
	else
		connInfo.table = text_to_cstring(table_text);

	if (VARSIZE(connector_text) - VARHDRSZ == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("connector type cannot be empty")));
	}
	connector = text_to_cstring(connector_text);

	appendStringInfo(&strinfo, "INSERT INTO %s (name, data)"
			" VALUES ('%s', jsonb_build_object('hostname', '%s', "
			"'port', %d, 'user', '%s', 'pwd', pgp_sym_encrypt('%s', '%s'), "
			"'srcdb', '%s', 'dstdb', '%s', 'table', '%s', 'connector', '%s') );",
			SYNCHDB_CONNINFO_TABLE,
			connInfo.name,
			connInfo.hostname,
			connInfo.port,
			connInfo.user,
			connInfo.pwd,
			SYNCHDB_SECRET,
			connInfo.src_db,
			connInfo.dst_db,
			connInfo.table,
			connector);

	PG_RETURN_INT32(ra_executeCommand(strinfo.data));
}
