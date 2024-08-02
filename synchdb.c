/*
 * synchdb.c
 *
 *  Created on: Jun. 24, 2024
 *      Author: caryh
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

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(synchdb_stop_engine_bgw);
PG_FUNCTION_INFO_V1(synchdb_start_engine_bgw);
PG_FUNCTION_INFO_V1(synchdb_get_state);

#define SYNCHDB_METADATA_DIR "pg_synchdb"
#define DBZ_ENGINE_JAR_FILE "dbz-engine-1.0.0.jar"

/* Pointer to shared-memory state. */
SynchdbSharedState *sdb_state = NULL;

/* GUC variables */
int	synchdb_worker_naptime = 5;
bool synchdb_dml_use_spi = false;

PGDLLEXPORT void synchdb_engine_main(Datum main_arg);

static int
dbz_engine_stop(JavaVM *jvm, JNIEnv *env, jclass *cls, jobject *obj)
{
	jmethodID stopEngine;
	jobject stopEngineObj;

	if (!jvm)
    {
        elog(WARNING, "jvm not initialized");
        return -1;
    }
	if (!obj)
    {
        elog(WARNING, "debezium runner object not initialized");
        return -1;
    }

    if (!cls)
    {
        elog(WARNING, "debezium runner class not initialized");
        return -1;
    }

    stopEngine = (*env)->GetMethodID(env, *cls, "stopEngine", "()V");
    if (stopEngine == NULL)
    {
        elog(WARNING, "Failed to find stopEngine method");
        return -1;
    }

    stopEngineObj = (*env)->CallObjectMethod(env, *obj, stopEngine);
    if (stopEngineObj == NULL)
    {
        elog(WARNING, "Failed to call stop engine");
        return -1;
    }
    return 0;
}

static int
dbz_engine_get_change(JavaVM *jvm, JNIEnv *env, jclass *cls, jobject *obj)
{
	jmethodID getChangeEvents, sizeMethod, getMethod;
	jobject changeEventsList;
    jint size;
    jclass listClass;

    if (!jvm)
    {
        elog(WARNING, "jvm not initialized");
		return -1;
    }

	if (!obj)
	{
        elog(WARNING, "debezium runner object not initialized");
        return -1;
	}

	if (!cls)
	{
        elog(WARNING, "debezium runner class not initialized");
        return -1;
	}

    getChangeEvents = (*env)->GetMethodID(env, *cls, "getChangeEvents", "()Ljava/util/List;");
    if (getChangeEvents == NULL)
    {
        elog(WARNING, "Failed to find getChangeEvents method");
        return -1;
    }

    changeEventsList = (*env)->CallObjectMethod(env, *obj, getChangeEvents);
    if (changeEventsList == NULL)
    {
        return -1;
    }

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

    size = (*env)->CallIntMethod(env, changeEventsList, sizeMethod);
    for (jint i = 0; i < size; i++)
    {
        jobject event = (*env)->CallObjectMethod(env, changeEventsList, getMethod, i);
		if (event == NULL)
		{
			elog(WARNING, "got a NULL DBZ Event at index %d\n", i);
			continue;
		}
		else
		{
			const char *eventStr = (*env)->GetStringUTFChars(env, (jstring)event, 0);
        	elog(WARNING, "DBZ Event: %s\n", eventStr);

        	fc_processDBZChangeEvent(eventStr);

        	(*env)->ReleaseStringUTFChars(env, (jstring)event, eventStr);
		}
    }
    return 0;
}

static int
dbz_engine_start(char * hostname, unsigned int port, char * user,
				 char * pwd, char * db, char * table, ConnectorType connectorType,
				 JavaVM *jvm, JNIEnv *env, jclass *cls, jobject *obj)
{
	jmethodID mid;
	jstring jHostname, jUser, jPassword, jDatabase, jTable;

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
    	return -1;
	}

	*cls = (*env)->FindClass(env, "com/example/DebeziumRunner");
	if (cls == NULL)
	{
		elog(WARNING, "Failed to find class");
		return -1;
	}

	mid = (*env)->GetMethodID(env, *cls, "startEngine",
			"(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;I)V");
	if (mid == NULL)
	{
		elog(WARNING, "Failed to find method");
		return -1;
	}

	*obj = (*env)->AllocObject(env, *cls);
	if (obj == NULL)
	{
		elog(WARNING, "Failed to allocate object");
		return -1;
	}

	jHostname = (*env)->NewStringUTF(env, hostname);
	jUser = (*env)->NewStringUTF(env, user);
	jPassword = (*env)->NewStringUTF(env, pwd);
	jDatabase = (*env)->NewStringUTF(env, db);
	jTable = (*env)->NewStringUTF(env, table);

	(*env)->CallVoidMethod(env, *obj, mid, jHostname, port, jUser, jPassword, jDatabase, jTable, connectorType);
	return 0;
}

/*
 * Helper function to construct whichever TupleDesc we need for a particular
 * call.
 */
static TupleDesc
synchdb_state_tupdesc(void)
{
	TupleDesc	tupdesc;
	AttrNumber	attrnum = 4;
	AttrNumber	a = 0;

	tupdesc = CreateTemplateTupleDesc(attrnum);

	/* todo: add more columns here per connector if needed */
	TupleDescInitEntry(tupdesc, ++a, "connector", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "pid", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "state", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, ++a, "err", TEXTOID, -1, 0);

	Assert(a == maxattr);
	return BlessTupleDesc(tupdesc);
}

/*
 * Allocate and initialize synchdb related shared memory, if not already
 * done, and set up backend-local pointer to that state.  Returns true if an
 * existing shared memory segment was found.
 */
static void
synchdb_init_shmem(void)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	sdb_state = ShmemInitStruct("synchdb",
								sizeof(SynchdbSharedState),
								&found);
	if (!found)
	{
		/* First time through ... */
		LWLockInitialize(&sdb_state->lock, LWLockNewTrancheId());
		sdb_state->mysqlinfo.pid = InvalidPid;
		sdb_state->oracleinfo.pid = InvalidPid;
		sdb_state->sqlserverinfo.pid = InvalidPid;
	}
	LWLockRelease(AddinShmemInitLock);

	LWLockRegisterTranche(sdb_state->lock.tranche, "synchdb");
}

static void
synchdb_detach_shmem(int code, Datum arg)
{
	pid_t enginepid;

	elog(WARNING, "synchdb detach shm ... connector type %d, code %d",
			DatumGetUInt32(arg), code);

	set_shm_connector_state(DatumGetUInt32(arg), STATE_STOPPED);

	enginepid = get_shm_connector_pid(DatumGetUInt32(arg));
	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	if (enginepid == MyProcPid)
		set_shm_connector_pid(DatumGetUInt32(arg), InvalidPid);
	LWLockRelease(&sdb_state->lock);
}

static void
prepare_bgw(BackgroundWorker * worker, char * hostname,
			unsigned int port, char * user, char * pwd, char * src_db,
			char * dst_db, char * table, char * connector)
{
	ConnectorType type = fc_get_connector_type(connector);
	switch(type)
	{
		case TYPE_MYSQL:
		{
			worker->bgw_main_arg = UInt32GetDatum(TYPE_MYSQL);
			snprintf(worker->bgw_name, BGW_MAXLEN, "synchdb engine: mysql@%s:%u", hostname, port);
			strcpy(worker->bgw_type, "synchdb engine: mysql");
			break;
		}
		case TYPE_ORACLE:
		{
			worker->bgw_main_arg = UInt32GetDatum(TYPE_ORACLE);
			snprintf(worker->bgw_name, BGW_MAXLEN, "synchdb engine: oracle@%s:%u", hostname, port);
			strcpy(worker->bgw_type, "synchdb engine: oracle");
			break;
		}
		case TYPE_SQLSERVER:
		{
			worker->bgw_main_arg = UInt32GetDatum(TYPE_SQLSERVER);
			snprintf(worker->bgw_name, BGW_MAXLEN, "synchdb engine: sqlserver@%s:%u", hostname, port);
			strcpy(worker->bgw_type, "synchdb engine: sqlserver");
			break;
		}
		/* todo: support more dbz connector types here */
		default:
		{
			elog(ERROR, "unsupported connector type");
			break;
		}
	}
	/* append destination database to worker->bgw_name for clarity */
	strcat(worker->bgw_name, " -> ");
	strcat(worker->bgw_name, dst_db);

	/*
	 * Format the extra args into the bgw_extra field
	 * todo: BGW_EXTRALEN is only 128 bytes, so the formatted string will be
	 * cut off here if exceeding this length. Maybe there is a better way to
	 * pass these args to background worker?
	 */
	snprintf(worker->bgw_extra, BGW_EXTRALEN, "%s:%u:%s:%s:%s:%s:%s",
			hostname, port, user, pwd, src_db, dst_db, table);
}
/*
 * SynchdbWorkerStateAsString
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
	}
	return "UNKNOWN";
}

/* public functions to access / change synchdb parameters in shared memory */
const char *
get_shm_connector_name(ConnectorType type)
{
	switch(type)
	{
		case TYPE_MYSQL:
			return "mysql";
		case TYPE_ORACLE:
			return "oracle";
		case TYPE_SQLSERVER:
			return "sqlserver";
		/* todo: support more dbz connector types here */
		default:
		{
			break;
		}
	}
	return "null";
}

pid_t
get_shm_connector_pid(ConnectorType type)
{
	switch(type)
	{
		case TYPE_MYSQL:
		{
			return sdb_state->mysqlinfo.pid;
		}
		case TYPE_ORACLE:
		{
			return sdb_state->oracleinfo.pid;
		}
		case TYPE_SQLSERVER:
		{
			return sdb_state->sqlserverinfo.pid;
		}
		/* todo: support more dbz connector types here */
		default:
		{
			return InvalidPid;
		}
	}
}

void
set_shm_connector_pid(ConnectorType type, pid_t pid)
{
	switch(type)
	{
		case TYPE_MYSQL:
		{
			sdb_state->mysqlinfo.pid = pid;
			break;
		}
		case TYPE_ORACLE:
		{
			sdb_state->oracleinfo.pid = pid;
			break;
		}
		case TYPE_SQLSERVER:
		{
			sdb_state->sqlserverinfo.pid = pid;
			break;
		}
		/* todo: support more dbz connector types here */
		default:
		{
			break;
		}
	}
}

const char *
get_shm_connector_errmsg(ConnectorType type)
{
	switch(type)
	{
		case TYPE_MYSQL:
			return (strlen(sdb_state->mysqlinfo.errmsg) > 0 ?
					sdb_state->mysqlinfo.errmsg : "no error");
		case TYPE_ORACLE:
			return (strlen(sdb_state->oracleinfo.errmsg) > 0 ?
					sdb_state->oracleinfo.errmsg : "no error");
		case TYPE_SQLSERVER:
			return (strlen(sdb_state->sqlserverinfo.errmsg) > 0 ?
					sdb_state->sqlserverinfo.errmsg : "no error");
		/* todo: support more dbz connector types here */
		default:
		{
			break;
		}
	}
	return "invalid connector type";
}

void
set_shm_connector_errmsg(ConnectorType type, char * err)
{
	if (!sdb_state)
		return;

	switch(type)
	{
		case TYPE_MYSQL:
		{
			if (!err)
			{
				memset(sdb_state->mysqlinfo.errmsg, 0, sizeof(sdb_state->mysqlinfo.errmsg));
				break;
			}
			memset(sdb_state->mysqlinfo.errmsg, 0, sizeof(sdb_state->mysqlinfo.errmsg));
			snprintf(sdb_state->mysqlinfo.errmsg,
					sizeof(sdb_state->mysqlinfo.errmsg),
					"%s", err);
			break;
		}
		case TYPE_ORACLE:
		{
			if (!err)
			{
				memset(sdb_state->oracleinfo.errmsg, 0, sizeof(sdb_state->oracleinfo.errmsg));
				break;
			}
			memset(sdb_state->oracleinfo.errmsg, 0, sizeof(sdb_state->oracleinfo.errmsg));
			snprintf(sdb_state->oracleinfo.errmsg,
					sizeof(sdb_state->oracleinfo.errmsg),
					"%s", err);
			break;
		}
		case TYPE_SQLSERVER:
		{
			if (!err)
			{
				memset(sdb_state->sqlserverinfo.errmsg, 0, sizeof(sdb_state->sqlserverinfo.errmsg));
				break;
			}
			memset(sdb_state->sqlserverinfo.errmsg, 0, sizeof(sdb_state->sqlserverinfo.errmsg));
			snprintf(sdb_state->sqlserverinfo.errmsg,
					sizeof(sdb_state->sqlserverinfo.errmsg),
					"%s", err);
			break;
		}
		/* todo: support more dbz connector types here */
		default:
		{
			break;
		}
	}
}

const char *
get_shm_connector_state(ConnectorType type)
{
	switch(type)
	{
		case TYPE_MYSQL:
			return (connectorStateAsString(sdb_state->mysqlinfo.state));
		case TYPE_ORACLE:
			return (connectorStateAsString(sdb_state->oracleinfo.state));
		case TYPE_SQLSERVER:
			return (connectorStateAsString(sdb_state->sqlserverinfo.state));
		/* todo: support more dbz connector types here */
		default:
			break;
	}
	return connectorStateAsString(STATE_UNDEF);
}

void
set_shm_connector_state(ConnectorType type, ConnectorState state)
{
	switch(type)
	{
		case TYPE_MYSQL:
		{
			sdb_state->mysqlinfo.state = state;
			break;
		}
		case TYPE_ORACLE:
		{
			sdb_state->sqlserverinfo.state = state;
			break;
		}
		case TYPE_SQLSERVER:
		{
			sdb_state->sqlserverinfo.state = state;
			break;
		}
		/* todo: support more dbz connector types here */
		default:
		{
			break;
		}
	}
}

void _PG_init(void)
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

void _PG_fini(void)
{

}

/*
 * Main entry point for the leader autoprewarm process.  Per-database workers
 * have a separate entry point.
 */
void
synchdb_engine_main(Datum main_arg)
{
	char * hostname = NULL, * user = NULL, * pwd = NULL, * src_db = NULL, * dst_db = NULL, * table = NULL;
	char * args = NULL, * tmp = NULL;
	unsigned int port, connectorType;
	pid_t enginepid;

	/* jvm */
	JavaVMInitArgs vm_args;
    JavaVMOption options[2];
	int ret;
	const char * dbzpath = getenv("DBZ_ENGINE_DIR");
	char javaopt[512] = {0}, defaultdbzpath[512] = {0};
	JavaVM *jvm = NULL;		/* represents java vm instance */
	JNIEnv *env = NULL;		/* represents JNI run-time environment */
	jclass cls; 	/* represents debezium runner java class */
	jobject obj;	/* represents debezium runner java class object */

	/* Parse the arguments from bgw_extra and main_arg */
	connectorType = DatumGetUInt32(main_arg);
	args = pstrdup(MyBgworkerEntry->bgw_extra);

	/* hostname */
	tmp = strtok(args, ":");
	if (tmp)
		hostname = pstrdup(tmp);
	/* port */
	tmp = strtok(NULL, ":");
	if (tmp)
		port = atoi(tmp);
	/* user */
	tmp = strtok(NULL, ":");
	if (tmp)
		user = pstrdup(tmp);
	/* password */
	tmp = strtok(NULL, ":");
	if (tmp)
		pwd = pstrdup(tmp);
	/* source database */
	tmp = strtok(NULL, ":");
	if (tmp)
		src_db = pstrdup(tmp);
	/* destination database */
	tmp = strtok(NULL, ":");
	if (tmp)
		dst_db = pstrdup(tmp);
	/* table - in the form of database.table */
	tmp = strtok(NULL, ":");
	if (tmp)
		table = pstrdup(tmp);

	pfree(args);

	elog(WARNING, "host %s, port %u user %s, pwd %s, src_db %s, dst_db %s, table %s, connectorType %u",
			hostname, port, user, pwd, src_db, dst_db, table, connectorType);

	/* Establish signal handlers; once that's done, unblock signals. */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to current database: NULL user - bootstrap superuser is used */
	BackgroundWorkerInitializeConnection(dst_db, NULL, 0);

	/* Create or attach to our synchdb shared memory */
	synchdb_init_shmem();

	/* Set on-detach hook */
	on_shmem_exit(synchdb_detach_shmem, main_arg);

	/*
	 * Store our PID in the shared memory area to prevent starting multiple
	 * synchdb workers
	 */
	enginepid = get_shm_connector_pid(connectorType);
	LWLockAcquire(&sdb_state->lock, LW_EXCLUSIVE);
	if (enginepid != InvalidPid)
	{
		LWLockRelease(&sdb_state->lock);
		ereport(LOG,
				(errmsg("synchdb mysql worker (%u) is already running under PID %d",
						connectorType,
						(int) enginepid)));
		return;
	}
	set_shm_connector_pid(connectorType, MyProcPid);
	LWLockRelease(&sdb_state->lock);

	elog(WARNING, "synchdb_engine_main starting ...");

	set_shm_connector_state(connectorType, STATE_INITIALIZING);
	set_shm_connector_errmsg(connectorType, NULL);

	if (!dbzpath)
	{
		elog(WARNING, "DBZ_ENGINE_DIR not set. Using default lib path %s/dbz_engine/%s",
				pkglib_path, DBZ_ENGINE_JAR_FILE);
		snprintf(defaultdbzpath, sizeof(defaultdbzpath), "%s/dbz_engine/%s",
				pkglib_path, DBZ_ENGINE_JAR_FILE);
		if (access(defaultdbzpath, F_OK) == -1)
		{
			elog(WARNING, "cannot find DBZ engine jar file from %s",
					defaultdbzpath);
			set_shm_connector_errmsg(connectorType, "cannot find DBZ engine jar file");
			return;
		}
		snprintf(javaopt, sizeof(javaopt), "-Djava.class.path=%s", defaultdbzpath);
	}
	else
	{
		elog(WARNING, "DBZ_ENGINE_DIR is set. DBZ engine jar location: %s", dbzpath);
		if (access(dbzpath, F_OK) == -1)
		{
			elog(WARNING, "cannot find DBZ engine jar file from %s",
					dbzpath);
			set_shm_connector_errmsg(connectorType, "cannot find DBZ engine jar file");
			return;
		}
		snprintf(javaopt, sizeof(javaopt), "-Djava.class.path=%s", dbzpath);
	}

	/*
	 * Path to the Java class:
	 * options[0].optionString = "-Djava.class.path=/home/ubuntu/synchdb/
	 * 		postgres/contrib/synchdbjni/dbz-engine/target/dbz-engine-1.0.0.jar";
	 */
	options[0].optionString = javaopt;
	options[1].optionString = "-Xrs";	/* this option disable JVM's signal handler */
	vm_args.version = JNI_VERSION_21;
	vm_args.nOptions = 2;
	vm_args.options = options;
	vm_args.ignoreUnrecognized = JNI_FALSE;

	/* Load and initialize a Java VM, return a JNI interface pointer in env */
	ret = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
	if (ret < 0 || !env)
	{
		elog(WARNING, "Unable to Launch JVM");
		set_shm_connector_errmsg(connectorType, "Unable to Launch JVM");
		return;
	}

	ret = dbz_engine_start(hostname, port, user, pwd, src_db, table, connectorType,
						   jvm, env, &cls, &obj);
	if (ret < 0)
	{
		elog(WARNING, "Failed to start dbz engine");
		set_shm_connector_errmsg(connectorType, "Failed to start dbz engine");
		return;
	}

	set_shm_connector_state(connectorType, STATE_SYNCING);
	while (!ShutdownRequestPending)
	{
		/* In case of a SIGHUP, just reload the configuration. */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 synchdb_worker_naptime * 1000,
						 PG_WAIT_EXTENSION);

		dbz_engine_get_change(jvm, env, &cls, &obj);

		/* Reset the latch, loop. */
		ResetLatch(MyLatch);
	}

	elog(WARNING, "synchdb_engine_main shutting down");
	ret = dbz_engine_stop(jvm, env, &cls, &obj);
	if (ret)
	{
		elog(WARNING, "failed to call dbz engine stop method");
	}

	if (jvm != NULL)
	{
		(*jvm)->DestroyJavaVM(jvm);
		jvm = NULL;
		env = NULL;
	}

	if(hostname)
		pfree(hostname);
	if(user)
		pfree(user);
	if(pwd)
		pfree(pwd);
	if(src_db)
		pfree(src_db);
	if(dst_db)
		pfree(dst_db);
	if(table)
		pfree(table);

	proc_exit(0);
}

Datum
synchdb_start_engine_bgw(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	/* input args */
	text * hostname_text;
	text * user_text;
	text * pwd_text;
	text * src_db_text;
	text * dst_db_text;
	text * table_text;
	text * connector_text;
	char * hostname;
	unsigned int port;
	char * user;
	char * pwd;
	char * src_db;
	char * dst_db;
	char * table;
	char * connector;

	/* sanity check on input arguments */
	hostname_text = PG_GETARG_TEXT_PP(0);
	if (VARSIZE(hostname_text) - VARHDRSZ == 0)
	{
		elog(ERROR, "hostname cannot be empty");
	}
	hostname = text_to_cstring(hostname_text);

	port = PG_GETARG_UINT32(1);
	if (port == 0 || port > 65535)
	{
		elog(ERROR, "invalid port number");
	}

	user_text = PG_GETARG_TEXT_PP(2);
	if (VARSIZE(user_text) - VARHDRSZ == 0)
	{
		elog(ERROR, "username cannot be empty");
	}
	user = text_to_cstring(user_text);

	pwd_text = PG_GETARG_TEXT_PP(3);
	if (VARSIZE(pwd_text) - VARHDRSZ == 0)
	{
		elog(ERROR, "password cannot be empty");
	}
	pwd = text_to_cstring(pwd_text);

	/* source database can be empty or NULL */
	src_db_text = PG_GETARG_TEXT_PP(4);
	if (VARSIZE(src_db_text) - VARHDRSZ == 0)
		src_db = "null";
	else
		src_db = text_to_cstring(src_db_text);

	dst_db_text = PG_GETARG_TEXT_PP(5);
	if (VARSIZE(dst_db_text) - VARHDRSZ == 0)
	{
		elog(ERROR, "destination database cannot be empty");
	}
	dst_db = text_to_cstring(dst_db_text);

	/* table can be empty or NULL */
	table_text = PG_GETARG_TEXT_PP(6);
	if (VARSIZE(table_text) - VARHDRSZ == 0)
		table = "null";
	else
		table = text_to_cstring(table_text);

	connector_text = PG_GETARG_TEXT_PP(7);
	if (VARSIZE(connector_text) - VARHDRSZ == 0)
	{
		elog(ERROR, "connector type cannot be empty");
	}
	connector = text_to_cstring(connector_text);

	/* prepare background worker */
	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_notify_pid = MyProcPid;

	strcpy(worker.bgw_library_name, "synchdb");
	strcpy(worker.bgw_function_name, "synchdb_engine_main");

	prepare_bgw(&worker, hostname, port, user, pwd, src_db, dst_db, table, connector);

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
	char * connector = text_to_cstring(PG_GETARG_TEXT_P(0));
	ConnectorType type = fc_get_connector_type(connector);
	pid_t pid;

	if (type == TYPE_UNDEF)
		elog(ERROR, "unsupported connector type");

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		elog(ERROR, "failed to init or attach to synchdb shared memory");

	pid = get_shm_connector_pid(type);
	if (pid != InvalidPid)
	{
		elog(WARNING, "terminating dbz connector (%d) with pid %d", type, (int) pid);
		DirectFunctionCall2(pg_terminate_backend, UInt32GetDatum(pid), Int64GetDatum(5000));
		set_shm_connector_pid(type, InvalidPid);
	}
	else
	{
		elog(WARNING, "dbz connector (%d) is not running", type);
		PG_RETURN_INT32(1);
	}

	PG_RETURN_INT32(0);
}

Datum
synchdb_get_state(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int * idx = NULL;

	/*
	 * attach or initialize synchdb shared memory area so we know what is
	 * going on
	 */
	synchdb_init_shmem();
	if (!sdb_state)
		elog(ERROR, "failed to init or attach to synchdb shared memory");

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
	idx = (int *) funcctx->user_fctx;

	/*
	 * todo: put 3 to a dynamic MACRO as the number of connector synchdb currently
	 * can support
	 */
	if (*idx < 3)
	{
		Datum		values[4];
		bool		nulls[4] = {0};
		HeapTuple	tuple;

		LWLockAcquire(&sdb_state->lock, LW_SHARED);
		values[0] = CStringGetTextDatum(get_shm_connector_name((*idx + 1)));
		values[1] = Int32GetDatum((int)get_shm_connector_pid((*idx + 1)));
		values[2] = CStringGetTextDatum(get_shm_connector_state((*idx + 1)));
		values[3] = CStringGetTextDatum(get_shm_connector_errmsg((*idx + 1)));
		LWLockRelease(&sdb_state->lock);

		*idx +=1;

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	SRF_RETURN_DONE(funcctx);
}
