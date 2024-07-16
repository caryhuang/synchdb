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
#include "format_converter.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(synchdb_start_engine);
PG_FUNCTION_INFO_V1(synchdb_get_changes);
PG_FUNCTION_INFO_V1(synchdb_stop_engine);

static JavaVM *jvm = NULL;
static JNIEnv *env = NULL;
static jclass cls; /* represents debezium runner java class */
static jobject obj;	/* represents debezium runner java class object */

void _PG_init(void)
{
	JavaVMInitArgs vm_args; // JVM initialization arguments
    JavaVMOption options[1];
	int ret;
	const char * dbzpath = getenv("DBZ_ENGINE_DIR");	
	char javaopt[512] = {0};

	if (!dbzpath)
	{
		elog(ERROR, "DBZ_ENGINE_DIR not set. Please set it to the path to Debezium engine jar file");
	}

	snprintf(javaopt, sizeof(javaopt), "-Djava.class.path=%s", dbzpath);
	
	if (!jvm)
	{
		// Path to the Java class
		//options[0].optionString = "-Djava.class.path=/home/ubuntu/synchdb/postgres/contrib/synchdbjni/dbz-engine/target/dbz-engine-1.0.0.jar";
		options[0].optionString = javaopt;
		vm_args.version = JNI_VERSION_21; // JNI version
		vm_args.nOptions = 1;
		vm_args.options = options;
		vm_args.ignoreUnrecognized = JNI_FALSE;
		
		// Load and initialize a Java VM, return a JNI interface pointer in env
		ret = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
		if (ret < 0 || !env)
		{
			elog(ERROR, "Unable to Launch JVM");
		}
	}
	else
	{
		elog(WARNING, "JVM already initialized");
		return;
	}
}

void _PG_fini(void)
{
	if (jvm != NULL)
	{
		// Destroy the JVM
		(*jvm)->DestroyJavaVM(jvm);
		jvm = NULL;
		env = NULL;
	}
}

Datum
synchdb_stop_engine(PG_FUNCTION_ARGS)
{
	jmethodID stopEngine;
	jobject stopEngineObj;

	if (!jvm)
    {
        elog(WARNING, "jvm not initialized");
        PG_RETURN_INT32(1);
    }
	if (!obj)
    {
        elog(WARNING, "debezium runner object not initialized");
        PG_RETURN_INT32(1);
    }

    if (!cls)
    {
        elog(WARNING, "debezium runner class not initialized");
        PG_RETURN_INT32(1);
    }

	// Get the stopEngine method ID
    stopEngine = (*env)->GetMethodID(env, cls, "stopEngine", "()V");
    if (stopEngine == NULL)
    {
        elog(WARNING, "Failed to find stopEngine method");
        PG_RETURN_INT32(1);
    }

    // Call the getChangeEvents method
    stopEngineObj = (*env)->CallObjectMethod(env, obj, stopEngine);
    if (stopEngineObj == NULL)
    {
        elog(WARNING, "Failed to call stop engine");
        PG_RETURN_INT32(1);
    }

	PG_RETURN_INT32(0);
}

Datum
synchdb_get_changes(PG_FUNCTION_ARGS)
{
	jclass listClass;
	jmethodID getChangeEvents, sizeMethod, getMethod;
	jobject changeEventsList;
    jint size;

    if (!jvm)
    {
        elog(WARNING, "jvm not initialized");
		PG_RETURN_INT32(1);
    }

	if (!obj)
	{
        elog(WARNING, "debezium runner object not initialized");
		PG_RETURN_INT32(1);
	}

	if (!cls)
	{
        elog(WARNING, "debezium runner class not initialized");
		PG_RETURN_INT32(1);
	}

    // Get the getChangeEvents method ID
    getChangeEvents = (*env)->GetMethodID(env, cls, "getChangeEvents", "()Ljava/util/List;");
    if (getChangeEvents == NULL)
    {
        elog(WARNING, "Failed to find getChangeEvents method");
        PG_RETURN_INT32(1);
    }

    // Call the getChangeEvents method
    changeEventsList = (*env)->CallObjectMethod(env, obj, getChangeEvents);
    if (changeEventsList == NULL)
    {
        elog(WARNING, "Failed to get change events list");
        PG_RETURN_INT32(1);
    }

    // Get the List class and its size method
    listClass = (*env)->FindClass(env, "java/util/List");
    if (listClass == NULL)
    {
        elog(WARNING, "Failed to find java list class");
        PG_RETURN_INT32(1);
    }

    sizeMethod = (*env)->GetMethodID(env, listClass, "size", "()I");
    if (sizeMethod == NULL)
    {
        elog(WARNING, "Failed to find java list.size method");
        PG_RETURN_INT32(1);
    }

    getMethod = (*env)->GetMethodID(env, listClass, "get", "(I)Ljava/lang/Object;");
    if (getMethod == NULL)
    {
        elog(WARNING, "Failed to find java list.get method");
        PG_RETURN_INT32(1);
    }

    size = (*env)->CallIntMethod(env, changeEventsList, sizeMethod);
	elog(WARNING, "there are %d dbz events", size);
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

	PG_RETURN_INT32(0);
}

Datum
synchdb_start_engine(PG_FUNCTION_ARGS)
{
	char * hostname = text_to_cstring(PG_GETARG_TEXT_P(0));
	unsigned int port = PG_GETARG_UINT32(1);
	char * user = text_to_cstring(PG_GETARG_TEXT_P(2));
	char * pwd = text_to_cstring(PG_GETARG_TEXT_P(3));
	char * db = text_to_cstring(PG_GETARG_TEXT_P(4));

	jmethodID mid;
	jstring jHostname, jUser, jPassword, jDatabase;

	if (!jvm)
	{
		elog(WARNING, "jvm not initialized");
    	PG_RETURN_INT32(1);
	}
	// Find the Java class
	cls = (*env)->FindClass(env, "com/example/DebeziumRunner");
	if (cls == NULL)
	{
		elog(WARNING, "Failed to find class");
    	PG_RETURN_INT32(1);
	}

	// Get the method ID of the Java method
	mid = (*env)->GetMethodID(env, cls, "startEngine",
			"(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
	if (mid == NULL)
	{
		elog(WARNING, "Failed to find method");
		PG_RETURN_INT32(1);
	}

	// Create a new instance of the Java class
	obj = (*env)->AllocObject(env, cls);
	if (obj == NULL)
	{
		elog(WARNING, "Failed to allocate object");
    	PG_RETURN_INT32(1);
	}

	// Call the Java method
	jHostname = (*env)->NewStringUTF(env, hostname);
	jUser = (*env)->NewStringUTF(env, user);
	jPassword = (*env)->NewStringUTF(env, pwd);
	jDatabase = (*env)->NewStringUTF(env, db);

	(*env)->CallVoidMethod(env, obj, mid, jHostname, port, jUser, jPassword, jDatabase);

    PG_RETURN_INT32(0);
}
