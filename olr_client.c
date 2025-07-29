/*
 * olr_client.c
 *
 * Implementation of Openlog Replicator Client
 *
 * Copyright (c) Hornetlabs Technology, Inc.
 *
 */

#include "OraProtoBuf.pb-c.h"
#include "olr_client.h"
#include "format_converter.h"
#include "storage/fd.h"
#include "utils/timestamp.h"
#include "utils/datetime.h"
#include "access/xact.h"
#include "utils/snapmgr.h"

/* extern globals */
extern int myConnectorId;
extern int dbz_offset_flush_interval_ms;
extern bool synchdb_log_event_on_error;
extern char * g_eventStr;

/* static globals */
static NetioContext g_netioCtx = {0};
static orascn g_scn = 0;
static orascn g_c_scn = 0;

int
olr_client_init(const char * hostname, unsigned int port)
{
	if (netio_connect(&g_netioCtx, hostname, port))
	{
		/* todo: hook up error messages with synchdb state view and use ERROR shutdown */
		elog(WARNING, "failed to connect to OLR");
		return -1;
	}
	elog(DEBUG1, "OLR connected");
	return 0;
}

int
olr_client_start_or_cont_replication(char * source, bool which)
{
	StringInfoData strinfo;
	ssize_t nbytes = 0;
	size_t len;
	unsigned char *buf = NULL;
	int retcode = -1, i = 0;

	OpenLogReplicator__Pb__RedoRequest request =
			OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__INIT;

	if (!g_netioCtx.is_connected)
	{
		elog(WARNING, "no connection established to openlog replicator");
		return -1;
	}

	request.code = which ? OPEN_LOG_REPLICATOR__PB__REQUEST_CODE__START :
			OPEN_LOG_REPLICATOR__PB__REQUEST_CODE__CONTINUE;
	request.database_name = source;
	request.tm_val_case = OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__TM_VAL_SCN;
	request.scn = olr_client_get_scn() == 0 ? olr_client_get_scn() :
			olr_client_get_scn() + 1;
	request.c_scn = olr_client_get_c_scn() == 0 ? olr_client_get_c_scn() :
			olr_client_get_c_scn() + 1; /* resume beyond last know c_scn */

//	request.seq = 1111111;

	/* schema request */
	request.n_schema = 1;
	request.schema = palloc0(sizeof(OpenLogReplicator__Pb__SchemaRequest  *) * request.n_schema);
	for (i = 0; i < request.n_schema; i++)
	{
		request.schema[i] = palloc0(sizeof(OpenLogReplicator__Pb__SchemaRequest));
		open_log_replicator__pb__schema_request__init(request.schema[i]);

	    request.schema[i]->mask = "0";
	    request.schema[i]->filter = pstrdup("{\"table\":[{\"owner\":\"DBZUSER\",\"table\":\"TEST_TABLE\"}]");
	}

	elog(WARNING, "requested scn %lu c_scn %lu, nschema %ld",
			request.scn, request.c_scn, request.n_schema);

	len = open_log_replicator__pb__redo_request__get_packed_size(&request);
	buf = palloc0(len + 4);

	/* message length - 4 bytes */
	memcpy(buf, &len, 4);

	/* encode message with protobuf-c */
	open_log_replicator__pb__redo_request__pack(&request, buf + 4);

	/* send encoded message to olr */
	nbytes = netio_write(&g_netioCtx, buf, len + 4);
	pfree(buf);

	/* free the schema request after it has been sent */
	for (i = 0; i < request.n_schema; i++)
		pfree(request.schema[i]);
	pfree(request.schema);

	/* read 4 byte length header from olr first */
	initStringInfo(&strinfo);
	nbytes = netio_read(&g_netioCtx, &strinfo, 4);
	if (nbytes == 4)
	{
		/* then read the payload */
		int size = -1;
		memcpy(&size, strinfo.data, sizeof(int));
		nbytes += netio_read(&g_netioCtx, &strinfo, size);
	}

	/* decode received message with protobuf-c */
	if (nbytes > 4)
	{
		/* skips 4 byte message length field */
		OpenLogReplicator__Pb__RedoResponse *response =
			open_log_replicator__pb__redo_response__unpack(NULL, nbytes - 4 ,
					(unsigned char*)strinfo.data + 4);
		pfree(strinfo.data);

		if (response)
		{
			retcode = response->code;
			return retcode;
		}
		else
		{
			elog(WARNING,"malformed protobuf message - NULL response");
			return -1;
		}
	}
	return -1;
}

int
olr_client_get_change(int myConnectorId, bool * dbzExitSignal, SynchdbStatistics * myBatchStats,
		bool * sendconfirm)
{
	ssize_t nbytes = 0;
	StringInfoData strinfo;
	int ret = -1;

	if (!g_netioCtx.is_connected)
	{
		/* todo: maybe retry connection indefinitely? */
		elog(WARNING, "no connection established to openlog replicator");
		return -1;
	}

	/* read 4 byte length header from olr first */
	initStringInfo(&strinfo);
	nbytes = netio_read(&g_netioCtx, &strinfo, 4);
	if (nbytes == 4)
	{
		/* then read the payload */
		int size = -1;
		memcpy(&size, strinfo.data, sizeof(int));
		nbytes += netio_read(&g_netioCtx, &strinfo, size);
	}

	/* decode received message with protobuf-c */
	if (nbytes > 4)
	{
		/* expected JSON payload - skip 4 bytes size header */
		if (synchdb_log_event_on_error)
			g_eventStr = strinfo.data;

		elog(WARNING, "%s", strinfo.data + 4);

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		elog(WARNING, "B----->");

		/* todo process 1 change event */
		ret = fc_processOLRChangeEvent(strinfo.data + 4, myBatchStats,
				get_shm_connector_name_by_id(myConnectorId), sendconfirm);

		PopActiveSnapshot();
		CommitTransactionCommand();
		elog(WARNING, "<-----C");

		if (strinfo.data)
			pfree(strinfo.data);

		if (synchdb_log_event_on_error)
			g_eventStr = NULL;

		return ret;
	}
	return -1;
}

void
olr_client_shutdown(void)
{
	if (g_netioCtx.is_connected)
	{
		netio_disconnect(&g_netioCtx);
		elog(WARNING, "OLR disconnected");
		memset(&g_netioCtx, 0, sizeof(g_netioCtx));
	}
}

int
olr_client_confirm_scn(char * source)
{
	ssize_t nbytes = 0;
	size_t len;
	unsigned char *buf = NULL;
	OpenLogReplicator__Pb__RedoRequest request =
			OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__INIT;

	if (olr_client_get_c_scn() == 0)
	{
		elog(WARNING, "no scn to confirm");
		return -1;
	}

	if (!g_netioCtx.is_connected)
	{
		/* todo: hook up error messages with synchdb state view and use ERROR shutdown */
		elog(WARNING, "no connection established to openlog replicator");
		return -1;
	}

	request.code = OPEN_LOG_REPLICATOR__PB__REQUEST_CODE__CONFIRM;
	request.database_name = source;
	request.tm_val_case = OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__TM_VAL_SCN;
	request.scn = olr_client_get_c_scn();

	len = open_log_replicator__pb__redo_request__get_packed_size(&request);
	buf = palloc0(len + 4);

	/* message length - 4 bytes */
	memcpy(buf, &len, 4);

	/* encode message with protobuf-c */
	open_log_replicator__pb__redo_request__pack(&request, buf + 4);

	/* send encoded message to olr */
	nbytes = netio_write(&g_netioCtx, buf, len + 4);
	elog(DEBUG1, "olr client sent %ld bytes to olr", nbytes);
	pfree(buf);

	return 0;
}

void
olr_client_set_scns(orascn scn, orascn c_scn)
{
	g_scn = scn > 0? scn : g_scn;
	g_c_scn = c_scn > 0 ? c_scn : g_c_scn;
}

orascn
olr_client_get_c_scn(void)
{
	return g_c_scn;
}

orascn
olr_client_get_scn(void)
{
	return g_scn;
}

bool
olr_client_write_scn_state(ConnectorType type, const char * name, const char * dstdb, bool force)
{
	int fd;
	static TimestampTz last_flush_time = 0;
	orascn buf[2] = {g_scn, g_c_scn};
	char * filename = psprintf(SYNCHDB_OFFSET_FILE_PATTERN,
			get_shm_connector_name(type), name, dstdb);

	TimestampTz now = GetCurrentTimestamp();

    /* Only return early if !force and not enough time has passed */
    if (!force && last_flush_time != 0 &&
        !TimestampDifferenceExceeds(last_flush_time, now, dbz_offset_flush_interval_ms))
    {
        return false;
    }

    elog(DEBUG1, "flushing scn file %s...", filename);
	fd = OpenTransientFile(filename, O_WRONLY | O_CREAT | O_TRUNC);
	if (fd < 0)
	{
		set_shm_connector_errmsg(myConnectorId, "cannot open scn file to write!");
		elog(ERROR, "can not open file \"%s\" for writing: %m", filename);
	}

	elog(DEBUG1, "flushing... scn %llu, c_scu %llu", g_scn, g_c_scn);
	if (write(fd, buf, sizeof(buf)) != sizeof(buf))
	{
		int save_errno = errno;
		CloseTransientFile(fd);
		errno = save_errno;
		set_shm_connector_errmsg(myConnectorId, "cannot write to scn file");
		elog(ERROR, "cannot write to file \"%s\": %m", filename);
	}
	CloseTransientFile(fd);
	last_flush_time = now;
	return true;
}

bool
olr_client_init_scn_state(ConnectorType type, const char * name, const char * dstdb)
{
	int fd;
	orascn buf[2];
	char * filename = psprintf(SYNCHDB_OFFSET_FILE_PATTERN,
			get_shm_connector_name(type), name, dstdb);

	elog(DEBUG1, "reading scn file %s...", filename);
	fd = OpenTransientFile(filename, O_RDONLY);
	if (fd < 0)
		return false;

	if (read(fd, buf, sizeof(buf)) != sizeof(buf))
	{
		int save_errno = errno;
		CloseTransientFile(fd);
		errno = save_errno;
		set_shm_connector_errmsg(myConnectorId, "cannot read scn from file");
		elog(ERROR, "cannot read from file \"%s\": %m", filename);
	}

	CloseTransientFile(fd);

	g_scn = buf[0];
	g_c_scn = buf[1];
	elog(LOG, "initialize scn = %llu, c_scn = %llu", g_scn, g_c_scn);
	return true;
}
