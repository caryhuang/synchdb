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

static NetioContext g_netioCtx = {0};

int
olr_client_init(const char * hostname, unsigned int port)
{
	if (netio_connect(&g_netioCtx, hostname, port))
	{
		/* todo: hook up error messages with synchdb state view and use ERROR shutdown */
		elog(WARNING, "failed to connect to OLR");
		return -1;
	}
	elog(WARNING, "OLR connected");
	return 0;
}

int
olr_client_start_replication(char * source, unsigned long long scn, int which)
{
	StringInfoData strinfo;
	ssize_t nbytes = 0;
	size_t len;
	unsigned char *buf = NULL;
	int retcode = -1;

	OpenLogReplicator__Pb__RedoRequest request =
			OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__INIT;

	if (!g_netioCtx.is_connected)
	{
		/* todo: hook up error messages with synchdb state view and use ERROR shutdown */
		elog(WARNING, "no connection established to openlog replicator");
		return -1;
	}

	request.code = which == 0 ? OPEN_LOG_REPLICATOR__PB__REQUEST_CODE__START :
			OPEN_LOG_REPLICATOR__PB__REQUEST_CODE__CONTINUE;
	request.database_name = source;
	request.tm_val_case = OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__TM_VAL_SCN;
	request.scn = scn;

	len = open_log_replicator__pb__redo_request__get_packed_size(&request);
	buf = palloc0(len + 4);

	/* message length - 4 bytes */
	memcpy(buf, &len, 4);

	/* encode message with protobuf-c */
	open_log_replicator__pb__redo_request__pack(&request, buf + 4);

	/* send encoded message to olr */
	nbytes = netio_write(&g_netioCtx, buf, len + 4);
	elog(WARNING, "olr client sent %ld bytes to olr", nbytes);
	pfree(buf);

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
	elog(WARNING, "olr client read %ld bytes from olr", nbytes);

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
			elog(WARNING," response code %d", retcode);
			return retcode;
		}
		else
		{
			/* todo: hook up error messages with synchdb state view and use ERROR shutdown */
			elog(WARNING,"malformed protobuf message - NULL response");
			return -1;
		}
	}

	/* todo: hook up error messages with synchdb state view and use ERROR shutdown */
	elog(WARNING, "no response or got read error from olr - %ld bytes is read", nbytes);
	return -1;
}

int
olr_client_get_change(int myConnectorId, bool * dbzExitSignal, SynchdbStatistics * myBatchStats)
{
	ssize_t nbytes = 0;
	StringInfoData strinfo;

	if (!g_netioCtx.is_connected)
	{
		/* todo: hook up error messages with synchdb state view and use ERROR shutdown */
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
	elog(DEBUG1, "olr client read %ld bytes from olr", nbytes);

	/* decode received message with protobuf-c */
	if (nbytes > 4)
	{
		/* expected JSON payload - skip 4 bytes size header */
		elog(WARNING, "%s", strinfo.data + 4);

		/* process 1 change event */
		fc_processOLRChangeEvent(strinfo.data + 4, myBatchStats,
				get_shm_connector_name_by_id(myConnectorId));

		if (strinfo.data)
			pfree(strinfo.data);

		return 0;
	}
	elog(DEBUG1, "no response or got read error from olr - %ld bytes is read", nbytes);
	return -1;
}

void
olr_client_confirm_scn(unsigned long long scn)
{

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
