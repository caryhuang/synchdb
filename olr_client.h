/*
 * olr_client.h
 *
 * Implementation of Openlog Replicator Client
 *
 * Copyright (c) Hornetlabs Technology, Inc.
 *
 */

#ifndef SYNCHDB_OLR_CLIENT_H_
#define SYNCHDB_OLR_CLIENT_H_

#include "postgres.h"
#include "synchdb.h"
#include "netio_utils.h"

/**
 * RedoResponseCode - Enum representing response code from OLR
 */
typedef enum _ResponseCode
{
	RES_READY = 0,
	RES_FAILED_START,
	RES_STARTING,
	RES_ALREADY_STARTED,
	RES_REPLICATE,
	RES_PAYLOAD,
	RES_INVALID_DATABASE,
	RES_INVALID_COMMAND
} ResponseCode;

int olr_client_start_replication(char * source, unsigned long long scn, int which);
int olr_client_init(const char * hostname, unsigned int port);
void olr_client_shutdown(void);
int olr_client_get_change(int myConnectorId, bool * dbzExitSignal, SynchdbStatistics * myBatchStats);
void olr_client_confirm_scn(unsigned long long scn);

#endif /* SYNCHDB_OLR_CLIENT_H_ */
