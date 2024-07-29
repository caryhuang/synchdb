/*
 * replication_agent.c
 *
 *  Created on: Jul. 16, 2024
 *      Author: caryh
 */

#include "postgres.h"
#include "fmgr.h"
#include "replication_agent.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "utils/snapmgr.h"

#include "access/table.h"
#include "executor/tuptable.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "utils/snapmgr.h"
#include "parser/parse_relation.h"

static int spi_execute(char * query)
{
	int ret = -1;
	PG_TRY();
	{
		/* Start a transaction and set a snapshot */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (SPI_connect() != SPI_OK_CONNECT)
		{
			elog(WARNING, "synchdb_pgsql - SPI_connect failed");
			return ret;
		}

		ret = SPI_exec(query, 0);
		switch (ret)
		{
			case SPI_OK_INSERT:
			case SPI_OK_UTILITY:
			case SPI_OK_DELETE:
			case SPI_OK_UPDATE:
			{
				elog(WARNING, "SPI OK with ret %d", ret);
				break;
			}
			default:
			{
				SPI_finish();
				elog(WARNING, "SPI_exec failed: %d", ret);
				return ret;
			}
		}

		ret = 0;
		// Finish the SPI connection
		if (SPI_finish() != SPI_OK_FINISH)
		{
			elog(WARNING, "SPI_finish failed");
		}

		/* Commit the transaction */
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
//		elog(WARNING, "caught an exception from handling PG DDL");
		SPI_finish();
		ret = -1;

		/* PG_CATCH would cause a resource leak, complaining pg_class not closed
		 * or something, so we will re-throw back to prevent this resource leak.
		 * This also means that when ERROR is encountered, (table exists), the
		 * synchdb operation is disrupted until next run... todo
		 */
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ret;
}

static int synchdb_handle_insert(List * colval, Oid tableoid)
{
	Relation rel;
	TupleDesc tupdesc;
	TupleTableSlot *slot;
	EState	   *estate;
	RangeTblEntry *rte;
	List	   *perminfos = NIL;
	ResultRelInfo *resultRelInfo;
	ListCell * cell;
	int i = 0;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	rel = table_open(tableoid, NoLock);

	/* initialize estate */
	estate = CreateExecutorState();

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->rellockmode = AccessShareLock;

	addRTEPermissionInfo(&perminfos, rte);

	ExecInitRangeTable(estate, list_make1(rte), perminfos);
	estate->es_output_cid = GetCurrentCommandId(true);

	/* initialize resultRelInfo */
	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

	/* turn colval into TupleTableSlot */
	tupdesc = RelationGetDescr(rel);
	slot = ExecInitExtraTupleSlot(estate, tupdesc, &TTSOpsVirtual);

	ExecClearTuple(slot);

	i = 0;
	foreach(cell, colval)
	{
		PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
		Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, i);
		Oid			typinput;
		Oid			typioparam;

		if (!strcasecmp(colval->value, "NULL"))
			slot->tts_isnull[i] = true;
		else
		{
			getTypeInputInfo(colval->datatype, &typinput, &typioparam);
			slot->tts_values[i] =
				OidInputFunctionCall(typinput, colval->value,
									 typioparam, attr->atttypmod);
			slot->tts_isnull[i] = false;
		}
		i++;
	}
	ExecStoreVirtualTuple(slot);

	/* We must open indexes here. */
	ExecOpenIndices(resultRelInfo, false);

	/* Do the insert. */
	ExecSimpleRelationInsert(resultRelInfo, estate, slot);

	/* Cleanup. */
	ExecCloseIndices(resultRelInfo);

	table_close(rel, NoLock);

	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);

	PopActiveSnapshot();
	CommitTransactionCommand();

	return 0;
}

int ra_executePGDDL(PG_DDL * pgddl)
{
	return spi_execute(pgddl->ddlquery);
}

int ra_executePGDML(PG_DML * pgdml)
{
	switch (pgdml->op)
	{
		case 'r':
		case 'c':
		{
			/* use direct heap insert */
			elog(WARNING, "direct heap insertion");
			return synchdb_handle_insert(pgdml->columnValuesAfter, pgdml->tableoid);
		}
		case 'u':
		case 'd':
		default:
		{
			/* all others, use SPI to execute */
			return spi_execute(pgdml->dmlquery);
		}
	}
	return -1;
}
