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
#include "replication/logicalrelation.h"

extern bool synchdb_dml_use_spi;

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

static int synchdb_handle_update(List * colvalbefore, List * colvalafter, Oid tableoid)
{
	Relation rel;
	TupleDesc tupdesc;
	TupleTableSlot * remoteslot, * localslot;
	EState	   *estate;
	RangeTblEntry *rte;
	List	   *perminfos = NIL;
	ResultRelInfo *resultRelInfo;
	ListCell * cell;
	int i = 0, ret = 0;
	EPQState	epqstate;
	bool found;
	Oid idxoid = InvalidOid;

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

	/* turn colvalbefore into TupleTableSlot */
	tupdesc = RelationGetDescr(rel);

	remoteslot = ExecInitExtraTupleSlot(estate, tupdesc, &TTSOpsVirtual);
	localslot = table_slot_create(rel, &estate->es_tupleTable);

	ExecClearTuple(remoteslot);

	i = 0;
	foreach(cell, colvalbefore)
	{
		PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
		Form_pg_attribute attr = TupleDescAttr(remoteslot->tts_tupleDescriptor, i);
		Oid			typinput;
		Oid			typioparam;

		if (!strcasecmp(colval->value, "NULL"))
			remoteslot->tts_isnull[i] = true;
		else
		{
			getTypeInputInfo(colval->datatype, &typinput, &typioparam);
			remoteslot->tts_values[i] =
				OidInputFunctionCall(typinput, colval->value,
									 typioparam, attr->atttypmod);
			remoteslot->tts_isnull[i] = false;
		}
		i++;
	}
	ExecStoreVirtualTuple(remoteslot);
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1, NIL);

	/* We must open indexes here. */
	ExecOpenIndices(resultRelInfo, false);

	/*
	 * check if there is a PK or relation identity index that we could use to
	 * locate the old tuple. If no identity or PK, there may potentially be
	 * other indexes created on other columns that can be used. But for now,
	 * we do not bother checking for them. Mark it as todo for later.
	 */
	idxoid = GetRelationIdentityOrPK(rel);
	if (OidIsValid(idxoid))
	{
		elog(WARNING, "attempt to find old tuple by index");
		found = RelationFindReplTupleByIndex(rel, idxoid,
											 LockTupleExclusive,
											 remoteslot, localslot);
	}
	else
	{
		elog(WARNING, "attempt to find old tuple by seq scan");
		found = RelationFindReplTupleSeq(rel, LockTupleExclusive,
										 remoteslot, localslot);
	}

	/*
	 * localslot should now contain the reference to the old tuple that is yet
	 * to be updated
	 */
	if (found)
	{
		/* turn colvalafter into TupleTableSlot */
		ExecClearTuple(remoteslot);

		i = 0;
		foreach(cell, colvalafter)
		{
			PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
			Form_pg_attribute attr = TupleDescAttr(remoteslot->tts_tupleDescriptor, i);
			Oid			typinput;
			Oid			typioparam;

			if (!strcasecmp(colval->value, "NULL"))
				remoteslot->tts_isnull[i] = true;
			else
			{
				getTypeInputInfo(colval->datatype, &typinput, &typioparam);
				remoteslot->tts_values[i] =
					OidInputFunctionCall(typinput, colval->value,
										 typioparam, attr->atttypmod);
				remoteslot->tts_isnull[i] = false;
			}
			i++;
		}
		ExecStoreVirtualTuple(remoteslot);

		EvalPlanQualSetSlot(&epqstate, remoteslot);

		ExecSimpleRelationUpdate(resultRelInfo, estate, &epqstate, localslot,
								 remoteslot);
	}
	else
	{
		elog(WARNING, "tuple to update not found");
		ret = -1;
	}

	/* Cleanup. */
	ExecCloseIndices(resultRelInfo);
	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);
	table_close(rel, NoLock);

	PopActiveSnapshot();
	CommitTransactionCommand();

	return ret;
}

static int synchdb_handle_delete(List * colvalbefore, Oid tableoid)
{
	Relation rel;
	TupleDesc tupdesc;
	TupleTableSlot * remoteslot, * localslot;
	EState	   *estate;
	RangeTblEntry *rte;
	List	   *perminfos = NIL;
	ResultRelInfo *resultRelInfo;
	ListCell * cell;
	int i = 0, ret = 0;
	EPQState	epqstate;
	bool found;
	Oid idxoid = InvalidOid;

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

	/* turn colvalbefore into TupleTableSlot */
	tupdesc = RelationGetDescr(rel);

	remoteslot = ExecInitExtraTupleSlot(estate, tupdesc, &TTSOpsVirtual);
	localslot = table_slot_create(rel, &estate->es_tupleTable);

	ExecClearTuple(remoteslot);

	i = 0;
	foreach(cell, colvalbefore)
	{
		PG_DML_COLUMN_VALUE * colval = (PG_DML_COLUMN_VALUE *) lfirst(cell);
		Form_pg_attribute attr = TupleDescAttr(remoteslot->tts_tupleDescriptor, i);
		Oid			typinput;
		Oid			typioparam;

		if (!strcasecmp(colval->value, "NULL"))
			remoteslot->tts_isnull[i] = true;
		else
		{
			getTypeInputInfo(colval->datatype, &typinput, &typioparam);
			remoteslot->tts_values[i] =
				OidInputFunctionCall(typinput, colval->value,
									 typioparam, attr->atttypmod);
			remoteslot->tts_isnull[i] = false;
		}
		i++;
	}
	ExecStoreVirtualTuple(remoteslot);
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1, NIL);

	/* We must open indexes here. */
	ExecOpenIndices(resultRelInfo, false);

	/*
	 * check if there is a PK or relation identity index that we could use to
	 * locate the old tuple. If no identity or PK, there may potentially be
	 * other indexes created on other columns that can be used. But for now,
	 * we do not bother checking for them. Mark it as todo for later.
	 */
	idxoid = GetRelationIdentityOrPK(rel);
	if (OidIsValid(idxoid))
	{
		elog(WARNING, "attempt to find old tuple by index");
		found = RelationFindReplTupleByIndex(rel, idxoid,
											 LockTupleExclusive,
											 remoteslot, localslot);
	}
	else
	{
		elog(WARNING, "attempt to find old tuple by seq scan");
		found = RelationFindReplTupleSeq(rel, LockTupleExclusive,
										 remoteslot, localslot);
	}

	/*
	 * localslot should now contain the reference to the old tuple that is yet
	 * to be updated
	 */
	if (found)
	{
		EvalPlanQualSetSlot(&epqstate, localslot);

		ExecSimpleRelationDelete(resultRelInfo, estate, &epqstate, localslot);
	}
	else
	{
		elog(WARNING, "tuple to delete not found");
		ret = -1;
	}

	/* Cleanup. */
	ExecCloseIndices(resultRelInfo);
	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);
	table_close(rel, NoLock);

	PopActiveSnapshot();
	CommitTransactionCommand();

	return ret;
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
			if (synchdb_dml_use_spi)
				return spi_execute(pgdml->dmlquery);
			else
				return synchdb_handle_insert(pgdml->columnValuesAfter, pgdml->tableoid);
		}
		case 'u':
		{
			if (synchdb_dml_use_spi)
				return spi_execute(pgdml->dmlquery);
			else
				return synchdb_handle_update(pgdml->columnValuesBefore,
											 pgdml->columnValuesAfter,
											 pgdml->tableoid);
		}
		case 'd':
		{
			if (synchdb_dml_use_spi)
				return spi_execute(pgdml->dmlquery);
			else
				return synchdb_handle_delete(pgdml->columnValuesBefore, pgdml->tableoid);
		}
		default:
		{
			/* all others, use SPI to execute regardless what synchdb_dml_use_spi is */
			return spi_execute(pgdml->dmlquery);
		}
	}
	return -1;
}
