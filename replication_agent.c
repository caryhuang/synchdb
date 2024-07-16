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

int ra_executePGDDL(PG_DDL * pgddl)
{
	int ret = -1;
	PG_TRY();
	{
		if (SPI_connect() != SPI_OK_CONNECT)
		{
			elog(WARNING, "synchdb_pgsql - SPI_connect failed");
			return ret;
		}

		ret = SPI_exec(pgddl->ddlquery, 0);
		if (ret != SPI_OK_UTILITY)
		{
			SPI_finish();
			elog(WARNING, "SPI_exec failed: %s", pgddl->ddlquery);
			return ret;
		}

		ret = 0;

		// Finish the SPI connection
		if (SPI_finish() != SPI_OK_FINISH)
		{
			elog(WARNING, "SPI_finish failed");
		}
	}
	PG_CATCH();
	{
		elog(WARNING, "caught an exception from handling PG DDL");
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
