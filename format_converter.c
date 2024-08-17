/*-------------------------------------------------------------------------
 *
 * format_converter.c
 *    Conversion utilities for Debezium change events to PostgreSQL format
 *
 * This file contains functions to parse Debezium (DBZ) change events,
 * convert them to PostgreSQL-compatible DDL and DML operations, and
 * execute those operations. It handles CREATE, DROP, INSERT, UPDATE,
 * and DELETE operations from various source databases (currently 
 * MySQL, Oracle, and SQL Server) and converts them to equivalent 
 * PostgreSQL commands.
 *
 * The main entry point is fc_processDBZChangeEvent(), which takes a
 * Debezium change event as input, parses it, converts it, and executes
 * the resulting PostgreSQL operation.
 *
 * Key functions:
 * - parseDBZDDL(): Parses Debezium DDL events
 * - parseDBZDML(): Parses Debezium DML events
 * - convert2PGDDL(): Converts DBZ DDL to PostgreSQL DDL
 * - convert2PGDML(): Converts DBZ DML to PostgreSQL DML
 * - processDataByType(): Handles data type conversions
 *
 * Copyright (c) Hornetlabs Technology, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"
#include "format_converter.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "access/table.h"
#include <time.h>
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "synchdb.h"

extern bool synchdb_dml_use_spi;

/* Function to remove double quotes from a string */
static void
remove_double_quotes(StringInfoData * str)
{
	char *src = str->data, *dst = str->data;
	int newlen = 0;

	while (*src)
	{
		if (*src != '"' && *src != '\\')
		{
			*dst++ = *src;
			newlen++;
		}
		src++;
	}
	*dst = '\0';
	str->len = newlen;
}

/* Function to get a string element from a JSONB path */
static int
getPathElementString(Jsonb * jb, char * path, StringInfoData * strinfoout)
{
	Datum * datum_elems = NULL;
	char * str_elems = NULL, * p = path;
	int numPaths = 0, curr = 0;
	char * pathcopy = pstrdup(path);
	Datum res;
	bool isnull;

	if (!strinfoout)
	{
		elog(WARNING, "strinfo is null");
		return -1;
	}

    /* Count the number of elements in the path */
	if (strstr(pathcopy, "."))
	{
		while (*p != '\0')
		{
			if (*p == '.')
			{
				numPaths++;
			}
			p++;
		}
		numPaths++; /* Add the last one */
	}
	else
	{
		numPaths = 1;
	}

	datum_elems = palloc0(sizeof(Datum) * numPaths);

    /* Parse the path into elements */
	if (strstr(pathcopy, "."))
	{
		str_elems= strtok(pathcopy, ".");
		if (str_elems)
		{
			datum_elems[curr] = CStringGetTextDatum(str_elems);
			curr++;
			while ((str_elems = strtok(NULL, ".")))
			{
				datum_elems[curr] = CStringGetTextDatum(str_elems);
				curr++;
			}
		}
	}
	else
	{
		/* only one level, just use pathcopy*/
		datum_elems[curr] = CStringGetTextDatum(pathcopy);
	}

    /* Get the element from JSONB */
    res = jsonb_get_element(jb, datum_elems, numPaths, &isnull, false);
    if (isnull)
    {
    	resetStringInfo(strinfoout);
    	appendStringInfoString(strinfoout, "NULL");
    	elog(WARNING, "%s = NULL", path);
    }
    else
    {
    	Jsonb *resjb = DatumGetJsonbP(res);
    	resetStringInfo(strinfoout);
		JsonbToCString(strinfoout, &resjb->root, VARSIZE(resjb));

		/*
		 * note: buf.data includes double quotes and escape char \.
		 * We need to remove them
		 */
		remove_double_quotes(strinfoout);
		elog(WARNING, "%s = %s", path, strinfoout->data);
    }

	pfree(datum_elems);
	pfree(pathcopy);
	return 0;
}

/* Function to get a JSONB element from a path */
static Jsonb *
getPathElementJsonb(Jsonb * jb, char * path)
{
	Datum * datum_elems = NULL;
	char * str_elems = NULL, * p = path;
	int numPaths = 0, curr = 0;
	char * pathcopy = pstrdup(path);
	bool isnull;
	Datum datout;
	Jsonb * out = NULL;

	if (strstr(pathcopy, "."))
	{
		/* count how many elements are in path*/
		while (*p != '\0')
		{
			if (*p == '.')
			{
				numPaths++;
			}
			p++;
		}
		/* add the last one */
		numPaths++;
	}
	else
	{
		numPaths = 1;
	}

	datum_elems = palloc0(sizeof(Datum) * numPaths);

	if (strstr(pathcopy, "."))
	{
		/* multi level paths, */
		str_elems= strtok(pathcopy, ".");
		if (str_elems)
		{
			datum_elems[curr] = CStringGetTextDatum(str_elems);
			curr++;
			while (str_elems)
			{
				/* parse the remaining elements */
				str_elems = strtok(NULL, ".");

				if (str_elems == NULL)
					break;

				datum_elems[curr] = CStringGetTextDatum(str_elems);
				curr++;
			}
		}
	}
	else
	{
		/* only one level, just use pathcopy*/
		datum_elems[curr] = CStringGetTextDatum(pathcopy);
	}
	datout = jsonb_get_element(jb, datum_elems, numPaths, &isnull, false);
	if (isnull)
		out = NULL;
	else
		out = DatumGetJsonbP(datout);

	pfree(datum_elems);
	pfree(pathcopy);
	return out;
}

/* Functions to destroy various structures */
static void
destroyDBZDDL(DBZ_DDL * ddlinfo)
{
	if (ddlinfo)
	{
		if (ddlinfo->id)
			pfree(ddlinfo->id);

		if (ddlinfo->type)
			pfree(ddlinfo->type);

		if (ddlinfo->primaryKeyColumnNames)
			pfree(ddlinfo->primaryKeyColumnNames);

		list_free_deep(ddlinfo->columns);

		pfree(ddlinfo);
	}
}

static void
destroyPGDDL(PG_DDL * ddlinfo)
{
	if (ddlinfo)
	{
		if (ddlinfo->ddlquery)
			pfree(ddlinfo->ddlquery);
		pfree(ddlinfo);
	}
}

static void
destroyPGDML(PG_DML * dmlinfo)
{
	if (dmlinfo)
	{
		if (dmlinfo->dmlquery)
			pfree(dmlinfo->dmlquery);

		if (dmlinfo->columnValuesBefore)
			list_free_deep(dmlinfo->columnValuesBefore);

		if (dmlinfo->columnValuesAfter)
			list_free_deep(dmlinfo->columnValuesAfter);
		pfree(dmlinfo);
	}
}

static void
destroyDBZDML(DBZ_DML * dmlinfo)
{
	if (dmlinfo)
	{
		if (dmlinfo->table)
			pfree(dmlinfo->table);

		if (dmlinfo->db)
			pfree(dmlinfo->db);

		if (dmlinfo->columnValuesBefore)
			list_free_deep(dmlinfo->columnValuesBefore);

		if (dmlinfo->columnValuesAfter)
			list_free_deep(dmlinfo->columnValuesAfter);

		pfree(dmlinfo);
	}
}

/* Function to parse Debezium DDL */
static DBZ_DDL *
parseDBZDDL(Jsonb * jb)
{
	Jsonb * ddlpayload = NULL;
	JsonbIterator *it;
	JsonbValue v;
	JsonbIteratorToken r;
	char * key = NULL;
	char * value = NULL;

	DBZ_DDL * ddlinfo = (DBZ_DDL*) palloc0(sizeof(DBZ_DDL));
	DBZ_DDL_COLUMN * ddlcol = NULL;

	/* get table name and action type */
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	/*
	 * todo: we only support parsing 1 set of DDL for now using hardcoded
	 * array index 0. Need to remove this limitation later
	 */
    getPathElementString(jb, "payload.tableChanges.0.id", &strinfo);
    ddlinfo->id = pstrdup(strinfo.data);

    getPathElementString(jb, "payload.tableChanges.0.table.primaryKeyColumnNames", &strinfo);
    ddlinfo->primaryKeyColumnNames = pstrdup(strinfo.data);

    getPathElementString(jb, "payload.tableChanges.0.type", &strinfo);
    ddlinfo->type = pstrdup(strinfo.data);

    /* free the data inside strinfo as we no longer needs it */
    pfree(strinfo.data);

    if (!strcmp(ddlinfo->id, "NULL") && !strcmp(ddlinfo->type, "NULL"))
    {
    	elog(WARNING, "no table change data. Stop parsing...");
    	destroyDBZDDL(ddlinfo);
    	return NULL;
    }

    if (!strcmp(ddlinfo->type, "CREATE"))
    {
		/* fetch payload.tableChanges.0.table.columns as jsonb */
		ddlpayload = getPathElementJsonb(jb, "payload.tableChanges.0.table.columns");

		/*
		 * following parser expects this json array named 'columns' from DBZ embedded:
		 * "columns": [
		 *   {
		 *     "name": "a",
		 *     "scale": null,
		 *     "length": null,
		 *     "comment": null,
		 *     "jdbcType": 4,
		 *     "optional": true,
		 *     "position": 1,
		 *     "typeName": "INT",
		 *     "generated": false,
		 *     "enumValues": null,
		 *     "nativeType": null,
		 *     "charsetName": null,
		 *     "typeExpression": "INT",
		 *     "autoIncremented": false,
		 *     "defaultValueExpression": null
		 *   },
		 *   ...... rest of array elements
		 *
		 */
		if (ddlpayload)
		{
			/* iterate this payload jsonb */
			it = JsonbIteratorInit(&ddlpayload->root);
			while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
			{
				switch (r)
				{
					case WJB_BEGIN_OBJECT:
						elog(WARNING, "parsing column --------------------");
						ddlcol = (DBZ_DDL_COLUMN *) palloc0(sizeof(DBZ_DDL_COLUMN));

						if (key)
						{
							pfree(key);
							key = NULL;
						}
						break;
					case WJB_END_OBJECT:
						/* append ddlcol to ddlinfo->columns list for further processing */
						ddlinfo->columns = lappend(ddlinfo->columns, ddlcol);

						break;
					case WJB_BEGIN_ARRAY:
						elog(DEBUG2, "Begin array under %s", key ? key : "NULL");
						if (key)
						{
							pfree(key);
							key = NULL;
						}
						break;
					case WJB_END_ARRAY:
						elog(DEBUG2, "End array");
						break;
					case WJB_KEY:
						key = pnstrdup(v.val.string.val, v.val.string.len);
						elog(DEBUG2, "Key: %s", key);

						break;
					case WJB_VALUE:
					case WJB_ELEM:
						switch (v.type)
						{
							case jbvNull:
								elog(DEBUG2, "Value: NULL");
								value = pnstrdup("NULL", strlen("NULL"));
								break;
							case jbvString:
								value = pnstrdup(v.val.string.val, v.val.string.len);
								elog(DEBUG2, "String Value: %s", value);
								break;
							case jbvNumeric:
							{
								value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
								elog(DEBUG2, "Numeric Value: %s", value);
								break;
							}
							case jbvBool:
								elog(DEBUG2, "Boolean Value: %s", v.val.boolean ? "true" : "false");
								if (v.val.boolean)
									value = pnstrdup("true", strlen("true"));
								else
									value = pnstrdup("false", strlen("false"));
								break;
							case jbvBinary:
								elog(DEBUG2, "Binary Value: [binary data]");
								break;
							default:
								elog(DEBUG2, "Unknown value type: %d", v.type);
								break;
						}
					break;
					default:
						elog(WARNING, "Unknown token: %d", r);
						break;
				}

				/* check if we have a key - value pair */
				if (key != NULL && value != NULL)
				{
					if (!strcmp(key, "name"))
					{
						elog(WARNING, "consuming %s = %s", key, value);
						ddlcol->name = pstrdup(value);
					}
					if (!strcmp(key, "length"))
					{
						elog(WARNING, "consuming %s = %s", key, value);
						ddlcol->length = strcmp(value, "NULL") == 0 ? 0 : atoi(value);
					}
					if (!strcmp(key, "optional"))
					{
						elog(WARNING, "consuming %s = %s", key, value);
						ddlcol->optional = strcmp(value, "true") == 0 ? true : false;
					}
					if (!strcmp(key, "position"))
					{
						elog(WARNING, "consuming %s = %s", key, value);
						ddlcol->position = atoi(value);
					}
					if (!strcmp(key, "typeName"))
					{
						elog(WARNING, "consuming %s = %s", key, value);
						ddlcol->typeName = pstrdup(value);
					}
					if (!strcmp(key, "enumValues"))
					{
						elog(WARNING, "consuming %s = %s", key, value);
						ddlcol->enumValues = pstrdup(value);
					}
					if (!strcmp(key, "charsetName"))
					{
						elog(WARNING, "consuming %s = %s", key, value);
						ddlcol->charsetName = pstrdup(value);
					}
					if (!strcmp(key, "autoIncremented"))
					{
						elog(WARNING, "consuming %s = %s", key, value);
						ddlcol->autoIncremented = strcmp(value, "true") == 0 ? true : false;
					}
					if (!strcmp(key, "defaultValueExpression"))
					{
						elog(WARNING, "consuming %s = %s", key, value);
						ddlcol->defaultValueExpression = pstrdup(value);
					}

					/* note: other key - value pairs ignored for now */
					pfree(key);
					pfree(value);
					key = NULL;
					value = NULL;
				}
			}
		}
		else
		{
			elog(WARNING, "failed to get payload.tableChanges.0.table.columns as jsonb");
			destroyDBZDDL(ddlinfo);
			return NULL;
		}
    }
    else if (!strcmp(ddlinfo->type, "DROP"))
    {
    	/* no further parsing needed for DROP, just return ddlinfo */
    	return ddlinfo;
    }
    else
    {
		elog(WARNING, "unknown ddl type %s", ddlinfo->type);
		destroyDBZDDL(ddlinfo);
		return NULL;
    }
	return ddlinfo;
}

/*
 * Function to split ID string into database, schema, and table.
 *
 * This function transforms id format 'database.schema.table' to
 * 'database.schema_table'. If the id format is 'database.table'
 * then no transformation is applied.
 */
static void
splitIdString(char * id, char ** db, char ** schema, char ** table)
{
	int dotCount = 0;
	char *p = NULL;

	for (p = id; *p != '\0'; p++)
	{
		if (*p == '.')
			dotCount++;
	}

	if (dotCount == 1)
	{
		/* assume to be: database.table */
		*db = strtok(id, ".");
		*schema = NULL;
		*table = strtok(NULL, ".");
	}
	else
	{
		/* assume to be: database.schema.table */
		*db = strtok(id, ".");
		*schema = strtok(NULL, ".");
		*table = strtok(NULL, ".");
	}
}

/* Function to transform DDL columns */
static void
transformDDLColumns(DBZ_DDL_COLUMN * col, ConnectorType conntype, StringInfoData * strinfo)
{
	switch (conntype)
	{
		case TYPE_MYSQL:
		{
			/*
			 * todo: column data type conversion cases for MySQL:
			 *  - if type is INT and autoincremented is true, translate to SERIAL
			 *  - if type is BIGINT and autoincremented is true, translate to BIGSERIAL
			 *  - if type is SMALLINT and autoincremented is true, translate to SMALLSERIAL
			 *  - if type is ENUM, translate to TEXT with length=0
			 *  - if type is GEOMETRY, translate to TEXT
			 */
			if (!strcasecmp(col->typeName, "INT") && col->autoIncremented)
				appendStringInfo(strinfo, " %s %s ", col->name, "SERIAL");
			else if (!strcasecmp(col->typeName, "BIGINT") && col->autoIncremented)
				appendStringInfo(strinfo, " %s %s ", col->name, "BIGSERIAL");
			else if (!strcasecmp(col->typeName, "SMALLINT") && col->autoIncremented)
				appendStringInfo(strinfo, " %s %s ", col->name, "SMALLSERIAL");
			else if (!strcasecmp(col->typeName, "ENUM"))
			{
				appendStringInfo(strinfo, " %s %s ", col->name, "TEXT");
				col->length = 0;
			}
			else if (!strcmp(col->typeName, "GEOMETRY"))
				appendStringInfo(strinfo, " %s %s ", col->name, "TEXT");
			else
				appendStringInfo(strinfo, " %s %s ", col->name, col->typeName);

			break;
		}
		case TYPE_ORACLE:
		{
			break;
		}
		case TYPE_SQLSERVER:
		{
			/*
			 * todo: column data type conversion cases for SqlServer:
			 *  - if type is int identity and autoincremented is true, translate to SERIAL
			 *  - if type is bigint identity and autoincremented is true, translate to BIGSERIAL
			 *  - if type is smallint identity and autoincremented is true, translate to SMALLSERIAL
			 *  - if type is ENUM, translate to TEXT with length=0
			 *  - if type is GEOMETRY, translate to TEXT
			 */
			if (!strcasecmp(col->typeName, "int identity") && col->autoIncremented)
				appendStringInfo(strinfo, " %s %s ", col->name, "SERIAL");
			else if (!strcasecmp(col->typeName, "bigint identity") && col->autoIncremented)
				appendStringInfo(strinfo, " %s %s ", col->name, "BIGSERIAL");
			else if (!strcasecmp(col->typeName, "int identity") && col->autoIncremented)
				appendStringInfo(strinfo, " %s %s ", col->name, "SMALLSERIAL");
			else if (!strcasecmp(col->typeName, "ENUM"))
			{
				appendStringInfo(strinfo, " %s %s ", col->name, "TEXT");
				col->length = 0;	/* this prevents adding a fixed size for TEXT */
			}
			else if (!strcmp(col->typeName, "GEOMETRY"))
				appendStringInfo(strinfo, " %s %s ", col->name, "TEXT");
			else
				appendStringInfo(strinfo, " %s %s ", col->name, col->typeName);

			/* set the length to 0 to prevent adding a fixed size for non-string columne types */
			if (strcasecmp(col->typeName, "varchar") && strcasecmp(col->typeName, "char") && col->length > 0)
				col->length = 0;

			break;
		}
		default:
		{
			/* unknown type, no special handling - may error out later when applying to PostgreSQL */
			appendStringInfo(strinfo, " %s %s ", col->name, col->typeName);
			break;
		}
	}
}

/* Function to convert Debezium DDL to PostgreSQL DDL */
static PG_DDL *
convert2PGDDL(DBZ_DDL * dbzddl, ConnectorType type)
{
	PG_DDL * pgddl = (PG_DDL*) palloc0(sizeof(PG_DDL));
	ListCell * cell;

	StringInfoData strinfo;

	initStringInfo(&strinfo);

	if (!strcmp(dbzddl->type, "CREATE"))
	{
		/* assume CREATE table */
		/* todo: dbzddl->id is can be exprssed in either database.table or
		 * database.schema.table formats. Right now we will transform like this:
		 *
		 * database.table:
		 * 	- database becomes schema in PG
		 * 	- table name stays
		 *
		 * database.schema.table:
		 * 	- database becomes schema in PG
		 * 	- schema is ignored
		 * 	- table name stays
		 *
		 * todo: We should make this behavior configurable in the future
		 */
		char * tmp = pstrdup(dbzddl->id);
		char * db = NULL, * schema = NULL, * table = NULL;

		splitIdString(dbzddl->id, &db, &schema, &table);

		/* database and table must be present. schema is optional */
		if (!db || !table)
		{
			/* save the error */
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
					dbzddl->id);
			set_shm_connector_errmsg(type, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		/* database mapped to schema */
		appendStringInfo(&strinfo, "CREATE SCHEMA IF NOT EXISTS %s; ", db);
		pfree(tmp);

		/* table stays as table, schema ignored */
		appendStringInfo(&strinfo, "CREATE TABLE IF NOT EXISTS %s.%s (", db, table);

		foreach(cell, dbzddl->columns)
		{
			DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);

			transformDDLColumns(col, type, &strinfo);

			/* if a length if specified, add it. For example VARCHAR(30)*/
			if (col->length > 0)
			{
				appendStringInfo(&strinfo, "(%d) ", col->length);
			}

			/* if it is marked as primary key */
			if (strstr(dbzddl->primaryKeyColumnNames, col->name))
			{
				appendStringInfo(&strinfo, "PRIMARY KEY ");
			}

			/* is it optional? */
			if (!col->optional)
			{
				appendStringInfo(&strinfo, "NOT NULL ");
			}

			/* does it have defaults? */
			if (col->defaultValueExpression && strlen(col->defaultValueExpression) > 0
					&& !col->autoIncremented)
			{
				appendStringInfo(&strinfo, "DEFAULT %s ", col->defaultValueExpression);
			}

			appendStringInfo(&strinfo, ",");
		}

		/* remove the last extra comma */
		strinfo.data[strinfo.len - 1] = '\0';
		strinfo.len = strinfo.len - 1;

		appendStringInfo(&strinfo, ");");
	}
	else if (!strcmp(dbzddl->type, "DROP"))
	{
		appendStringInfo(&strinfo, "DROP TABLE IF EXISTS %s;", dbzddl->id);
	}

	pgddl->ddlquery = pstrdup(strinfo.data);

	/* free the data inside strinfo as we no longer needs it */
	pfree(strinfo.data);

	elog(WARNING, "pgsql: %s ", pgddl->ddlquery);
	return pgddl;
}

/*
 * this function performs necessary data conversions to convert input data
 * as string and output a processed string based on type
 */
static char *
processDataByType(char * in, Oid type, bool addquote, ConnectorType conntype)
{
	char * out = NULL;

	if (!in || strlen(in) == 0)
		return NULL;

	if (!strcmp(in, "NULL") || !strcmp(in, "null"))
		return NULL;

	switch(type)
	{
		case INT8OID:
		case INT2OID:
		case BOOLOID:
		case INT4OID:
		case FLOAT8OID:
		case FLOAT4OID:
		case NUMERICOID:
		{
			/* no extra processing for nunmeric types */
			out = (char *) palloc0(strlen(in) + 1);
			strncpy(out, in, strlen(in));
			break;
		}
		case TEXTOID:
		case VARCHAROID:
		case CSTRINGOID:
		{
			if (addquote)
			{
				size_t i = 0, j = 0;
				size_t outlen = 0;

				/* escape possible single quotes */
				for (i = 0; i < strlen(in); i++)
				{
					if (in[i] == '\'')
					{
						/* single quote will be escaped so +2 in size */
						outlen += 2;
					}
					else
					{
						outlen++;
					}
				}

				/* 2 more to account for open and closing quotes */
				out = (char *) palloc0(outlen + 2 + 1);

				out[j++] = '\'';
				for (i = 0; i < strlen(in); i++)
				{
					if (in[i] == '\'')
					{
						out[j++] = '\'';
						out[j++] = '\'';
					}
					else
					{
						out[j++] = in[i];
					}
				}
				out[j++] = '\'';
			}
			else
			{
				out = (char *) palloc0(strlen(in) + 1);
				strncpy(out, in, strlen(in));
			}
			break;
		}
		case DATEOID:
		{
			/*
			 * DBZ uses number of days since epoch 1970-01-01 to represent a date.
			 * to decode, we need to add this number to epoch to get the final
			 * date value and converts it to string as output
			 */
			unsigned int daysSinceEpoch = atoi(in);
			struct tm epoch = {0};
			time_t epoch_time, target_time;
			struct tm *target_date;
			char datestr[10 + 1]; /* yyyy-mm-dd */

			/* since 1970-01-01 */
			epoch.tm_year = 70;
			epoch.tm_mon = 0;
			epoch.tm_mday = 1;

			epoch_time = mktime(&epoch);
			target_time = epoch_time + (daysSinceEpoch * 24 * 60 * 60);

			/*
			 * Convert to struct tm in GMT timezone for now
			 * todo: convert to local timezone?
			 */
			target_date = gmtime(&target_time);
			strftime(datestr, 11, "%Y-%m-%d", target_date);

			if (addquote)
			{
				/* date string needs quotes as well */
				out = (char *) palloc0(strlen(datestr) + 2 + 1);
				snprintf(out, strlen(datestr) + 2 + 1, "'%s'", datestr);
			}
			else
			{
				out = (char *) palloc0(strlen(datestr) + 1);
				strncpy(out, datestr, strlen(datestr));
			}
			break;
		}
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case TIMETZOID:
		default:
		{
			/* todo: support more */
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "unsupported data type %d", type);
			set_shm_connector_errmsg(conntype, msg);
			elog(ERROR, "%s", msg);
		}
	}
	return out;
}

static int
list_sort_cmp(const ListCell *a, const ListCell *b)
{
	DBZ_DML_COLUMN_VALUE * colvala = (DBZ_DML_COLUMN_VALUE *) lfirst(a);
	DBZ_DML_COLUMN_VALUE * colvalb = (DBZ_DML_COLUMN_VALUE *) lfirst(b);

	if (colvala->position < colvalb->position)
		return -1;
	if (colvala->position > colvalb->position)
		return 1;
	return 0;
}

/*
 * todo: currently, this function converts DBZ DML to a DML query to be sent to
 * PostgreSQL's SPI for fast implementation. In the future, we can convert into
 * a string of tuples following PostgreSQL replication wire protocol and send this
 * stream directly to PostgreSQL's logical replication APIs to handle.
 */
static PG_DML *
convert2PGDML(DBZ_DML * dbzdml, ConnectorType type)
{
	PG_DML * pgdml = (PG_DML*) palloc0(sizeof(PG_DML));
	ListCell * cell, * cell2;

	StringInfoData strinfo;

	initStringInfo(&strinfo);

	/* copy identification data to PG_DML */
	pgdml->op = dbzdml->op;
	pgdml->tableoid = dbzdml->tableoid;

	switch(dbzdml->op)
	{
		case 'r':
		case 'c':
		{
			if (synchdb_dml_use_spi)
			{
				/* --- Convert to use SPI to handler DML --- */
				appendStringInfo(&strinfo, "INSERT INTO %s.%s(", dbzdml->db, dbzdml->table);
				foreach(cell, dbzdml->columnValuesAfter)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					appendStringInfo(&strinfo, "%s,", colval->name);
				}
				strinfo.data[strinfo.len - 1] = '\0';
				strinfo.len = strinfo.len - 1;
				appendStringInfo(&strinfo, ") VALUES (");

				foreach(cell, dbzdml->columnValuesAfter)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					char * data = processDataByType(colval->value, colval->datatype, true, type);

					if (data != NULL)
					{
						appendStringInfo(&strinfo, "%s,", data);
						pfree(data);
					}
					else
					{
						appendStringInfo(&strinfo, "%s,", "null");
					}
				}
				/* remove extra "," */
				strinfo.data[strinfo.len - 1] = '\0';
				strinfo.len = strinfo.len - 1;

				appendStringInfo(&strinfo, ");");
			}
			else
			{
				/* --- Convert to use Heap AM to handler DML --- */
				foreach(cell, dbzdml->columnValuesAfter)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					PG_DML_COLUMN_VALUE * pgcolval = palloc0(sizeof(PG_DML_COLUMN_VALUE));

					char * data = processDataByType(colval->value, colval->datatype, false, type);

					if (data != NULL)
					{
						pgcolval->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval->value = pstrdup("NULL");

					pgcolval->datatype = colval->datatype;

					pgdml->columnValuesAfter = lappend(pgdml->columnValuesAfter, pgcolval);
				}
				pgdml->columnValuesBefore = NULL;
			}
			break;
		}
		case 'd':
		{
			if (synchdb_dml_use_spi)
			{
				/* --- Convert to use SPI to handler DML --- */
				appendStringInfo(&strinfo, "DELETE FROM %s.%s WHERE ", dbzdml->db, dbzdml->table);
				foreach(cell, dbzdml->columnValuesBefore)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					char * data;

					appendStringInfo(&strinfo, "%s = ", colval->name);
					data = processDataByType(colval->value, colval->datatype, true, type);
					if (data != NULL)
					{
						appendStringInfo(&strinfo, "%s", data);
						pfree(data);
					}
					else
					{
						appendStringInfo(&strinfo, "%s", "null");
					}
					appendStringInfo(&strinfo, " AND ");
				}
				/* remove extra " AND " */
				strinfo.data[strinfo.len - 5] = '\0';
				strinfo.len = strinfo.len - 5;

				appendStringInfo(&strinfo, ";");
			}
			else
			{
				/* --- Convert to use Heap AM to handler DML --- */
				foreach(cell, dbzdml->columnValuesBefore)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					PG_DML_COLUMN_VALUE * pgcolval = palloc0(sizeof(PG_DML_COLUMN_VALUE));

					char * data = processDataByType(colval->value, colval->datatype, false, type);

					if (data != NULL)
					{
						pgcolval->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval->value = pstrdup("NULL");

					pgcolval->datatype = colval->datatype;
					pgdml->columnValuesBefore = lappend(pgdml->columnValuesBefore, pgcolval);
				}
				pgdml->columnValuesAfter = NULL;
			}
			break;
		}
		case 'u':
		{
			if (synchdb_dml_use_spi)
			{
				/* --- Convert to use SPI to handler DML --- */
				appendStringInfo(&strinfo, "UPDATE %s.%s SET ", dbzdml->db, dbzdml->table);
				foreach(cell, dbzdml->columnValuesAfter)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					char * data;

					appendStringInfo(&strinfo, "%s = ", colval->name);
					data = processDataByType(colval->value, colval->datatype, true, type);
					if (data != NULL)
					{
						appendStringInfo(&strinfo, "%s,", data);
						pfree(data);
					}
					else
					{
						appendStringInfo(&strinfo, "%s", "null");
					}
				}
				/* remove extra "," */
				strinfo.data[strinfo.len - 1] = '\0';
				strinfo.len = strinfo.len - 1;

				appendStringInfo(&strinfo,  " WHERE ");
				foreach(cell, dbzdml->columnValuesBefore)
				{
					DBZ_DML_COLUMN_VALUE * colval = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					char * data;

					appendStringInfo(&strinfo, "%s = ", colval->name);
					data = processDataByType(colval->value, colval->datatype, true, type);
					if (data != NULL)
					{
						appendStringInfo(&strinfo, "%s", data);
						pfree(data);
					}
					else
					{
						appendStringInfo(&strinfo, "%s", "null");
					}
					appendStringInfo(&strinfo, " AND ");
				}

				/* remove extra " AND " */
				strinfo.data[strinfo.len - 5] = '\0';
				strinfo.len = strinfo.len - 5;

				appendStringInfo(&strinfo, ";");
			}
			else
			{
				/* --- Convert to use Heap AM to handler DML --- */
				forboth(cell, dbzdml->columnValuesAfter, cell2, dbzdml->columnValuesBefore)
				{
					DBZ_DML_COLUMN_VALUE * colval_after = (DBZ_DML_COLUMN_VALUE *) lfirst(cell);
					DBZ_DML_COLUMN_VALUE * colval_before = (DBZ_DML_COLUMN_VALUE *) lfirst(cell2);
					PG_DML_COLUMN_VALUE * pgcolval_after = palloc0(sizeof(PG_DML_COLUMN_VALUE));
					PG_DML_COLUMN_VALUE * pgcolval_before = palloc0(sizeof(PG_DML_COLUMN_VALUE));

					char * data = processDataByType(colval_after->value, colval_after->datatype, false, type);

					if (data != NULL)
					{
						pgcolval_after->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval_after->value = pstrdup("NULL");

					pgcolval_after->datatype = colval_after->datatype;
					pgdml->columnValuesAfter = lappend(pgdml->columnValuesAfter, pgcolval_after);

					data = processDataByType(colval_before->value, colval_before->datatype, false, type);

					if (data != NULL)
					{
						pgcolval_before->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval_before->value = pstrdup("NULL");

					pgcolval_before->datatype = colval_before->datatype;
					pgdml->columnValuesBefore = lappend(pgdml->columnValuesBefore, pgcolval_before);
				}
			}
			break;
		}
		default:
		{
			elog(ERROR, "op %c not supported", dbzdml->op);
			destroyPGDML(pgdml);
			return NULL;
		}
	}

	pgdml->dmlquery = pstrdup(strinfo.data);

	/* free the data inside strinfo as we no longer needs it */
	pfree(strinfo.data);

	elog(WARNING, "pgdml->dmlquery %s", pgdml->dmlquery);
	return pgdml;
}

static DBZ_DML *
parseDBZDML(Jsonb * jb, char op, ConnectorType type)
{
	StringInfoData strinfo;
	Jsonb * dmlpayload = NULL;
	JsonbIterator *it;
	JsonbValue v;
	JsonbIteratorToken r;
	char * key = NULL;
	char * value = NULL;
	DBZ_DML * dbzdml = NULL;
	DBZ_DML_COLUMN_VALUE * colval = NULL;
	Oid schemaoid;
	Relation rel;
	TupleDesc tupdesc;
	int attnum, j = 0;

	HTAB * typeidhash;
	HASHCTL hash_ctl;
	NameOidEntry * entry;
	bool found;

	initStringInfo(&strinfo);

	dbzdml = (DBZ_DML *) palloc0(sizeof(DBZ_DML));

	getPathElementString(jb, "payload.source.db", &strinfo);
	dbzdml->db = pstrdup(strinfo.data);

	getPathElementString(jb, "payload.source.table", &strinfo);
	dbzdml->table = pstrdup(strinfo.data);

	if (!dbzdml->db || !dbzdml->table)
	{
		elog(WARNING, "no db or table specified in DML response");
		destroyDBZDML(dbzdml);

		if(strinfo.data)
			pfree(strinfo.data);

		return NULL;
	}

	dbzdml->op = op;
	/*
	 * before parsing, we need to make sure the target namespace and table
	 * do exist in PostgreSQL, and also fetch their attribute type IDs. PG
	 * automatically converts upper case letters to lower when they are
	 * created. However, catalog lookups are case sensitive so here we must
	 * convert db and table to all lower case letters.
	 */
	for (j = 0; j < strlen(dbzdml->db); j++)
		dbzdml->db[j] = (char) pg_tolower((unsigned char) dbzdml->db[j]);

	for (j = 0; j < strlen(dbzdml->table); j++)
		dbzdml->table[j] = (char) pg_tolower((unsigned char) dbzdml->table[j]);

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	schemaoid = get_namespace_oid(dbzdml->db, false);
	if (!OidIsValid(schemaoid))
	{
		char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
		snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for schema '%s'", dbzdml->db);
		set_shm_connector_errmsg(type, msg);

		/* trigger pg's error shutdown routine */
		elog(ERROR, "%s", msg);
	}

	dbzdml->tableoid = get_relname_relid(dbzdml->table, schemaoid);
	if (!OidIsValid(dbzdml->tableoid))
	{
		char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
		snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for table '%s'", dbzdml->table);
		set_shm_connector_errmsg(type, msg);

		/* trigger pg's error shutdown routine */
		elog(ERROR, "%s", msg);
	}

	elog(WARNING, "namespace %s.%s has PostgreSQL OID %d", dbzdml->db, dbzdml->table, dbzdml->tableoid);

	/* prepare a temporary hash table for datatype look up with column name */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = NAMEDATALEN;
	hash_ctl.entrysize = sizeof(NameOidEntry);
	hash_ctl.hcxt = CurrentMemoryContext;

	typeidhash = hash_create("Name to OID Hash Table",
							 512, // limit to 512 columns max
							 &hash_ctl,
							 HASH_ELEM | HASH_CONTEXT);

	/*
	 * get the column data type IDs for all columns from PostgreSQL catalog
	 * The type IDs are stored in typeidhash temporarily for the parser
	 * below to look up
	 */
	rel = table_open(dbzdml->tableoid, NoLock);
	tupdesc = RelationGetDescr(rel);

	for (attnum = 1; attnum <= tupdesc->natts; attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
		elog(DEBUG2, "column %d: name %s, type %u, length %d",
				attnum,
				NameStr(attr->attname),
				attr->atttypid,
				attr->attlen);

		entry = (NameOidEntry *) hash_search(typeidhash, NameStr(attr->attname), HASH_ENTER, &found);
		if (!found)
		{
			strncpy(entry->name, NameStr(attr->attname), NAMEDATALEN);
			entry->oid = attr->atttypid;
			entry->position = attnum;
			elog(DEBUG2, "Inserted name '%s' with OID %u and position %d", entry->name, entry->oid, entry->position);
		}
		else
		{
			elog(DEBUG2, "Name '%s' already exists with OID %u and position %d", entry->name, entry->oid, entry->position);
		}

	}
	table_close(rel, NoLock);

	PopActiveSnapshot();
	CommitTransactionCommand();

	switch(op)
	{
		case 'c':	/* create: data created after initial sync (INSERT) */
		case 'r':	/* read: initial data read */
		{
			/* sample payload:
			 * "payload": {
			 * 		"before": null,
			 * 		"after" : {
			 * 			"order_number": 10001,
			 * 			"order_date": 16816,
			 * 			"purchaser": 1001,
			 * 			"quantity": 1,
			 * 			"product_id": 102
			 * 		}
			 * 	}
			 *
			 * 	This parser expects the payload to contain only scalar values. In some special
			 * 	cases like geometry column type, the payload could contain sub element like:
			 * 	"after" : {
			 * 		"id"; 1,
			 * 		"g": {
			 * 			"wkb": "AQIAAAACAAAAAAAAAAAAAEAAAAAAAADwPwAAAAAAABhAAAAAAAAAGEA=",
			 * 			"srid": null
			 * 		},
			 * 		"h": null
			 * 	}
			 * 	in this case, the parser will parse the entire sub element as string under the key "g"
			 * 	in the above example.
			 */
			dmlpayload = getPathElementJsonb(jb, "payload.after");
			if (dmlpayload)
			{
				int pause = 0;
				it = JsonbIteratorInit(&dmlpayload->root);
				while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
				{
					switch (r)
					{
						case WJB_BEGIN_OBJECT:
							elog(WARNING, "start of object (%s) --------------------", key ? key : "null");

							if (key != NULL)
							{
								elog(WARNING, "sub element detected, skip subsequent parsing");
								pause = 1;
							}
							break;
						case WJB_END_OBJECT:
							if (pause)
							{
								elog(WARNING, "sub element ended, resume parsing operation");
								pause = 0;
								if (key)
								{
									int pathsize = strlen("payload.after.") + strlen(key) + 1;
									char * tmpPath = (char *) palloc0 (pathsize);

									elog(WARNING, "parse the entire sub element under %s as string", key);

									snprintf(tmpPath, pathsize, "payload.after.%s", key);
									getPathElementString(jb, tmpPath, &strinfo);
									value = pstrdup(strinfo.data);
									if(tmpPath)
										pfree(tmpPath);
								}
							}
							elog(WARNING, "end of object (%s) --------------------", key ? key : "null");
							break;
						case WJB_BEGIN_ARRAY:
							elog(WARNING, "start of array (%s) --- array type not expected or handled yet",
									key ? key : "null");
							if (key)
							{
								pfree(key);
								key = NULL;
							}
							break;
						case WJB_END_ARRAY:
							elog(WARNING, "end of array (%s) --- array type not expected or handled yet",
																key ? key : "null");
							break;
						case WJB_KEY:
							if (pause)
								break;

							key = pnstrdup(v.val.string.val, v.val.string.len);
							elog(DEBUG2, "Key: %s", key);
							break;
						case WJB_VALUE:
						case WJB_ELEM:
							if (pause)
								break;
							switch (v.type)
							{
								case jbvNull:
									elog(DEBUG2, "Value: NULL");
									value = pnstrdup("NULL", strlen("NULL"));
									break;
								case jbvString:
									value = pnstrdup(v.val.string.val, v.val.string.len);
									elog(DEBUG2, "String Value: %s", value);
									break;
								case jbvNumeric:
								{
									value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
									elog(DEBUG2, "Numeric Value: %s", value);
									break;
								}
								case jbvBool:
									elog(DEBUG2, "Boolean Value: %s", v.val.boolean ? "true" : "false");
									if (v.val.boolean)
										value = pnstrdup("true", strlen("true"));
									else
										value = pnstrdup("false", strlen("false"));
									break;
								case jbvBinary:
									elog(WARNING, "Binary Value: not handled yet");
									value = pnstrdup("NULL", strlen("NULL"));
									break;
								default:
									elog(WARNING, "Unknown value type: %d", v.type);
									value = pnstrdup("NULL", strlen("NULL"));
									break;
							}
						break;
						default:
							elog(WARNING, "Unknown token: %d", r);
							break;
					}

					/* check if we have a key - value pair */
					if (key != NULL && value != NULL)
					{
						colval = (DBZ_DML_COLUMN_VALUE *) palloc0(sizeof(DBZ_DML_COLUMN_VALUE));
						colval->name = pstrdup(key);
						colval->value = pstrdup(value);

						/* look up its data type */
						entry = (NameOidEntry *) hash_search(typeidhash, colval->name, HASH_FIND, &found);
						if (found)
						{
							colval->datatype = entry->oid;
							colval->position = entry->position;
						}
						else
							elog(WARNING, "cannot find data type for column %s. None-existent column?", colval->name);

						elog(WARNING, "consumed %s = %s, type %d", colval->name, colval->value, colval->datatype);
						dbzdml->columnValuesAfter = lappend(dbzdml->columnValuesAfter, colval);

						pfree(key);
						pfree(value);
						key = NULL;
						value = NULL;
					}
				}
			}
			break;
		}
		case 'd':	/* delete: data deleted after initial sync (DELETE) */
		{
			/* sample payload:
			 * "payload": {
			 * 		"before" : {
			 * 			"id": 1015,
			 * 			"first_name": "first",
			 * 			"last_name": "last",
			 * 			"email": "abc@mail.com"
			 * 		},
			 * 		"after": null
			 * 	}
			 */
			dmlpayload = getPathElementJsonb(jb, "payload.before");
			if (dmlpayload)
			{
				int pause = 0;
				it = JsonbIteratorInit(&dmlpayload->root);
				while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
				{
					switch (r)
					{
						case WJB_BEGIN_OBJECT:
							elog(WARNING, "start of object (%s) --------------------", key ? key : "null");

							if (key != NULL)
							{
								elog(WARNING, "sub element detected, skip subsequent parsing");
								pause = 1;
							}
							break;
						case WJB_END_OBJECT:
							if (pause)
							{
								elog(WARNING, "sub element ended, resume parsing operation");
								pause = 0;
								if (key)
								{
									int pathsize = strlen("payload.before.") + strlen(key) + 1;
									char * tmpPath = (char *) palloc0 (pathsize);

									elog(WARNING, "parse the entire sub element under %s as string", key);

									snprintf(tmpPath, pathsize, "payload.before.%s", key);
									getPathElementString(jb, tmpPath, &strinfo);
									value = pstrdup(strinfo.data);
									if(tmpPath)
										pfree(tmpPath);
								}
							}
							elog(WARNING, "end of object (%s) --------------------", key ? key : "null");
							break;
						case WJB_BEGIN_ARRAY:
							elog(WARNING, "start of array (%s) --- array type not expected or handled yet",
									key ? key : "null");
							if (key)
							{
								pfree(key);
								key = NULL;
							}
							break;
						case WJB_END_ARRAY:
							elog(WARNING, "end of array (%s) --- array type not expected or handled yet",
																key ? key : "null");
							break;
						case WJB_KEY:
							if (pause)
								break;

							key = pnstrdup(v.val.string.val, v.val.string.len);
							elog(DEBUG2, "Key: %s", key);
							break;
						case WJB_VALUE:
						case WJB_ELEM:
							if (pause)
								break;
							switch (v.type)
							{
								case jbvNull:
									elog(DEBUG2, "Value: NULL");
									value = pnstrdup("NULL", strlen("NULL"));
									break;
								case jbvString:
									value = pnstrdup(v.val.string.val, v.val.string.len);
									elog(DEBUG2, "String Value: %s", value);
									break;
								case jbvNumeric:
								{
									value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
									elog(DEBUG2, "Numeric Value: %s", value);
									break;
								}
								case jbvBool:
									elog(DEBUG2, "Boolean Value: %s", v.val.boolean ? "true" : "false");
									if (v.val.boolean)
										value = pnstrdup("true", strlen("true"));
									else
										value = pnstrdup("false", strlen("false"));
									break;
								case jbvBinary:
									elog(WARNING, "Binary Value: not handled yet");
									value = pnstrdup("NULL", strlen("NULL"));
									break;
								default:
									elog(WARNING, "Unknown value type: %d", v.type);
									value = pnstrdup("NULL", strlen("NULL"));
									break;
							}
						break;
						default:
							elog(WARNING, "Unknown token: %d", r);
							break;
					}

					/* check if we have a key - value pair */
					if (key != NULL && value != NULL)
					{
						colval = (DBZ_DML_COLUMN_VALUE *) palloc0(sizeof(DBZ_DML_COLUMN_VALUE));
						colval->name = pstrdup(key);
						colval->value = pstrdup(value);
						/* look up its data type */
						entry = (NameOidEntry *) hash_search(typeidhash, colval->name, HASH_FIND, &found);
						if (found)
						{
							colval->datatype = entry->oid;
							colval->position = entry->position;
						}
						else
							elog(ERROR, "cannot find data type for column %s. None-existent column?", colval->name);

						elog(WARNING, "consumed %s = %s, type %d", colval->name, colval->value, colval->datatype);
						dbzdml->columnValuesBefore = lappend(dbzdml->columnValuesBefore, colval);

						pfree(key);
						pfree(value);
						key = NULL;
						value = NULL;
					}
				}
			}
			break;
		}
		case 'u':	/* update: data updated after initial sync (UPDATE) */
		{
			/* sample payload:
			 * "payload": {
			 * 		"before" : {
			 * 			"order_number": 10006,
			 * 			"order_date": 17449,
			 * 			"purchaser": 1003,
			 * 			"quantity": 5,
			 * 			"product_id": 107
			 * 		},
			 * 		"after": {
			 * 			"order_number": 10006,
			 * 			"order_date": 17449,
			 * 			"purchaser": 1004,
			 * 			"quantity": 5,
			 * 			"product_id": 107
			 * 		}
			 * 	}
			 */
			int i = 0;
			for (i = 0; i < 2; i++)
			{
				/* need to parse before and after */
				if (i == 0)
					dmlpayload = getPathElementJsonb(jb, "payload.before");
				else
					dmlpayload = getPathElementJsonb(jb, "payload.after");
				if (dmlpayload)
				{
					int pause = 0;
					it = JsonbIteratorInit(&dmlpayload->root);
					while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
					{
						switch (r)
						{
							case WJB_BEGIN_OBJECT:
								elog(WARNING, "start of object (%s) --------------------", key ? key : "null");

								if (key != NULL)
								{
									elog(WARNING, "sub element detected, skip subsequent parsing");
									pause = 1;
								}
								break;
							case WJB_END_OBJECT:
								if (pause)
								{
									elog(WARNING, "sub element ended, resume parsing operation");
									pause = 0;
									if (key)
									{
										int pathsize = (i == 0 ? strlen("payload.before.") + strlen(key) + 1 :
												strlen("payload.after.") + strlen(key) + 1);
										char * tmpPath = (char *) palloc0 (pathsize);

										elog(WARNING, "parse the entire sub element under %s as string", key);
										if (i == 0)
											snprintf(tmpPath, pathsize, "payload.before.%s", key);
										else
											snprintf(tmpPath, pathsize, "payload.after.%s", key);
										getPathElementString(jb, tmpPath, &strinfo);
										value = pstrdup(strinfo.data);
										if(tmpPath)
											pfree(tmpPath);
									}
								}
								elog(WARNING, "end of object (%s) --------------------", key ? key : "null");
								break;
							case WJB_BEGIN_ARRAY:
								elog(WARNING, "start of array (%s) --- array type not expected or handled yet",
										key ? key : "null");
								if (key)
								{
									pfree(key);
									key = NULL;
								}
								break;
							case WJB_END_ARRAY:
								elog(WARNING, "end of array (%s) --- array type not expected or handled yet",
																	key ? key : "null");
								break;
							case WJB_KEY:
								if (pause)
									break;

								key = pnstrdup(v.val.string.val, v.val.string.len);
								elog(DEBUG2, "Key: %s", key);
								break;
							case WJB_VALUE:
							case WJB_ELEM:
								if (pause)
									break;
								switch (v.type)
								{
									case jbvNull:
										elog(DEBUG2, "Value: NULL");
										value = pnstrdup("NULL", strlen("NULL"));
										break;
									case jbvString:
										value = pnstrdup(v.val.string.val, v.val.string.len);
										elog(DEBUG2, "String Value: %s", value);
										break;
									case jbvNumeric:
									{
										value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
										elog(DEBUG2, "Numeric Value: %s", value);
										break;
									}
									case jbvBool:
										elog(DEBUG2, "Boolean Value: %s", v.val.boolean ? "true" : "false");
										if (v.val.boolean)
											value = pnstrdup("true", strlen("true"));
										else
											value = pnstrdup("false", strlen("false"));
										break;
									case jbvBinary:
										elog(WARNING, "Binary Value: not handled yet");
										value = pnstrdup("NULL", strlen("NULL"));
										break;
									default:
										elog(WARNING, "Unknown value type: %d", v.type);
										value = pnstrdup("NULL", strlen("NULL"));
										break;
								}
							break;
							default:
								elog(WARNING, "Unknown token: %d", r);
								break;
						}

						/* check if we have a key - value pair */
						if (key != NULL && value != NULL)
						{
							colval = (DBZ_DML_COLUMN_VALUE *) palloc0(sizeof(DBZ_DML_COLUMN_VALUE));
							colval->name = pstrdup(key);
							colval->value = pstrdup(value);
							/* look up its data type */
							entry = (NameOidEntry *) hash_search(typeidhash, colval->name, HASH_FIND, &found);
							if (found)
							{
								colval->datatype = entry->oid;
								colval->position = entry->position;
							}
							else
								elog(ERROR, "cannot find data type for column %s. None-existent column?", colval->name);

							elog(WARNING, "consumed %s = %s, type %d", colval->name, colval->value, colval->datatype);
							if (i == 0)
								dbzdml->columnValuesBefore = lappend(dbzdml->columnValuesBefore, colval);
							else
								dbzdml->columnValuesAfter = lappend(dbzdml->columnValuesAfter, colval);

							pfree(key);
							pfree(value);
							key = NULL;
							value = NULL;
						}
					}
				}
			}
			break;
		}
		default:
		{
			elog(WARNING, "op %c not supported", op);
			if(strinfo.data)
				pfree(strinfo.data);

			destroyDBZDML(dbzdml);
			return NULL;
		}
	}

	/*
	 * finally, we need to sort dbzdml->columnValuesBefore and dbzdml->columnValuesAfter
	 * based on position to align with PostgreSQL's attnum
	 */
	if (dbzdml->columnValuesBefore != NULL)
		list_sort(dbzdml->columnValuesBefore, list_sort_cmp);

	if (dbzdml->columnValuesAfter != NULL)
		list_sort(dbzdml->columnValuesAfter, list_sort_cmp);

	if (strinfo.data)
		pfree(strinfo.data);

	return dbzdml;
}

ConnectorType
fc_get_connector_type(const char * connector)
{
	if (!strcasecmp(connector, "mysql"))
	{
		return TYPE_MYSQL;
	}
	else if (!strcasecmp(connector, "oracle"))
	{
		return TYPE_ORACLE;
	}
	else if (!strcasecmp(connector, "sqlserver"))
	{
		return TYPE_SQLSERVER;
	}
	/* todo: support more dbz connector types here */
	else
	{
		return TYPE_UNDEF;
	}
}

/* Main function to process Debezium change event */
int
fc_processDBZChangeEvent(const char * event)
{
	Datum jsonb_datum;
	Jsonb *jb;
	StringInfoData strinfo;
	ConnectorType type;

	initStringInfo(&strinfo);

    /* Convert event string to JSONB */
    jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(event));
    jb = DatumGetJsonbP(jsonb_datum);

    /* Get connector type */
    getPathElementString(jb, "payload.source.connector", &strinfo);

    type = fc_get_connector_type(strinfo.data);

    /* Check if it's a DDL or DML event */
    getPathElementString(jb, "payload.ddl", &strinfo);
    getPathElementString(jb, "payload.op", &strinfo);

    if (!strcmp(strinfo.data, "NULL"))
    {
        /* Process DDL event */
    	DBZ_DDL * dbzddl = NULL;
    	PG_DDL * pgddl = NULL;

    	/* (1) parse */
    	elog(WARNING, "parsing DBZ DDL change event...");
    	set_shm_connector_state(type, STATE_PARSING);
    	dbzddl = parseDBZDDL(jb);
    	if (!dbzddl)
    	{
    		elog(WARNING, "malformed DDL event");
    		set_shm_connector_state(type, STATE_SYNCING);
    		return -1;
    	}

    	elog(WARNING, "converting to PG DDL change event...");
    	/* (2) convert */
    	set_shm_connector_state(type, STATE_CONVERTING);
    	pgddl = convert2PGDDL(dbzddl, type);
    	if (!pgddl)
    	{
    		elog(WARNING, "failed to convert DBZ DDL to PG DDL change event");
    		set_shm_connector_state(type, STATE_SYNCING);
    		destroyDBZDDL(dbzddl);
    		return -1;
    	}

    	/* (3) execute */
    	elog(WARNING, "executing PG DDL change event...");
    	set_shm_connector_state(type, STATE_EXECUTING);
    	if(ra_executePGDDL(pgddl, type))
    	{
    		elog(WARNING, "failed to execute PG DDL change event");
    		set_shm_connector_state(type, STATE_SYNCING);
    		destroyDBZDDL(dbzddl);
    		destroyPGDDL(pgddl);
    		return -1;
    	}

    	/* (4) update offset */
       	set_shm_dbz_offset(type);

    	/* (5) clean up */
    	set_shm_connector_state(type, STATE_SYNCING);
    	elog(WARNING, "execution completed. Clean up...");
    	destroyDBZDDL(dbzddl);
    	destroyPGDDL(pgddl);
    }
    else
    {
        /* Process DML event */
    	DBZ_DML * dbzdml = NULL;
    	PG_DML * pgdml = NULL;

    	elog(WARNING, "this is DML change event");

    	/* (1) parse */
    	set_shm_connector_state(type, STATE_PARSING);
    	dbzdml = parseDBZDML(jb, strinfo.data[0], type);
    	if (!dbzdml)
		{
			elog(WARNING, "malformed DNL event");
			set_shm_connector_state(type, STATE_SYNCING);
			return -1;
		}

    	/* (2) convert */
    	set_shm_connector_state(type, STATE_CONVERTING);
    	pgdml = convert2PGDML(dbzdml, type);
    	if (!pgdml)
    	{
    		elog(WARNING, "failed to convert DBZ DML to PG DML change event");
    		set_shm_connector_state(type, STATE_SYNCING);
    		destroyDBZDML(dbzdml);
    		return -1;
    	}

    	/* (3) execute */
    	set_shm_connector_state(type, STATE_EXECUTING);
    	elog(WARNING, "executing PG DML change event...");
    	if(ra_executePGDML(pgdml, type))
    	{
    		elog(WARNING, "failed to execute PG DML change event");
    		set_shm_connector_state(type, STATE_SYNCING);
        	destroyDBZDML(dbzdml);
        	destroyPGDML(pgdml);
    		return -1;
    	}

    	/* (4) update offset */
       	set_shm_dbz_offset(type);

       	/* (5) clean up */
    	set_shm_connector_state(type, STATE_SYNCING);
    	elog(WARNING, "execution completed. Clean up...");
    	destroyDBZDML(dbzdml);
    	destroyPGDML(pgdml);
    }
	return 0;
}
