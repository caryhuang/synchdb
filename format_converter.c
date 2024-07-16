/*
 * format_converter.c
 *
 *  Created on: Jul. 11, 2024
 *      Author: caryh
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"
#include "format_converter.h"
#include "catalog/pg_type.h"

static void remove_double_quotes(StringInfoData * str)
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

static int getPathElementString(Jsonb * jb, char * path, StringInfoData * strinfoout)
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

		/* note: buf.data includes double quotes and escape char \.
		 * We need to remove them
		 */
		remove_double_quotes(strinfoout);
		elog(WARNING, "%s = %s", path, strinfoout->data);
    }

	pfree(datum_elems);
	pfree(pathcopy);
	return 0;
}

static Jsonb * getPathElementJsonb(Jsonb * jb, char * path)
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

static void destroyDBZDDL(DBZ_DDL * ddlinfo)
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

static void destroyPGDDL(PG_DDL * ddlinfo)
{
	if (ddlinfo)
	{
		if (ddlinfo->ddlquery)
			pfree(ddlinfo->ddlquery);
		pfree(ddlinfo);
	}
}

static DBZ_DDL * parseDBZDDL(Jsonb * jb)
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

	/* todo: we only support parsing 1 set of DDL for now using hardcoded
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
//							strvalue = pnstrdup(v.val.string.val, v.val.string.len);
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
	return ddlinfo;
}

static PG_DDL * convert2PGDDL(DBZ_DDL * dbzddl)
{
	PG_DDL * pgddl = (PG_DDL*) palloc0(sizeof(PG_DDL));
	ListCell * cell;

	StringInfoData strinfo;

	initStringInfo(&strinfo);

	if (dbzddl->id != NULL && strlen(dbzddl->id) > 0)
	{
		/* todo: dbzddl->id is expressed in database.table format and for now
		 * we automatically map it to schema.table within current database
		 * (whatever it is). So here we extract the database part from dbzddl->id
		 * and try to create a schema if not exist. We should make this behavior
		 * configurable in the future
		 */
		char * tmp = pstrdup(dbzddl->id);
		char * schema = strtok(tmp, ".");
		appendStringInfo(&strinfo, "CREATE SCHEMA IF NOT EXISTS %s; ", schema);
		pfree(tmp);

		/* if id is valid, we treat it as CREATE TABLE */
		appendStringInfo(&strinfo, "CREATE TABLE IF NOT EXISTS %s (", dbzddl->id);

		foreach(cell, dbzddl->columns)
		{
			DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);

			/*
			 * todo: column data type conversion cases:
			 *  - if type is INT and autoincremented is true, translate to SERIAL
			 *  - if type is BIGINT and autoincremented is true, translate to BIGSERIAL
			 *  - if type is SMALLINT and autoincremented is true, translate to SMALLSERIAL
			 *  - if type is ENUM, translate to TEXT with length=0
			 *  - if type is GEOMETRY, translate to TEXT
			 */
			if (!strcmp(col->typeName, "INT") && col->autoIncremented)
				appendStringInfo(&strinfo, " %s %s ", col->name, "SERIAL");
			else if (!strcmp(col->typeName, "BIGINT") && col->autoIncremented)
				appendStringInfo(&strinfo, " %s %s ", col->name, "BIGSERIAL");
			else if (!strcmp(col->typeName, "SMALLINT") && col->autoIncremented)
				appendStringInfo(&strinfo, " %s %s ", col->name, "SMALLSERIAL");
			else if (!strcmp(col->typeName, "ENUM"))
			{
				appendStringInfo(&strinfo, " %s %s ", col->name, "TEXT");
				col->length = 0;	/* this prevents adding a fixed size for TEXT */
			}
			else if (!strcmp(col->typeName, "GEOMETRY"))
				appendStringInfo(&strinfo, " %s %s ", col->name, "TEXT");
			else
				appendStringInfo(&strinfo, " %s %s ", col->name, col->typeName);

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

	pgddl->ddlquery = pstrdup(strinfo.data);

	/* free the data inside strinfo as we no longer needs it */
	pfree(strinfo.data);

	elog(WARNING, "pgsql: %s ", pgddl->ddlquery);
	return pgddl;
}

static int parseDMLEvents()
{
	return 0;
}

int fc_processDBZChangeEvent(const char * event)
{
	Datum jsonb_datum;
	Jsonb *jb;
	StringInfoData strinfo;

	initStringInfo(&strinfo);

    jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(event));
    jb = DatumGetJsonbP(jsonb_datum);

    getPathElementString(jb, "payload.source.db", &strinfo);
    getPathElementString(jb, "payload.source.connector", &strinfo);
    getPathElementString(jb, "payload.source.file", &strinfo);
    getPathElementString(jb, "payload.source.pos", &strinfo);
    getPathElementString(jb, "payload.ddl", &strinfo);
    getPathElementString(jb, "payload.tableChanges", &strinfo);
    getPathElementString(jb, "payload.op", &strinfo);

    if (!strcmp(strinfo.data, "NULL"))
    {
    	DBZ_DDL * dbzddl = NULL;
    	PG_DDL * pgddl = NULL;

    	/* (1) parse */
    	elog(WARNING, "parsing DBZ DDL change event...");
    	dbzddl = parseDBZDDL(jb);
    	if (!dbzddl)
    	{
    		elog(WARNING, "malformed DDL event");
    		return -1;
    	}
    	elog(WARNING, "converting to PG DDL change event...");

    	/* (2) convert */
    	pgddl = convert2PGDDL(dbzddl);
    	if (!pgddl)
    	{
    		elog(WARNING, "failed to convert DBZ DDL to PG DDL change event");
    		destroyDBZDDL(dbzddl);
    		return -1;
    	}

    	/* (3) execute */
    	elog(WARNING, "executing PG DDL change event...");
    	if(ra_executePGDDL(pgddl))
    	{
    		elog(WARNING, "failed to execute PG DDL change event");
    		destroyDBZDDL(dbzddl);
    		destroyPGDDL(pgddl);
    		return -1;
    	}

    	/* (4) clean up */
    	elog(WARNING, "execution completed. Clean up...");
    	destroyDBZDDL(dbzddl);
    	destroyPGDDL(pgddl);
    }
    else
    {
    	elog(WARNING, "this is DML change event");
    	parseDMLEvents();
    }

	return 0;
}
