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
#include "common/base64.h"
#include "port/pg_bswap.h"

extern bool synchdb_dml_use_spi;
extern int myConnectorId;

static HTAB * mysqlDatatypeHash;
static HTAB * sqlserverDatatypeHash;

DatatypeHashEntry mysql_defaultTypeMappings[] =
{
	{{"INT", true}, "SERIAL", 0},
	{{"BIGINT", true}, "BIGSERIAL", 0},
	{{"SMALLINT", true}, "SMALLSERIAL", 0},
	{{"MEDIUMINT", true}, "SERIAL", 0},
	{{"ENUM", false}, "TEXT", 0},
	{{"SET", false}, "TEXT", 0},
	{{"BIGINT", false}, "BIGINT", 0},
	{{"BIGINT UNSIGNED", false}, "NUMERIC", -1},
	{{"NUMERIC UNSIGNED", false}, "NUMERIC", -1},
	{{"DEC", false}, "DECIMAL", -1},
	{{"DEC UNSIGNED", false}, "DECIMAL", -1},
	{{"DECIMAL UNSIGNED", false}, "DECIMAL", -1},
	{{"FIXED", false}, "DECIMAL", -1},
	{{"FIXED UNSIGNED", false}, "DECIMAL", -1},
	{{"BIT(1)", false}, "BOOLEAN", 0},
	{{"BIT", false}, "BIT", -1},
	{{"BOOL", false}, "BOOLEAN", -1},
	{{"DOUBLE", false}, "DOUBLE PRECISION", 0},
	{{"DOUBLE PRECISION", false}, "DOUBLE PRECISION", 0},
	{{"DOUBLE PRECISION UNSIGNED", false}, "DOUBLE PRECISION", 0},
	{{"DOUBLE UNSIGNED", false}, "DOUBLE PRECISION", 0},
	{{"REAL", false}, "REAL", 0},
	{{"REAL UNSIGNED", false}, "REAL", 0},
	{{"FLOAT", false}, "REAL", 0},
	{{"FLOAT UNSIGNED", false}, "REAL", 0},
	{{"INT", false}, "INT", 0},
	{{"INT UNSIGNED", false}, "BIGINT", 0},
	{{"INTEGER", false}, "INT", 0},
	{{"INTEGER UNSIGNED", false}, "BIGINT", 0},
	{{"MEDIUMINT", false}, "INT", 0},
	{{"MEDIUMINT UNSIGNED", false}, "INT", 0},
	{{"YEAR", false}, "INT", 0},
	{{"SMALLINT", false}, "SMALLINT", 0},
	{{"SMALLINT UNSIGNED", false}, "INT", 0},
	{{"TINYINT", false}, "SMALLINT", 0},
	{{"TINYINT UNSIGNED", false}, "SMALLINT", 0},
	{{"DATETIME", false}, "TIMESTAMP", -1},
	{{"TIMESTAMP", false}, "TIMESTAMPTZ", -1},
	{{"BINARY", false}, "BYTEA", 0},
	{{"VARBINARY", false}, "BYTEA", 0},
	{{"BLOB", false}, "BYTEA", 0},
	{{"MEDIUMBLOB", false}, "BYTEA", 0},
	{{"LONGBLOB", false}, "BYTEA", 0},
	{{"TINYBLOB", false}, "BYTEA", 0},
	{{"LONG VARCHAR", false}, "TEXT", -1},
	{{"LONGTEXT", false}, "TEXT", -1},
	{{"MEDIUMTEXT", false}, "TEXT", -1},
	{{"TINYTEXT", false}, "TEXT", -1},
	{{"JSON", false}, "JSONB", -1},
	/* spatial types - map to TEXT by default */
	{{"GEOMETRY", false}, "TEXT", -1},
	{{"GEOMETRYCOLLECTION", false}, "TEXT", -1},
	{{"GEOMCOLLECTION", false}, "TEXT", -1},
	{{"LINESTRING", false}, "TEXT", -1},
	{{"MULTILINESTRING", false}, "TEXT", -1},
	{{"MULTIPOINT", false}, "TEXT", -1},
	{{"MULTIPOLYGON", false}, "TEXT", -1},
	{{"POINT", false}, "TEXT", -1},
	{{"POLYGON", false}, "TEXT", -1}
};
#define SIZE_MYSQL_DATATYPE_MAPPING (sizeof(mysql_defaultTypeMappings) / sizeof(DatatypeHashEntry))

DatatypeHashEntry sqlserver_defaultTypeMappings[] =
{
	{{"int identity", true}, "SERIAL", 0},
	{{"bigint identity", true}, "BIGSERIAL", 0},
	{{"smallint identity", true}, "SMALLSERIAL", 0},
	{{"enum", false}, "TEXT", 0},
	{{"int", false}, "INT", 0},
	{{"bigint", false}, "BIGINT", 0},
	{{"smallint", false}, "SMALLINT", 0},
	{{"tinyint", false}, "SMALLINT", 0},
	{{"numeric", false}, "NUMERIC", 0},
	{{"decimal", false}, "NUMERIC", 0},
	{{"bit(1)", false}, "BOOL", 0},
	{{"bit", false}, "BIT", 0},
	{{"money", false}, "MONEY", 0},
	{{"smallmoney", false}, "MONEY", 0},
	{{"real", false}, "REAL", 0},
	{{"float", false}, "REAL", 0},
	{{"date", false}, "DATE", 0},
	{{"time", false}, "TIME", 0},
	{{"datetime", false}, "TIMESTAMP", 0},
	{{"datetime2", false}, "TIMESTAMP", 0},
	{{"datetimeoffset", false}, "TIMESTAMPTZ", 0},
	{{"smalldatetime", false}, "TIMESTAMP", 0},
	{{"char", false}, "CHAR", 0},
	{{"varchar", false}, "VARCHAR", -1},
	{{"text", false}, "TEXT", 0},
	{{"nchar", false}, "CHAR", 0},
	{{"nvarchar", false}, "VARCHAR", -1},
	{{"ntext", false}, "TEXT", 0},
	{{"binary", false}, "BYTEA", 0},
	{{"varbinary", false}, "BYTEA", 0},
	{{"image", false}, "BYTEA", 0},
	{{"uniqueidentifier", false}, "UUID", 0},
	{{"xml", false}, "TEXT", 0},
	/* spatial types - map to TEXT by default */
	{{"geometry", false}, "TEXT", 0},
	{{"geography", false}, "TEXT", 0},
};

#define SIZE_SQLSERVER_DATATYPE_MAPPING (sizeof(sqlserver_defaultTypeMappings) / sizeof(DatatypeHashEntry))

static void
bytearray_to_escaped_string(const unsigned char *byte_array, size_t length, char *output_string)
{
	char *ptr = NULL;

	if (!output_string)
		return;

	strcpy(output_string, "'\\x");
	ptr = output_string + 3; /* Skip "'\\x" */

	for (size_t i = 0; i < length; i++)
	{
		sprintf(ptr, "%02X", byte_array[i]);
		ptr += 2;
	}

	// Close the string with a single quote
	strcat(ptr, "'");
}

static long
derive_value_from_byte(const unsigned char * bytes, int len)
{
	long value = 0;
	int i;

	/* Convert the byte array to an integer */
	for (i = 0; i < len; i++)
	{
		value = (value << 8) | bytes[i];
	}

	/*
	 * If the value is signed and the most significant bit (MSB) is set,
	 * sign-extend the value
	 */
	if ((bytes[0] & 0x80))
	{
		value |= -((long) 1 << (len * 8));
	}
	return value;
}

static void
reverse_byte_array(unsigned char * array, int length)
{
	size_t start = 0;
	size_t end = length - 1;
	while (start < end)
	{
		unsigned char temp = array[start];
		array[start] = array[end];
		array[end] = temp;
		start++;
		end--;
	}
}

static void
trim_leading_zeros(char *str)
{
	int i = 0, j = 0;
	while (str[i] == '0')
	{
		i++;
	}

	if (str[i] == '\0')
	{
		str[0] = '0';
		str[1] = '\0';
		return;
	}

	while (str[i] != '\0')
	{
		str[j++] = str[i++];
	}
	str[j] = '\0';
}

static void
prepend_zeros(char *str, int num_zeros)
{
    int original_len = strlen(str);
    int new_len = original_len + num_zeros;
    char * temp = palloc0(new_len + 1);

    for (int i = 0; i < num_zeros; i++)
    {
        temp[i] = '0';
    }

    for (int i = 0; i < original_len; i++)
    {
        temp[i + num_zeros] = str[i];
    }
    temp[new_len] = '\0';
    strcpy(str, temp);
    pfree(temp);
}


static void
byte_to_binary(unsigned char byte, char * binary_str)
{
	for (int i = 7; i >= 0; i--)
	{
		binary_str[7 - i] = (byte & (1 << i)) ? '1' : '0';
	}
	binary_str[8] = '\0';
}

static void
bytes_to_binary_string(const unsigned char * bytes, size_t len, char * binary_str)
{
	char byte_str[9];
	size_t i = 0;
	binary_str[0] = '\0';

	for (i = 0; i < len; i++)
	{
		byte_to_binary(bytes[i], byte_str);
		strcat(binary_str, byte_str);
	}
}

/* Function to find exact match from given line */
static bool
find_exact_string_match(char * line, char * wordtofind)
{
	char * p = strstr(line, wordtofind);
	if ((p == line) || (p != NULL && !isalnum((unsigned char)p[-1])))
	{
		p += strlen(wordtofind);
		if (!isalnum((unsigned char)*p))
			return true;
		else
			return false;
	}
	return false;
}

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

/*
 * this function constructs primary key clauses based on jsonin. jsonin
 * is expected to be a json array with string element, for example:
 * ["col1","col2"]
 */
static void
populate_primary_keys(StringInfoData * strinfo, const char * jsonin, bool alter)
{
	Datum jsonb_datum;
	Jsonb * jb;
	JsonbIterator *it;
	JsonbIteratorToken r;
	JsonbValue v;
	char * value = NULL;
	bool isfirst = true;

	jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(jsonin));
	jb = DatumGetJsonbP(jsonb_datum);

	it = JsonbIteratorInit(&jb->root);
	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		switch (r)
		{
			case WJB_BEGIN_ARRAY:
				break;
			case WJB_END_ARRAY:
				/*
				 * if at least one primary key is appended, we need to remove the last comma
				 * and close parenthesis
				 */
				if (!isfirst)
				{
					strinfo->data[strinfo->len - 1] = '\0';
					strinfo->len = strinfo->len - 1;
					appendStringInfo(strinfo, ")");
				}
				break;
			case WJB_VALUE:
			case WJB_ELEM:
			{
				switch(v.type)
				{
					case jbvString:
						value = pnstrdup(v.val.string.val, v.val.string.len);
						elog(DEBUG1, "primary key column: %s", value);
						if (alter)
						{
							if (isfirst)
							{
								appendStringInfo(strinfo, ", ADD PRIMARY KEY(");
								appendStringInfo(strinfo, "%s,", value);
								isfirst = false;
							}
							else
							{
								appendStringInfo(strinfo, "%s,", value);
							}
						}
						else
						{
							if (isfirst)
							{
								appendStringInfo(strinfo, ", PRIMARY KEY(");
								appendStringInfo(strinfo, "%s,", value);
								isfirst = false;
							}
							else
							{
								appendStringInfo(strinfo, "%s,", value);
							}
						}
						pfree(value);
						break;
					case jbvNull:
					case jbvNumeric:
					case jbvBool:
					case jbvBinary:
					default:
						elog(ERROR, "Unknown or unexpected value type: %d while "
								"parsing primaryKeyColumnNames", v.type);
						break;
				}
				break;
			}
			case WJB_BEGIN_OBJECT:
			case WJB_END_OBJECT:
			case WJB_KEY:
			default:
			{
				elog(ERROR, "Unknown or unexpected token: %d while "
						"parsing primaryKeyColumnNames", r);
				break;
			}
		}
	}
}
/* Function to get a string element from a JSONB path */
static int
getPathElementString(Jsonb * jb, char * path, StringInfoData * strinfoout, bool removequotes)
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
    	elog(DEBUG1, "%s = NULL", path);
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
		if (removequotes)
			remove_double_quotes(strinfoout);
		elog(DEBUG1, "%s = %s", path, strinfoout->data);
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
    getPathElementString(jb, "payload.tableChanges.0.id", &strinfo, true);
    ddlinfo->id = pstrdup(strinfo.data);

    getPathElementString(jb, "payload.tableChanges.0.table.primaryKeyColumnNames", &strinfo, false);
    ddlinfo->primaryKeyColumnNames = pstrdup(strinfo.data);

    getPathElementString(jb, "payload.tableChanges.0.type", &strinfo, true);
    ddlinfo->type = pstrdup(strinfo.data);

    /* free the data inside strinfo as we no longer needs it */
    pfree(strinfo.data);

    if (!strcmp(ddlinfo->id, "NULL") && !strcmp(ddlinfo->type, "NULL"))
    {
    	elog(DEBUG1, "no table change data. Stop parsing...");
    	destroyDBZDDL(ddlinfo);
    	return NULL;
    }

    if (!strcmp(ddlinfo->type, "CREATE") || !strcmp(ddlinfo->type, "ALTER"))
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
		 * columns arrat may contains another array of enumValues, this is ignored
		 * for now as enums are mapped to text as of now
		 *
		 *	   "enumValues":
		 *     [
         *         "'fish'",
         *         "'mammal'",
         *         "'bird'"
         *     ]
		 */
		if (ddlpayload)
		{
			int pause = 0;
			/* iterate this payload jsonb */
			it = JsonbIteratorInit(&ddlpayload->root);
			while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
			{
				switch (r)
				{
					case WJB_BEGIN_OBJECT:
						elog(DEBUG1, "parsing column --------------------");
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
						elog(DEBUG1, "Begin array under %s", key ? key : "NULL");
						if (key)
						{
							elog(DEBUG1, "sub array detected, skip it");
							pause = 1;
							pfree(key);
							key = NULL;
						}
						break;
					case WJB_END_ARRAY:
						elog(DEBUG1, "End array");
						if (pause)
						{
							elog(DEBUG1, "sub array ended, resume parsing operation");
							pause = 0;
						}
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
								elog(DEBUG2, "Binary Value: [binary data]");
								break;
							default:
								elog(DEBUG2, "Unknown value type: %d", v.type);
								break;
						}
					break;
					default:
						elog(DEBUG1, "Unknown token: %d", r);
						break;
				}

				/* check if we have a key - value pair */
				if (key != NULL && value != NULL)
				{
					if (!strcmp(key, "name"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->name = pstrdup(value);
					}
					if (!strcmp(key, "length"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->length = strcmp(value, "NULL") == 0 ? 0 : atoi(value);
					}
					if (!strcmp(key, "optional"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->optional = strcmp(value, "true") == 0 ? true : false;
					}
					if (!strcmp(key, "position"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->position = atoi(value);
					}
					if (!strcmp(key, "typeName"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->typeName = pstrdup(value);
					}
					if (!strcmp(key, "enumValues"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->enumValues = pstrdup(value);
					}
					if (!strcmp(key, "charsetName"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->charsetName = pstrdup(value);
					}
					if (!strcmp(key, "autoIncremented"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->autoIncremented = strcmp(value, "true") == 0 ? true : false;
					}
					if (!strcmp(key, "defaultValueExpression"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->defaultValueExpression = pstrdup(value);
					}
					if (!strcmp(key, "scale"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->scale = strcmp(value, "NULL") == 0 ? 0 : atoi(value);
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
			DatatypeHashEntry * entry;
			DatatypeHashKey key = {0};
			bool found = 0;

			/* special lookup case: BIT with length 1 */
			if (!strcasecmp(col->typeName, "BIT") && col->length == 1)
			{
				key.autoIncremented = col->autoIncremented;
				snprintf(key.extTypeName, sizeof(key.extTypeName), "%s(%d)",
						col->typeName, col->length);
			}
			else
			{
				key.autoIncremented = col->autoIncremented;
				strcpy(key.extTypeName, col->typeName);
			}

			entry = (DatatypeHashEntry *) hash_search(mysqlDatatypeHash, &key, HASH_FIND, &found);
			if (!found)
			{
				/* no mapping found, so no transformation done */
				elog(DEBUG1, "no transformation done for %s (autoincrement %d)",
						key.extTypeName, key.autoIncremented);
				appendStringInfo(strinfo, " %s %s ", col->name, col->typeName);
			}
			else
			{
				/* use the mapped values and sizes */
				elog(DEBUG1, "transform %s (autoincrement %d) to %s with length %d",
						key.extTypeName, key.autoIncremented, entry->pgsqlTypeName,
						entry->pgsqlTypeLength);
				appendStringInfo(strinfo, " %s %s ", col->name, entry->pgsqlTypeName);

				if (entry->pgsqlTypeLength != -1)
					col->length = entry->pgsqlTypeLength;
			}
			break;
		}
		case TYPE_ORACLE:
		{
			break;
		}
		case TYPE_SQLSERVER:
		{
			DatatypeHashEntry * entry;
			DatatypeHashKey key = {0};
			bool found = 0;

			/* special lookup case: BIT with length 1 */
			if (!strcasecmp(col->typeName, "bit") && col->length == 1)
			{
				key.autoIncremented = col->autoIncremented;
				snprintf(key.extTypeName, sizeof(key.extTypeName), "%s(%d)",
						col->typeName, col->length);
			}
			else
			{
				key.autoIncremented = col->autoIncremented;
				strcpy(key.extTypeName, col->typeName);
			}
			entry = (DatatypeHashEntry *) hash_search(sqlserverDatatypeHash, &key, HASH_FIND, &found);
			if (!found)
			{
				/* no mapping found, so no transformation done */
				elog(DEBUG1, "no transformation done for %s (autoincrement %d)",
						key.extTypeName, key.autoIncremented);
				appendStringInfo(strinfo, " %s %s ", col->name, col->typeName);
			}
			else
			{
				/* use the mapped values and sizes */
				elog(DEBUG1, "transform %s (autoincrement %d) to %s with length %d",
						key.extTypeName, key.autoIncremented, entry->pgsqlTypeName,
						entry->pgsqlTypeLength);
				appendStringInfo(strinfo, " %s %s ", col->name, entry->pgsqlTypeName);

				if (entry->pgsqlTypeLength != -1)
					col->length = entry->pgsqlTypeLength;

				/*
				 * special handling for sqlserver: the scale parameter for timestamp,
				 * and time date types are sent as "scale" not as "length" as in
				 * mysql case. So we need to use the scale value here
				 */
				if (col->scale > 0 && (find_exact_string_match(entry->pgsqlTypeName, "TIMESTAMP") ||
						find_exact_string_match(entry->pgsqlTypeName, "TIME") ||
						find_exact_string_match(entry->pgsqlTypeName, "TIMESTAMPTZ")))
				{
					/* postgresql can only support up to 6 */
					if (col->scale > 6)
						appendStringInfo(strinfo, "(6) ");
					else
						appendStringInfo(strinfo, "(%d) ", col->scale);
				}
			}
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
		char * db = NULL, * schema = NULL, * table = NULL;

		splitIdString(dbzddl->id, &db, &schema, &table);

		/* database and table must be present. schema is optional */
		if (!db || !table)
		{
			/* save the error */
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
					dbzddl->id);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		/* database mapped to schema */
		appendStringInfo(&strinfo, "CREATE SCHEMA IF NOT EXISTS %s; ", db);

		/* table stays as table, schema ignored */
		appendStringInfo(&strinfo, "CREATE TABLE IF NOT EXISTS %s.%s (", db, table);

		foreach(cell, dbzddl->columns)
		{
			DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);

			transformDDLColumns(col, type, &strinfo);

			/* if both length and scale are specified, add them. For example DECIMAL(10,2) */
			if (col->length > 0 && col->scale > 0)
			{
				appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
			}

			/* if a only length if specified, add it. For example VARCHAR(30)*/
			if (col->length > 0 && col->scale == 0)
			{
				/* make sure it does not exceed postgresql allowed maximum */
				if (col->length > 10485760)
					col->length = 10485760;
				appendStringInfo(&strinfo, "(%d) ", col->length);
			}

			/* if there is UNSIGNED operator found in col->typeName, add CHECK constraint */
			if (strstr(col->typeName, "UNSIGNED"))
			{
				appendStringInfo(&strinfo, "CHECK (%s >= 0) ", col->name);
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

		/*
		 * finally, declare primary keys if any. iterate dbzddl->primaryKeyColumnNames
		 * and build into primary key(x, y, z) clauses. todo
		 */
		populate_primary_keys(&strinfo, dbzddl->primaryKeyColumnNames, false);

		appendStringInfo(&strinfo, ");");
	}
	else if (!strcmp(dbzddl->type, "DROP"))
	{
		appendStringInfo(&strinfo, "DROP TABLE IF EXISTS %s;", dbzddl->id);
	}
	else if (!strcmp(dbzddl->type, "ALTER"))
	{
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
		char * db = NULL, * schema = NULL, * table = NULL;
		int i = 0, attnum;
		Oid schemaoid = -1;
		Oid tableoid = -1;
		bool found = false, altered = false;

		Relation rel;
		TupleDesc tupdesc;

		splitIdString(dbzddl->id, &db, &schema, &table);

		/* database and table must be present. schema is optional */
		if (!db || !table)
		{
			/* save the error */
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "malformed id field in dbz change event: %s",
					dbzddl->id);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		/*
		 * For ALTER, we must obtain the current schema in PostgreSQL and identify
		 * which column is the new column added. We will first check if table exists
		 * and then add its column to a temporary hash table that we can compare
		 * with the new column list.
		 */
		for (i = 0; i < strlen(db); i++)
			db[i] = (char) pg_tolower((unsigned char) db[i]);

		for (i = 0; i < strlen(table); i++)
			table[i] = (char) pg_tolower((unsigned char) table[i]);

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		schemaoid = get_namespace_oid(db, false);
		if (!OidIsValid(schemaoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for schema '%s'", db);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		tableoid = get_relname_relid(table, schemaoid);
		if (!OidIsValid(tableoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for table '%s'", table);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		elog(WARNING, "namespace %s.%s has PostgreSQL OID %d", db, table, tableoid);

		/* put PostgreSQL table column names into pgsqlColNameList */
		rel = table_open(tableoid, NoLock);
		tupdesc = RelationGetDescr(rel);
		table_close(rel, NoLock);

		PopActiveSnapshot();
		CommitTransactionCommand();

		appendStringInfo(&strinfo, "ALTER TABLE %s.%s ", db, table);

		if (list_length(dbzddl->columns) > tupdesc->natts)
		{
			elog(WARNING, "adding new column");
			altered = false;
			foreach(cell, dbzddl->columns)
			{
				DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
				found = false;
				for (attnum = 1; attnum <= tupdesc->natts; attnum++)
				{
					Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
					if (!strcasecmp(col->name, NameStr(attr->attname)))
					{
						found = true;
					}
				}
				if (!found)
				{
					elog(WARNING, "adding new column %s", col->name);
					altered = true;
					appendStringInfo(&strinfo, "ADD COLUMN");

					transformDDLColumns(col, type, &strinfo);

					/* if both length and scale are specified, add them. For example DECIMAL(10,2) */
					if (col->length > 0 && col->scale > 0)
					{
						appendStringInfo(&strinfo, "(%d, %d) ", col->length, col->scale);
					}

					/* if a only length if specified, add it. For example VARCHAR(30)*/
					if (col->length > 0 && col->scale == 0)
					{
						/* make sure it does not exceed postgresql allowed maximum */
						if (col->length > 10485760)
							col->length = 10485760;
						appendStringInfo(&strinfo, "(%d) ", col->length);
					}

					/* if there is UNSIGNED operator found in col->typeName, add CHECK constraint */
					if (strstr(col->typeName, "UNSIGNED"))
					{
						appendStringInfo(&strinfo, "CHECK (%s >= 0) ", col->name);
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
			}

			if (altered)
			{
				/* something has been altered, continue to wrap up... */
				/* remove the last extra comma */
				strinfo.data[strinfo.len - 1] = '\0';
				strinfo.len = strinfo.len - 1;

				/*
				 * finally, declare primary keys if any. iterate dbzddl->primaryKeyColumnNames
				 * and build into primary key(x, y, z) clauses. todo
				 */
				populate_primary_keys(&strinfo, dbzddl->primaryKeyColumnNames, true);
			}
			else
			{
				elog(WARNING, "no column altered");
				pfree(pgddl);
				return NULL;
			}
		}
		else
		{
			elog(WARNING, "dropping old column");
			altered = false;
			for (attnum = 1; attnum <= tupdesc->natts; attnum++)
			{
				Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
				found = false;

				/* skip special attname indicated a dropped column */
				if (strstr(NameStr(attr->attname), "pg.dropped"))
					continue;

				foreach(cell, dbzddl->columns)
				{
					DBZ_DDL_COLUMN * col = (DBZ_DDL_COLUMN *) lfirst(cell);
					if (!strcasecmp(col->name, NameStr(attr->attname)))
					{
						found = true;
					}
				}
				if (!found)
				{
					elog(WARNING, "dropping old column %s", NameStr(attr->attname));
					altered = true;
					appendStringInfo(&strinfo, "DROP COLUMN %s,", NameStr(attr->attname));
				}
			}
			if(altered)
			{
				/* something has been altered, continue to wrap up... */
				/* remove the last extra comma */
				strinfo.data[strinfo.len - 1] = '\0';
				strinfo.len = strinfo.len - 1;
			}
			else
			{
				elog(WARNING, "no column altered");
				pfree(pgddl);
				return NULL;
			}
		}
	}

	pgddl->ddlquery = pstrdup(strinfo.data);

	/* free the data inside strinfo as we no longer needs it */
	pfree(strinfo.data);

	elog(DEBUG1, "pgsql: %s ", pgddl->ddlquery);
	return pgddl;
}

/*
 * this function performs necessary data conversions to convert input data
 * as string and output a processed string based on type
 */
static char *
processDataByType(DBZ_DML_COLUMN_VALUE * colval, bool addquote, ConnectorType conntype)
{
	char * out = NULL;
	char * in = colval->value;

	if (!in || strlen(in) == 0)
		return NULL;

	if (!strcasecmp(in, "NULL"))
		return NULL;

	elog(DEBUG1," processing %s with value %s", colval->name, colval->value);

	switch(colval->datatype)
	{
		case BOOLOID:
		case INT8OID:
		case INT2OID:
		case INT4OID:
		case FLOAT8OID:
		case FLOAT4OID:
		{
			/* no extra processing for nunmeric types */
			out = (char *) palloc0(strlen(in) + 1);
			strlcpy(out, in, strlen(in) + 1);
			break;
		}
		case MONEYOID:
		case NUMERICOID:
		{
			int newlen = 0, decimalpos = 0;
			long value = 0;
			char buffer[32] = {0};
			int tmpoutlen = pg_b64_dec_len(strlen(in));
			unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen + 1);


			tmpoutlen = pg_b64_decode(in, strlen(in), (char *)tmpout, tmpoutlen);

			value = derive_value_from_byte(tmpout, tmpoutlen);

			snprintf(buffer, sizeof(buffer), "%ld", value);
			if (colval->scale > 0)
			{
				if (strlen(buffer) > colval->scale)
				{
					/* ex: 123 -> 1.23*/
					newlen = strlen(buffer) + 1;	/* plus 1 decimal */
					out = (char *) palloc0(newlen + 1);	/* plus 1 terminating null */
					decimalpos = strlen(buffer) - colval->scale;
					strncpy(out, buffer, decimalpos);
					out[decimalpos] = '.';
					strcpy(out + decimalpos + 1, buffer + decimalpos);
				}
				else if (strlen(buffer) == colval->scale)
				{
					/* ex: 123 -> 0.123 */
					newlen = strlen(buffer) + 2;	/* plus 1 decimal and 1 zero */
					out = (char *) palloc0(newlen + 1);	/* plus 1 terminating null */
					snprintf(out, newlen + 1, "0.%s", buffer);
				}
				else
				{
					/* ex: 1 -> 0.001*/
					int scale_factor = 1, i = 0;
					double res = 0.0;

					/* plus 1 decimal and 1 zero and the zeros as a result of left shift */
					newlen = strlen(buffer) + (colval->scale - strlen(buffer)) + 2;
					out = (char *) palloc0(newlen + 1);	/* plus 1 terminating null */

					for (i = 0; i< colval->scale; i++)
						scale_factor *= 10;

					res = (double)value / (double)scale_factor;
					snprintf(out, newlen + 1, "%g", res);
				}
			}
			else
			{
				/* make scale = 4 to account for cents */
				if (colval->datatype == MONEYOID)
				{
					colval->scale = 4;
					if (strlen(buffer) > colval->scale)
					{
						newlen = strlen(buffer) + 1;	/* plus 1 decimal */
						out = (char *) palloc0(newlen + 1);	/* plus 1 terminating null */

						decimalpos = strlen(buffer) - colval->scale;
						strncpy(out, buffer, decimalpos);
						out[decimalpos] = '.';
						strcpy(out + decimalpos + 1, buffer + decimalpos);
					}
					else if (strlen(buffer) == colval->scale)
					{
						/* ex: 123 -> 0.123 */
						newlen = strlen(buffer) + 2;	/* plus 1 decimal and 1 zero */
						out = (char *) palloc0(newlen + 1);	/* plus 1 terminating null */
						snprintf(out, newlen + 1, "0.%s", buffer);
					}
					else
					{
						/* ex: 1 -> 0.001*/
						int scale_factor = 1, i = 0;
						double res = 0.0;

						/* plus 1 decimal and 1 zero and the zeros as a result of left shift */
						newlen = strlen(buffer) + (colval->scale - strlen(buffer)) + 2;
						out = (char *) palloc0(newlen + 1);	/* plus 1 terminating null */

						for (i = 0; i< colval->scale; i++)
							scale_factor *= 10;

						res = (double)value / (double)scale_factor;
						snprintf(out, newlen + 1, "%g", res);
					}

				}
				else
				{
					newlen = strlen(buffer);	/* no decimal */
					out = (char *) palloc0(newlen + 1);
					strlcpy(out, buffer, newlen + 1);
				}
			}
			pfree(tmpout);
			break;
		}
		case BPCHAROID:
		case TEXTOID:
		case VARCHAROID:
		case CSTRINGOID:
		case TIMESTAMPTZOID:
		case JSONBOID:
		case UUIDOID:
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
				strlcpy(out, in, strlen(in) + 1);
			}
			break;
		}
		case VARBITOID:
		case BITOID:
		{
			int tmpoutlen = pg_b64_dec_len(strlen(in));
			unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen);

			tmpoutlen = pg_b64_decode(in, strlen(in), (char*)tmpout, tmpoutlen);
			if (addquote)
			{
				/* 8 bits per byte + 2 single quotes + b + terminating null */
				char * tmp = NULL;
				out = (char *) palloc0((tmpoutlen * 8) + 2 + 1 + 1);
				tmp = out;
				reverse_byte_array(tmpout, tmpoutlen);
				strcat(tmp, "'b");
				tmp += 2;
				bytes_to_binary_string(tmpout, tmpoutlen, tmp);
				trim_leading_zeros(tmp);
				if (strlen(tmp) < colval->typemod)
					prepend_zeros(tmp, colval->typemod - strlen(tmp));

				strcat(tmp, "'");
			}
			else
			{
				/* 8 bits per byte + terminating null */
				out = (char *) palloc0(tmpoutlen * 8 + 1);
				reverse_byte_array(tmpout, tmpoutlen);
				bytes_to_binary_string(tmpout, tmpoutlen, out);
				trim_leading_zeros(out);
				if (strlen(out) < colval->typemod)
					prepend_zeros(out, colval->typemod - strlen(out));
			}
			pfree(tmpout);

			break;
		}
		case DATEOID:
		{
			/*
			 * we need to process these time related values based on the timerep
			 * that has been determined during the parsing stage
			 */
			long long input = atoll(in);
			time_t dayssinceepoch = 0;
			struct tm epoch = {0};
			time_t epoch_time, target_time;
			struct tm *target_date;
			char datestr[10 + 1]; /* YYYY-MM-DD */

			switch (colval->timerep)
			{
				case TIME_DATE:
					/* number of days since epoch, no conversion needed */
					dayssinceepoch = (time_t) input;
					break;
				case TIME_TIMESTAMP:
					/* number of milliseconds since epoch - convert to days since epoch */
					dayssinceepoch = (time_t)(input / 86400000LL);
					break;
				case TIME_MICROTIMESTAMP:
					/* number of microseconds since epoch - convert to days since epoch */
					dayssinceepoch = (time_t)(input / 86400000000LL);
					break;
				case TIME_NANOTIMESTAMP:
					/* number of microseconds since epoch - convert to days since epoch */
					dayssinceepoch = (time_t)(input / 86400000000000LL);
					break;
				case TIME_UNDEF:
				default:
				{
					set_shm_connector_errmsg(myConnectorId, "no time representation available to"
							"process DATEOID value");
					elog(ERROR, "no time representation available to process DATEOID value");
				}
			}

			/* since 1970-01-01 */
			epoch.tm_year = 70;
			epoch.tm_mon = 0;
			epoch.tm_mday = 1;

			epoch_time = mktime(&epoch);
			target_time = epoch_time + (dayssinceepoch * 24 * 60 * 60);

			/*
			 * Convert to struct tm in GMT timezone for now
			 * todo: convert to local timezone?
			 */
			target_date = gmtime(&target_time);
			strftime(datestr, sizeof(datestr), "%Y-%m-%d", target_date);

			if (addquote)
			{
				out = (char *) palloc0(strlen(datestr) + 2 + 1);
				snprintf(out, strlen(datestr) + 2 + 1, "'%s'", datestr);
			}
			else
			{
				out = (char *) palloc0(strlen(datestr) + 1);
				strlcpy(out, datestr,strlen(datestr) + 1);
			}
			break;
		}
		case TIMESTAMPOID:
		{
			/*
			 * we need to process these time related values based on the timerep
			 * that has been determined during the parsing stage
			 */
			long long input = atoll(in);
			time_t seconds = 0, remains = 0;
			struct tm *tm_info;
			char timestamp[26 + 1] = {0};	/* yyyy-MM-ddThh:mm:ss.xxxxxx */

			switch (colval->timerep)
			{
				case TIME_TIMESTAMP:
					/* milliseconds since epoch - convert to seconds since epoch */
					seconds = (time_t)(input / 1000);
					remains = input % 1000;
					break;
				case TIME_MICROTIMESTAMP:
					/* microseconds since epoch - convert to seconds since epoch */
					seconds = (time_t)(input / 1000 / 1000);
					remains = input % 1000000;
					break;
				case TIME_NANOTIMESTAMP:
					/* microseconds since epoch - convert to seconds since epoch */
					seconds = (time_t)(input / 1000 / 1000 / 1000);
					remains = input % 1000000000;
					break;
				case TIME_ZONEDTIMESTAMP:
					/*
					 * sent as string - just treat it like a string and skip the
					 * rest of processing logic
					 */
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
						strlcpy(out, in, strlen(in) + 1);
					}

					/* skip the rest of processing */
					return out;
				case TIME_UNDEF:
				default:
				{
					set_shm_connector_errmsg(myConnectorId, "no time representation available to"
							"process TIMESTAMPOID value");
					elog(ERROR, "no time representation available to process TIMESTAMPOID value");
				}
			}
			tm_info = gmtime(&seconds);

			if (colval->typemod > 0)
			{
				/*
				 * it means we could include additional precision to timestamp. PostgreSQL
				 * supports up to 6 digits of precision. We always put 6, PostgreSQL will
				 * round it up or down as defined by table schema
				 */
				snprintf(timestamp, sizeof(timestamp), "%04d-%02d-%02dT%02d:%02d:%02d.%06ld",
						tm_info->tm_year + 1900,
						tm_info->tm_mon + 1,
						tm_info->tm_mday,
						tm_info->tm_hour,
						tm_info->tm_min,
						tm_info->tm_sec,
						remains);
			}
			else
			{
				snprintf(timestamp, sizeof(timestamp), "%04d-%02d-%02dT%02d:%02d:%02d",
						tm_info->tm_year + 1900,
						tm_info->tm_mon + 1,
						tm_info->tm_mday,
						tm_info->tm_hour,
						tm_info->tm_min,
						tm_info->tm_sec);
			}

			if (addquote)
			{
				out = (char *) palloc0(strlen(timestamp) + 2 + 1);
				snprintf(out, strlen(timestamp) + 2 + 1, "'%s'", timestamp);
			}
			else
			{
				out = (char *) palloc0(strlen(timestamp) + 1);
				strlcpy(out, timestamp, strlen(timestamp) + 1);
			}
			break;
		}
		case TIMEOID:
		{
			/*
			 * we need to process these time related values based on the timerep
			 * that has been determined during the parsing stage
			 */
			unsigned long long input = atoll(in);
			time_t seconds = 0, remains = 0;
			char time[15 + 1] = {0};	/* hh:mm:ss.xxxxxx */

			switch(colval->timerep)
			{
				case TIME_TIME:
					/* milliseconds since midnight - convert to seconds since midnight */
					seconds = (time_t)(input / 1000);
					remains = input % 1000;
					break;
				case TIME_MICROTIME:
					/* microseconds since midnight - convert to seconds since midnight */
					seconds = (time_t)(input / 1000 / 1000);
					remains = input % 1000000;
					break;
				case TIME_NANOTIME:
					/* nanoseconds since midnight - convert to seconds since midnight */
					seconds = (time_t)(input / 1000 / 1000 / 1000);
					remains = input % 1000000000;
					break;
				case TIME_UNDEF:
				default:
				{
					set_shm_connector_errmsg(myConnectorId, "no time representation available to"
							"process TIMEOID value");
					elog(ERROR, "no time representation available to process TIMEOID value");
				}
			}
			if (colval->typemod > 0)
			{
				snprintf(time, sizeof(time), "%02d:%02d:%02d.%06ld",
						(int)((seconds / (60 * 60)) % 24),
						(int)((seconds / 60) % 60),
						(int)(seconds % 60),
						remains);
			}
			else
			{
				snprintf(time, sizeof(time), "%02d:%02d:%02d",
						(int)((seconds / (60 * 60)) % 24),
						(int)((seconds / 60) % 60),
						(int)(seconds % 60));
			}

			if (addquote)
			{
				out = (char *) palloc0(strlen(time) + 2 + 1);
				snprintf(out, strlen(time) + 2 + 1, "'%s'", time);
			}
			else
			{
				out = (char *) palloc0(strlen(time) + 1);
				strlcpy(out, time, strlen(time) + 1);
			}
			break;
		}
		case BYTEAOID:
		{
			int tmpoutlen = pg_b64_dec_len(strlen(in));
			unsigned char * tmpout = (unsigned char *) palloc0(tmpoutlen);

			tmpoutlen = pg_b64_decode(in, strlen(in), (char*)tmpout, tmpoutlen);

			if (addquote)
			{
				/* hexstring + 2 single quotes + '\x' + terminating null */
				out = (char *) palloc0((tmpoutlen * 2) + 2 + 2 + 1);
				bytearray_to_escaped_string(tmpout, tmpoutlen, out);
			}
			else
			{
				/* bytearray + terminating null */
				out = (char *) palloc0(tmpoutlen + 1);
				memcpy(out, tmpout, tmpoutlen);
			}
			pfree(tmpout);
			break;
		}
		case TIMETZOID:
		default:
		{
			/* todo: support more */
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "unsupported data type %d", colval->datatype);
			set_shm_connector_errmsg(myConnectorId, msg);
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
					char * data = processDataByType(colval, true, type);

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

					char * data = processDataByType(colval, false, type);

					if (data != NULL)
					{
						pgcolval->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval->value = pstrdup("NULL");

					pgcolval->datatype = colval->datatype;
					pgcolval->position = colval->position;

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
					data = processDataByType(colval, true, type);
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

					char * data = processDataByType(colval, false, type);

					if (data != NULL)
					{
						pgcolval->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval->value = pstrdup("NULL");

					pgcolval->datatype = colval->datatype;
					pgcolval->position = colval->position;

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
					data = processDataByType(colval, true, type);
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
					data = processDataByType(colval, true, type);
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

					char * data = processDataByType(colval_after, false, type);

					if (data != NULL)
					{
						pgcolval_after->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval_after->value = pstrdup("NULL");

					pgcolval_after->datatype = colval_after->datatype;
					pgcolval_after->position = colval_after->position;
					pgdml->columnValuesAfter = lappend(pgdml->columnValuesAfter, pgcolval_after);

					data = processDataByType(colval_before, false, type);

					if (data != NULL)
					{
						pgcolval_before->value = pstrdup(data);
						pfree(data);
					}
					else
						pgcolval_before->value = pstrdup("NULL");

					pgcolval_before->datatype = colval_before->datatype;
					pgcolval_before->position = colval_before->position;
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

	elog(DEBUG1, "pgdml->dmlquery %s", pgdml->dmlquery);
	return pgdml;
}

static void
get_additional_parameters(Jsonb * jb, DBZ_DML_COLUMN_VALUE * colval, bool isbefore, int pos)
{
	StringInfoData strinfo;
	char path[SYNCHDB_JSON_PATH_SIZE] = {0};

	if (!colval || !colval->name || colval->datatype == InvalidOid)
		return;

	initStringInfo(&strinfo);

	switch (colval->datatype)
	{
		case NUMERICOID:
		{
			/* spcial numeric case: need to obtain scale and precision from json */
			elog(DEBUG1, "numeric: retrieving additional scale and precision parameters");

			snprintf(path, SYNCHDB_JSON_PATH_SIZE, "schema.fields.%d.fields.%d.parameters.scale",
					isbefore ? 0 : 1, pos);

			getPathElementString(jb, path, &strinfo, true);

			if (!strcasecmp(strinfo.data, "NULL"))
				colval->scale = -1;	/* has no scale */
			else
				colval->scale = atoi(strinfo.data);	/* has scale */
			break;
		}
		case DATEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMETZOID:
		{
			snprintf(path, SYNCHDB_JSON_PATH_SIZE, "schema.fields.%d.fields.%d.name",
					isbefore ? 0 : 1, pos);

			getPathElementString(jb, path, &strinfo, true);

			if (!strcasecmp(strinfo.data, "NULL"))
				colval->timerep = TIME_UNDEF;	/* has no specific representation */
			else
			{
				if (find_exact_string_match(strinfo.data, "io.debezium.time.Date"))
					colval->timerep = TIME_DATE;
				else if (find_exact_string_match(strinfo.data, "io.debezium.time.Time"))
					colval->timerep = TIME_TIME;
				else if (find_exact_string_match(strinfo.data, "io.debezium.time.MicroTime"))
					colval->timerep = TIME_MICROTIME;
				else if (find_exact_string_match(strinfo.data, "io.debezium.time.NanoTime"))
					colval->timerep = TIME_NANOTIME;
				else if (find_exact_string_match(strinfo.data, "io.debezium.time.Timestamp"))
					colval->timerep = TIME_TIMESTAMP;
				else if (find_exact_string_match(strinfo.data, "io.debezium.time.MicroTimestamp"))
					colval->timerep = TIME_MICROTIMESTAMP;
				else if (find_exact_string_match(strinfo.data, "io.debezium.time.NanoTimestamp"))
					colval->timerep = TIME_NANOTIMESTAMP;
				else if (find_exact_string_match(strinfo.data, "io.debezium.time.ZonedTimestamp"))
					colval->timerep = TIME_ZONEDTIMESTAMP;
				else
					colval->timerep = TIME_UNDEF;
				elog(DEBUG1, "timerep %d", colval->timerep);
			}
			break;
		}
		default:
			break;
	}
	if(strinfo.data)
		pfree(strinfo.data);
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

	getPathElementString(jb, "payload.source.db", &strinfo, true);
	dbzdml->db = pstrdup(strinfo.data);

	getPathElementString(jb, "payload.source.table", &strinfo, true);
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
		set_shm_connector_errmsg(myConnectorId, msg);

		/* trigger pg's error shutdown routine */
		elog(ERROR, "%s", msg);
	}

	dbzdml->tableoid = get_relname_relid(dbzdml->table, schemaoid);
	if (!OidIsValid(dbzdml->tableoid))
	{
		char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
		snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for table '%s'", dbzdml->table);
		set_shm_connector_errmsg(myConnectorId, msg);

		/* trigger pg's error shutdown routine */
		elog(ERROR, "%s", msg);
	}

	elog(DEBUG1, "namespace %s.%s has PostgreSQL OID %d", dbzdml->db, dbzdml->table, dbzdml->tableoid);

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
			entry->typemod = attr->atttypmod;
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
							elog(DEBUG1, "start of object (%s) --------------------", key ? key : "null");

							if (key != NULL)
							{
								elog(DEBUG1, "sub element detected, skip subsequent parsing");
								pause = 1;
							}
							break;
						case WJB_END_OBJECT:
							if (pause)
							{
								elog(DEBUG1, "sub element ended, resume parsing operation");
								pause = 0;
								if (key)
								{
									int pathsize = strlen("payload.after.") + strlen(key) + 1;
									char * tmpPath = (char *) palloc0 (pathsize);

									elog(DEBUG1, "parse the entire sub element under %s as string", key);

									snprintf(tmpPath, pathsize, "payload.after.%s", key);
									getPathElementString(jb, tmpPath, &strinfo, false);
									value = pstrdup(strinfo.data);
									if(tmpPath)
										pfree(tmpPath);
								}
							}
							elog(DEBUG1, "end of object (%s) --------------------", key ? key : "null");
							break;
						case WJB_BEGIN_ARRAY:
							elog(DEBUG1, "start of array (%s) --- array type not expected or handled yet",
									key ? key : "null");
							if (key)
							{
								pfree(key);
								key = NULL;
							}
							break;
						case WJB_END_ARRAY:
							elog(DEBUG1, "end of array (%s) --- array type not expected or handled yet",
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
							colval->typemod = entry->typemod;

							/*
							 * get additional parameters if applicable - this assumes the position
							 * in dbz json array is the same as the position created in PostgreSQL
							 * table. If later we introduced column mappings or both have different
							 * number of columns. This part needs update too - todo
							 */
							get_additional_parameters(jb, colval, false, entry->position - 1);
						}
						else
							elog(WARNING, "cannot find data type for column %s. None-existent column?", colval->name);

						elog(DEBUG1, "consumed %s = %s, type %d", colval->name, colval->value, colval->datatype);
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
							elog(DEBUG1, "start of object (%s) --------------------", key ? key : "null");

							if (key != NULL)
							{
								elog(DEBUG1, "sub element detected, skip subsequent parsing");
								pause = 1;
							}
							break;
						case WJB_END_OBJECT:
							if (pause)
							{
								elog(DEBUG1, "sub element ended, resume parsing operation");
								pause = 0;
								if (key)
								{
									int pathsize = strlen("payload.before.") + strlen(key) + 1;
									char * tmpPath = (char *) palloc0 (pathsize);

									elog(DEBUG1, "parse the entire sub element under %s as string", key);

									snprintf(tmpPath, pathsize, "payload.before.%s", key);
									getPathElementString(jb, tmpPath, &strinfo, false);
									value = pstrdup(strinfo.data);
									if(tmpPath)
										pfree(tmpPath);
								}
							}
							elog(DEBUG1, "end of object (%s) --------------------", key ? key : "null");
							break;
						case WJB_BEGIN_ARRAY:
							elog(DEBUG1, "start of array (%s) --- array type not expected or handled yet",
									key ? key : "null");
							if (key)
							{
								pfree(key);
								key = NULL;
							}
							break;
						case WJB_END_ARRAY:
							elog(DEBUG1, "end of array (%s) --- array type not expected or handled yet",
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
							colval->typemod = entry->typemod;

							get_additional_parameters(jb, colval, true, entry->position - 1);
						}
						else
							elog(ERROR, "cannot find data type for column %s. None-existent column?", colval->name);

						elog(DEBUG1, "consumed %s = %s, type %d", colval->name, colval->value, colval->datatype);
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
								elog(DEBUG1, "start of object (%s) --------------------", key ? key : "null");

								if (key != NULL)
								{
									elog(DEBUG1, "sub element detected, skip subsequent parsing");
									pause = 1;
								}
								break;
							case WJB_END_OBJECT:
								if (pause)
								{
									elog(DEBUG1, "sub element ended, resume parsing operation");
									pause = 0;
									if (key)
									{
										int pathsize = (i == 0 ? strlen("payload.before.") + strlen(key) + 1 :
												strlen("payload.after.") + strlen(key) + 1);
										char * tmpPath = (char *) palloc0 (pathsize);

										elog(DEBUG1, "parse the entire sub element under %s as string", key);
										if (i == 0)
											snprintf(tmpPath, pathsize, "payload.before.%s", key);
										else
											snprintf(tmpPath, pathsize, "payload.after.%s", key);
										getPathElementString(jb, tmpPath, &strinfo, false);
										value = pstrdup(strinfo.data);
										if(tmpPath)
											pfree(tmpPath);
									}
								}
								elog(DEBUG1, "end of object (%s) --------------------", key ? key : "null");
								break;
							case WJB_BEGIN_ARRAY:
								elog(DEBUG1, "start of array (%s) --- array type not expected or handled yet",
										key ? key : "null");
								if (key)
								{
									pfree(key);
									key = NULL;
								}
								break;
							case WJB_END_ARRAY:
								elog(DEBUG1, "end of array (%s) --- array type not expected or handled yet",
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
								colval->typemod = entry->typemod;

								if (i == 0)
									get_additional_parameters(jb, colval, true, entry->position - 1);
								else
									get_additional_parameters(jb, colval, false, entry->position - 1);
							}
							else
								elog(ERROR, "cannot find data type for column %s. None-existent column?", colval->name);

							elog(DEBUG1, "consumed %s = %s, type %d", colval->name, colval->value, colval->datatype);
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

static void
init_mysql(void)
{
	HASHCTL	info;
	int i = 0;
	DatatypeHashEntry * entry;
	bool found = 0;

	info.keysize = sizeof(DatatypeHashKey);
	info.entrysize = sizeof(DatatypeHashEntry);
	info.hcxt = CurrentMemoryContext;

	mysqlDatatypeHash = hash_create("mysql datatype hash",
							 256,
							 &info,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	for (i = 0; i < SIZE_MYSQL_DATATYPE_MAPPING; i++)
	{
		entry = (DatatypeHashEntry *) hash_search(mysqlDatatypeHash, &(mysql_defaultTypeMappings[i].key), HASH_ENTER, &found);
		if (!found)
		{
			entry->key.autoIncremented = mysql_defaultTypeMappings[i].key.autoIncremented;
			memset(entry->key.extTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strncpy(entry->key.extTypeName,
					mysql_defaultTypeMappings[i].key.extTypeName,
					strlen(mysql_defaultTypeMappings[i].key.extTypeName));

			entry->pgsqlTypeLength = mysql_defaultTypeMappings[i].pgsqlTypeLength;
			memset(entry->pgsqlTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strncpy(entry->pgsqlTypeName,
					mysql_defaultTypeMappings[i].pgsqlTypeName,
					strlen(mysql_defaultTypeMappings[i].pgsqlTypeName));

			elog(DEBUG1, "Inserted mapping '%s' <-> '%s'", entry->key.extTypeName, entry->pgsqlTypeName);
		}
		else
		{
			elog(DEBUG1, "mapping exists '%s' <-> '%s'", entry->key.extTypeName, entry->pgsqlTypeName);
		}
	}
}

static void
init_sqlserver(void)
{
	HASHCTL	info;
	int i = 0;
	DatatypeHashEntry * entry;
	bool found = 0;

	info.keysize = sizeof(DatatypeHashKey);
	info.entrysize = sizeof(DatatypeHashEntry);
	info.hcxt = CurrentMemoryContext;

	sqlserverDatatypeHash = hash_create("sqlserver datatype hash",
							 256,
							 &info,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	for (i = 0; i < SIZE_SQLSERVER_DATATYPE_MAPPING; i++)
	{
		entry = (DatatypeHashEntry *) hash_search(sqlserverDatatypeHash, &(sqlserver_defaultTypeMappings[i].key), HASH_ENTER, &found);
		if (!found)
		{
			entry->key.autoIncremented = sqlserver_defaultTypeMappings[i].key.autoIncremented;
			memset(entry->key.extTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strncpy(entry->key.extTypeName,
					sqlserver_defaultTypeMappings[i].key.extTypeName,
					strlen(sqlserver_defaultTypeMappings[i].key.extTypeName));

			entry->pgsqlTypeLength = sqlserver_defaultTypeMappings[i].pgsqlTypeLength;
			memset(entry->pgsqlTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
			strncpy(entry->pgsqlTypeName,
					sqlserver_defaultTypeMappings[i].pgsqlTypeName,
					strlen(sqlserver_defaultTypeMappings[i].pgsqlTypeName));

			elog(DEBUG1, "Inserted mapping '%s' <-> '%s'", entry->key.extTypeName, entry->pgsqlTypeName);
		}
		else
		{
			elog(DEBUG1, "mapping exists '%s' <-> '%s'", entry->key.extTypeName, entry->pgsqlTypeName);
		}
	}
}

void
fc_initFormatConverter(ConnectorType connectorType)
{
	switch (connectorType)
	{
		case TYPE_MYSQL:
		{
			init_mysql();
			break;
		}
		case TYPE_ORACLE:
		{
			break;
		}
		case TYPE_SQLSERVER:
		{
			init_sqlserver();
			break;
		}
		default:
		{
			set_shm_connector_errmsg(myConnectorId, "unsupported connector type");
			elog(ERROR, "unsupported connector type");
		}
	}
}

void
fc_deinitFormatConverter(ConnectorType connectorType)
{
	switch (connectorType)
	{
		case TYPE_MYSQL:
		{
			hash_destroy(mysqlDatatypeHash);
			break;
		}
		case TYPE_ORACLE:
		{
			break;
		}
		case TYPE_SQLSERVER:
		{
			hash_destroy(sqlserverDatatypeHash);
			break;
		}
		default:
		{
			set_shm_connector_errmsg(myConnectorId, "unsupported connector type");
			elog(ERROR, "unsupported connector type");
		}
	}
}


bool
fc_load_rules(ConnectorType connectorType, const char * rulefile)
{
	FILE *file = fopen(rulefile, "r");
	char *json_string;
	long jsonlength = 0;
	Datum jsonb_datum;
	Jsonb * jb;
	JsonbIterator *it;
	JsonbIteratorToken r;
	JsonbValue v;
	bool inarray = false;
	char * array = NULL;
	char * key = NULL;
	char * value = NULL;
	bool found = 0;

	HTAB * rulehash = NULL;
	DatatypeHashEntry hashentry;
	DatatypeHashEntry * entrylookup;

	switch (connectorType)
	{
		case TYPE_MYSQL:
			rulehash = mysqlDatatypeHash;
			break;
		case TYPE_ORACLE:
			rulehash = NULL; /* todo */
			break;
		case TYPE_SQLSERVER:
			rulehash = sqlserverDatatypeHash;
			break;
		default:
		{
			set_shm_connector_errmsg(myConnectorId, "unsupported connector type");
			elog(ERROR, "unsupported connector type");
		}
	}

	if (!rulehash)
	{
		set_shm_connector_errmsg(myConnectorId, "data type hash not initialized");
		elog(ERROR, "data type hash not initialized");
	}

	if (!file)
	{
		set_shm_connector_errmsg(myConnectorId, "cannot open rule file");
		elog(ERROR, "Cannot open rule file: %s", rulefile);
	}

	/* Get the file size */
	fseek(file, 0, SEEK_END);
	jsonlength = ftell(file);
	fseek(file, 0, SEEK_SET);

	/* Allocate memory to store file contents */
	json_string = palloc0(jsonlength + 1);
	fread(json_string, 1, jsonlength, file);
	json_string[jsonlength] = '\0';

	fclose(file);

	jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(json_string));
	jb = DatumGetJsonbP(jsonb_datum);

	/*
	 * This parser expects json in this format:
	 * {
     *   "translation_rules":
     *   [
     *       {
     *           "translate_from": "LINESTRING",
     *           "translate_from_autoinc": false,
     *           "translate_to": "LINESTRING",
     *           "translate_to_size": -1
     *       },
     *       {
     *           "translate_from": "LINESTRING",
     *           "translate_from_autoinc": false,
     *           "translate_to": "POINT",
     *           "translate_to_size": -1
     *       }
     *       ...
     *       ...
     *       ...
     *   ]
	 * }
	 *
	 */
	it = JsonbIteratorInit(&jb->root);
	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		switch (r)
		{
			case WJB_BEGIN_ARRAY:
			{
				elog(DEBUG1, "begin array %s", array ? array : "NULL");
				inarray = true;
				if (strcasecmp(array, "translation_rules"))
				{
					set_shm_connector_errmsg(myConnectorId, "unexpected array token name found");
					elog(ERROR,"unexpected array token name: %s. Parser expects "
							"\"translation_rules\"", array);
				}
				break;
			}
			case WJB_END_ARRAY:
			{
				elog(DEBUG1, "end array %s", array ? array : "NULL");
				inarray = false;
				break;
			}
			case WJB_VALUE:
			case WJB_ELEM:
			{
				switch(v.type)
				{
					case jbvString:
						value = pnstrdup(v.val.string.val, v.val.string.len);
						elog(DEBUG1, "String Value: %s, key: %s", value, key ? key : "NULL");
						break;
					case jbvNull:
					{
						elog(DEBUG1, "Value: NULL");
						value = pnstrdup("NULL", strlen("NULL"));
						break;
					}
					case jbvNumeric:
					{
						value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
						elog(DEBUG1, "Numeric Value: %s, key: %s", value, key ? key : "NULL");
						break;
					}
					case jbvBool:
					{
						elog(DEBUG1, "Boolean Value: %s, key: %s", v.val.boolean ? "true" : "false", key ? key : "NULL");
						if (v.val.boolean)
							value = pnstrdup("true", strlen("true"));
						else
							value = pnstrdup("false", strlen("false"));
						break;
					}
					case jbvBinary:
					default:
					{
						set_shm_connector_errmsg(myConnectorId, "unexpected value type found in rule file");
						elog(ERROR, "Unknown or unexpected value type: %d while "
								"parsing primaryKeyColumnNames", v.type);
						break;
					}
				}
				break;
			}
			case WJB_KEY:
			{
				if (inarray)
				{
					key = pnstrdup(v.val.string.val, v.val.string.len);
					elog(DEBUG1, "key %s", key);
				}
				else
				{
					array = pnstrdup(v.val.string.val, v.val.string.len);
					elog(DEBUG1, "array %s", array);
				}
				break;
			}
			case WJB_BEGIN_OBJECT:
			{
				/* beginning of a json array element. Initialize hashkey */
				elog(DEBUG1, "begin object");
				memset(&hashentry, 0, sizeof(hashentry));
				break;
			}
			case WJB_END_OBJECT:
			{
				elog(DEBUG1, "end object");
				if (inarray)
				{
					elog(DEBUG1," data type mapping: from %s(%d) to %s(%d)",
							hashentry.key.extTypeName, hashentry.key.autoIncremented,
							hashentry.pgsqlTypeName, hashentry.pgsqlTypeLength);

					entrylookup = (DatatypeHashEntry *) hash_search(rulehash,
							&(hashentry.key), HASH_ENTER, &found);

					/* found or not, just update or insert it */
					if (!found)
					{
						entrylookup->key.autoIncremented = hashentry.key.autoIncremented;
						memset(entrylookup->key.extTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
						strncpy(entrylookup->key.extTypeName,
								hashentry.key.extTypeName,
								strlen(hashentry.key.extTypeName));

						entrylookup->pgsqlTypeLength = hashentry.pgsqlTypeLength;
						memset(entrylookup->pgsqlTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
						strncpy(entrylookup->pgsqlTypeName,
								hashentry.pgsqlTypeName,
								strlen(hashentry.pgsqlTypeName));

						elog(WARNING, "Inserted mapping '%s' <-> '%s'", entrylookup->key.extTypeName, entrylookup->pgsqlTypeName);
					}
					else
					{
						entrylookup->key.autoIncremented = hashentry.key.autoIncremented;
						memset(entrylookup->key.extTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
						strncpy(entrylookup->key.extTypeName,
								hashentry.key.extTypeName,
								strlen(hashentry.key.extTypeName));

						entrylookup->pgsqlTypeLength = hashentry.pgsqlTypeLength;
						memset(entrylookup->pgsqlTypeName, 0, SYNCHDB_DATATYPE_NAME_SIZE);
						strncpy(entrylookup->pgsqlTypeName,
								hashentry.pgsqlTypeName,
								strlen(hashentry.pgsqlTypeName));

						elog(WARNING, "Updated mapping '%s' <-> '%s'", entrylookup->key.extTypeName, entrylookup->pgsqlTypeName);
					}

				}
				break;
			}
			default:
			{
				set_shm_connector_errmsg(myConnectorId, "unexpected token found in rule file");
				elog(ERROR, "Unknown or unexpected token: %d while "
						"parsing rule file", r);
				break;
			}
		}

		/* check if we have a key - value pair */
		if (key != NULL && value != NULL)
		{
			if (!strcmp(key, "translate_from"))
			{
				elog(DEBUG1, "consuming %s = %s", key, value);
				strncpy(hashentry.key.extTypeName, value, strlen(value));
			}

			if (!strcmp(key, "translate_from_autoinc"))
			{
				elog(DEBUG1, "consuming %s = %s", key, value);
				if (!strcasecmp(value, "true"))
					hashentry.key.autoIncremented = true;
				else
					hashentry.key.autoIncremented = false;
			}

			if (!strcmp(key, "translate_to"))
			{
				elog(DEBUG1, "consuming %s = %s", key, value);
				strncpy(hashentry.pgsqlTypeName, value, strlen(value));
			}

			if (!strcmp(key, "translate_to_size"))
			{
				elog(DEBUG1, "consuming %s = %s", key, value);
				hashentry.pgsqlTypeLength = atoi(value);
			}

			pfree(key);
			pfree(value);
			key = NULL;
			value = NULL;
		}
	}
	return true;
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
    getPathElementString(jb, "payload.source.connector", &strinfo, true);

    type = fc_get_connector_type(strinfo.data);

    /* Check if it's a DDL or DML event */
    getPathElementString(jb, "payload.ddl", &strinfo, true);

    getPathElementString(jb, "payload.op", &strinfo, true);

    if (!strcmp(strinfo.data, "NULL"))
    {
        /* Process DDL event */
    	DBZ_DDL * dbzddl = NULL;
    	PG_DDL * pgddl = NULL;

    	/* (1) parse */
    	elog(DEBUG1, "parsing DBZ DDL change event...");
    	set_shm_connector_state(myConnectorId, STATE_PARSING);
    	dbzddl = parseDBZDDL(jb);
    	if (!dbzddl)
    	{
    		elog(DEBUG1, "malformed DDL event");
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		return -1;
    	}

    	elog(DEBUG1, "converting to PG DDL change event...");
    	/* (2) convert */
    	set_shm_connector_state(myConnectorId, STATE_CONVERTING);
    	pgddl = convert2PGDDL(dbzddl, type);
    	if (!pgddl)
    	{
    		elog(WARNING, "failed to convert DBZ DDL to PG DDL change event");
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		destroyDBZDDL(dbzddl);
    		return -1;
    	}

    	/* (3) execute */
    	elog(DEBUG1, "executing PG DDL change event...");
    	set_shm_connector_state(myConnectorId, STATE_EXECUTING);
    	if(ra_executePGDDL(pgddl, type))
    	{
    		elog(WARNING, "failed to execute PG DDL change event");
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		destroyDBZDDL(dbzddl);
    		destroyPGDDL(pgddl);
    		return -1;
    	}

    	/* (4) update offset */
       	set_shm_dbz_offset(myConnectorId);

    	/* (5) clean up */
    	set_shm_connector_state(myConnectorId, STATE_SYNCING);
    	elog(DEBUG1, "execution completed. Clean up...");
    	destroyDBZDDL(dbzddl);
    	destroyPGDDL(pgddl);
    }
    else
    {
        /* Process DML event */
    	DBZ_DML * dbzdml = NULL;
    	PG_DML * pgdml = NULL;

    	elog(DEBUG1, "this is DML change event");

    	/* (1) parse */
    	set_shm_connector_state(myConnectorId, STATE_PARSING);
    	dbzdml = parseDBZDML(jb, strinfo.data[0], type);
    	if (!dbzdml)
		{
			elog(WARNING, "malformed DNL event");
			set_shm_connector_state(myConnectorId, STATE_SYNCING);
			return -1;
		}

    	/* (2) convert */
    	set_shm_connector_state(myConnectorId, STATE_CONVERTING);
    	pgdml = convert2PGDML(dbzdml, type);
    	if (!pgdml)
    	{
    		elog(WARNING, "failed to convert DBZ DML to PG DML change event");
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		destroyDBZDML(dbzdml);
    		return -1;
    	}

    	/* (3) execute */
    	set_shm_connector_state(myConnectorId, STATE_EXECUTING);
    	elog(DEBUG1, "executing PG DML change event...");
    	if(ra_executePGDML(pgdml, type))
    	{
    		elog(WARNING, "failed to execute PG DML change event");
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
        	destroyDBZDML(dbzdml);
        	destroyPGDML(pgdml);
    		return -1;
    	}

    	/* (4) update offset */
       	set_shm_dbz_offset(myConnectorId);

       	/* (5) clean up */
    	set_shm_connector_state(myConnectorId, STATE_SYNCING);
    	elog(DEBUG1, "execution completed. Clean up...");
    	destroyDBZDML(dbzdml);
    	destroyPGDML(pgdml);
    }
	return 0;
}
