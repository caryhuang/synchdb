import common
import time

from common import run_pg_query, run_pg_query_one, run_remote_query, run_remote_query_one, create_synchdb_connector, getConnectorName, getDbname, verify_default_type_mappings, create_and_start_synchdb_connector

def test_AllDefaultDataTypes(pg_cursor, dbvendor):
    name = getConnectorName(dbvendor) + "_addt"
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "no_data")
    assert result == 0

    if dbvendor == "mysql":
        query = """
        CREATE TABLE mytable (
            a DEC(20,4) unsigned,
            b decimal(10,4),
            c varchar(20),
            d fixed,
            e fixed(5,2) unsigned,
            f bit(64),
            g bool,
            h double,
            i double precision,
            j double precision unsigned,
            k double unsigned,
            l real,
            m real unsigned,
            n float,
            o float unsigned,
            p int unsigned,
            q integer unsigned,
            r mediumint,
            s mediumint unsigned,
            t year,
            u smallint,
            v smallint unsigned,
            w tinyint,
            x tinyint unsigned,
            y date,
            z datetime,
            aa time,
            bb timestamp,
            cc BINARY(16),
            dd VARBINARY(255),
            ee BLOB,
            ff LONGBLOB,
            gg TINYBLOB,
            hh long varchar,
            ii longtext,
            jj mediumtext,
            kk tinytext,
            ll JSON,
            mm int,
            nn int auto_increment,
            primary key(nn),
            oo timestamp(6),
            pp time(6));
        """
    elif dbvendor == "sqlserver":
        query = """
        CREATE TABLE mytable (
            a bit,
            b DECIMAL(20,4),
            c DECIMAL(10, 4),
            d DECIMAL(10, 2),
            e DECIMAL(5, 2),
            f bit,
            g float,
            h real,
            i int,
            j smallint,
            k tinyint,
            l bigint,
            m numeric(10,2),
            n money,
            o smallmoney,
            p date,
            q time,
            r datetime2,
            s datetimeoffset,
            t datetime,
            u smalldatetime,
            v char,
            w varchar(48),
            x text,
            y nchar,
            z nvarchar(48),
            aa ntext,
            bb BINARY(10),
            cc VARBINARY(50),
            dd IMAGE,
            ee GEOMETRY,
            ff GEOGRAPHY,
            gg NVARCHAR(2000),
            hh XML,
            ii VARBINARY(4096),
            jj UNIQUEIDENTIFIER,
            kk time(6),
            ll datetime2(6),
            mm datetimeoffset(6));
        """
    else:
        query = """
        CREATE TABLE mytable (
            id NUMBER PRIMARY KEY,                   
            binary_double_col BINARY_DOUBLE,         
            binary_float_col BINARY_FLOAT,           
            float_col FLOAT(10),                      
            number_col NUMBER(10,2),                  
            long_col LONG,
            date_col DATE,                            
            interval_ds_col INTERVAL DAY TO SECOND,   
            interval_ym_col INTERVAL YEAR TO MONTH,   
            timestamp_col TIMESTAMP,                  
            timestamp_tz_col TIMESTAMP WITH TIME ZONE, 
            timestamp_ltz_col TIMESTAMP WITH LOCAL TIME ZONE, 
            char_col CHAR(10),                        
            nchar_col NCHAR(10),                      
            nvarchar2_col NVARCHAR2(50),              
            varchar_col VARCHAR(50),                  
            varchar2_col VARCHAR2(50),                
            raw_col RAW(100),                         
            bfile_col BFILE,                          
            blob_col BLOB,                            
            clob_col CLOB,                            
            nclob_col NCLOB,                          
            rowid_col ROWID,                          
            urowid_col UROWID);
            commit;
        """

    run_remote_query(dbvendor, query)
    if dbvendor == "oracle":
        time.sleep(30)
    else:
        time.sleep(10)
    
    # verify default data type mappings
    rows = run_pg_query(pg_cursor, f"SELECT ext_atttypename, pg_atttypename FROM synchdb_att_view WHERE name = '{name}'")
    assert len(rows) > 0
    for row in rows:
        assert verify_default_type_mappings(row[0], row[1], dbvendor) == True

def test_CreateObjmapEntries(pg_cursor, dbvendor):
    print("create table name mapping")
    print("create column name mapping")
    print("create data type mapping")
    print("create transform expression")
    assert True

def test_CreateObjmapEntriesWithError(pg_cursor, dbvendor):
    assert True

def test_DeleteObjmapEntries(pg_cursor, dbvendor):
    assert True

def test_ReloadObjmapEntries(pg_cursor, dbvendor):
    assert True

def test_TableNameMapping(pg_cursor, dbvendor):
    assert True

def test_AlterTableNameMapping(pg_cursor, dbvendor):
    assert True

def test_ColumnNameMapping(pg_cursor, dbvendor):
    assert True

def test_AlterColumnNameMapping(pg_cursor, dbvendor):
    assert True

def test_DataTypeMapping(pg_cursor, dbvendor):
    assert True

def test_AlterDataTypeMapping(pg_cursor, dbvendor):
    assert True

def test_DataTypeMappingWithError(pg_cursor, dbvendor):
    assert True

def test_TransformExpression(pg_cursor, dbvendor):
    assert True

def test_AlterTransformExpression(pg_cursor, dbvendor):
    assert True

def test_TransformExpressionWithError(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Numeric(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Datetime(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Date(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Time(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Bit(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Timestamp(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Binary(pg_cursor, dbvendor):
    assert True

def test_ConvertString2Interval(pg_cursor, dbvendor):
    assert True


