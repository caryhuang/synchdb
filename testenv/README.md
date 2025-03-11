## Prepare a sample MySQL Database
We can start a sample MySQL database for testing using docker compose. The user credentials are described in the `synchdb-mysql-test.yaml` file
``` BASH
docker compose -f synchdb-mysql-test.yaml up -d
```

Login to MySQL as `root` and grant permissions to user `mysqluser` to perform real-time CDC
``` SQL
mysql -h 127.0.0.1 -u root -p

GRANT replication client on *.* to mysqluser;
GRANT replication slave  on *.* to mysqluser;
GRANT RELOAD ON *.* TO 'mysqluser'@'%';
FLUSH PRIVILEGES;
```

Exit mysql client tool:
```
\q
```
## Prepare a sample SQL Server Database
We can start a sample SQL Server database for testing using docker compose. The user credentials are described in the `synchdb-sqlserver-test.yaml` file
```
docker compose -f synchdb-sqlserver-test.yaml up -d
```
use synchdb-sqlserver-withssl-test.yaml file for the SQL Server with SSL certificate enabled.

You may not have SQL Server client tool installed, you could login to SQL Server container to access its client tool.

Find out the container ID for SQL server:
``` BASH
id=$(docker ps | grep sqlserver | awk '{print $1}')
```

Copy the database schema into SQL Server container:
``` BASH
docker cp inventory.sql $id:/
```

Log in to SQL Server container:
``` BASH
docker exec -it $id bash
```

Build the database according to the schema:
``` BASH
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -i /inventory.sql
```

Run some simple queries (add -N -C if you are using SSL enabled SQL Server):
``` BASH
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -Q "insert into orders(order_date, purchaser, quantity, product_id) values( '2024-01-01', 1003, 2, 107)"

/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -Q "select * from orders"
```

## Prepare a sample Oracle Database
We can pull the official free Oracle database for evaluation and testing directly and run the necessary setups to get it ready for testing with SynchDB. It comes with a free container database called `FREE` and a free pluggable database called `FREEPDB1`.

run the docker image
``` BASH
docker run -d -p 1521:1521 container-registry.oracle.com/database/free:latest
```

login to Oracle container
``` BASH
id=$(docker ps | grep oracle | awk '{print $1}')
docker exec -it $id bash
```

create a recovery folder for archive
``` BASH
mkdir /opt/oracle/oradata/recovery_area
```

sets a password for the default sys user
``` SQL
sqlplus / as sysdba
	Alter user sys identified by oracle;
	Exit
```

sets up log archive
```SQL
sqlplus /nolog

	CONNECT sys/oracle as sysdba;
	alter system set db_recovery_file_dest_size = 10G;
	alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
	shutdown immediate;
	startup mount;
	alter database archivelog;
	alter database open;
	archive log list;
	exit

sqlplus sys/oracle@//localhost:1521/FREE as sysdba

	ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
	ALTER PROFILE DEFAULT LIMIT FAILED_LOGIN_ATTEMPTS UNLIMITED;
	exit;

sqlplus sys/oracle@//localhost:1521/FREE as sysdba

	CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
	exit;
	
sqlplus sys/oracle@//localhost:1521/FREEPDB1 as sysdba

	CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
	exit;

```

create logminer user and grant proper permissions. User `c##dbzuser` with password `dbz` will be created below and these are the credentials SynchDB will use to connect
``` SQL
sqlplus sys/oracle@//localhost:1521/FREE as sysdba

	CREATE USER c##dbzuser IDENTIFIED BY dbz DEFAULT TABLESPACE LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS CONTAINER=ALL;
	GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
	GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
	GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
	GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
	GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ANY DICTIONARY TO c##dbzuser CONTAINER=ALL;
	GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
	GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
	GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;
	GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
	GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER=ALL; 
	GRANT SELECT ON V_$MYSTAT TO c##dbzuser CONTAINER=ALL;
	GRANT SELECT ON V_$STATNAME TO c##dbzuser CONTAINER=ALL; 
	GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO C##DBZUSER;
	GRANT SELECT ON DBA_HIST_SNAPSHOT TO C##DBZUSER;
	GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO PUBLIC;
	Exit
```

enlarge redo log file sizes. Feel free to add more log file groups as needed
``` SQL
sqlplus sys/oracle@//localhost:1521/FREE as sysdba
	ALTER DATABASE ADD LOGFILE GROUP 4 ('/opt/oracle/oradata/FREE/redo04.log') SIZE 1G;
	ALTER DATABASE ADD LOGFILE GROUP 5 ('/opt/oracle/oradata/FREE/redo05.log') SIZE 1G;
	ALTER DATABASE ADD LOGFILE GROUP 6 ('/opt/oracle/oradata/FREE/redo06.log') SIZE 1G;
	ALTER DATABASE ADD LOGFILE GROUP 7 ('/opt/oracle/oradata/FREE/redo07.log') SIZE 1G;
	ALTER DATABASE ADD LOGFILE GROUP 8 ('/opt/oracle/oradata/FREE/redo08.log') SIZE 1G;
	exit
```

login as `c##dbzuser` and create test table
``` SQL
sqlplus c##dbzuser@//localhost:1521/FREE
	CREATE TABLE test_table (
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
	    urowid_col UROWID
	);

	INSERT INTO test_table (
    	id, binary_double_col, binary_float_col, float_col, number_col, 
	    long_col, date_col, interval_ds_col, interval_ym_col, timestamp_col, 
    	timestamp_tz_col, timestamp_ltz_col, char_col, nchar_col, 
    	nvarchar2_col, varchar_col, varchar2_col, raw_col, 
    	bfile_col, blob_col, clob_col, nclob_col, rowid_col, urowid_col
	) VALUES (
    	1, 12345.6789, 1234.56, 9876.54321, 1000.50, 
    	'This is a long text', 
	    TO_DATE('2024-01-31', 'YYYY-MM-DD'), 
    	INTERVAL '2 03:04:05' DAY TO SECOND, 
	    INTERVAL '1-6' YEAR TO MONTH, 
    	TIMESTAMP '2024-01-31 10:30:00', 
	    TIMESTAMP '2024-01-31 10:30:00 -08:00', 
	    SYSTIMESTAMP, 
	    'A', 
    	N'B', 
	    N'Unicode Text', 
    	'Text Data', 
	    'More Text Data', 
	    HEXTORAW('DEADBEEF'), 
    	BFILENAME('MY_DIR', 'file.pdf'),
	    TO_BLOB(HEXTORAW('DEADBEEF')), 
	    TO_CLOB('This is a non-empty CLOB text data'),
	    TO_NCLOB('This is a non-empty NCLOB text data'),
	    NULL, 
	    NULL
	);

	commit;
	exit
```

enable supplemental log data captuer for all columns (for proper capture of UPDATE and DELETE change events)
``` SQL
sqlplus c##dbzuser@//localhost:1521/FREE
	ALTER TABLE TEST_TABLE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
	exit

```


