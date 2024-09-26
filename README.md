
## Introduction

SynchDB is a PostgreSQL extension designed to replicate data from one or more heterogeneous databases (such as MySQL, MS SQLServer, Oracle, etc.) directly to PostgreSQL in a fast and reliable way. PostgreSQL serves as the destination from multiple heterogeneous database sources. No middleware or third-party software is required to orchestrate the data synchronization between heterogeneous databases and PostgreSQL. SynchDB extension itself is capable of handling all the data synchronization needs.

It provides two key work modes that can be invoked using the built-in SQL functions:
* Sync mode (for initial data synchronization)
* Follow mode (for replicate incremental changes after initial sync)

**Sync mode** copies tables from heterogeneous database into PostgreSQL, including its schema, indexes, triggers, other table properties as well as current data it holds.
**Follow mode** subscribes to tables in a heterogeneous database to obtain incremental changes and apply them to the same tables in PostgreSQL, similar to PostgreSQL logical replication

## Requirement
The following software is required to build and run SynchDB. The versions listed are the versions tested during development. Older versions may still work.
* Java Development Kit 22. Download [here](https://www.oracle.com/ca-en/java/technologies/downloads/)
* Apache Maven 3.9.8. Download [here](https://maven.apache.org/download.cgi)
* PostgreSQL 16.3 Source. Git clone [here](https://github.com/postgres/postgres). Refer to this [wiki](https://wiki.postgresql.org/wiki/Compile_and_Install_from_source_code) for PostgreSQL build requirements
* Docker compose 2.28.1 (for testing). Refer to [here](https://docs.docker.com/compose/install/linux/)
* Unix based operating system like Ubuntu 22.04 or MacOS

## Build Procedure
### Prepare Source

Clone the PostgreSQL source and switch to 16.3 release tag
```
git clone https://github.com/postgres/postgres.git
cd postgres
git checkout REL_16_3
```

Clone the SynchDB source from within the extension folder
Note: Branch *(synchdb-devel)[https://github.com/Hornetlabs/synchdb/tree/synchdb-devel]* is used for development so far.
```
cd contrib/
git clone https://github.com/Hornetlabs/synchdb.git
```

### Prepare Tools
#### Maven
If you are working on Ubuntu 22.04.4 LTS, install the Maven as below:
```
sudo apt install maven
```

if you are using MacOS, you can use the brew command to install maven (refer (here)[https://brew.sh/] for how to install Homebrew) without any other settings:
```
brew install maven
```

#### Install Java SDK (OpenJDK)
If you are working on Ubuntu 22.04.4 LTS, install the OpenJDK  as below:
```
sudo apt install openjdk-21-jdk
```

If you are working on MacOS, please install the JDK with brew command:
```
brew install openjdk@22
```

### Build and Install PostgreSQL
This can be done by following the standard build and install procedure as described [here](https://www.postgresql.org/docs/current/install-make.html). Generally, it consists of:

```
cd /home/$USER/postgres
./configure
make
sudo make install
```

You should build and install the default extensions as well:
```
cd /home/$USER/postgres/contrib
make
sudo make install
```

### Build Debezium Runner Engine
With Java and Maven setup, we are ready to build Debezium Runner Engine:

```
cd /home/$USER/postgres/contrib/synchdb
make build_dbz
```

then install Debezium engine to PostgreSQL lib path
```
sudo make install_dbz
```

### Build SynchDB PostgreSQL Extension
With the Java `lib` and `include` installed in your system, SynchDB can be built by:
```
cd /home/$USER/postgres/contrib/synchdb
make
sudo make install
```

### Configure your Linker (Ubuntu)
Lastly, we also need to tell your system's linker where the newly added Java library is located in your system.

```
# Dynamically set JDK paths
JAVA_PATH=$(which java)
JDK_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
JDK_LIB_PATH=${JDK_HOME_PATH}/lib

echo $JDK_LIB_PATH
echo $JDK_LIB_PATH/server

sudo echo "$JDK_LIB_PATH" ｜ sudo tee -a /etc/ld.so.conf.d/x86_64-linux-gnu.conf
sudo echo "$JDK_LIB_PATH/server" | sudo tee -a /etc/ld.so.conf.d/x86_64-linux-gnu.conf
```
Note, for mac with M1/M2 chips, you need to the two lines into /etc/ld.so.conf.d/aarch64-linux-gnu.conf

Run ldconfig to reload:
```
sudo ldconfig
```

Ensure synchdo.so extension can link to libjvm Java library on your system:
```
ldd synchdb.so
        linux-vdso.so.1 (0x00007ffeae35a000)
        libjvm.so => /usr/lib/jdk-22.0.1/lib/server/libjvm.so (0x00007fc1276c1000)
        libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007fc127498000)
        libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007fc127493000)
        libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007fc12748e000)
        librt.so.1 => /lib/x86_64-linux-gnu/librt.so.1 (0x00007fc127489000)
        libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007fc1273a0000)
        /lib64/ld-linux-x86-64.so.2 (0x00007fc128b81000)

```

## Run SynchDB Test
### Prepare a sample MySQL Database
We can start a sample MySQL database for testing using docker compose. The user credentials are described in the `synchdb-mysql-test.yaml` file
```
docker compose -f synchdb-mysql-test.yaml up -d
```

Login to MySQL as `root` and grant permissions to user `mysqluser` to perform real-time CDC
```
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
### Prepare a sample SQL Server Database
We can start a sample SQL Server database for testing using docker compose. The user credentials are described in the `synchdb-sqlserver-test.yaml` file
```
docker compose -f synchdb-sqlserver-test.yaml up -d
```
use synchdb-sqlserver-withssl-test.yaml file for the SQL Server with SSL certificate enabled.

You may not have SQL Server client tool installed, you could login to SQL Server container to access its client tool.

Find out the container ID for SQL server:
```
id=$(docker ps | grep sqlserver | awk '{print $1}')
```

Copy the database schema into SQL Server container:
```
docker cp inventory.sql $id:/
```

Log in to SQL Server container:
```
docker exec -it $id bash
```

Build the database according to the schema:
```
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -i /inventory.sql
```

Run some simple queries (add -N -C if you are using SSL enabled SQL Server):
```
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -Q "insert into orders(order_date, purchaser, quantity, product_id) values( '2024-01-01', 1003, 2, 107)"

/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -Q "select * from orders"
```

### Prepare a sample PostgreSQL database
Initialize and start a new PostgreSQL database:
```
initdb -D synchdbtest
pg_ctl -D synchdbtest -l logfile start
```

SynchDB extension requires pgcrypto to encrypt certain sensitive credential data. Please make sure it is installed prior to installing SynchDB. Alternatively, you can include `CASCADE` clause in `CREATE EXTENSION` to automatically install dependencies:

```
CREATE EXTENSION synchdb CASCADE;
```

### Create a Connection Information
After SynchDB is installed, we can create a connection information entries that represents how to connect to a remote heterogeneous database and what to replicate. This can be done with `synchdb_add_conninfo()` function.

### Usage of `synchdb_add_conninfo`
`synchdb_add_conninfo()` takes these arguments:
* `name` - a unique identifier that represents this connection info
* `hostname` - the IP address of heterogeneous database.
* `port` - the port number to connect to.
* `username` - user name to use to connect.
* `password` - password to authenticate the username.
* `source database` (optional) - the database name to replicate changes from. For example, the database that exists in MySQL. If empty, all databases from MySQL are replicated.
* `destination` database - the database to apply changes to - For example, the database that exists in PostgreSQL. Must be a valid database that exists.
* `table` (optional) - expressed in the form of `[database].[table]` that must exists in MySQL so the engine will only replicate the specified tables. If empty, all tables are replicated.
* `connector` - the connector to use (MySQL, Oracle, or SQLServer). Currently only MySQL and SQLServer are supported.

For example:
```
# create a mysql conninfo called 'mysqlconn' to replicate from source database 'inventory' to destination database 'postgres'
SELECT synchdb_add_conninfo('mysqlconn','127.0.0.1',3306,'mysqluser', 'mysqlpwd', 'inventory', 'postgres', '', 'mysql');

# create a second mysql conninfo called 'mysqlconn2' to replicate from source database 'inventory' to destination database 'mysqldb2'
SELECT synchdb_add_conninfo('mysqlconn2','127.0.0.1',3306,'mysqluser', 'mysqlpwd', 'inventory', 'mysqldb2', '', 'mysql');

# create a sqlserver conninfo called 'sqlserverconn' to replicate from source database 'testDB' to destination database 'sqlserverdb'
SELECT synchdb_add_conninfo('sqlserverconn','127.0.0.1',1433,'sa', 'Password!', 'testDB', 'sqlserverdb', '', 'sqlserver');

# create a second sqlserver conninfo called 'sqlserverconn2' to replicate from source database 'testDB' to destination database 'sqlserverdb2'
SELECT synchdb_add_conninfo('sqlserverconn2','127.0.0.1',1433,'sa', 'Password!', 'testDB', 'sqlserverdb2', '', 'sqlserver');

```

Other Examples:

To create a connection information to replicate only specific tables (`orders` and `customers`) from MySQL:
```
# create another mysql conninfo called 'mysqlconn3' to replicate from source database 'inventory''s orders and customers tabls to destination database 'mysqldb3'
SELECT synchdb_add_conninfo('mysqlconn3', '127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'mysqldb3', 'inventory.orders,inventory.customers', 'mysql');
```

The replicated changes will be mapped to a schema in destination database. For example, a table named `orders` from database `inventory` in MySQL will be replicated to a table named `orders` under schema `inventory`.

### Review all Connection Information Created
All connection information are created in the table `synchdb_conninfo`. We are free to view its content and make modification as required. Please note that the password of a user credential is encrypted by pgcrypto using a key only known to synchdb. So please do not modify the password field or it may be decrypted incorrectly if tempered. See below for an example output: 

```
postgres=# \x
Expanded display is on.

postgres=# select * from synchdb_conninfo;
-[ RECORD 1 ]-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name | mysqlconn
data | {"pwd": "\\xc30d040703024828cc4d982e47b07bd23901d03e40da5995d2a631fb89d49f748b87247aee94070f71ecacc4990c3e71cad9f68d57c440de42e35bcc78fd145feab03452e454284289db", "port": 3306, "user": "mysqluser", "dstdb": "postgres", "srcdb": "inventory", "table": "null", "hostname": "192.168.1.86", "connector": "mysql"}
-[ RECORD 2 ]-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name | sqlserverconn
data | {"pwd": "\\xc30d0407030231678e1bb0f8d3156ad23a010ca3a4b0ad35ed148f8181224885464cdcfcec42de9834878e2311b343cd184fde65e0051f75d6a12d5c91d0a0403549fe00e4219215eafe1b", "port": 1433, "user": "sa", "dstdb": "sqlserverdb", "srcdb": "testDB", "table": "null", "hostname": "192.168.1.86", "connector": "sqlserver"}
```

Use the SQL function `synchdb_start_engine_bgw` to spawn a new background worker to capture CDC from a heterogeneous database such as MySQL.

### Start a Connector Worker
We can use `synchdb_start_engine_bgw` function to start a connector worker. It takes one argument which is the connection information name created above. Based on the connection information, the command will spawn a worker with specific replication configuration. For example:

```
select synchdb_start_engine_bgw('mysqlconn');
select synchdb_start_engine_bgw('sqlserverconn');
```

`synchdb_start_engine_bgw()` function also marks a connection info as `active`, which is eligible for automatic-relaunch at server restarts. See below for more details.

### Check Running Connector Workers
You can check the running workers using the `ps` command:
```
ps -ef | grep postgres
```

Sample Output:
```
ubuntu    153865       1  0 15:49 ?        00:00:00 /usr/local/pgsql/bin/postgres -D ../../synchdbtest
ubuntu    153866  153865  0 15:49 ?        00:00:00 postgres: checkpointer
ubuntu    153867  153865  0 15:49 ?        00:00:00 postgres: background writer
ubuntu    153869  153865  0 15:49 ?        00:00:00 postgres: walwriter
ubuntu    153870  153865  0 15:49 ?        00:00:00 postgres: autovacuum launcher
ubuntu    153871  153865  0 15:49 ?        00:00:00 postgres: logical replication launcher
ubuntu    153875  153865  0 15:49 ?        00:00:46 postgres: synchdb engine: sqlserver@127.0.0.1:1433 -> sqlserverdb
ubuntu    153900  153865  0 15:49 ?        00:00:46 postgres: synchdb engine: mysql@127.0.0.1:3306 -> postgres
```

As you can see, there are 2 additional background workers(synchdb engine), one replicating from MySQL, the other replicating from SQL server.

### View the Connector States
We can execute the `synchdb_state_view` view to examine all the running connectors and their states. Currently, synchdb can support up to 30 running workers.

See below for an example output:
```
postgres=# select * from synchdb_state_view;
 id | connector | conninfo_name  |  pid   |  state  |   err    |                                          last_dbz_offset
----+-----------+----------------+--------+---------+----------+---------------------------------------------------------------------------------------------------
  0 | mysql     | mysqlconn      | 461696 | syncing | no error | {"ts_sec":1725644339,"file":"mysql-bin.000004","pos":138466,"row":1,"server_id":223344,"event":2}
  1 | mysql     | mysqlconn2     | 461535 | syncing | no error | {"ts_sec":1725644339,"file":"mysql-bin.000004","pos":138466,"row":1,"server_id":223344,"event":2}
  2 | sqlserver | sqlserverconn  | 461739 | syncing | no error | {"event_serial_no":1,"commit_lsn":"00000100:00000c00:0003","change_lsn":"00000100:00000c00:0002"}
  3 | sqlserver | sqlserverconn2 | 461971 | syncing | no error | offset file not flushed yet
  4 | null      |                |     -1 | stopped | no error | no offset
  5 | null      |                |     -1 | stopped | no error | no offset
  6 | null      |                |     -1 | stopped | no error | no offset
  7 | null      |                |     -1 | stopped | no error | no offset
  8 | null      |                |     -1 | stopped | no error | no offset
  9 | null      |                |     -1 | stopped | no error | no offset
 10 | null      |                |     -1 | stopped | no error | no offset
 11 | null      |                |     -1 | stopped | no error | no offset
 12 | null      |                |     -1 | stopped | no error | no offset
 13 | null      |                |     -1 | stopped | no error | no offset
 14 | null      |                |     -1 | stopped | no error | no offset
 15 | null      |                |     -1 | stopped | no error | no offset
 16 | null      |                |     -1 | stopped | no error | no offset
 17 | null      |                |     -1 | stopped | no error | no offset
 18 | null      |                |     -1 | stopped | no error | no offset
 19 | null      |                |     -1 | stopped | no error | no offset
 20 | null      |                |     -1 | stopped | no error | no offset
 21 | null      |                |     -1 | stopped | no error | no offset
 22 | null      |                |     -1 | stopped | no error | no offset
 23 | null      |                |     -1 | stopped | no error | no offset
 24 | null      |                |     -1 | stopped | no error | no offset
 25 | null      |                |     -1 | stopped | no error | no offset
 26 | null      |                |     -1 | stopped | no error | no offset
 27 | null      |                |     -1 | stopped | no error | no offset
 28 | null      |                |     -1 | stopped | no error | no offset
 29 | null      |                |     -1 | stopped | no error | no offset
```

Column Details:
* id: unique identifier of a connector slot
* connector: the type of connector (mysql, oracle, sqlserver...etc)
* conninfo_name: the associated connection info name created by `synchdb_add_conninfo`
* pid: the PID of the connector worker process
* state: the state of the connector. Possible states are:
	* stopped
	* initializing
	* paused
	* syncing
	* parsing
	* converting
	* executing
	* updating offset
	* unknown
* err: the last error message encountered by the worker which would have caused it to exit
* last_dbz_offset: the last Debezium offset captured by synchdb. Note that this may not reflect the current and real-time offset value of the connector engine. Rather, this is shown as a checkpoint that we could restart from this offeet point if needed.

### Pause a Connector Worker
We can use `synchdb_pause_engine()` SQL function to pause a runnng connector. This will halt the Debezium engine from replicating from the remote heterogeneous database. When paused, it is possible to alter the Debezium connector's offset value to replicate from a specific point in the past using `synchdb_set_offset()` SQL routine. It takes `conninfo_name` as its argument which can be found from the output of `synchdb_get_state()` view.

For example:
```
SELECT synchdb_pause_engine('mysqlconn');
```

### Update Connector Offset
We can use `synchdb_set_offset()` SQL function to change a connector worker's starting offset. This can only be done when the connector is put into `paused` state. The function takes 2 parameters, conninfo_name and a valid offset string, both of which can be found from the output of `synchdb_get_state()` view.

For example:
```
SELECT synchdb_set_offset('mysqlconn', '{"ts_sec":1725644339,"file":"mysql-bin.000004","pos":138466,"row":1,"server_id":223344,"event":2}'); 
```

### Resume Connector Offset
We can use `synchdb_resume_engine()` SQL function to resume Debezium operation from a paused state. This function takes conninfo_name as its only parameter, which can be found from the output of `synchdb_get_state()` view.

For example:
```
SELECT synchdb_resume_engine('mysqlconn');
```

### Stopping a Worker
We can use `synchdb_stop_engine_bgw()` SQL function to stop a running or paused connector worker. This function takes conninfo_name as its only parameter, which can be found from the output of `synchdb_get_state()` view.

For example:
```
select synchdb_stop_engine_bgw('mysqlconn');
select synchdb_stop_engine_bgw('mysqlconn2');
```

`synchdb_stop_engine_bgw()` function also marks a connection info as `inactive`, which prevents the connection from automatic-relaunch at server restarts. See below for more details.

### Metadata Files
During the synchdb operations, metadata files (offsets and schemahistory files) are generated by DBZ engine and they are by default located in `$PGDATA/pg_synchdb` directory:

```
ls $PGDATA/pg_synchdb
```

Sample output:
```
mysql_mysqlconn_offsets.dat        sqlserver_sqlserverconn_offsets.dat
mysql_mysqlconn_schemahistory.dat  sqlserver_sqlserverconn_schemahistory.dat
```

Each DBZ engine worker has its own pair of metadata files. These store the schema information to build the source tables and the offsets to resume upon restarting.

### Restarting Replication from the Beginning
To restart replication from the beginning:
1. Stop the Debezium engine:
```
select synchdb_stop_engine_bgw('mysqlconn');
```
2. Remove the metadata files:
```
rm $PGDATA/pg_synchdb/mysql_mysqlconn_offsets.dat
rm $PGDATA/pg_synchdb/mysql_mysqlconn_schemahistory.dat
```
3. Restart the Debezium engine:
```
select synchdb_start_engine_bgw('mysqlconn');
```

### Automatic Worker Launch at Server Restarts
It is possible to automatically Launch a connector worker that has been created by `synchdb_add_conninfo()` and started by `synchdb_start_engine_bgw()` at PostgreSQL server restart. To do so, we will need to add `synchdb` to `shared_preload_libraries` GUC option:

```
shared_preload_libraries = 'synchdb'
```

With `synchdb` preloaded and GUC option `synchdb.synchdb_auto_launcher` set to `true`, PostgreSQL will launch a `synchdb_auto_launcher` background worker that will retrieve all the conninfos in `synchdb_conninfo` table that is marked as `active` ( has `isactive` flag set to `true`). Then, it will start them automatically in the same state as when they exited. If `synchdb`.

## Synchdb GUC Variables
These are the current GUC variables specifically reserved for synchdb. More is expected to be added:

* synchdb.naptime - the frequency of a synchdb connector worker (in seconds) to fetch new changes from Debezium embedded engine (default: 5)
* synchdb.dml_use_spi - option to use SPI to handle DML operations. True = use SPI, false = use heap access method (default: false)
* synchdb.synchdb_auto_launcher - option to automatic launch connector workers at server restarts. This option works when synchdb is included in shared_preload_library option (default: true)

## Architecture
SynchDB extension consists of six major components:
* Debezium Runner Engine (Java)
* SynchDB Launcher
* SynchDB Worker
* Format Converter
* Replication Agent
* Table Synch Agent

Refer to the architecture diagram for a visual representation of the components and their interactions.
![img](https://www.highgo.ca/wp-content/uploads/2024/07/synchdb.drawio.png)

### Debezium Runner Engine (Java)
* A java application utilizing Debezium embedded library.
* Supports various connector implementations to replicate change data from various database types such as MySQL, Oracle, SQL Server, etc.
* Invoked by `SynchDB Worker` to initialize Debezium embedded library and receive change data.
* Send the change data to `SynchDB Worker` in generalized JSON format for further processing.

### SynchDB Launcher
* Responsible for creating and destroying SynchDB workers using PostgreSQL's background worker APIs.
* Configure each worker's connector type, destination database IPs, ports, etc.

### SynchDB Worker
* Instantiats a `Debezium Runner Engine` to replicate changes from a specific connector type.
* Communicate with Debezium Runner via JNI to receive change data in JSON formats.
* Transfer the JSON change data to `Format Converter` module for further processing.

### Format Converter
* Parse the JSON change data using PostgreSQL Jsonb APIs
* Transform DDL queries to PostgreSQL compatible queries and translate heterogeneous column data type to PostgreSQL compatible types.
* Transform DML queries to PostgreSQL compatible queries and handle data processing based on column data types
* Produces raw HeapTupleData which can be fed directly to Heap Access Method within PostgreSQL for faster executions.

### Replication Agent
* Processes the outputs from **`Format Converter`**.
* **`Format Converter`** will produce HeapTupleData format outputs, then **`Replication Agent`** will invoke PostgreSQL's heap access method routines to handle them.

### Table Sync Agent
* Design details and implementation are not available yet. TBD
* Intended to provide a more efficient alternative to perform initial table synchronization.

## Feature Highlights and TODOs

### Debezium Runner Engine (Java)
* support more connector types

### SynchDB Launcher
* Support more connector types
* Define and implement potential configuration parameters
* Configurable starting offsets for each worker
* Utility SQL functions to display worker status, current offsets, potential errors...etc
* Automatic worker re-launch upon PostgreSQL restart
* Support multiple workers for the same connector

### SynchDB Worker
* Support more column data type translations to PostgreSQL based on connector types
* Support ALTER TABLE, TRUNCATE, CREATE INDEX, DROP INDEX...

### Format Converter
* Support more data conversions based on column data type
* Support HeapTupleData conversions for DMLs

### Replication Agent
* Support direct data update via Heap Access Method in addition to SPI

### Table Synch Agent
* Design and POC.
* Proposed features:
	* Same table concurrency
	* Concurrent table synchronization
	* ...
