## Introduction

SynchDB is a PostgreSQL extension designed to replicate data from one or more heterogeneous databases (such as MySQL) directly to PostgreSQL in a fast and reliable way. PostgreSQL serves as the destinaton from multiple heterogeneous database sources. No middleware or third party software is required to orchaestrate the data syncrhonization between heterogeneous and PostgreSQL databases. SynchDB extension itself is capable of orchestrating all the data syncrhonization needs.

It provides two key work modes that can be invoked using the built-in SQL functions:
* Sync mode (for initial data synchronization)
* Follow mode (for replicate incremental changse after initial sync)

Sync mode copies a table from heterogeneous database into PostgreSQL, including its schema, indexes, triggers, other table properties as well as current data it holds. Follow mode subscribes to a table in heterogeneous database and obtain incremental changes and apply them to the same table PostgreSQL similar to PostgreSQL logical replication

## Requirement
The following software is required to build and run SynchDB. The versions listed are the versions tested during development. Older versions may still work.
* Java Development Kit 22. Download [here](https://www.oracle.com/ca-en/java/technologies/downloads/)
* Apache Maven 3.9.8. Download [here](https://maven.apache.org/download.cgi)
* PostgreSQL 16.3 Source. Git clone [here](https://github.com/postgres/postgres). Refer to this [wiki](https://wiki.postgresql.org/wiki/Compile_and_Install_from_source_code) for PostgreSQL build requirements
* docker compose 2.28.1 (for testing). Refer to [here](https://docs.docker.com/compose/install/linux/)
* Unix based operating system like (Ubuntu 22.04)

## Build Procedure
### Prepare Source

Clone the PostgreSQL source and switch to 16.3 release tag
```
git clone https://github.com/postgres/postgres.git
cd postgres
git checkout REL_16_3
```

Clone the SynchDB source from within the extension folder

```
cd contrib/
git clone https://github.com/Hornetlabs/synchdb.git
```

### Prepare Tools
#### Maven
Once Maven is downloaded (for example, apache-maven-3.9.8-bin.tar.gz), extract it:
```
tar xzvf apache-maven-3.9.8-bin.tar.gz -C /home/$USER
```
Add `mvn` to your $PATH environment variable so you can use `mvn` anywhere in the system:
```
export PATH=$PATH:/home/$USER/apache-maven-3.9.8/bin
```

Optionally, you can also add the above export command in .bashrc file so it is automatically run when you log in to system
```
echo "export PATH=$PATH:/home/$USER/apache-maven-3.9.8/bin" >> /home/$USER/.bashrc
```

#### Java
Once java is downloaded (for example, jdk-22_linux-x64_bin.tar.gz), extract it:
```
tar xzvf jdk-22_linux-x64_bin.tar.gz -C /home/$USER
```

Install java `include` and `lib` in your system paths so that SynchDB can link to them. The procedure installs them as symbolic links, you may also physically copy the files to the system paths.
```
sudo mkdir /usr/include/jdk-22.0.1
sudo mkdir /usr/lib/jdk-22.0.1
sudo ln -s /home/$USER/java/jdk-22.0.1/include /usr/include/jdk-22.0.1/include
sudo ln -s /home/$USER/java/jdk-22.0.1/lib /usr/lib/jdk-22.0.1/lib
```

Add Java biaries to your $PATH environment variable so you can use Java tools anywhere in the system:
```
export PATH=$PATH:/home/$USER/jdk-22.0.1/bin
```

Optionally, you can also add the above export command in .bashrc file so it is automatically run when you log in to system
```
echo "export PATH=$PATH:/home/$USER/jdk-22.0.1/bin" >> /home/$USER/.bashrc
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

then install dbz engine to PostgreSQL lib path
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

### Configure your Linker
Lastly, we also need to tell your system's linker where the newly added Java library is located in your system
```
sudo echo "/usr/lib/jdk-22.0.1/lib" >> /etc/ld.so.conf.d/x86_64-linux-gnu.conf
sudo echo "/usr/lib/jdk-22.0.1/lib/server" >> /etc/ld.so.conf.d/x86_64-linux-gnu.conf
```

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
We can start a sample SQL server database for testing using docker compose. The user credentials are described in the `synchdb-sqlserver-test.yaml` file
```
docker compose -f synchdb-sqlserver-test.yaml up -d
```

You may not have sql server client tool installed, you could login to SQL server container to access its client tool.

Find out the container ID for SQL server:
```
id=$(docker ps | grep sqlserver | awk '{print $1}')
```

copy the database schema into sql server container
```
docker cp inventory.sql $id:/
```

login to sqlserver container
```
docker exec -it $id bash
```

build the database accordign to schema
```
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -i /inventory.sql
```

run some simple queries
```
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -q "insert into orders(order_date, purchaser, quantity, product_id) values( '2024-01-01', 1003, 2, 107)"

/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -q "select * from orders"
```

### Prepare a sample PostgreSQL database
Initialize and start a new PostgreSQL database:

```
initdb -D synchdbtest
pg_ctl -D synchdbtest -l logfile start
```

Install SynchDB: Please make sure you have set the path to the Debezium Runner Engine Jar in enviornment variable DBZ_ENGINE_DIR. Otherwise, the extension will fail to create.
```
CREATE EXTENSION synchdb;
```

Use the SQL function `synchdb_start_engine_bgw` to spawn a new background worker to capture CDC from a heterogeneous database such as MySQL. `synchdb_start_engine_bgw` takes these arguments:
* hostname - the IP address of heterogeneous database
* port - the port number to connect to
* username - user name to use to connect
* password - password to authenticate the username
* source database (optional) - the database name to replicate changes from. For example, the database that exists in MySQL. If empty, all databases from MySQL are replicated.
* destination database - the database to apply changes to - For example, the database that exists in PostgreSQL. Must be a valid database that exists.
* table (optional) - expressed in the form of [database].[table] that must exists in MySQL so the engine will only replicate the specified tables. If empty, all tables are replicated 
* connector - the connector to use. Can be MySQL, Oracle, or SQLServer regardless of capital or lower case. Currently only MySQL is supported.

For example:
```
select synchdb_start_engine_bgw('127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', '', 'mysql');
```

This command will spawn a dedicated background worker that will replicate all tables from the 'inventory' database from MySQL database. 

If we are only interested in a few tables, we can call the SQL function like this:
```
select synchdb_start_engine_bgw('127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', 'inventory.orders,inventory.customers', 'mysql');
```

which will only replicate changes from `order` and `customers` tables under database `inventory` from MySQL database.

You may start another synchdb background worker replicating from SQL server to a different database within PostgreSQL :

```
create database sqlserverdb;
select synchdb_start_engine_bgw('127.0.0.1',1433, 'sa', 'Password!', 'testDB', 'sqlserverdb', '', 'sqlserver');
```

The above will spawn a second worker to replicate changes from SQL server to a PostgreSQL database called `sqlserverdb`

```
ps -ef | grep postgres
ubuntu    153865       1  0 15:49 ?        00:00:00 /usr/local/pgsql/bin/postgres -D ../../synchdbtest
ubuntu    153866  153865  0 15:49 ?        00:00:00 postgres: checkpointer
ubuntu    153867  153865  0 15:49 ?        00:00:00 postgres: background writer
ubuntu    153869  153865  0 15:49 ?        00:00:00 postgres: walwriter
ubuntu    153870  153865  0 15:49 ?        00:00:00 postgres: autovacuum launcher
ubuntu    153871  153865  0 15:49 ?        00:00:00 postgres: logical replication launcher
ubuntu    153875  153865  0 15:49 ?        00:00:46 postgres: synchdb engine: sqlserver@127.0.0.1:1433 -> sqlserverdb
ubuntu    153900  153865  0 15:49 ?        00:00:46 postgres: synchdb engine: mysql@127.0.0.1:3306 -> postgres
```

As you can see, there are 2 additional background workers, one replicating from MySQL, the other replicating from SQL server.

To stop a running synchdb background worker. Use the `synchdb_stop_engine_bgw` SQL function. `synchdb_stop_engine_bgw` takes this argument:
* connector - the conenctor type expressed as string, can be MySQL, Oracle, SQLServer regardless of capital or lower case. Currently only MySQL is supported.

For example:
```
select synchdb_stop_engine_bgw('mysql');
select synchdb_stop_engine_bgw('sqlserver');
```

The replicated changes will be mapped to a schema in destination database. For example, a table named `orders` from database `inventory` in MySQL will be replicated to a table named `orders` under schema `inventory`.

During the synchdb operations, metadata files (offsets and schemahistory files) will be generated by DBZ engine and they are by default located in `$PGDATA/pg_synchdb` directory:

```
ls $PGDATA/pg_synchdb
mysql_mysqluser@127.0.0.1_3306_offsets.dat        sqlserver_sa@127.0.0.1_1433_offsets.dat
mysql_mysqluser@127.0.0.1_3306_schemahistory.dat  sqlserver_sa@127.0.0.1_1433_schemahistory.dat

```

each DBZ engine worker has its own pair of metadata files. These store the schema information to build the source tables and the offsets to resume upon restarting.

If you would like synchdb to start replicating from the very beginning (for MySQL as an example). Follow these steps:

stop the dbz engine
```
select synchdb_stop_engine_bgw('mysql');
``` 

remove the files
```
rm $PGDATA/pg_synchdb/mysql_mysqluser@127.0.0.1_3306_offsets.dat
rm $PGDATA/pg_synchdb/mysql_mysqluser@127.0.0.1_3306_schemahistory.dat
```

restart the dbz engine
```
select synchdb_start_engine_bgw('127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', '', 'mysql');
```

## Architecture
SynchDB extension consists of 6 major components:
* Debezium Runner Engine (Java)
* SynchDB Launcher
* SynchDB Worker
* Format Converter
* Replication Agent
* Table Synch Agent

![img](https://www.highgo.ca/wp-content/uploads/2024/07/synchdb.drawio.png)

### Debezium Runner Engine (Java)
* it is a java application that utilizes Debezium embedded library that supports various connector implementations to replicate change data from various database types such as MySQL, Oracle, SQL server...etc.
* it is to be invokved by `SynchDB Worker` to initialize Debezium embedded library and to receive change data.
* the change data is sent to `SynchDB Worker` in generalized JSON format to be further processed. 

### SynchDB Laucnher
* responsible for creating and destroying synchdb workers using PostgreSQL's background worker APIs. 
* configure each worker's connector type, destination database IPs, ports...etc.

### SynchDB Worker
* responsible for instantiating a `Debezium Runner Engine` to replicate changes from a particular connector type.
* communicate with Debezium Runner via JNI to receive change data in JSON formats.
* transfer the JSON change data to `Format Converter` module for furher processing.
* 

### Format Converter
* parse the JSON change data using PostgreSQL Jsonb APIs
* transform DDL queries to PostgreSQL compatible queries and translate heterogeneous column data type to PostgreSQL compatible types.
* transform DML queries to PostgreSQL compatible queries and handle data processing based on column data types
* Currently, format converter produces PostgreSQL compatible queries for both DDL and DML, which is then passed to `Replication Agent` for furhter processing.
* in the future, for DMLs, we may change the outputs to be raw HeapTupleData which can be fed directly to Heap Access Method within PostgreSQL for faster executions.

### Replication Agent
* Takes the outputs from `Format Converter` and process them.
* Currently, `Format Converter` produces PostgreSQL compatible queries for both DDL and DML. so `Replication Agent` will invok PostgreSQL's SPI to execute these queries.
* In the future when `Format Converter` can produce HeapTupleData format outputs, then `Replication Agent` will have an option to invoke PostgreSQL's heap access method routines to handle them.

### Sync Agent
* design details and implementation are not available yet. TBD
* supposed to provide a more efficient alternative to perform initial table synchronization.

## Feature Highlights and TODOs

### Debezium Runner Engine (Java)
* support more connector types

### SynchDB Launcher
* support more connector types
* define and implement potential configuration parameters
* configurable starting offsets for each worker
* utility SQL functions to display worker status, current offsets, potential errors...etc
* automatic worker re-launch upon PostgreSQL restart
* support multiple workers for the same connector

### SynchDB Worker
* support more column data type translations to PostgreSQL based on connector types
* support ALTER TABLE, TRUNCATE, CREATE INDEX, DROP INDEX...

### Format Converter
* support more data conversions based on column data type
* support HeapTupleData conversions for DMLs

### Replication Agent
* support direct data update via Heap Access Method in addition to SPI

### Table Synch Agent
* design and POC
* proposed features:
	* same table concurrency
	* Concurrent table synchronization
	* ...
