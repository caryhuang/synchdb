## Introduction

SynchDB is a PostgreSQL extension designed to replicate data from one or more heterogeneous databases (such as MySQL, MS SQLServer, Oracle, etc.) directly to PostgreSQL in a fast and reliable way. PostgreSQL serves as the destination from multiple heterogeneous database sources. No middleware or third party software is required to orchestrate the data synchronization between heterogeneous databases and PostgreSQL. SynchDB extension itself is capable of handling all the data synchronization needs.

It provides two key work modes that can be invoked using the built-in SQL functions:
* Sync mode (for initial data synchronization)
* Follow mode (for replicate incremental changes after initial sync)

*Sync mode* copies tables from heterogeneous database into PostgreSQL, including its schema, indexes, triggers, other table properties as well as current data it holds.
*Follow mode* subscribes to tables in a heterogeneous database to obtain incremental changes and apply them to the same tables in PostgreSQL, similar to PostgreSQL logical replication

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
Once Maven is downloaded (for example, apache-maven-3.9.8-bin.tar.gz), extract it:
```
tar xzvf apache-maven-3.9.8-bin.tar.gz -C /home/$USER
```
Add `mvn` to your $PATH environment variable so you can use `mvn` anywhere in the system:
```
export PATH=$PATH:/home/$USER/apache-maven-3.9.8/bin
```

Optionally, you can also add the above export command in .bashrc file so so it is automatically run when you log in to system.
```
echo "export PATH=$PATH:/home/$USER/apache-maven-3.9.8/bin" >> /home/$USER/.bashrc
```

if you are using MacOS, you can use the brew command to install maven (refer (here)[https://brew.sh/] for how to install Homebrew) without any other settings:
```
brew install maven
```

#### Install Java SDK
Once java is downloaded (for example, jdk-22_linux-x64_bin.tar.gz), extract it:
```
tar xzvf jdk-22_linux-x64_bin.tar.gz -C /home/$USER
```

Add Java binaries to your $PATH environment variable so you can use Java tools anywhere in the system:
```
export PATH=$PATH:/home/$USER/jdk-22.0.1/bin
```

Optionally, you can also add the above export command in .bashrc file so it is automatically run when you log in to system
```
echo "export PATH=$PATH:/home/$USER/jdk-22.0.1/bin" >> /home/$USER/.bashrc
```

#### Install Java SDK on MacOS

If you are working on MacOS, please install the JDK with brew command:
```
brew install openjdk@22
```
If you need to have openjdk@22 first in your PATH, run:
```
echo 'export PATH="/opt/homebrew/opt/openjdk@22/bin:$PATH"' >> ~/.zshrc
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
Modify the below JDK directory as needed based on your JDK installation.
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
We can start a sample SQL Server database for testing using docker compose. The user credentials are described in the `synchdb-sqlserver-test.yaml` file
```
docker compose -f synchdb-sqlserver-test.yaml up -d
```

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

Run some simple queries:
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

Install SynchDB extension to the database:
Please make sure you have set the path to the Debezium Runner Engine Jar in environment variable DBZ_ENGINE_DIR. Otherwise, the extension will fail to create.
```
CREATE EXTENSION synchdb;
```

Use the SQL function `synchdb_start_engine_bgw` to spawn a new background worker to capture CDC from a heterogeneous database such as MySQL.

## Usage of `synchdb_start_engine_bgw`
`synchdb_start_engine_bgw` takes these arguments:
* `hostname` - the IP address of heterogeneous database.
* `port` - the port number to connect to.
* `username` - user name to use to connect.
* `password` - password to authenticate the username.
* `source database` (optional) - the database name to replicate changes from. For example, the database that exists in MySQL. If empty, all databases from MySQL are replicated.
* `destination` database - the database to apply changes to - For example, the database that exists in PostgreSQL. Must be a valid database that exists.
* `table` (optional) - expressed in the form of `[database].[table]` that must exists in MySQL so the engine will only replicate the specified tables. If empty, all tables are replicated.
* `connector` - the connector to use (MySQL, Oracle, or SQLServer). Currently only MySQL and SQLServer are supported.

### Example Usage
To replicate all tables from the 'inventory' database from MySQL:
```
select synchdb_start_engine_bgw('127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', '', 'mysql');
```
This command will spawn a dedicated background worker that will replicate all tables from the 'inventory' database from MySQL database.

To replicate only specific tables (`orders` and `customers`):
```
select synchdb_start_engine_bgw('127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', 'inventory.orders,inventory.customers', 'mysql');
```

The replicated changes will be mapped to a schema in destination database. For example, a table named `orders` from database `inventory` in MySQL will be replicated to a table named `orders` under schema `inventory`.

To replicate from SQL Server to a different database in PostgreSQL:
```
create database sqlserverdb;
select synchdb_start_engine_bgw('127.0.0.1',1433, 'sa', 'Password!', 'testDB', 'sqlserverdb', '', 'sqlserver');
```

The above will spawn a second worker to replicate changes from SQL server to a PostgreSQL database called `sqlserverdb`.

### Checking Running Workers
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

### Stopping a Worker
To stop a running SynchDB background worker. Use the `synchdb_stop_engine_bgw` SQL function:
```
select synchdb_stop_engine_bgw('mysql');
select synchdb_stop_engine_bgw('sqlserver');
```

 `synchdb_stop_engine_bgw` takes this argument:
* connector - the connector type expressed as string, can be MySQL, Oracle, SQLServer regardless of capital or lower case. Currently only `mysql` and `sqlserver` are supported.

### Metadata Files
During the synchdb operations, metadata files (offsets and schemahistory files) are generated by DBZ engine and they are by default located in `$PGDATA/pg_synchdb` directory:

```
ls $PGDATA/pg_synchdb
```

Sample output:
```
mysql_mysqluser@127.0.0.1_3306_offsets.dat        sqlserver_sa@127.0.0.1_1433_offsets.dat
mysql_mysqluser@127.0.0.1_3306_schemahistory.dat  sqlserver_sa@127.0.0.1_1433_schemahistory.dat
```

Each DBZ engine worker has its own pair of metadata files. These store the schema information to build the source tables and the offsets to resume upon restarting.

### Restarting Replication from the Beginning
To restart replication from the beginning:
1. Stop the Debezium engine:
```
select synchdb_stop_engine_bgw('mysql');
```
2. Remove the metadata files:
```
rm $PGDATA/pg_synchdb/mysql_mysqluser@127.0.0.1_3306_offsets.dat
rm $PGDATA/pg_synchdb/mysql_mysqluser@127.0.0.1_3306_schemahistory.dat
```
3. Restart the Debezium engine:
```
select synchdb_start_engine_bgw('127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', '', 'mysql');
```

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
* Send the change data to `SynchDB Worker` in generalized JSON format for further processeing.

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
* Processes the outputs from `Format Converter`.
* `Format Converter` will produce HeapTupleData format outputs, then `Replication Agent` will invoke PostgreSQL's heap access method routines to handle them.

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
