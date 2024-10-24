## Introduction

SynchDB is a PostgreSQL extension designed to replicate data from one or more heterogeneous databases (such as MySQL, MS SQLServer, Oracle, etc.) directly to PostgreSQL in a fast and reliable way. PostgreSQL serves as the destination from multiple heterogeneous database sources. No middleware or third-party software is required to orchestrate the data synchronization between heterogeneous databases and PostgreSQL. SynchDB extension itself is capable of handling all the data synchronization needs.

Visit SynchDB documentation site [here](https://docs.synchdb.com/) for more details.

## Architecture
SynchDB extension consists of six major components:
* Debezium Runner Engine (Java)
* SynchDB Launcher
* SynchDB Worker
* Format Converter
* Replication Agent
* Table Synch Agent (TBD)

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

## Build Requirement
The following software is required to build and run SynchDB. The versions listed are the versions tested during development. Older versions may still work.
* Java Development Kit 17 or later. Download [here](https://www.oracle.com/ca-en/java/technologies/downloads/)
* Apache Maven 3.6.3 or later. Download [here](https://maven.apache.org/download.cgi)
* PostgreSQL 16.3 Source. Git clone [here](https://github.com/postgres/postgres). Refer to this [wiki](https://wiki.postgresql.org/wiki/Compile_and_Install_from_source_code) for PostgreSQL build requirements
* Docker compose 2.28.1 (for testing). Refer to [here](https://docs.docker.com/compose/install/linux/)
* Unix based operating system like Ubuntu 22.04 or MacOS

## Build Procedure
### Prepare Source

Clone the PostgreSQL source and switch to 16.3 release tag
``` BASH
git clone https://github.com/postgres/postgres.git --branch REL_16_3
cd postgres
```

Clone the SynchDB source from within the extension folder
``` BASH
cd contrib/
git clone https://github.com/Hornetlabs/synchdb.git
```

### Prepare Tools
#### Maven
If you are working on Ubuntu 22.04.4 LTS, install the Maven as below:
``` BASH
sudo apt install maven
```

if you are using MacOS, you can use the brew command to install maven (refer (here)[https://brew.sh/] for how to install Homebrew) without any other settings:
``` BASH
brew install maven
```

#### Install Java (OpenJDK)
If you are working on Ubuntu 22.04.4 LTS, install the OpenJDK  as below:
``` BASH
sudo apt install openjdk-21-jdk
```

If you are working on MacOS, please install the JDK with brew command:
``` BASH
brew install openjdk@22
```

### Build and Install PostgreSQL
This can be done by following the standard build and install procedure as described [here](https://www.postgresql.org/docs/current/install-make.html). Generally, it consists of:

``` BASH
cd /home/$USER/postgres
./configure
make
sudo make install
```

You should build and install the default extensions as well:
``` BASH
cd /home/$USER/postgres/contrib
make
sudo make install
```

### Build Debezium Runner Engine
The commands below build and install the Debezium Runner Engine jar file to your PostgreSQL's lib folder.

``` BASH
cd /home/$USER/postgres/contrib/synchdb
make build_dbz
sudo make install_dbz
```

### Build SynchDB PostgreSQL Extension
The commands below build and install SynchDB extension to your PostgreSQL's lib and share folder.

``` BASH
cd /home/$USER/postgres/contrib/synchdb
make
sudo make install
```

### Configure your Linker (Ubuntu)
Lastly, we also need to tell your system's linker where the newly added Java library (libjvm.so) is located in your system.

``` BASH
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
``` BASH
sudo ldconfig
```

Ensure synchdo.so extension can link to libjvm Java library on your system:
``` BASH
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
Refer to `testenv/README.md` or visit our documentation page [here](https://docs.synchdb.com/user-guide/prepare_tests_env/) to prepare a sample MySQL and SQLServer database instances for testing.

SynchDB extension requires pgcrypto to encrypt certain sensitive credential data. Please make sure it is installed prior to installing SynchDB. Alternatively, you can include `CASCADE` clause in `CREATE EXTENSION` to automatically install dependencies:

``` SQL
CREATE EXTENSION synchdb CASCADE;
```

### Create a Connection Information
With SynchDB installed, we can create one or more connector information entries that represent the details to connect to a remote heterogeneous database and what to replicate. This can be done with `synchdb_add_conninfo()` function.

For example:
``` SQL
# create a mysql conninfo called 'mysqlconn' to replicate inventory.orders and inventory.customers tables from source database 'inventory' to destination database 'postgres' using default transform rules
SELECT synchdb_add_conninfo('mysqlconn','127.0.0.1',3306,'mysqluser', 'mysqlpwd', 'inventory', 'postgres', 'inventory.orders,inventory.customers', 'mysql', '');

# create a sqlserver conninfo called 'sqlserverconn' to replicate all tables from source database 'testDB' to destination database 'sqlserverdb' using default transform rules
SELECT synchdb_add_conninfo('sqlserverconn','127.0.0.1',1433,'sa', 'Password!', 'testDB', 'sqlserverdb', '', 'sqlserver', '');
```

### Review all Connection Information Created
All connection information are created in the table `synchdb_conninfo`. We are free to view its content and make modification as required. Please note that the password of a user credential is encrypted by pgcrypto using a key only known to synchdb. So please do not modify the password field or it may be decrypted incorrectly if tempered. See below for an example output: 

``` SQL
postgres=# \x
Expanded display is on.

postgres=# select * from synchdb_conninfo;
-[ RECORD 1 ]-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name | mysqlconn
data | {"pwd": "\\xc30d040703024828cc4d982e47b07bd23901d03e40da5995d2a631fb89d49f748b87247aee94070f71ecacc4990c3e71cad9f68d57c440de42e35bcc78fd145feab03452e454284289db", "port": 3306, "user": "mysqluser", "dstdb": "postgres", "srcdb": "inventory", "table": "inventory.orders,inventory.customers", "hostname": "192.168.1.86", "connector": "mysql", "null"}
-[ RECORD 2 ]-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
name | sqlserverconn
data | {"pwd": "\\xc30d0407030231678e1bb0f8d3156ad23a010ca3a4b0ad35ed148f8181224885464cdcfcec42de9834878e2311b343cd184fde65e0051f75d6a12d5c91d0a0403549fe00e4219215eafe1b", "port": 1433, "user": "sa", "dstdb": "sqlserverdb", "srcdb": "testDB", "table": "null", "hostname": "192.168.1.86", "connector": "sqlserver", "null"}
```

### Start a Connector Worker
Use `synchdb_start_engine_bgw()` function to start a connector worker. It takes one argument which is the connection information name created above. the commands below will spawn a worker for each conenctor with the specified replication configuration.

``` SQL
select synchdb_start_engine_bgw('mysqlconn');
select synchdb_start_engine_bgw('sqlserverconn');
```

### View the Connector States
Use `synchdb_state_view()` to examine all the running connectors and their states. Currently, synchdb can support up to 30 running workers.

``` SQL
postgres=# select * from synchdb_state_view;
 id | connector | conninfo_name  |  pid   |  state  |   err    |                                          last_dbz_offset
----+-----------+----------------+--------+---------+----------+---------------------------------------------------------------------------------------------------
  0 | mysql     | mysqlconn      | 461696 | syncing | no error | {"ts_sec":1725644339,"file":"mysql-bin.000004","pos":138466,"row":1,"server_id":223344,"event":2}
  1 | sqlserver | sqlserverconn  | 461739 | syncing | no error | {"event_serial_no":1,"commit_lsn":"00000100:00000c00:0003","change_lsn":"00000100:00000c00:0002"}
  3 | null      |                |     -1 | stopped | no error | no offset
  4 | null      |                |     -1 | stopped | no error | no offset
  5 | null      |                |     -1 | stopped | no error | no offset
  ...
  ...
```

### Stop a Connector Worker
Use `synchdb_stop_engine_bgw()` SQL function to stop a running or paused connector worker. This function takes `conninfo_name` as its only parameter, which can be found from the output of `synchdb_get_state()` view.

``` SQL
select synchdb_stop_engine_bgw('mysqlconn');
```
