## Introduction

SynchDB is a PostgreSQL extension designed to replicate data from one or more heterogeneous databases (such as MySQL) directly to PostgreSQL in a fast and reliable way. PostgreSQL serves as the destinaton from multiple heterogeneous database sources. No middleware or third party software is required to orchaestrate the data syncrhonization between heterogeneous and PostgreSQL databases. SynchDB extension itself is capable of orchestrating all the data syncrhonization needs.

It provides two key work modes that can be invoked using the built-in SQL functions:
* Sync mode (for initial data synchronization)
* Follow mode (for replicate incremental changse after initial sync)

Sync mode copies a table from heterogeneous database into PostgreSQL, including its schema, indexes, triggers, other table properties as well as current data it holds
follow mode subscribes to a table in heterogeneous database and obtain incremental changes and apply them to the same table PostgreSQL similar to PostgreSQL logical replication

## Architecture
SynchDB extension consists of 4 major components:
* heterogeneous connector (MySQL for now)
* format converter
* replication agent
* sync agent

![img](https://www.highgo.ca/wp-content/uploads/2024/05/syncdb-Page-2.drawio.png)

### Heterogeneous connector
* communicates with a remote heterogeneous database (such as MySQL) with appropriate driver.
* obtains heterogeneous data and pass it down to format converter.

### Format Converter
* formats heterogeneous data into a format that PostgreSQL understands.
* formatted data can be used by replication and sync agent.

### replication Agent
* triggers heterogeneous connector to obtain incremental changes from heterogeneous database (such as MySQL) .
* takes the formatted data from format converter and interacts with PostgreSQL core to insert, update, delete or truncate a desginated table.

### Sync Agent
* triggers initial table sync via heterogeneous connector
* takes the formatted data from format converter and interacts with PostgreSQL core to synchronized schema, indexes, and other table properties as well as copies initial data.

## General Scope of Work
* all features to be made into a single PostgreSQL extension called SynchDB
* starts with MySQL as the only heterogeneous database to synchronize to PostgreSQL
* support 2 operating modes, sync vs follow and user is able to choose which one to use via a SQL function call
* allow user to query current copy status with a few SQL function calls
* ...


## Feature Highlights (More to Come)
* synce mode for initial data, schema, index, trigger...etc synchronizaation
* follow mode for logically replicating incremental data changes
* DDL and DML Synchronization
* Concurrent table synchronization
* same table concurrency
* index concurrency
* ...

