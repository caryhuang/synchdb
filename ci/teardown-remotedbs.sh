#!/bin/bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

# read pg major version, error if not provided
DBTYPE=${DBTYPE:?please provide database type}

# get codename from release file
. /etc/os-release
codename=${VERSION#*(}
codename=${codename%)*}

# we'll do everything with absolute paths
basedir="$(pwd)"

function teardown_mysql()
{
	echo "tearing down mysql..."
	docker-compose -f testenv/mysql/synchdb-mysql-test.yaml down
}

function teardown_sqlserver()
{

	echo "tearing down sqlserver..."
	docker-compose -f testenv/sqlserver/synchdb-sqlserver-test.yaml down
}

function teardown_oracle()
{
	echo "tearing down oracle..."
	docker-compose -f testenv/oracle/synchdb-oracle-test.yaml down
}

function teardown_ora19c()
{
	echo "tearing down ora19c..."
	docker stop ora19c
	docker rm ora19c
}

function teardown_hammerdb()
{
	echo "tearing down hammerdb..."
	docker stop	hammerdb
	docker rm hammerdb
}

function teardown_olr()
{
	echo "tearing down olr..."
	docker stop OpenLogReplicator
	docker rm OpenLogReplicator
}

function teardown_oranet()
{
	echo "tearing down oranet..."
	docker network rm oranet
}

function teardown_remotedb()
{
	dbtype="$1"

	case "$dbtype" in
		"mysql")
			teardown_mysql
			;;
		"sqlserver")
			teardown_sqlserver
			;;
		"oracle")
			teardown_oracle
			;;
		"ora19c")
			teardown_ora19c
			;;
		"hammerdb")
			teardown_hammerdb
			;;
		"olr")
			teardown_olr
			teardown_ora19c
			teardown_oranet
			;;
		*)
			echo "$dbtype not supported"
			exit 1
			;;
	esac
}

teardown_remotedb "${DBTYPE}"
