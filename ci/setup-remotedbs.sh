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

# get the project and clear out the git repo (reduce workspace size
rm -rf "${basedir}/.git"


function setup_mysql()
{
	echo "setting up mysql"
	docker-compose -f testenv/mysql/synchdb-mysql-test.yaml up -d
	echo "sleep to give container time to startup..."
	sleep 20  # Give containers time to fully start
	docker exec -i mysql mysql -uroot -pmysqlpwdroot -e "SELECT VERSION();" || { echo "Failed to connect to MySQL"; docker logs mysql; exit 1; }
	docker exec -i mysql mysql -uroot -pmysqlpwdroot << EOF
GRANT replication client ON *.* TO mysqluser;
GRANT replication slave ON *.* TO mysqluser;
GRANT RELOAD ON *.* TO 'mysqluser'@'%';
FLUSH PRIVILEGES;
EOF
	exit0
}

function setup_sqlserver()
{

	echo "setting up sqlserver"
	docker-compose -f testenv/sqlserver/synchdb-sqlserver-test.yaml up -d
	echo "sleep to give container time to startup..."
	sleep 20  # Give containers time to fully start
	id=$(docker ps | grep sqlserver | awk '{print $1}')
	docker cp inventory.sql $id:/
	docker exec -i $id /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -C -Q "SELECT @@VERSION"
	docker exec -i $id /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -C -i /inventory.sql
	exit 0
}

function setup_oracle()
{
	echo "setting up oracle"
	exit 0
}

function setup_remotedb()
{
	dbtype="$1"

	case "$dbtype" in
		"mysql")
			setup_mysql
			;;
		"sqlserver")
			setup_sqlserver
			;;
		"oracle")
			setup_oracle
			;;
		*)
			echo "$dbtype not supported"
			exit 1
			;;
	esac
}

setup_remotedb "${DBTYPE}"
