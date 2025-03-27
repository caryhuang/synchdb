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


function test_mysql()
{
	echo "testing mysql..."
	id=$(docker ps | grep sqlserver | awk '{print $1}')

	psql -d postgres -c "SELECT synchdb_add_conninfo('mysqlconn', '127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', '', 'mysql');"
    if [ $? -ne 0 ]; then
        echo "failed to create connector"
        exit 1
    fi

	psql -d postgres -c "SELECT synchdb_start_engine_bgw('mysqlconn');"
    if [ $? -ne 0 ]; then
        echo "failed to start connector"
        exit 1
    fi

	sleep 5
	syncing_src_count=$(docker exec -i mysql mysql -umysqluser -pmysqlpwd -sN -e "SELECT COUNT(*) from inventory.orders" | tr -d ' \r\n')
	syncing_dst_count=$(psql -d postgres -t -c "SELECT COUNT(*) from inventory.orders;" | tr -d ' \n')
	if [ "$syncing_src_count" -ne "$syncing_dst_count" ]; then
		echo "initial snapshot failed. orders table count mismatch: src:$syncing_src_count vs dst:$syncing_dst_count"
		exit 1
	fi
	echo "initial snapshot done, orders table count matched: src:$syncing_src_count vs dst:$syncing_dst_count"

	docker exec -i mysql mysql -umysqluser -pmysqlpwd -e "INSERT INTO inventory.orders(order_date, purchaser, quantity, product_id) VALUES ('2024-01-01', 1003, 2, 107)"
	sleep 5
	syncing_src_count=$(docker exec -i mysql mysql -umysqluser -pmysqlpwd -sN -e "SELECT COUNT(*) from inventory.orders" | tr -d ' \r\n')
	syncing_dst_count=$(psql -d postgres -t -c "SELECT COUNT(*) from inventory.orders;" | tr -d ' \n')
	if [ "$syncing_src_count" -ne "$syncing_dst_count" ]; then
		echo "CDC failed. orders table count mismatch: src:$syncing_src_count vs dst:$syncing_dst_count"
		exit 1
	fi

	exit 0
}

function test_sqlserver()
{

	echo "testing sqlserver..."
	psql -d postgres -c "SELECT synchdb_add_conninfo('sqlserverconn', '127.0.0.1', 1433, 'sa', 'Password!', 'testDB', 'postgres', '', 'sqlserver');"
	if [ $? -ne 0 ]; then
    	echo "failed to create connector"
    	exit 1
	fi
	
	psql -d postgres -c "SELECT synchdb_start_engine_bgw('sqlserverconn');"
    if [ $? -ne 0 ]; then
        echo "failed to start connector"
        exit 1
    fi
	
	sleep 5
	syncing_src_count=$(docker exec -i $id /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -d testDB -C -Q "SELECT COUNT(*) from orders" -h -1 | sed -n '1p' | tr -d ' \r\n')
	syncing_dst_count=$(psql -d postgres -t -c "SELECT COUNT(*) from testDB.orders;" | tr -d ' \n')
	if [ "$syncing_src_count" -ne "$syncing_dst_count" ]; then
		echo "initial snapshot failed. orders table count mismatch: src:$syncing_src_count vs dst:$syncing_dst_count"
		exit 1
	fi
	echo "initial snapshot done, orders table count matched: src:$syncing_src_count vs dst:$syncing_dst_count"

	docker exec -i $id /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -d testDB -C -Q "INSERT INTO orders(order_date, purchaser, quantity, product_id) VALUES ('2024-01-01', 1003, 2, 107)"
	sleep 5
	syncing_src_count=$(docker exec -i $id /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -d testDB -C -Q "SELECT COUNT(*) from orders" -h -1 | sed -n '1p' | tr -d ' \r\n')
    syncing_dst_count=$(psql -d postgres -t -c "SELECT COUNT(*) from testDB.orders;" | tr -d ' \n')

	if [ "$syncing_src_count" -ne "$syncing_dst_count" ]; then
		echo "CDC failed. orders table count mismatch: src:$syncing_src_count vs dst:$syncing_dst_count"
		exit 1
	fi
	exit 0
}

function test_oracle()
{
	echo "testing oracle..."
	exit 0
}

function test_synchdb()
{
	dbtype="$1"

	case "$dbtype" in
		"mysql")
			test_mysql
			;;
		"sqlserver")
			test_sqlserver
			;;
		"oracle")
			test_oracle
			;;
		*)
			echo "$dbtype not supported"
			exit 1
			;;
	esac
}

# CREATE synchdb
echo "CREATE synchdb extension..."

psql -d postgres -c "CREATE EXTENSION synchdb CASCADE;"
if [ $? -ne 0 ];
	echo "failed to create synchdb extension"
	exit 1
fi

test_synchdb "${DBTYPE}"
