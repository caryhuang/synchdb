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

	sleep 10
	syncing_src_count=$(docker exec -i mysql mysql -umysqluser -pmysqlpwd -sN -e "SELECT COUNT(*) from inventory.orders" | tr -d ' \r\n')
	syncing_dst_count=$(psql -d postgres -t -c "SELECT COUNT(*) from inventory.orders;" | tr -d ' \n')
	if [ "$syncing_src_count" -ne "$syncing_dst_count" ]; then
		echo "initial snapshot failed. orders table count mismatch: src:$syncing_src_count vs dst:$syncing_dst_count"
		exit 1
	fi
	echo "initial snapshot test done, orders table count matched: src:$syncing_src_count vs dst:$syncing_dst_count"

	docker exec -i mysql mysql -umysqluser -pmysqlpwd -e "INSERT INTO inventory.orders(order_date, purchaser, quantity, product_id) VALUES ('2024-01-01', 1003, 2, 107)"
	sleep 10
	syncing_src_count=$(docker exec -i mysql mysql -umysqluser -pmysqlpwd -sN -e "SELECT COUNT(*) from inventory.orders" | tr -d ' \r\n')
	syncing_dst_count=$(psql -d postgres -t -c "SELECT COUNT(*) from inventory.orders;" | tr -d ' \n')
	if [ "$syncing_src_count" -ne "$syncing_dst_count" ]; then
		echo "CDC failed. orders table count mismatch: src:$syncing_src_count vs dst:$syncing_dst_count"
		exit 1
	fi
	echo "CDC test done, orders table count matched: src:$syncing_src_count vs dst:$syncing_dst_count"

	exit 0
}

function test_sqlserver()
{

	echo "testing sqlserver..."
	id=$(docker ps | grep sqlserver | awk '{print $1}')
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
	
	sleep 10
	syncing_src_count=$(docker exec -i $id /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -d testDB -C -Q "SELECT COUNT(*) from orders" -h -1 | sed -n '1p' | tr -d ' \r\n')
	syncing_dst_count=$(psql -d postgres -t -c "SELECT COUNT(*) from testDB.orders;" | tr -d ' \n')
	if [ "$syncing_src_count" -ne "$syncing_dst_count" ]; then
		echo "initial snapshot failed. orders table count mismatch: src:$syncing_src_count vs dst:$syncing_dst_count"
		exit 1
	fi
	echo "initial snapshot test done, orders table count matched: src:$syncing_src_count vs dst:$syncing_dst_count"

	docker exec -i $id /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -d testDB -C -Q "INSERT INTO orders(order_date, purchaser, quantity, product_id) VALUES ('2024-01-01', 1003, 2, 107)"
	sleep 10
	syncing_src_count=$(docker exec -i $id /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -d testDB -C -Q "SELECT COUNT(*) from orders" -h -1 | sed -n '1p' | tr -d ' \r\n')
    syncing_dst_count=$(psql -d postgres -t -c "SELECT COUNT(*) from testDB.orders;" | tr -d ' \n')

	if [ "$syncing_src_count" -ne "$syncing_dst_count" ]; then
		echo "CDC failed. orders table count mismatch: src:$syncing_src_count vs dst:$syncing_dst_count"
		exit 1
	fi
	echo "CDC test done, orders table count matched: src:$syncing_src_count vs dst:$syncing_dst_count"
	exit 0
}

function test_oracle()
{
	echo "testing oracle..."
	id=$(docker ps | grep oracle | awk '{print $1}')
	psql -d postgres -c "SELECT synchdb_add_conninfo('oracleconn','127.0.0.1', 1521, 'c##dbzuser', 'dbz', 'free', 'postgres', '', 'oracle');"
    if [ $? -ne 0 ]; then
        echo "failed to create connector"
        exit 1
    fi

    psql -d postgres -c "SELECT synchdb_start_engine_bgw('oracleconn');"
    if [ $? -ne 0 ]; then
        echo "failed to start connector"
        exit 1
    fi

	sleep 40
	syncing_src_count=$(docker exec -i $id sqlplus -S 'c##dbzuser/dbz@//localhost:1521/FREE' <<EOF | awk '{print $1}'
SET HEADING OFF;
SET FEEDBACK OFF;
SET PAGESIZE 0;
SELECT count(*) FROM test_table;
exit
EOF
)
	syncing_dst_count=$(psql -d postgres -t -c "SELECT COUNT(*) from free.test_table;" | tr -d ' \n')
    if [ "$syncing_src_count" -ne "$syncing_dst_count" ]; then
        echo "initial snapshot failed. orders table count mismatch: src:$syncing_src_count vs dst:$syncing_dst_count"
        exit 1
    fi
    echo "initial snapshot test done, orders table count matched: src:$syncing_src_count vs dst:$syncing_dst_count"

	docker exec -i $id sqlplus 'c##dbzuser/dbz@//localhost:1521/FREE' <<EOF
INSERT INTO test_table (
    id, binary_double_col, binary_float_col, float_col, number_col,
    long_col, date_col, interval_ds_col, interval_ym_col, timestamp_col,
    timestamp_tz_col, timestamp_ltz_col, char_col, nchar_col,
    nvarchar2_col, varchar_col, varchar2_col, raw_col,
    bfile_col, blob_col, clob_col, nclob_col, rowid_col, urowid_col
) VALUES (
    3, 12345.6789, 1234.56, 9876.54321, 1000.50,
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
    EMPTY_BLOB(),
    EMPTY_CLOB(),
    EMPTY_CLOB(),
    NULL,
    NULL
);
exit;
EOF
	sleep 20
	syncing_src_count=$(docker exec -i $id sqlplus -S 'c##dbzuser/dbz@//localhost:1521/FREE' <<EOF | awk '{print $1}'
SET HEADING OFF;
SET FEEDBACK OFF;
SET PAGESIZE 0;
SELECT count(*) FROM test_table;
exit
EOF
)
    syncing_dst_count=$(psql -d postgres -t -c "SELECT COUNT(*) from free.test_table;" | tr -d ' \n')
	 if [ "$syncing_src_count" -ne "$syncing_dst_count" ]; then
        echo "CDC failed. orders table count mismatch: src:$syncing_src_count vs dst:$syncing_dst_count"
        exit 1
    fi
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
if [ $? -ne 0 ]; then
	echo "failed to create synchdb extension"
	exit 1
fi

test_synchdb "${DBTYPE}"
