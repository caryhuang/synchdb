#!/bin/bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

# read pg major version, error if not provided
DBTYPE=${DBTYPE:?please provide database type}
WHICH=${WHICH:?please provide which hammerdb type, or n/a}

# get codename from release file
. /etc/os-release
codename=${VERSION#*(}
codename=${codename%)*}

# we'll do everything with absolute paths
basedir="$(pwd)"

MAX_TRIES=200

function wait_for_container_ready()
{
	name=$1
	keyword=$2
	ready=0

	set +e
	docker ps | grep $name
	if [ $? -ne 0 ]; then
		echo "$name is not running or failed to start!"
	fi	

	for ((i=1; i<=MAX_TRIES; i++));
	do
		docker logs "$name" 2>&1 | grep "$keyword"
		if [ $? -eq 0 ]; then
			echo "$name is ready to use"
			ready=1
			break
		else
			echo "Still waiting for $name to become ready..."
			sleep 10
		fi
	done

	if [ $ready -ne 1 ]; then
		echo "giving up after $MAX_TRIES on $name..."
		exit 1
	fi
	set -e
}

function setup_mysql()
{
	echo "setting up mysql..."
	docker-compose -f testenv/mysql/synchdb-mysql-test.yaml up -d
	echo "sleep to give container time to startup..."
	sleep 30  # Give containers time to fully start
	docker exec -i mysql mysql -uroot -pmysqlpwdroot -e "SELECT VERSION();" || { echo "Failed to connect to MySQL"; docker logs mysql; exit 1; }
	docker exec -i mysql mysql -uroot -pmysqlpwdroot << EOF
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqluser'@'%';
GRANT replication client ON *.* TO 'mysqluser'@'%';
GRANT replication slave ON *.* TO 'mysqluser'@'%';
GRANT all privileges on tpcc.* to mysqluser;
GRANT RELOAD ON *.* TO 'mysqluser'@'%';
FLUSH PRIVILEGES;
EOF
	exit 0
}

function setup_sqlserver()
{

	echo "setting up sqlserver..."
	docker-compose -f testenv/sqlserver/synchdb-sqlserver-test.yaml up -d
	echo "sleep to give container time to startup..."
	sleep 30  # Give containers time to fully start
	docker cp ./testenv/sqlserver/inventory.sql sqlserver:/
	docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -C -Q "SELECT @@VERSION" > /dev/null
	docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -U sa -P 'Password!' -C -i /inventory.sql > /dev/null
	exit 0
}

function setup_oracle()
{
	needsetup=0

	set +e
	echo "setting up oracle..."
	docker ps -a | grep "oracle"
	if [ $? -ne 0 ]; then
        # container has not run before. Run a new one
        docker-compose -f testenv/oracle/synchdb-oracle-test.yaml up -d
		wait_for_container_ready "oracle" "DATABASE IS READY TO USE"
        needsetup=1
    else
        docker ps -a | grep "oracle" | grep -i "Exited"
        if [ $? -eq 0 ]; then
            # container has been run before but stopped. Start it
            docker start oracle
            wait_for_container_ready "oracle" "DATABASE IS READY TO USE"
        else
            docker ps -a | grep "oracle" | grep -i "Up"
            if [ $? -eq 0 ]; then
                # container is running. Just use it
                echo "using exisitng oracle 19c container..."
            fi
        fi
    fi

	if [ $needsetup -ne 1 ]; then
        echo "skip setting up oracle, assuming it done..."
        exit 0
    fi
    set -e

	docker exec -i oracle mkdir /opt/oracle/oradata/recovery_area
	docker exec -i oracle sqlplus / as sysdba <<EOF
Alter user sys identified by oracle;
exit;
EOF
	sleep 1
	docker exec -i oracle sqlplus /nolog <<EOF
CONNECT sys/oracle as sysdba;
alter system set db_recovery_file_dest_size = 30G;
alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
archive log list;
exit;
EOF
	sleep 1
	docker exec -i oracle sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<EOF
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER PROFILE DEFAULT LIMIT FAILED_LOGIN_ATTEMPTS UNLIMITED;
exit;
EOF
	docker exec -i oracle sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<EOF
CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
EOF
	docker exec -i oracle sqlplus sys/oracle@//localhost:1521/FREEPDB1 as sysdba <<EOF
CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
EOF
	docker exec -i oracle sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<'EOF'
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
ALTER DATABASE ADD LOGFILE GROUP 4 ('/opt/oracle/oradata/FREE/redo04.log') SIZE 512M;
ALTER DATABASE ADD LOGFILE GROUP 5 ('/opt/oracle/oradata/FREE/redo05.log') SIZE 512M;
ALTER DATABASE ADD LOGFILE GROUP 6 ('/opt/oracle/oradata/FREE/redo06.log') SIZE 512M;
GRANT CONNECT, RESOURCE TO C##DBZUSER;
GRANT CREATE SESSION TO C##DBZUSER;
GRANT CREATE TABLE TO C##DBZUSER;
GRANT CREATE VIEW TO C##DBZUSER;
GRANT CREATE PROCEDURE TO C##DBZUSER;
GRANT CREATE SEQUENCE TO C##DBZUSER;
GRANT CREATE TRIGGER TO C##DBZUSER;
GRANT CREATE ANY INDEX TO C##DBZUSER;
GRANT CREATE ANY TABLE TO C##DBZUSER;
GRANT CREATE ANY PROCEDURE TO C##DBZUSER;
GRANT ALTER ANY TABLE TO C##DBZUSER;
GRANT EXECUTE ANY PROCEDURE TO C##DBZUSER;
GRANT DROP ANY TABLE TO C##DBZUSER;
GRANT DROP ANY INDEX TO C##DBZUSER;
GRANT DROP ANY SEQUENCE TO C##DBZUSER;
GRANT DROP ANY PROCEDURE TO C##DBZUSER;
GRANT DROP ANY VIEW TO C##DBZUSER;
GRANT DROP ANY TRIGGER TO C##DBZUSER;
GRANT DROP ANY SYNONYM TO C##DBZUSER;
GRANT DROP ANY MATERIALIZED VIEW TO C##DBZUSER;
exit;
EOF

	docker exec -i oracle sqlplus 'c##dbzuser/dbz@//localhost:1521/FREE' <<EOF
CREATE TABLE orders (
order_number NUMBER PRIMARY KEY,
order_date DATE,
purchaser NUMBER,
quantity NUMBER,
product_id NUMBER);
commit;
ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
exit;
EOF

	docker exec -i oracle sqlplus 'c##dbzuser/dbz@//localhost:1521/FREE' <<EOF
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10001, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10002, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10003, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10004, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
commit;
exit;
EOF
	exit 0
}

function setup_ora19c()
{
	needsetup=0

	set +e
	echo "setting up oracle 19c..."
	docker ps -a | grep "ora19c"
	if [ $? -ne 0 ]; then
		# container has not run before. Run a new one
    	docker run -d --name ora19c -p 1521:1521 -e ORACLE_SID=FREE -e ORACLE_PWD=oracle -e ORACLE_CHARACTERSET=AL32UTF8 doctorkirk/oracle-19c
		wait_for_container_ready "ora19c" "DATABASE IS READY TO USE"
		needsetup=1	
	else
		docker ps -a | grep "ora19c" | grep -i "Exited"
		if [ $? -eq 0 ]; then
			# container has been run before but stopped. Start it
			docker start 19c-oracle
			wait_for_container_ready "ora19c" "DATABASE IS READY TO USE"
		else
			docker ps -a | grep "ora19c" | grep -i "Up"
			if [ $? -eq 0 ]; then
				# container is running. Just use it
				echo "using exisitng oracle 19c container..."
			fi
		fi
	fi

	if [ $needsetup -ne 1 ]; then
		echo "skip setting up oracle 19c, assuming it done..."
		exit 0
	fi
	set -e

	docker exec -i ora19c mkdir /opt/oracle/oradata/recovery_area
	sleep 1
	docker exec -i ora19c sqlplus /nolog <<EOF
CONNECT sys/oracle as sysdba;
alter system set db_recovery_file_dest_size = 30G;
alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
archive log list;
exit;
EOF

	sleep 1
	docker exec -i ora19c sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<EOF
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER PROFILE DEFAULT LIMIT FAILED_LOGIN_ATTEMPTS UNLIMITED;
exit;
EOF

docker exec -i ora19c sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<EOF
CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
EOF

docker exec -i ora19c sqlplus sys/oracle@//localhost:1521/FREE as sysdba <<'EOF'
CREATE USER DBZUSER IDENTIFIED BY dbz DEFAULT TABLESPACE LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS;
GRANT CREATE SESSION TO DBZUSER;
GRANT SET CONTAINER TO DBZUSER;
GRANT SELECT ON V_$DATABASE TO DBZUSER;
GRANT FLASHBACK ANY TABLE TO DBZUSER;
GRANT SELECT ANY TABLE TO DBZUSER;
GRANT SELECT_CATALOG_ROLE TO DBZUSER;
GRANT EXECUTE_CATALOG_ROLE TO DBZUSER;
GRANT SELECT ANY TRANSACTION TO DBZUSER;
GRANT LOGMINING TO DBZUSER;
GRANT SELECT ANY DICTIONARY TO DBZUSER;
GRANT CREATE TABLE TO DBZUSER;
GRANT LOCK ANY TABLE TO DBZUSER;
GRANT CREATE SEQUENCE TO DBZUSER;	
GRANT EXECUTE ON DBMS_LOGMNR TO DBZUSER;
GRANT EXECUTE ON DBMS_LOGMNR_D TO DBZUSER;	
GRANT SELECT ON V_$LOG TO DBZUSER;
GRANT SELECT ON V_$LOG_HISTORY TO DBZUSER;	
GRANT SELECT ON V_$LOGMNR_LOGS TO DBZUSER ;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO DBZUSER ;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO DBZUSER;
GRANT SELECT ON V_$LOGFILE TO DBZUSER ;
GRANT SELECT ON V_$ARCHIVED_LOG TO DBZUSER ;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO DBZUSER;
GRANT SELECT ON V_$TRANSACTION TO DBZUSER; 
GRANT SELECT ON V_$MYSTAT TO DBZUSER;
GRANT SELECT ON V_$STATNAME TO DBZUSER; 	
GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO DBZUSER;
GRANT SELECT ON DBA_HIST_SNAPSHOT TO DBZUSER;
GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO PUBLIC;	
GRANT CREATE ANY DIRECTORY TO DBZUSER;

GRANT SELECT, FLASHBACK ON SYS.CCOL$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.CDEF$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.COL$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.DEFERRED_STG$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.ECOL$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.LOB$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.LOBCOMPPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.LOBFRAG$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.OBJ$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TAB$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TABCOMPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TABPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TABSUBPART$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.TS$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON SYS.USER$ TO DBZUSER;
GRANT SELECT, FLASHBACK ON XDB.XDB$TTSET TO DBZUSER;
GRANT FLASHBACK ANY TABLE TO DBZUSER;
GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO DBZUSER;
GRANT SELECT ON SYS.V_$DATABASE TO DBZUSER;
GRANT SELECT ON SYS.V_$DATABASE_INCARNATION TO DBZUSER;
GRANT SELECT ON SYS.V_$LOG TO DBZUSER;
GRANT SELECT ON SYS.V_$LOGFILE TO DBZUSER;
GRANT SELECT ON SYS.V_$PARAMETER TO DBZUSER;
GRANT SELECT ON SYS.V_$STANDBY_LOG TO DBZUSER;
GRANT SELECT ON SYS.V_$TRANSPORTABLE_PLATFORM TO DBZUSER;
DECLARE
    CURSOR C1 IS SELECT TOKSUF FROM XDB.XDB$TTSET;
    CMD VARCHAR2(2000);
BEGIN
    FOR C IN C1 LOOP
        CMD := 'GRANT SELECT, FLASHBACK ON XDB.X$NM' || C.TOKSUF || ' TO DBZUSER';
        EXECUTE IMMEDIATE CMD;
        CMD := 'GRANT SELECT, FLASHBACK ON XDB.X$QN' || C.TOKSUF || ' TO DBZUSER';
        EXECUTE IMMEDIATE CMD;
        CMD := 'GRANT SELECT, FLASHBACK ON XDB.X$PT' || C.TOKSUF || ' TO DBZUSER';
        EXECUTE IMMEDIATE CMD;
    END LOOP;
END;
/
EOF

    docker exec -i ora19c sqlplus 'DBZUSER/dbz@//localhost:1521/FREE' <<EOF
CREATE TABLE orders (
order_number NUMBER PRIMARY KEY,
order_date DATE,
purchaser NUMBER,
quantity NUMBER,
product_id NUMBER);
commit;
ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
exit;
EOF

    docker exec -i ora19c sqlplus 'DBZUSER/dbz@//localhost:1521/FREE' <<EOF
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10001, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10002, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10003, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
INSERT INTO orders(order_number, order_date, purchaser, quantity, product_id) VALUES (10004, TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1003, 2, 107);
commit;
exit;
EOF

}

function setup_hammerdb()
{
	echo "setting up hammerdb for $which..."
	docker run -d --name hammerdb hgneon/hammerdb:5.0
	echo "sleep to give container time to startup..."
	sleep 5
	
	docker cp ./testenv/hammerdb/${which}_buildschema.tcl hammerdb:/buildschema.tcl
	docker cp ./testenv/hammerdb/${which}_runtpcc.tcl hammerdb:/runtpcc.tcl
	exit 0
}

function setup_remotedb()
{
	dbtype="$1"
	which="$2"

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
		"ora19c")
			setup_ora19c
			;;
		"hammerdb")
			setup_hammerdb $which
			;;
		*)
			echo "$dbtype not supported"
			exit 1
			;;
	esac
}

setup_remotedb "${DBTYPE}" "${WHICH}"
