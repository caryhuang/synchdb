
import subprocess
import socket

MYSQL_HOST="127.0.0.1"
MYSQL_PORT=3306
MYSQL_USER="mysqluser"
MYSQL_PASS="mysqlpwd"
MYSQL_DB="inventory"

SQLSERVER_HOST="127.0.0.1"
SQLSERVER_PORT=1433
SQLSERVER_USER="sa"
SQLSERVER_PASS="Password!"
SQLSERVER_DB="testDB"

ORACLE_HOST="127.0.0.1"
ORACLE_PORT=1521
ORACLE_USER="c##dbzuser"
ORACLE_PASS="dbz"
ORACLE_DB="FREE"


def getConnectorName(dbvendor):
    if dbvendor == "mysql":
        return "mysqlconn"
    elif dbvendor == "sqlserver":
        return "sqlserverconn"
    else:
        return "oracleconn"

def getDbname(dbvendor):
    if dbvendor == "mysql":
        return MYSQL_DB
    elif dbvendor == "sqlserver":
        return SQLSERVER_DB
    else:
        return ORACLE_DB

def run_pg_query(cursor, query):
    cursor.execute(query)
    if cursor.description:  # Only fetch if query returns results
        return cursor.fetchall()
    return None

def run_pg_query_one(cursor, query):
    cursor.execute(query)
    row = cursor.fetchone()
    return row

## compare data types. Note: not all combinations are included here
## just enough to get the tests to run and pass... Feel free to add
## more for more complex tests
def verify_default_type_mappings(exttype, pgtype, dbvendor):

    if exttype == pgtype:
        return True

    if exttype == "int" and pgtype == "integer":
        return True
    elif exttype == "varchar" and pgtype == "character varying":
        return True
    elif exttype == "enum" and pgtype == "text":
        return True
    elif exttype == "geometry" and pgtype == "text":
        return True
    elif exttype == "float" and pgtype == "real":
        return True
    elif exttype == "double" and pgtype == "double precision":
        return True
    elif exttype == "int identity" and pgtype == "integer":
        return True
    elif exttype == "number" and pgtype == "numeric":
        return True
    elif exttype == "date" and pgtype == "timestamp without time zone":
        return True

    print(f"type unmatched: {exttype} : {pgtype} for {dbvendor}")
    return False

def run_remote_query(where, query):
    
    try:
        if where == "mysql":
            result = subprocess.check_output(["docker", "exec", "-i", "mysql", "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASS}", "-D", f"{MYSQL_DB}", "-sN", "-e", f"{query}"], text=True ).strip()
            rows = []
            for line in result.splitlines():
                cols = line.strip().split("\t")
                rows.append(tuple(cols))
        
        elif where == "sqlserver":
            container_id = subprocess.check_output(
            "docker ps | grep sqlserver | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running SQL Server container found.")

            result = subprocess.check_output(["docker", "exec", "-i", f"{container_id}", "/opt/mssql-tools18/bin/sqlcmd", f"-U{SQLSERVER_USER}", f"-P{SQLSERVER_PASS}", "-d", f"{SQLSERVER_DB}", "-C", "-h", "-1", "-W", "-s", "\t", "-Q", f"{query}"], text=True).strip()
            
            rows = []
            for line in result.splitlines():
                line = line.strip()
                if not line or "rows affected" in line.lower():
                    continue  # skip empty lines and metadata
                cols = line.split("\t")
                rows.append(tuple(cols))
        else:
            container_id = subprocess.check_output(
            "docker ps | grep oracle | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running SQL Server container found.")
            
            sql = f"""
            SET HEADING OFF;
            SET FEEDBACK OFF;
            SET PAGESIZE 0;
            {query};
            exit
            """
            
            result = subprocess.check_output(["docker", "exec", "-i", f"{container_id}", "sqlplus", "-S", f"{ORACLE_USER}/{ORACLE_PASS}@//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_DB}"], input=sql, text=True).strip()

            rows = []
            for line in result.splitlines():
                line = line.strip()
                if not line:
                    continue  # skip empty lines
                cols = line.split()  # or line.split("\t") if using tab separators
                rows.append(tuple(cols))

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed with return code {e.returncode}")
        print(f"[ERROR] Output:\n{e.output}")
        raise

    except Exception as ex:
        print(f"[ERROR] Unexpected error: {ex}")
        raise

    
    return rows

def run_remote_query_one(where, query):

    print(f"send query to {where}")
    try:
        if where == "mysql":
            container_id = subprocess.check_output(
            "docker ps | grep mysql | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running MySQL container found.")

            result = subprocess.check_output(["docker", "exec", "-i", "mysql", "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASS}", "-D", f"{MYSQL_DB}", "-sN", "-e", f"{query}"], text=True).strip()
            cleanres = result.strip().replace("\r", "").replace("\n", "\t")
            fields = cleanres.split("\t")

        elif where == "sqlserver":
            container_id = subprocess.check_output(
            "docker ps | grep sqlserver | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running SQL Server container found.")

            result = subprocess.check_output(["docker", "exec", "-i", f"{container_id}", "/opt/mssql-tools18/bin/sqlcmd", f"-U{SQLSERVER_USER}", f"-P{SQLSERVER_PASS}", "-d", f"{SQLSERVER_DB}", "-C", "-h", "-1", "-W", "-s", "\t", "-Q", f"{query}"], text=True).strip()
            cleanres = "".join(line for line in result.splitlines()if "rows affected" not in line.lower())
            fields = cleanres.strip().split("\t")

        else:
            container_id = subprocess.check_output(
            "docker ps | grep oracle | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running Oracle container found.")
            
            sql = f"""
            SET HEADING OFF;
            SET FEEDBACK OFF;
            SET PAGESIZE 0;
            {query};
            exit
            """
            
            result = subprocess.check_output(["docker", "exec", "-i", f"{container_id}", "sqlplus", "-S", f"{ORACLE_USER}/{ORACLE_PASS}@//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_DB}"], input=sql, text=True).strip()
            
            cleanres=result
            fields = cleanres.split()

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed with return code {e.returncode}")
        print(f"[ERROR] Output:\n{e.output}")
        raise

    except Exception as ex:
        print(f"[ERROR] Unexpected error: {ex}")
        raise

    return tuple(fields)

def create_synchdb_connector(cursor, vendor, name):
    if vendor == "mysql":
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{MYSQL_HOST}', {MYSQL_PORT}, '{MYSQL_USER}', '{MYSQL_PASS}', '{MYSQL_DB}', 'postgres', 'null', 'mysql');")

    elif vendor == "sqlserver":
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{SQLSERVER_HOST}', {SQLSERVER_PORT}, '{SQLSERVER_USER}', '{SQLSERVER_PASS}', '{SQLSERVER_DB}', 'postgres', 'null', 'sqlserver');")

    else:
        result = run_pg_query_one(cursor, f"SELECT synchdb_add_conninfo('{name}','{ORACLE_HOST}', {ORACLE_PORT}, '{ORACLE_USER}', '{ORACLE_PASS}', '{ORACLE_DB}', 'postgres', 'null', 'oracle');")

    return result

