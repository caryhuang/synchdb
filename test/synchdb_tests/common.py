
import subprocess

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


def run_pg_query(cursor, query):
    cursor.execute(query)
    if cursor.description:  # Only fetch if query returns results
        return cursor.fetchall()
    return None

def run_pg_query_one(cursor, query):
    cursor.execute(query)
    return cursor.fetchone()

def run_remote_query(where, query):
    
    try:
        if where == "mysql":
            result = subprocess.check_output(["docker", "exec", "-i", "mysql", "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASS}", "-D", f"{MYSQL_DB}", "-sN", "-e", f"{query}"], text=True).strip()
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

    try:
        if where == "mysql":
            result = subprocess.check_output(["docker", "exec", "-i", "mysql", "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASS}", "-D", f"{MYSQL_DB}", "-sN", "-e", f"{query}"], text=True).strip()
            cleanres = result.strip().replace("\r", "").replace("\n", "")

        elif where == "sqlserver":
            container_id = subprocess.check_output(
            "docker ps | grep sqlserver | awk '{print $1}'",
            shell=True, text=True
            ).strip()

            if not container_id:
                raise RuntimeError("No running SQL Server container found.")

            result = subprocess.check_output(["docker", "exec", "-i", f"{container_id}", "/opt/mssql-tools18/bin/sqlcmd", f"-U{SQLSERVER_USER}", f"-P{SQLSERVER_PASS}", "-d", f"{SQLSERVER_DB}", "-C", "-h", "-1", "-W", "-s", "\t", "-Q", f"{query}"], text=True).strip()
            cleanres = "".join(line for line in result.splitlines()if "rows affected" not in line.lower())

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
            
            cleanres=result

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed with return code {e.returncode}")
        print(f"[ERROR] Output:\n{e.output}")
        raise

    except Exception as ex:
        print(f"[ERROR] Unexpected error: {ex}")
        raise

    return cleanres
