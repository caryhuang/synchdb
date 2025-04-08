import common
from common import run_pg_query, run_pg_query_one, run_remote_query, run_remote_query_one

#todo
def testCreate(pg_cursor):
    print("connector created")
    result = run_pg_query(pg_cursor, "select version()")
    print(result)

def testDelete(pg_cursor):
    print("connector deleted")

def testCounts(dbvendor):
    print(dbvendor)
    assert 2+2==4

def testInitSnap(dbvendor):
    print("testing mysql")
    resultset = run_remote_query(dbvendor, "select * from orders")
    print(resultset)
    result = run_remote_query_one(dbvendor, "select count(*) from orders")
    print(result)

