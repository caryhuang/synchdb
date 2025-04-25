import os
import subprocess
import time
import re
from common import run_pg_query, run_pg_query_one, getConnectorName, create_and_start_synchdb_connector


def test_tpcc_buildschema(pg_cursor, dbvendor, hammerdb):
    name = getConnectorName(dbvendor) + "_tpcc"

    subprocess.run(["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/buildschema.tcl"], check=True, stdout=subprocess.DEVNULL) 
    
    result = create_and_start_synchdb_connector(pg_cursor, dbvendor, name, "initial", srcdb="tpcc")
    assert result == 0
    
    # try to get the first non-zero timestammps from synchdb_stats_view
    loopcnt=0
    while True:
        rows = run_pg_query_one(pg_cursor, f"SELECT ddls, dmls, avg_batch_size, batches_done, first_src_ts, first_dbz_ts, first_pg_ts FROM synchdb_stats_view WHERE name = '{name}'")
        if rows != None and rows[4] != 0 and rows[5] != 0 and rows[6] != 0:
            ddls = rows[0]
            dmls = rows[1]
            avgbatchsize = rows[2]
            batchesdone = rows[3]
            first_src_ts = rows[4]
            first_dbz_ts = rows[5]
            first_pg_ts = rows[6]
            break
        else:
            if loopcnt <= 20:
                print("sleep 1s before trying to fetch first timestamps again...")
                loopcnt = loopcnt + 1
            else:
                print(f"no timestamps are fetched after {loopcnt} fetches...")
                break
            time.sleep(1)

    assert first_src_ts > 0 and first_dbz_ts > 0 and first_pg_ts > 0
    print(f"first timetamps are: {first_src_ts} {first_dbz_ts} {first_pg_ts}, total batches done = {batchesdone}")

    time.sleep(20)

    lasttup=None
    while True:
        currtup = run_pg_query_one(pg_cursor, f"SELECT ddls, dmls, avg_batch_size, batches_done, last_src_ts, last_dbz_ts, last_pg_ts FROM synchdb_stats_view WHERE name = '{name}'")
        print(currtup)

        if currtup == lasttup:
            print("no new updates in stats. Considering current sync session as done")
            ddls = currtup[0]
            dmls = currtup[1]
            avgbatchsize = currtup[2]
            batchesdone = currtup[3]
            last_src_ts = currtup[4]
            last_dbz_ts = currtup[5]
            last_pg_ts = currtup[6]
            print(f"last timestamps are {last_src_ts} {last_dbz_ts} {last_pg_ts}")
            break
        else:
            lasttup = currtup
        time.sleep(20)

    assert last_src_ts > 0 and last_dbz_ts > 0 and last_pg_ts > 0
    
    pg_diff_ms = last_pg_ts - first_pg_ts
    print(f"initial snapshotting tpcc tables takes {pg_diff_ms} ms with average batch size = {avgbatchsize}, DML processed = {dmls}, total batches done = {batchesdone}")
    
    assert True

# TPCC test is running at the same time as synchdb replication
def test_tpcc_run(pg_cursor, dbvendor, hammerdb, tpccmode):
    name = getConnectorName(dbvendor) + "_tpcc"
    
    # this test depends on test_tpcc_buildschema, so connector must be running now as a result from previous test run
    rows = run_pg_query_one(pg_cursor, f"SELECT state FROM synchdb_state_view WHERE name ='{name}'")
    assert rows[0] == "polling"
    
    # reset stats
    rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_reset_stats('{name}')")
    assert rows[0] == 0

    #subprocess.Popen(["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"], stdout=subprocess.DEVNULL)
    if tpccmode == "serial":
        # we pause connector first, and resume only after tpcc test run has completed 
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_pause_engine('{name}')")
        assert rows[0] == 0

        #subprocess.run(["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"], check=True)
        nopm, tpm = map(int, re.search(r"achieved (\d+) NOPM from (\d+)",
            subprocess.run(
                ["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"],
                check=True, capture_output=True, text=True
            ).stdout
        ).groups())
        rows = run_pg_query_one(pg_cursor, f"SELECT synchdb_resume_engine('{name}')")
        assert rows[0] == 0
    else:
        # run tpcc test in background while synchdb is polling
        proc = subprocess.Popen(["docker", "exec", "hammerdb", "./hammerdbcli", "auto", "/runtpcc.tcl"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True)
    
    # try to get the first non-zero timestammps from synchdb_stats_view
    loopcnt=0
    while True:
        rows = run_pg_query_one(pg_cursor, f"SELECT ddls, dmls, avg_batch_size, batches_done, first_src_ts, first_dbz_ts, first_pg_ts FROM synchdb_stats_view WHERE name = '{name}'")
        if rows != None and rows[4] != 0 and rows[5] != 0 and rows[6] != 0:
            ddls = rows[0]
            dmls = rows[1]
            avgbatchsize = rows[2]
            batchesdone = rows[3]
            first_src_ts = rows[4]
            first_dbz_ts = rows[5]
            first_pg_ts = rows[6]
            break
        else:
            if loopcnt <= 20:
                print("sleep 1s before trying to fetch first timestamps again...")
                loopcnt = loopcnt + 1
            else:
                print(f"no timestamps are fetched after {loopcnt} fetches...")
                break
            time.sleep(1)
    
    assert first_src_ts > 0 and first_dbz_ts > 0 and first_pg_ts > 0
    print(f"first timetamps are: {first_src_ts} {first_dbz_ts} {first_pg_ts}, total batches done = {batchesdone}")

    time.sleep(5)
    lasttup=None
    while True:
        currtup = run_pg_query_one(pg_cursor, f"SELECT ddls, dmls, avg_batch_size, batches_done, last_src_ts, last_dbz_ts, last_pg_ts FROM synchdb_stats_view WHERE name = '{name}'")
        print(currtup)

        if currtup == lasttup:
            print("no new updates in stats. Considering current sync session as done")
            ddls = currtup[0]
            dmls = currtup[1]
            avgbatchsize = currtup[2]
            batchesdone = currtup[3]
            last_src_ts = currtup[4]
            last_dbz_ts = currtup[5]
            last_pg_ts = currtup[6]
            print(f"last timestamps are {last_src_ts} {last_dbz_ts} {last_pg_ts}")
            break
        else:
            lasttup = currtup
        time.sleep(20)

    assert last_src_ts > 0 and last_dbz_ts > 0 and last_pg_ts > 0

    pg_diff_ms = last_pg_ts - first_pg_ts

    # in non-serial mode, read nopm and tpm from PIPE
    if tpccmode == "parallel":
        stdout, stderr = proc.communicate()
        match = re.search(r"achieved (\d+) NOPM from (\d+)", stdout)
        if match:
            nopm, tpm = map(int, match.groups())
        else:
            raise RuntimeError("TPCC result not found in output")

    print(f"tpcc CDC test takes {pg_diff_ms} ms with average batch size = {avgbatchsize}, DML processed = {dmls}, total batches done = {batchesdone}. {dbvendor} tpcc results: nopm = {nopm} tpm = {tpm}")

    assert True
