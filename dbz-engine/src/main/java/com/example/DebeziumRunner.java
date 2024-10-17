package com.example;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.ChangeEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.LinkedList;
import java.util.Queue;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumRunner {
	private static final Logger logger = LoggerFactory.getLogger(DebeziumRunner.class);
	private List<String> changeEvents = new ArrayList<>();
	private DebeziumEngine<ChangeEvent<String, String>> engine;
	private ExecutorService executor;
	private Future<?> future;
	private String lastDbzMessage;
	private boolean lastDbzSuccess;
	private Throwable lastDbzError;
	private HashMap<Integer, ChangeRecordBatch> activeBatchHash = new HashMap<>();
	private BatchManager batchManager = new BatchManager();

	final int TYPE_MYSQL = 1;
	final int TYPE_ORACLE = 2;
	final int TYPE_SQLSERVER = 3;

	/* BatchMaanger represents a Batch request queue */
	public class BatchManager
	{
		private Queue<ChangeRecordBatch> batchQueue;
		private int batchid;

		public BatchManager()
		{
			this.batchQueue = new LinkedList<>();
			this.batchid = 0;
		}

		public void addBatch(ChangeRecordBatch batch)
		{
			batch.batchid = this.batchid;
			batchQueue.offer(batch);
			this.batchid++;
			logger.info("added a batch task: id = " + batch.batchid + " size = " + batch.records.size());
		}

		public ChangeRecordBatch getNextBatch()
		{
			return batchQueue.poll();
		}
	}
	
	/* ChangeRecordBatch represents a batch with our own identifier 'batchid' added to it */
	public class ChangeRecordBatch
	{
		public int batchid;
		public final List<ChangeEvent<String, String>> records;
		public final DebeziumEngine.RecordCommitter committer;

		public ChangeRecordBatch(List<ChangeEvent<String, String>> records, DebeziumEngine.RecordCommitter committer) 
		{
			this.records = new ArrayList<>(records);
			this.committer = committer;
		}
	}
	

	public void startEngine(String hostname, int port, String user, String password, String database, String table, int connectorType, String name, String snapshot_mode) throws Exception
	{
		String offsetfile = "/dev/shm/offsets.dat";
		String schemahistoryfile = "/dev/shm/schemahistory.dat";

		Properties props = new Properties();

        /* Setting connector specific properties */
        props.setProperty("name", "engine");
		switch(connectorType)
		{
			case TYPE_MYSQL:
			{
		        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
				int hash = name.hashCode();
				long unsignedhash = Integer.toUnsignedLong(hash);
				long serverid = 1 + (unsignedhash % (4294967295L - 1));

				logger.warn("derived server id " + serverid);
				props.setProperty("database.server.id", String.valueOf(serverid));	/* todo: make configurable */

				offsetfile = "pg_synchdb/mysql_" + name + "_offsets.dat";
				schemahistoryfile = "pg_synchdb/mysql_" + name + "_schemahistory.dat";

				break;
			}
			case TYPE_ORACLE:
			{
		        props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");
				offsetfile = "pg_synchdb/oracle_" + name + "_offsets.dat";
				schemahistoryfile = "pg_synchdb/oracle_" + name + "_schemahistory.dat";
				break;
			}
			case TYPE_SQLSERVER:
			{
		        props.setProperty("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");
				props.setProperty("database.encrypt", "false");	/* todo: enable tls */
				offsetfile = "pg_synchdb/sqlserver_" + name + "_offsets.dat";
				schemahistoryfile = "pg_synchdb/sqlserver_" + name + "_schemahistory.dat";
				break;
			}
		}
		
		/* setting common properties */
		if (database.equals("null"))
			logger.warn("database is null - skip setting database.include.list property");
		else
		{
			props.setProperty("database.include.list", database);
			props.setProperty("database.names", database);
		}

		if (table.equals("null"))
			logger.warn("table is null - skip setting table.include.list property");
		else
			props.setProperty("table.include.list", table);
		
		if (snapshot_mode.equals("null"))
			logger.warn("snapshot_mode is null - skip setting snapshot.mode property");
		else
			props.setProperty("snapshot.mode", snapshot_mode);

		props.setProperty("database.hostname", hostname);
		props.setProperty("database.port", String.valueOf(port));
		props.setProperty("database.user", user);
		props.setProperty("database.password", password);
		props.setProperty("topic.prefix", "synchdb-connector");
		props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
		props.setProperty("schema.history.internal.file.filename", schemahistoryfile);
		props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
		props.setProperty("offset.storage.file.filename", offsetfile);
		props.setProperty("offset.flush.interval.ms", "60000");
		props.setProperty("schema.history.internal.store.only.captured.tables.ddl", "true");

		logger.info("Hello from DebeziumRunner class!");

		DebeziumEngine.CompletionCallback completionCallback = (success, message, error) ->
		{
			lastDbzMessage = message.replace("\n", " ").replace("\r", " ");
			lastDbzSuccess = success;
			lastDbzError = error;
		};
		
		engine = DebeziumEngine.create(Json.class)
                .using(props)
				.using(completionCallback)
				.notifying((records, committer) -> {
					synchronized (this)
					{
						if (batchManager == null)
						{
							batchManager = new BatchManager();
						}
						if (activeBatchHash == null)
						{
							activeBatchHash = new HashMap<>();
						}
						
						batchManager.addBatch(new ChangeRecordBatch(records, committer));
					}
				})
				.build();

		executor = Executors.newSingleThreadExecutor();

		logger.info("submit future to executor");
		future = executor.submit(() ->
		{
			try
			{
				engine.run();
			}
			catch (Exception e)
			{
				logger.error("Task failed with exception: " + e.getMessage());
				throw e;
			}
		});
    }

	public void stopEngine() throws Exception
	{
		if (engine != null)
		{
			engine.close();
			engine = null; // clear the reference to ensure it's properly garbage collected
		}
		if (executor != null)
		{
			logger.info("stopping executor...");
            executor.shutdown();
            try
			{
                if (!executor.awaitTermination(5, TimeUnit.SECONDS))
				{
                    executor.shutdownNow();
                }
            } 
			catch (InterruptedException e) 
			{
                executor.shutdownNow();
            }
        }
		logger.info("done...");
	}

	public List<String> getChangeEvents()
	{
		List<String> listCopy;
		synchronized (this)
		{
			if (activeBatchHash == null)
			{
				activeBatchHash = new HashMap<>();
			}
			if (batchManager == null)
			{
				batchManager = new BatchManager();
			}

	        if (!future.isDone())
			{
				int i = 0;
				listCopy = new ArrayList<>();
				ChangeRecordBatch myNextBatch;
				
				myNextBatch = batchManager.getNextBatch();
				if (myNextBatch != null)
				{
					logger.info("Debezium -> Synchdb: sent batchid(" + myNextBatch.batchid + ") with size(" + myNextBatch.records.size() + ")");
					/* first element: batch id */
					listCopy.add("B-" + String.valueOf(myNextBatch.batchid));

					/* remaining elements: individual changes*/
					for (i = 0; i < myNextBatch.records.size(); i++)
					{
						listCopy.add(myNextBatch.records.get(i).value());
					}

					/* save this batch in active batch hash struct */
					activeBatchHash.put(myNextBatch.batchid, myNextBatch);;
				}
			}
			else
			{
				/* conector task is not running, get exit messages */
				logger.warn("connector is no longer running");
				logger.info("success flag = " + lastDbzSuccess + " | message = " + lastDbzMessage +
						" | error = " + lastDbzError);

				/*
				 * add the last captured connector exit message and send it to synchdb
				 * the K- prefix indicated an error rather than a change event
				 */
				listCopy = new ArrayList<>();
				listCopy.add("K-" + lastDbzSuccess + ";" + lastDbzMessage);
			}
		}

		return listCopy;
    }

	/* 
	 * method to mark a batch as done. This would cause dbz engine to commit the offset.
	 * if markbatchdone = true, the entire batch task is marked as completed.
	 * if markbatchdon = false, it indicates a partial completion, then we will only mark
	 * task from 'markfrom' to 'markto' as completed within the batch
	 *
	 */
	public void markBatchComplete(int batchid, boolean markall, int markfrom, int markto) throws InterruptedException
    {
		int i = 0;
		ChangeRecordBatch myBatch;

		if (activeBatchHash == null)
		{
			activeBatchHash = new HashMap<>();
			return;
		}
		
		logger.info("Debezium receivd batchid(" + batchid + ") completion request");		
		myBatch = activeBatchHash.get(batchid);
		if (myBatch == null)
		{
			logger.error("batch id " + batchid + " is not found in active batch hash");
			return;
		}
		
		if (markall)
		{
			logger.warn("debezium marked all records in batchid(" + batchid + ") as processed");

			for (i = 0; i < myBatch.records.size(); i++)
			{
				myBatch.committer.markProcessed(myBatch.records.get(i));
			}

			/* mark this batch complete to allow debezium to commit and flush offset */
			myBatch.committer.markBatchFinished();
			
			/* remove hash entry at batch completion */
			activeBatchHash.remove(batchid);
		}
		else
		{
			/* sanity check on the given range */
			if (markfrom >= myBatch.records.size() ||
				markto >= myBatch.records.size() ||
				markfrom < 0 || markto < 0)
			{
				logger.error("invalid range to mark completion: markfrom = " + markfrom + 
						" markto = " + markto + " sizeof batch = " + myBatch.records.size());
				return;
			}

			/* mark only the tasks within given range */
			for (i = markfrom; i <= markto; i++)
			{
				myBatch.committer.markProcessed(myBatch.records.get(i));
				logger.info("marked record(" + i + ") as processed within batchid " + batchid);
			}

			logger.warn("debezium marked " + (markto - markfrom + 1) + " records in batchid(" + batchid + ") as processed");
			/* we assumes that the only case to mark a batch as partially done is during error
			 * encounter and that we will exit soon after this function call. So let's mark this
			 * batch as finished and remove it from active batch hash
			 */
			myBatch.committer.markBatchFinished();
			activeBatchHash.remove(batchid);
		}
	}
	
	public String getConnectorOffset(int connectorType, String db, String name)
	{
		File inputFile = null;
		Map<ByteBuffer, ByteBuffer> originalData = null;
		String ret = "NULL";
		String key = null;

		switch (connectorType)
		{
			case TYPE_MYSQL:
			{
				inputFile = new File("pg_synchdb/mysql_" + name + "_offsets.dat");
				key = "[\"engine\",{\"server\":\"synchdb-connector\"}]";
				break;
			}
			case TYPE_ORACLE:
			{
				inputFile = new File("pg_synchdb/oracle_" + name + "_offsets.dat");
				/* todo */
				break;
			}
			case TYPE_SQLSERVER:
			{
				inputFile = new File("pg_synchdb/sqlserver_" + name + "_offsets.dat");
				key = "[\"engine\",{\"server\":\"synchdb-connector\",\"database\":\"" + db + "\"}]";
				break;
			}
		}

		if (!inputFile.exists())
        {
            logger.info("dbz offset file does not exist yet. Skipping");
			ret = "offset file not flushed yet";
            return ret;
        }

		ByteBuffer keyBuffer = ByteBuffer.wrap(key.getBytes(StandardCharsets.US_ASCII));
		originalData = readOffsetFile(inputFile);
		for (Map.Entry<ByteBuffer, ByteBuffer> entry : originalData.entrySet())
		{
			if (entry.getKey().equals(keyBuffer))
			{
				ret = StandardCharsets.UTF_8.decode(entry.getValue()).toString();
				logger.info("offset = " + ret);
			}
			else
			{
				//logger.warn("key entry not found");
			}
		}
		return ret;
	}

	public void setConnectorOffset(String filename, int type, String db, String newvalue)
	{
		File inputFile = new File(filename);
		String key = null;
		String value = newvalue;
		Map<byte[], byte[]> rawData = new HashMap<>();
		Map<ByteBuffer, ByteBuffer> originalData = null;

		
		switch (type)
		{
			case TYPE_MYSQL:
			{
				key = "[\"engine\",{\"server\":\"synchdb-connector\"}]";
				break;
			}
			case TYPE_ORACLE:
			{
				key = "[\"engine\",{\"server\":\"synchdb-connector\",\"database\":\"" + db + "\"}]";
				/* todo */
				break;
			}
			case TYPE_SQLSERVER:
			{
				key = "[\"engine\",{\"server\":\"synchdb-connector\",\"database\":\"" + db + "\"}]";
				break;
			}
		}

		if (!inputFile.exists())
		{
			logger.warn("dbz offset file does not exist yet. Skip setting offset");
			return;
		}

		originalData = readOffsetFile(inputFile);	
		ByteBuffer keyBuffer = ByteBuffer.wrap(key.getBytes(StandardCharsets.US_ASCII));
		ByteBuffer valueBuffer = ByteBuffer.wrap(value.getBytes(StandardCharsets.US_ASCII));
		for (Map.Entry<ByteBuffer, ByteBuffer> entry : originalData.entrySet())
		{
			if (entry.getKey().equals(keyBuffer))
			{
				logger.warn("updating existing offset record: " + key + " with new value: " + value);
				rawData.put(entry.getKey().array(), valueBuffer.array());
			}
			else
			{
				logger.warn("inserting new offset record: " + key + " with new value: " + value);
				rawData.put(keyBuffer.array(), valueBuffer.array());
			}
		}
		writeOffsetFile(inputFile, rawData);
	}
	
	public Map<ByteBuffer, ByteBuffer> readOffsetFile(File inputFile) {
        Map<ByteBuffer, ByteBuffer> originalData = new HashMap<>();

        try (FileInputStream fileIn = new FileInputStream(inputFile);
             ObjectInputStream objectIn = new ObjectInputStream(fileIn)) {
            Object obj = objectIn.readObject();
            if (!(obj instanceof HashMap)) {
                throw new IllegalStateException("Expected HashMap but found " + obj.getClass());
            }
            Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
            for (Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
                ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
                ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
                originalData.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return originalData;
    }

    public void writeOffsetFile(File outputFile, Map<byte[], byte[]> rawData) {
        try (FileOutputStream fileOut = new FileOutputStream(outputFile);
             ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {
                objectOut.writeObject(rawData);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
	public static void main(String[] args)
	{
		/* testing code can be put here */
    }
}
