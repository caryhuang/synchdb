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

	final int TYPE_MYSQL = 1;
	final int TYPE_ORACLE = 2;
	final int TYPE_SQLSERVER = 3;

	public void startEngine(String hostname, int port, String user, String password, String database, String table, int connectorType, String name) throws Exception
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

		logger.info("Hello from DebeziumRunner class!");

		DebeziumEngine.CompletionCallback completionCallback = (success, message, error) ->
		{
			//logger.warn("success = " + success + " message = " + message + " error = " + error);
			lastDbzMessage = message.replace("\n", " ").replace("\r", " ");
			lastDbzSuccess = success;
			lastDbzError = error;
		};

		engine = DebeziumEngine.create(Json.class)
                .using(props)
				.using(completionCallback)
                .notifying(record -> {
                    System.out.println(record);
                    if (changeEvents == null)
                    {
                        changeEvents = new ArrayList<>();
                    }
                    //synchronized (changeEvents)
                    synchronized (this)
                    {
						if (record.value() == null)
						{
							logger.info("skipping a null change record");
						}
						else
						{
							logger.info("adding a change record of length " + record.value().length());
                        	changeEvents.add(record.value());
						}
                        logger.info("there are " + changeEvents.size() + " change events stored");
                    }
                })
				.build();

		System.out.println("executing...");
		executor = Executors.newSingleThreadExecutor();

		logger.warn("submit future to executor");
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
		//executor.execute(engine);
		
		System.out.println("done...");
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
			System.out.println("stopping executor...");
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
		System.out.println("done...");
	}

	public List<String> getChangeEvents()
	{
		List<String> listCopy;

		synchronized (this)
		{
			if (changeEvents == null)
			{
				logger.warn("changeEvents is null, initializing empty list");
				changeEvents = new ArrayList<>();
			}

	        if (!future.isDone())
			{
				/* conector task is running, get changes */
				listCopy = new ArrayList<>(changeEvents);

				/* empty the changeEvents as they have been consumed */
				changeEvents.clear();
				logger.info("Retrieved {} change events", listCopy.size());
			}
			else
			{
				/* conector task is not running, get exit messages */
				logger.warn("connector is no longer running");
				logger.warn("success flag = " + lastDbzSuccess + " | message = " + lastDbzMessage +
						" | error = " + lastDbzError);

				/*
				 * add the last captured connector exit message and send it to synchdb
				 * the K- prefix indicated an error rather than a change event
				 */
				listCopy = new ArrayList<>();
				listCopy.add("K-" + lastDbzSuccess + ";" + lastDbzMessage);
				logger.info("Prepared {} change events", listCopy.size());
			}
		}

		return listCopy;
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
            logger.warn("dbz offset file does not exist yet. Skipping");
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
				logger.warn("offset = " + ret);
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
		/* testing codes */

		/* ---------- 1) dbz runner test ---------- */
		/*
		DebeziumRunner runner = new DebeziumRunner();
        try
		{
            runner.startEngine("192.168.1.86", 3306, "mysqluser", "mysqlpwd", "inventory", "null", 1);
        }
		catch (Exception e)
		{
            System.err.println("An error occurred while starting the engine:");
            e.printStackTrace();
        }
		List<String> res = runner.getChangeEvents();
		System.out.println("there are " + res.size() + " change events");
		*/

		/* ---------- 2) offset update test ---------- */
		/*
		DebeziumRunner runner = new DebeziumRunner();
		File inputFile = new File(args[0]);
		File outputFile = new File(args[1]);
		String newKey = args[2];
		String newValue = (args.length == 4) ? args[3] : null;
		Map<ByteBuffer, ByteBuffer> originalData = runner.readOffsetFile(inputFile);
		Map<byte[], byte[]> rawData = new HashMap<>();


		ByteBuffer keyBuffer = ByteBuffer.wrap(newKey.getBytes(StandardCharsets.US_ASCII));
		ByteBuffer valueBuffer = (newValue != null) ? ByteBuffer.wrap(newValue.getBytes(StandardCharsets.US_ASCII)) : null;

		for (Map.Entry<ByteBuffer, ByteBuffer> entry : originalData.entrySet()) {
			    System.out.println("Entry Key: " + StandardCharsets.UTF_8.decode(entry.getKey()).toString());
			    System.out.println("Entry Val: " + StandardCharsets.UTF_8.decode(entry.getValue()).toString());
			if (newValue != null) {
				if (entry.getKey().equals(keyBuffer)) {
					// update value
					System.out.println("update value");
					rawData.put(entry.getKey().array(), valueBuffer.array());
				} else {
					// insert new entry
					System.out.println("insert value");
					rawData.put(keyBuffer.array(), valueBuffer.array());
				}
			} else {
				//rawData.put(entry.getKey().array(), valueBuffer.array());
				originalData.remove(keyBuffer);
			}
		}

		runner.writeOffsetFile(outputFile, rawData);
		*/
		String filename = args[0];
		int type = Integer.parseInt(args[1]);
		String db = args[2];
		String value = args[3];

		DebeziumRunner runner = new DebeziumRunner();
		runner.setConnectorOffset(filename, type, db, value);
		//runner.getConnectorOffset(1,null);
		//runner.getConnectorOffset(3,"testDB");
		//runner.setConnectorOffset(1, "mysql-bin.000003|40603|1|223344|1723067034", null);
		//runner.setConnectorOffset(3, "0000006d:00000e20:0007|0000006d:00000e20:0007|4567", "testDB");

    }
}
