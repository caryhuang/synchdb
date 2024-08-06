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
	final int TYPE_MYSQL = 1;
	final int TYPE_ORACLE = 2;
	final int TYPE_SQLSERVER = 3;

	public void startEngine(String hostname, int port, String user, String password, String database, String table, int connectorType) throws Exception
	{
		String offsetfile = "/dev/shm/offsets.dat";
		String schemahistoryfile = "/dev/shm/schemahistory.dat";

		Properties props = new Properties();
        // Set Debezium properties...
        props.setProperty("name", "engine");
		switch(connectorType)
		{
			case TYPE_MYSQL:
			{
		        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
				offsetfile = "pg_synchdb/mysql_offsets.dat";
				schemahistoryfile = "pg_synchdb/mysql_schemahistory.dat";
				break;
			}
			case TYPE_ORACLE:
			{
		        props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");
				offsetfile = "pg_synchdb/oracle_offsets.dat";
				schemahistoryfile = "pg_synchdb/oracle_schemahistory.dat";
				break;
			}
			case TYPE_SQLSERVER:
			{
		        props.setProperty("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");
				offsetfile = "pg_synchdb/sqlserver_offsets.dat";
				schemahistoryfile = "pg_synchdb/sqlserver_schemahistory.dat";

				/* sqlserver requires database - cannot be null */
				if (database.equals("null"))
					logger.error("database cannot be null");
				else
					props.setProperty("database.names", database);

				props.setProperty("database.encrypt", "false");
				break;
			}
		}
		props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
		props.setProperty("offset.storage.file.filename", offsetfile);
		props.setProperty("offset.flush.interval.ms", "60000");
		
		/* begin connector properties */
		if (database.equals("null"))
			logger.warn("database is null - skip setting database.include.list property");
		else
			props.setProperty("database.include.list", database);

		if (table.equals("null"))
			logger.warn("table is null - skip setting table.include.list property");
		else
			props.setProperty("table.include.list", table);

		props.setProperty("database.hostname", hostname);
		props.setProperty("database.port", String.valueOf(port));
		props.setProperty("database.user", user);
		props.setProperty("database.password", password);
		props.setProperty("database.server.id", "223344");
		props.setProperty("topic.prefix", "my-app-connector");
		props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
		props.setProperty("schema.history.internal.file.filename", schemahistoryfile);

        // Set other required properties...
		logger.info("Hello from DebeziumRunner class!");

		engine = DebeziumEngine.create(Json.class)
                .using(props)
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
                }).build();

		System.out.println("executing...");
		executor = Executors.newSingleThreadExecutor();
		executor.execute(engine);
		
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
		/* make a copy of current change list for returning */
		List<String> listcopy = new ArrayList<>(changeEvents);

		/* empty the changeEvents as they have been consumed */
		changeEvents.clear();

		return listcopy;
		/*
        synchronized (this) {

			if (changeEvents == null)
			{
				changeEvents = new ArrayList<>();
			}
            return new ArrayList<>(changeEvents);
        }
		*/
    }
	
	public String getConnectorOffset(int connectorType)
	{
		return "todo";
	}

	public void setConnectorOffset(int connectorType, String offsetstring)
	{
		File inputFile = null;
		File outputFile = null;
		String key = null;
		String value = null;
		Map<byte[], byte[]> rawData = new HashMap<>();
		Map<ByteBuffer, ByteBuffer> originalData = null;

		switch (connectorType)
		{
			case TYPE_MYSQL:
			{
				inputFile = new File("pg_synchdb/mysql_offsets.dat");
				outputFile = new File("pg_synchdb/mysql_offsets.datout");
				key = "[\"engine\",{\"server\":\"my-app-connector\"}]";

				originalData = readOffsetFile(inputFile);	
				String[] elements = offsetstring.split("\\|");
				value = "{\"file\":" + "\"" + elements[0] + "\"," +
						"\"pos\":" + elements[1] + "," +
					   	"\"row\":" + elements[2] + "," +
						"\"server_id\":" + elements[3] + "," +
						"\"ts_sec\":" + elements[4] + "}";
				break;
			}
			case TYPE_ORACLE:
			{
				inputFile = new File("pg_synchdb/oracle_offsets.dat");
				outputFile = new File("pg_synchdb/oracle_offsets.datout");
				originalData = readOffsetFile(inputFile);
				/* todo */
				break;
			}
			case TYPE_SQLSERVER:
			{
				inputFile = new File("pg_synchdb/sqlserver_offsets.dat");
				outputFile = new File("pg_synchdb/sqlserver_offsets.datout");
				key = "[\"engine\",{\"server\":\"my-app-connector\"}]";
				originalData = readOffsetFile(inputFile);
				
				String[] elements = offsetstring.split("\\|");
				value = "{\"change_lsn\":" + "\"" + elements[0] + "\"," +
						"\"commit_lsn\":" + "\"" + elements[1] + "\"," +
						"\"event_serial_number\":" + "\"" + elements[2] + "\"}";
				break;
			}
		}

		ByteBuffer keyBuffer = ByteBuffer.wrap(key.getBytes(StandardCharsets.US_ASCII));
		ByteBuffer valueBuffer = ByteBuffer.wrap(value.getBytes(StandardCharsets.US_ASCII));
		for (Map.Entry<ByteBuffer, ByteBuffer> entry : originalData.entrySet())
		{
			System.out.println("Entry Key: " + StandardCharsets.UTF_8.decode(entry.getKey()).toString());
			System.out.println("Entry Val: " + StandardCharsets.UTF_8.decode(entry.getValue()).toString());
			System.out.println("New Key: " + key);
			System.out.println("New Val: " + value);
			if (entry.getKey().equals(keyBuffer))
			{
				//System.out.println("found");
				rawData.put(entry.getKey().array(), valueBuffer.array());
			}
			else
			{
				/* todo: somehow it always returns not found. we still add the existing key
				 * rather than the new key
				 */
				//System.out.println("not found");
				rawData.put(entry.getKey().array(), valueBuffer.array());
				//rawData.put(keyBuffer.array(), valueBuffer.array());
			}
		}
		writeOffsetFile(outputFile, rawData);
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
		DebeziumRunner runner = new DebeziumRunner();
		runner.setConnectorOffset(1, "mysql-bin.000003|33048|1|223344|1722965389");

    }
}
