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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumRunner {
	private static final Logger logger = LoggerFactory.getLogger(DebeziumRunner.class);
	private List<String> changeEvents = new ArrayList<>();
	private DebeziumEngine<ChangeEvent<String, String>> engine;
	private ExecutorService executor;
	
	public void startEngine(String hostname, int port, String user, String password, String database) throws Exception
	{
        Properties props = new Properties();
        // Set Debezium properties...
        props.setProperty("name", "engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
		props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
		props.setProperty("offset.storage.file.filename", "/dev/shm/offsets.dat");
		props.setProperty("offset.flush.interval.ms", "60000");
		
		/* begin connector properties */
		props.setProperty("database.hostname", hostname);
		props.setProperty("database.port", String.valueOf(port));
		props.setProperty("database.user", user);
		props.setProperty("database.password", password);
		props.setProperty("database.server.id", "223344");
		props.setProperty("topic.prefix", "my-app-connector");
		props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
		props.setProperty("schema.history.internal.file.filename", "/dev/shm/schemahistory.dat");

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
		return new ArrayList<>(changeEvents);
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

	public static void main(String[] args)
	{
		DebeziumRunner runner = new DebeziumRunner();
        try
		{
            runner.startEngine("192.168.1.86", 3306, "mysqluser", "mysqlpwd", "inventory");
        }
		catch (Exception e)
		{
            System.err.println("An error occurred while starting the engine:");
            e.printStackTrace();
        }
		List<String> res = runner.getChangeEvents();
		System.out.println("there are " + res.size() + " change events");
    }
	
}
