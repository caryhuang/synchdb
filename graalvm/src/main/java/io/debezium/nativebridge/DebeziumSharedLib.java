package io.debezium.nativebridge;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.config.Configuration;
import io.debezium.engine.format.Json;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

public class DebeziumSharedLib {
	private static DebeziumEngine<?> engine;
	//private ExecutorService executor;

	static {
        try {
            // Explicitly register the Oracle JDBC driver
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
	@CEntryPoint(name = "startEngine")
	public static void startEngine(IsolateThread thread, CCharPointer configPath)
	{
		Properties props = new Properties();

    	System.out.println("initialize logging");
		/* Initialize Logging */
		//Logger logger = Logger.getRootLogger();
        //ConsoleAppender consoleAppender = new ConsoleAppender();
        //consoleAppender.setLayout(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"));
        //consoleAppender.setTarget(ConsoleAppender.SYSTEM_OUT);
        //consoleAppender.activateOptions();
        //logger.addAppender(consoleAppender);
		//logger.setLevel(Level.DEBUG);

    	System.out.println("Starting Debezium engine");

		props.setProperty("name", "engine");

		// MYSQL PROPS - BUGGY //
		/*
		//props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
		props.setProperty("database.include.list", "inventory");
		props.setProperty("database.names", "inventory");
		props.setProperty("database.hostname", "127.0.0.1");
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "mysqluser");
        props.setProperty("database.password", "mysqlpwd");
		props.setProperty("database.server.id", "11111111");
		*/
		////////////////////////

		// ORACLE PROPS //
		props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");
		props.setProperty("schema.include.list", "DDBBZZ");
        props.setProperty("lob.enabled", "true");
        props.setProperty("unavailable.value.placeholder", "__synchdb_unavailable_value");
		props.setProperty("database.dbname", "FREE");
		props.setProperty("database.hostname", "10.55.13.17");
        props.setProperty("database.port", "1521");
        props.setProperty("database.user", "DDBBZZ");
        props.setProperty("database.password", "dbz");
		props.setProperty("snapshot.locking.mode", "none");
		/////////////////
		
        props.setProperty("topic.prefix", "synchdb-connector");
        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", "schemahistory.dat");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "offset.dat");

        engine = DebeziumEngine
                .create(Json.class)
                .using(props)
                .notifying((records, committer) -> {
					System.out.println("notified");
                    for (ChangeEvent<String, String> record : records) {
                        System.out.println("Received change: " + record.value());
                        committer.markProcessed(record);
                    }
                    committer.markBatchFinished();
                })
				.using((success, message, error) -> {
        			System.err.println("Engine terminated: " + message);
        			if (error != null) {
            		error.printStackTrace();
        			}
    			})
                .build();

        Executors.newSingleThreadExecutor().execute(engine);
	}

    @CEntryPoint(name = "stopEngine")
    public static void stopEngine(IsolateThread thread)
	{
    	System.out.println("Stopping Debezium engine");
    	try {
        	if (engine != null) {
            	engine.close();
				//executor.shutdownNow();
				java.util.concurrent.Executors.newCachedThreadPool().shutdownNow();
            	System.out.println("Engine stopped successfully.");
        	}
    	} catch (Exception e) {
        	System.err.println("Error stopping engine: " + e.getMessage());
   		}
    }

	public static void main(String[] args) {
        // No-op
    }
}

