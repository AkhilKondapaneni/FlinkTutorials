package org.psyncopate.flink.connectors.deltalake;

import io.delta.flink.source.DeltaSource;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.psyncopate.flink.connectors.PropertyFilesLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaLakeSourceMapStateJob {

    public static void main(String[] args) throws Exception {
        final Logger logger = LoggerFactory.getLogger(DeltaLakeSinkWithoutPartitionJob.class);
        //org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        //config.setBoolean("classloader.check-leaked-classloader", false);
        
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //Load Properties
        Properties properties = PropertyFilesLoader.loadProperties("delta-lake.properties");

        // Define Delta table path
        String deltaTablePath = "file:///opt/flink/delta-lake/" + properties.getProperty("deltaTable.name") + "_with_partition";

        // Set parallelism to 1 to write to a single output file
        env.setParallelism(1);
        /* env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            3, // Number of restart attempts
            Time.seconds(10)) // Delay between restarts
        ); */
        
        //env.setStateBackend(new RocksDBStateBackend("file:///opt/flink/checkpoints"));

        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///opt/flink/checkpoints");
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file:///opt/flink/savepoints");
        env.configure(config);
        
        

        // Create the Delta source
        DataStream<RowData> deltaSourceStream = createBoundedDeltaSourceAllColumns(env, deltaTablePath);

        // Apply ProcessFunction to count records - Key by zip code
        DataStream<RowData> processedStream = deltaSourceStream.keyBy(rowData-> rowData.getString(3).toString()).process(new CountProcessFunction(logger));

        processedStream
        .map(rowData -> {
            // Assuming rowData is GenericRowData
            GenericRowData genericRowData = (GenericRowData) rowData;

            // Extract the fields
            String name = genericRowData.getString(0).toString();
            int age = genericRowData.getInt(1);
            String email = genericRowData.getString(2).toString();
            String zip = genericRowData.getString(3).toString();
            boolean isActive = genericRowData.getBoolean(4);
            long count = genericRowData.getLong(5); // Assuming the count is the last field

            // Format the string for printing
            return String.format("Name: %s, Age: %d, Email: %s, Zip: %s, isActive: %b, ZipCodeUserCount: %d",
                name, age, email, zip, isActive, count);
        }).map(logMessage -> {
            logger.info(logMessage);
            return logMessage;
        });
        

        // Print the data from the Delta source
        processedStream.print();

        // Execute the Flink job
        env.execute("Flink Delta Lake Source Job");
    }

    // Function to create Delta source for bounded (batch) data read
    public static DataStream<RowData> createBoundedDeltaSourceAllColumns(
            StreamExecutionEnvironment env,
            String deltaTablePath) {

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setBoolean("hadoop.security.shutdownhooks", false);        

        // Create DeltaSource for reading RowData in bounded mode
        DeltaSource<RowData> deltaSource = DeltaSource
            .forContinuousRowData(
                new Path(deltaTablePath),
                new Configuration())
            .build();

        // Add source to the Flink environment
        return env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
    }

    // Define a custom ProcessFunction to count records
    public static class CountProcessFunction extends KeyedProcessFunction<String, RowData, RowData> {
        private final Logger logger;
        private transient MapState<String, Long> zipCodeCountState;

        public CountProcessFunction(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            // Initialize the MapState for persisting the record count
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>(
                "zipCodeCountState",   // State name
                Types.STRING,         // Key type
                Types.LONG            // Value type (count)
            );
            zipCodeCountState = getRuntimeContext().getMapState(descriptor);
            //logger.info("Initial Record count from state: {}", recordCountState.get("recordsRead"));
        }

        @Override
        public void processElement(RowData value, Context ctx, Collector<RowData> out) throws Exception {
            if (value instanceof BinaryRowData) {

                BinaryRowData binaryRowData = (BinaryRowData) value;
                
                // Extract the existing fields from the BinaryRowData
                String zip = binaryRowData.getString(3).toString();
                String name = binaryRowData.getString(0).toString();  // Name
                int age = binaryRowData.getInt(1);                   // Age
                String email = binaryRowData.getString(2).toString(); // Email
                boolean isActive = binaryRowData.getBoolean(4);       // isActive flag
            
                // Retrieve the current count for the zip code from the state
                Long currentCount = zipCodeCountState.get(zip);
                if (currentCount == null) {
                    currentCount = 0L;
                }

                // Increment the count
                currentCount += 1;

                // Update the state with the new count for this zip code
                zipCodeCountState.put(zip, currentCount);

                // Log the updated count for the zip code
                //logger.info("Zip Code: {}, User Count: {}, Name: {}, Age: {}, Email: {}, isActive: {}",zip, currentCount, name, age, email, isActive);

                // Update the existing GenericRowData's 6th field with the zip code user count
                GenericRowData updatedRowData = new GenericRowData(6);  // 6 fields now
                updatedRowData.setField(0, StringData.fromString(name));
                updatedRowData.setField(1, age);
                updatedRowData.setField(2, StringData.fromString(email));
                updatedRowData.setField(3, StringData.fromString(zip));
                updatedRowData.setField(4, isActive);
                updatedRowData.setField(5, currentCount); // Add the zip code user count

                // Here, just emitting the RowData as is
                out.collect(updatedRowData);
                
            }else {
                throw new FlinkRuntimeException("Expected GenericRowData but got " + value.getClass());
            }
        }

        @Override
        public void close() throws Exception {
            // Optionally log final counts from state when closing
            logger.info("Final counts per zip code:");
            for (String zip : zipCodeCountState.keys()) {
                logger.info("Zip: {} | Count: {}", zip, zipCodeCountState.get(zip));
            }
        }
    }
}

