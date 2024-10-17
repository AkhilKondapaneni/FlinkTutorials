package org.psyncopate.flink.connectors.deltalake;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.psyncopate.flink.connectors.PropertyFilesLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

import org.apache.flink.table.data.StringData; 

import io.delta.flink.sink.DeltaSink;

import org.apache.flink.table.types.logical.BooleanType;

import java.util.Random;

public class DeltaLakeSinkWithoutPartitionJob {

    public static void main(String[] args) throws Exception {

        final Logger logger = LoggerFactory.getLogger(DeltaLakeSinkWithoutPartitionJob.class);

        //org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        //config.setBoolean("classloader.check-leaked-classloader", false);
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //Load Properties
        Properties properties = PropertyFilesLoader.loadProperties("delta-lake.properties");

        // Create a sample data stream of RowData
        DataStream<RowData> dataStream = env.addSource(new SampleSource(properties));

        // Define Delta table path
        String deltaTablePath = "file:///opt/flink/delta-lake/" + properties.getProperty("deltaTable.name") + "_without_partition";
        

        // Define the schema for the RowData
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("age", new IntType()),
                new RowType.RowField("email", new VarCharType()),
                new RowType.RowField("isActive", new BooleanType())
            ));
                
        // Create and add the Delta sink
        createDeltaSink(dataStream, deltaTablePath, rowType);

        // Execute the Flink job
        env.execute("Flink Delta Lake Sink Job without Partition");
    }

    // Function to create Delta sink for RowData
    public static DataStream<RowData> createDeltaSink(
            DataStream<RowData> stream,
            String deltaTablePath,
            RowType rowType) {

        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new Path(deltaTablePath), // Path to Delta Lake
                        new Configuration(),      // Delta Lake configuration
                        rowType)                  // RowType defining the schema
                .build();

        stream.sinkTo(deltaSink);
        return stream;
    }

    // Sample source to generate random data as RowData
    public static class SampleSource implements SourceFunction<RowData> {
        private volatile boolean isRunning = true;
        private final Random random = new Random();
        private final Properties properties;

        public SampleSource(Properties properties) {
            this.properties = properties;
        }

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            int i=0;
            int no_of_records = Integer.parseInt(properties.getProperty("deltaTable.noOfRecords"));
            while (i<no_of_records) {
                String name = "User" + random.nextInt(100);
                int age = 20 + random.nextInt(30);
                String email = "user" + random.nextInt(100) + "@example.com";
                boolean isActive = random.nextBoolean();

                // Create a RowData with the data
                GenericRowData rowData = new GenericRowData(4); // 4 fields: name, age, email, isActive
                rowData.setField(0, StringData.fromString(name));
                rowData.setField(1, age);
                rowData.setField(2, StringData.fromString(email));
                rowData.setField(3, isActive);

                ctx.collect(rowData);
                i++;
                // Sleep for a short time to simulate streaming data
                Thread.sleep(800);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
