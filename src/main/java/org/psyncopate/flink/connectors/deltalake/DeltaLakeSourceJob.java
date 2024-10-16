package org.psyncopate.flink.connectors.deltalake;

import io.delta.flink.source.DeltaSource;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.psyncopate.flink.connectors.PropertyFilesLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaLakeSourceJob {

    public static void main(String[] args) throws Exception {
        final Logger logger = LoggerFactory.getLogger(DeltaLakeSinkWithoutPartitionJob.class);

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //Load Properties
        Properties properties = PropertyFilesLoader.loadProperties("delta-lake.properties");

        // Define Delta table path
        String deltaTablePath = "file:///" + properties.getProperty("deltaTable.path") + "_with_partition";
        
        // Create the Delta source
        DataStream<RowData> deltaSourceStream = createBoundedDeltaSourceAllColumns(env, deltaTablePath);

        // Print the data from the Delta source
        deltaSourceStream.print();

        // Execute the Flink job
        env.execute("Flink Delta Lake Source Job");
    }

    // Function to create Delta source for bounded (batch) data read
    public static DataStream<RowData> createBoundedDeltaSourceAllColumns(
            StreamExecutionEnvironment env,
            String deltaTablePath) {

        // Create DeltaSource for reading RowData in bounded mode
        DeltaSource<RowData> deltaSource = DeltaSource
            .forContinuousRowData(
                new Path(deltaTablePath),
                new Configuration())
            .build();

        // Add source to the Flink environment
        return env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
    }
}

