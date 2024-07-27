package org.psyncopate;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleStreamProcessing {
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a DataStream from a list of elements
        DataStream<String> textStream = env.fromElements("Flink", "Spark", "Hadoop","Fivetran","Flux","Kafka");

        // Apply a filter transformation to keep only elements starting with "F"
        DataStream<String> filteredStream = textStream.filter((FilterFunction<String>) value -> value.startsWith("F"));

        // Print the filtered stream to the console
        filteredStream.print();

        // Execute the job
        env.execute("Simple Stream Processing");
    }
}
