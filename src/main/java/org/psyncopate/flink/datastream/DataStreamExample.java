package org.psyncopate.flink.datastream;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Example datastream
        DataStream<String> text = env.fromElements("Flink", "DataStream", "API", "Example");

        // Apply a simple transformation
        DataStream<String> result = text.map(value -> "Hello, " + value);

        // Print the result
        result.print();

        // Execute the program
        env.execute("DataStream Example");
    }
}

