package org.psyncopate.flink.dataset;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class DatasetExample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Example dataset
        DataSet<String> text = env.fromElements("Flink", "Dataset", "API", "Example");

        // Apply a simple transformation
        DataSet<String> result = text.map(value -> "Hello, " + value);

        // Print the result
        result.print();
    }
}
