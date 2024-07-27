package org.psyncopate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreamProcessing {
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Set parallelism to 1 to ensure sequential execution

        // Create a DataStream from a list of words
        DataStream<String> textStream = env.fromElements("apple", "banana", "orange", "apple", "banana");

        // Print the initial stream of words
        textStream.print("Initial Stream");

        // Transform each word into a tuple (word, 1)
        DataStream<Tuple2<String, Integer>> wordStream = textStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) {
                return new Tuple2<>(value, 1);
            }
        });

        // Print the stream after mapping
        wordStream.print("Mapped Stream");

        // Key the stream by the word
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordStream.keyBy(value -> value.f0);

        // Print the keyed stream
        keyedStream.print("Keyed Stream");

        // Sum the counts for each word
        DataStream<Tuple2<String, Integer>> wordCounts = keyedStream.sum(1);

        // Print the final word counts
        wordCounts.print("Word Counts");

        // Execute the job
        env.execute("Word Count Stream Processing");
    }
}
