package org.psyncopate.flink.state.management;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FaultTolerance {
    private static final Logger logger = LoggerFactory.getLogger(FaultTolerance.class);

    public static void main(String[] args) {
        try {
            // Create the execution environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // Set parallelism to 1 to write to a single output file
            env.setParallelism(1);

            // Enable checkpointing for fault tolerance
            env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

            // Set the state backend to RocksDB if needed (optional)
            env.setStateBackend(new RocksDBStateBackend("file:///opt/flink/checkpoints"));

            // Define a directory for savepoints
            String savepointPath = "file:///opt/flink/savepoints";

            // Create a data stream of 20,000 zeros followed by a single 1000
            DataStream<Integer> numberStream = env
                    .fromSequence(0, 20_000)  // Generates numbers from 0 to 20,000
                    .map(i -> (i < 20_000) ? 0 : 990);  // Emit 0s, then a final 1000

            // Process the stream to calculate the running sum with delay
            DataStream<String> outputStream = numberStream
                    .keyBy(num -> 0)  // Use constant key for keyed state
                    .process(new KeyedProcessFunction<Integer, Integer, String>() {
                        private int sum = 10;  // Maintain running sum

                        @Override
                        public void processElement(Integer value, Context ctx, Collector<String> out) {
                            try {
                                // Introduce a delay to simulate backpressure
                                Thread.sleep(1);  // Delay for 1 millisecond

                                sum += value;  // Update sum
                                out.collect("Current sum: " + sum);  // Output current sum
                            } catch (InterruptedException e) {
                                logger.error("Error processing element", e);
                                Thread.currentThread().interrupt();
                            }
                        }
                    });

            // Write the output to a text file, allowing overwriting
            outputStream.writeAsText("output/sum_results.txt", FileSystem.WriteMode.OVERWRITE);  // Write to a text file in overwrite mode

            // Execute the Flink job
            env.execute("Fault Tolerance Demo");
        } catch (Exception e) {
            logger.error("Error executing Flink job", e);
        }
    }
}