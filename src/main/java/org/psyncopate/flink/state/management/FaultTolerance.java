package org.psyncopate.flink.state.management;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class FaultTolerance {
    private static final Logger logger = LoggerFactory.getLogger(FaultTolerance.class);

    public static void main(String[] args) throws Exception {
        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism to 1 to ensure single output file
        env.setParallelism(1);

        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);  // Checkpointing every 0.5 seconds

        // Set the state backend to RocksDB
        //env.setStateBackend(new RocksDBStateBackend("file:///opt/flink/checkpoints"));

        // Create a custom data stream source that introduces a delay between emitting numbers
        DataStream<Long> numberStream = env.addSource(new StatefulSource());

        // Add logging before processing the stream
        logger.info("Data stream created, starting to process the stream");
        System.out.println("Data stream created, starting to process the stream");

        // Process the stream to log and output the current number
        DataStream<String> outputStream = numberStream
                .keyBy(num -> 0)  // Keep a constant key
                .process(new KeyedProcessFunction<Integer, Long, String>() {

                    @Override
                    public void processElement(Long value, Context ctx, Collector<String> out) {
                        try {
                            // Log and output current number
                            logger.info("Processing number: " + value);
                            System.out.println("Processing number: " + value);

                            out.collect("Processed: " + value);

                            // Log after processing
                            logger.info("Processed number: " + value);
                            System.out.println("Processed number: " + value);
                        } catch (Exception e) {
                            logger.error("Error processing number: " + value, e);
                            System.err.println("Error processing number: " + value);
                            e.printStackTrace();
                        }
                    }
                });

        // Add logging before writing the output
        //logger.info("Writing output to file");
        //System.out.println("Writing output to file");

        /*// Define the FileSink with rolling policy
        final FileSink<String> sink = FileSink
                .forRowFormat(new Path("mnt/flink/output"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(15))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(org.apache.flink.configuration.MemorySize.ofMebiBytes(1024))
                                .build())
                .build();
*/
        // Write the output to the sink
        //outputStream.sinkTo(sink);

        // Add logging before executing the job
        logger.info("Executing Flink job");
        System.out.println("Executing Flink job");

        // Execute the Flink job
        env.execute("Fault Tolerance Demo with Single Output File");
    }

    public static class StatefulSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {
        private volatile boolean isRunning = true;
        private long currentNumber = 0;
        private transient ListState<Long> checkpointedState;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning && currentNumber <= 20000) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(currentNumber);
                    currentNumber++;
                }
                Thread.sleep(2000);  // Introduce 2000ms delay between numbers
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
                    "sourceState",
                    Types.LONG
            );

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                for (Long number : checkpointedState.get()) {
                    currentNumber = number;
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            checkpointedState.add(currentNumber);
        }
    }
}