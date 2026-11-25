import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * ================================================================================
 * APACHE FLINK CHECKPOINTING - CODE EXAMPLES
 * ================================================================================
 *
 * This file demonstrates checkpointing mechanisms in Apache Flink with practical
 * examples for staff engineer preparation.
 *
 * Topics Covered:
 * 1. Understanding Checkpointing
 * 2. Checkpoint Configuration
 * 3. Checkpointing Modes
 * 4. State Backends
 * 5. Savepoints
 * 6. Checkpoint Monitoring
 * 7. Restart Strategies
 * 8. Exactly-Once Processing
 * 9. Checkpoint Tuning
 * ================================================================================
 */

public class Checkpointing {

    /**
     * Section 1: Understanding Checkpointing
     * Demonstrates what checkpointing is and why it's important
     */
    public static void section1_understanding_checkpointing() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 1: UNDERSTANDING CHECKPOINTING");
        System.out.println("================================================================================");

        System.out.println("\n--- What is Checkpointing? ---");
        System.out.println("Checkpointing: Periodic snapshots of application state");
        System.out.println("  - Captures state of all operators");
        System.out.println("  - Enables fault tolerance");
        System.out.println("  - Supports exactly-once semantics");
        System.out.println("  - Automatic recovery from failures");

        System.out.println("\n--- How Checkpointing Works ---");
        System.out.println("1. Checkpoint Coordinator triggers checkpoint");
        System.out.println("2. Sources inject barriers into stream");
        System.out.println("3. Barriers flow through operators");
        System.out.println("4. Operators snapshot state when barriers arrive");
        System.out.println("5. State is written to state backend");
        System.out.println("6. Coordinator confirms checkpoint completion");

        System.out.println("\n--- Checkpoint Barriers ---");
        System.out.println("Barriers: Special markers in data stream");
        System.out.println("  - Flow with data");
        System.out.println("  - Partition state into checkpoints");
        System.out.println("  - Align state across operators");

        System.out.println("\n--- Benefits ---");
        System.out.println("1. Fault tolerance: Automatic recovery");
        System.out.println("2. Exactly-once: No data loss, no duplicates");
        System.out.println("3. State management: Persistent state");
        System.out.println("4. Versioning: Savepoints for upgrades");
    }

    /**
     * Section 2: Checkpoint Configuration
     * Demonstrates how to configure checkpointing
     */
    public static void section2_checkpoint_configuration() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 2: CHECKPOINT CONFIGURATION");
        System.out.println("================================================================================");

        System.out.println("\n--- Basic Configuration ---");
        System.out.println("StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();");
        System.out.println("");
        System.out.println("// Enable checkpointing (interval in milliseconds)");
        System.out.println("env.enableCheckpointing(5000);  // Checkpoint every 5 seconds");

        System.out.println("\n--- Advanced Configuration ---");
        System.out.println("// Checkpoint interval");
        System.out.println("env.enableCheckpointing(5000);");
        System.out.println("");
        System.out.println("// Checkpointing mode");
        System.out.println("env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);");
        System.out.println("");
        System.out.println("// Checkpoint timeout");
        System.out.println("env.getCheckpointConfig().setCheckpointTimeout(60000);  // 60 seconds");
        System.out.println("");
        System.out.println("// Minimum time between checkpoints");
        System.out.println("env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);  // 1 second");
        System.out.println("");
        System.out.println("// Maximum concurrent checkpoints");
        System.out.println("env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);");
        System.out.println("");
        System.out.println("// Externalized checkpoints");
        System.out.println("env.getCheckpointConfig().enableExternalizedCheckpoints(");
        System.out.println("    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);");

        System.out.println("\n--- Configuration Parameters ---");
        System.out.println("Checkpoint interval: Time between checkpoints");
        System.out.println("  - Default: disabled");
        System.out.println("  - Typical: 1-5 minutes");
        System.out.println("  - Trade-off: overhead vs recovery time");

        System.out.println("\nCheckpoint timeout: Time to complete checkpoint");
        System.out.println("  - Default: 10 minutes");
        System.out.println("  - Adjust based on state size");

        System.out.println("\nMin pause between checkpoints:");
        System.out.println("  - Prevents checkpoint storm");
        System.out.println("  - Default: 0");
        System.out.println("  - Typical: 1-5 seconds");

        System.out.println("\nMax concurrent checkpoints:");
        System.out.println("  - Default: 1");
        System.out.println("  - Higher = more parallelism, more overhead");
    }

    /**
     * Section 3: Checkpointing Modes
     * Demonstrates different checkpointing modes
     */
    public static void section3_checkpointing_modes() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 3: CHECKPOINTING MODES");
        System.out.println("================================================================================");

        System.out.println("\n--- Exactly-Once Mode ---");
        System.out.println("env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);");
        System.out.println("");
        System.out.println("Exactly-once semantics:");
        System.out.println("  - Barriers align state");
        System.out.println("  - No data loss");
        System.out.println("  - No duplicates");
        System.out.println("  - Slower due to alignment");
        System.out.println("  - Requires barrier alignment");

        System.out.println("\n--- At-Least-Once Mode ---");
        System.out.println("env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);");
        System.out.println("");
        System.out.println("At-least-once semantics:");
        System.out.println("  - No barrier alignment");
        System.out.println("  - No data loss");
        System.out.println("  - Possible duplicates");
        System.out.println("  - Faster than exactly-once");
        System.out.println("  - Lower latency");

        System.out.println("\n--- Choosing the Right Mode ---");
        System.out.println("Use exactly-once when:");
        System.out.println("  - Data accuracy is critical");
        System.out.println("  - Duplicates are unacceptable");
        System.out.println("  - Financial transactions, analytics");

        System.out.println("\nUse at-least-once when:");
        System.out.println("  - Latency is critical");
        System.out.println("  - Duplicates are acceptable");
        System.out.println("  - Real-time analytics, logging");

        System.out.println("\n--- Example ---");
        System.out.println("StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();");
        System.out.println("env.enableCheckpointing(5000);");
        System.out.println("");
        System.out.println("// For exactly-once (default)");
        System.out.println("env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);");
        System.out.println("");
        System.out.println("// For at-least-once (lower latency)");
        System.out.println("env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);");
    }

    /**
     * Section 4: State Backends
     * Demonstrates different state backend configurations
     */
    public static void section4_state_backends() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 4: STATE BACKENDS");
        System.out.println("================================================================================");

        System.out.println("\n--- State Backend Types ---");
        System.out.println("1. MemoryStateBackend: In-memory, limited size");
        System.out.println("2. FsStateBackend: File system, larger state");
        System.out.println("3. RocksDBStateBackend: Embedded database, very large state");

        System.out.println("\n--- MemoryStateBackend ---");
        System.out.println("env.setStateBackend(new MemoryStateBackend());");
        System.out.println("");
        System.out.println("MemoryStateBackend characteristics:");
        System.out("  - State in JVM heap");
        System.out("  - Fast access");
        System.out("  - Limited by heap size");
        System.out("  - Not recommended for production");
        System.out("  - Good for testing, small state");

        System.out.println("\n--- FsStateBackend ---");
        System.out.println("env.setStateBackend(new FsStateBackend(\"hdfs:///checkpoints\"));");
        System.out.println("");
        System.out.println("FsStateBackend characteristics:");
        System.out("  - State in file system");
        System.out("  - Larger state support");
        System.out("  - Good for moderate state");
        System.out("  - Checkpoints in HDFS/S3");
        System.out("  - Recommended for production");

        System.out.println("\n--- RocksDBStateBackend ---");
        System.out.println("env.setStateBackend(new RocksDBStateBackend(\"hdfs:///checkpoints\"));");
        System.out.println("");
        System.out.println("RocksDBStateBackend characteristics:");
        System.out.println("  - Embedded key-value store");
        System.out("  - Very large state support");
        System.out("  - State on disk");
        System.out("  - Slower than memory");
        System.out("  - Best for very large state");
        System.out("  - Recommended for production with large state");

        System.out.println("\n--- Choosing State Backend ---");
        System.out.println("MemoryStateBackend:");
        System.out("  - Testing, development");
        System.out("  - Small state (< few GB)");

        System.out.println("\nFsStateBackend:");
        System.out("  - Production");
        System.out("  - Moderate state (< tens of GB)");
        System.out("  - Need faster access than RocksDB");

        System.out.println("\nRocksDBStateBackend:");
        System.out("  - Production with large state");
        System.out("  - Very large state (> tens of GB)");
        System.out("  - State exceeds memory");

        System.out.println("\n--- Example Configuration ---");
        System.out.println("StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();");
        System.out.println("env.enableCheckpointing(5000);");
        System.out.println("");
        System.out.println("// Use RocksDB for production");
        System.out.println("env.setStateBackend(new RocksDBStateBackend(\"file:///tmp/flink-checkpoints\"));");
        System.out.println("");
        System.out.println("// Or use FsStateBackend");
        System.out.println("env.setStateBackend(new FsStateBackend(\"file:///tmp/flink-checkpoints\"));");
    }

    /**
     * Section 5: Savepoints
     * Demonstrates savepoint functionality
     */
    public static void section5_savepoints() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 5: SAVEPOINTS");
        System.out.println("================================================================================");

        System.out.println("\n--- What are Savepoints? ---");
        System.out.println("Savepoints: Manual checkpoints");
        System.out.println("  - Triggered manually");
        System.out.println("  - Used for upgrades, maintenance");
        System.out.println("  - Versioning");
        System.out("  - A/B testing");

        System.out.println("\n--- Savepoint vs Checkpoint ---");
        System.out.println("Checkpoint:");
        System.out("  - Automatic");
        System.out.println("  - Periodic");
        System.out("  - Focused on fault tolerance");
        System.out("  - Short-lived");

        System.out.println("\nSavepoint:");
        System.out.println("  - Manual");
        System.out("  - On-demand");
        System.out("  - Focused on upgrades, maintenance");
        System.out("  - Long-lived");

        System.out.println("\n--- Triggering Savepoints ---");
        System.out.println("Command line:");
        System.out.println("flink savepoint <jobId> [targetDirectory]");
        System.out.println("");
        System.out.println("Example:");
        System.out.println("flink savepoint 6b5c5b5b5b5b5b5b5b5b5b5b5b5b5b5b file:///tmp/savepoints");

        System.out.println("\n--- Restoring from Savepoint ---");
        System.out.println("Command line:");
        System.out.println("flink run -s <savepointPath> <jarFile>");
        System.out.println("");
        System.out.println("Example:");
        System.out.println("flink run -s file:///tmp/savepoints/savepoint-123 /path/to/job.jar");

        System.out.println("\n--- Programmatic Savepoint ---");
        System.out.println("// In Flink 1.15+, you can trigger savepoints via REST API");
        System.out.println("POST /jobs/:jobid/savepoints");

        System.out.println("\n--- Use Cases ---");
        System.out.println("1. Application upgrades:");
        System.out.println("   - Take savepoint");
        System.out.println("   - Update application");
        System.out.println("   - Restore from savepoint");

        System.out.println("\n2. A/B testing:");
        System.out.println("   - Take savepoint");
        System.out.println("   - Run different versions");
        System.out.println("   - Compare results");

        System.out.println("\n3. Maintenance:");
        System.out.println("   - Take savepoint");
        System.out.println("   - Stop cluster");
        System.out.println("   - Perform maintenance");
        System.out.println("   - Restore from savepoint");

        System.out.println("\n--- Best Practices ---");
        System.out.println("1. Test savepoint/restore in development");
        System.out.println("2. Use consistent savepoint directory");
        System.out.println("3. Document savepoint locations");
        System.out.println("4. Clean up old savepoints");
        System.out.println("5. Verify state compatibility");
    }

    /**
     * Section 6: Checkpoint Monitoring
     * Demonstrates how to monitor checkpoints
     */
    public static void section6_checkpoint_monitoring() {
        System.out.println="\n================================================================================";
        System.out.println("SECTION 6: CHECKPOINT MONITORING");
        System.out.println("================================================================================";

        System.out.println("\n--- Monitoring Metrics ---");
        System.out.println("Key checkpoint metrics:");
        System.out.println("  - numCheckpoints: Total number of checkpoints");
        System.out.println("  - numCompletedCheckpoints: Successfully completed");
        System.out.println("  - numFailedCheckpoints: Failed checkpoints");
        System.out.println("  - latestCompletedCheckpointTime: Time of last completion");
        System.out.println("  - checkpointDuration: Time to complete checkpoint");

        System.out.println("\n--- Monitoring via Flink Web UI ---");
        System.out.println("Flink Web UI → Jobs → Checkpoints tab");
        System.out.println("");
        System.out.println("Metrics available:");
        System.out.println("  - Checkpoint history");
        System.out.println("  - Checkpoint duration");
        System.out.println("  - State size");
        System.out.println("  - Buffered data");
        System.out.println("  - Alignment duration");

        System.out.println("\n--- Monitoring via REST API ---");
        System.out.println("GET /jobs/:jobid/checkpoints");
        System.out.println("");
        System.out.println("Response includes:");
        System.out.println("  - Checkpoint ID");
        System.out.println("  - Status");
        System.out.println("  - Duration");
        System.out.println("  - State size");

        System.out.println("\n--- Common Issues ---");
        System.out.println("Issue 1: Checkpoint timeout");
        System.out.println("  - Symptom: Checkpoints failing");
        System.out.println("  - Cause: State too large, slow backend");
        System.out.println("  - Solution: Increase timeout, optimize state");

        System.out.println("\nIssue 2: High checkpoint duration");
        System.out.println("  - Symptom: Checkpoints taking long time");
        System.out.println("  - Cause: Large state, slow network");
        System.out.println("  - Solution: Reduce state, faster backend");

        System.out.println("\nIssue 3: Frequent checkpoint failures");
        System.out.println("  - Symptom: Many failed checkpoints");
        System.out.println("  - Cause: Resource constraints, network issues");
        System.out.println("  - Solution: Increase resources, check network");

        System.out.println("\n--- Monitoring Checklist ---");
        System.out.println("[ ] Check checkpoint success rate");
        System.out.println("[ ] Monitor checkpoint duration");
        System.out.println("[ ] Track state size");
        System.out.println("[ ] Check alignment time");
        System.out.println("[ ] Monitor backend performance");
    }

    /**
     * Section 7: Restart Strategies
     * Demonstrates restart strategy configuration
     */
    public static void section7_restart_strategies() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 7: RESTART STRATEGIES");
        System.out.println("================================================================================");

        System.out.println("\n--- Restart Strategy Types ---");
        System.out.println("1. Fixed delay: Fixed number of restarts with delay");
        System.out.println("2. Failure rate: Restart based on failure rate");
        System.out.println("3. No restart: No automatic restart");
        System.out.println("4. Fallback: Use cluster default");

        System.out.println("\n--- Fixed Delay Restart ---");
        System.out.println("env.setRestartStrategy(RestartStrategies.fixedDelayRestart(");
        System.out.println("    3,  // max restart attempts");
        System.out.println("    Time.of(10, TimeUnit.SECONDS)  // delay between restarts");
        System.out.println("));");
        System.out.println("");
        System.out.println("Fixed delay restart:");
        System.out.println("  - Restart up to N times");
        System.println("  - Fixed delay between restarts");
        System.out.println("  - Good for transient failures");

        System.out.println("\n--- Failure Rate Restart ---");
        System.out.println("env.setRestartStrategy(RestartStrategies.failureRateRestart(");
        System.out.println("    3,  // max failures per interval");
        System.out.println("    Time.of(5, TimeUnit.MINUTES),  // failure interval");
        System.out.println("    Time.of(10, TimeUnit.SECONDS)  // delay between restarts");
        System.out.println("));");
        System.out.println("");
        System.out.println("Failure rate restart:");
        System.out.println("  - Restart if failure rate exceeded");
        System.out.println("  - Time-based failure window");
        System.out.println("  - Good for handling bursts of failures");

        System.out.println("\n--- No Restart ---");
        System.out.println("env.setRestartStrategy(RestartStrategies.noRestart());");
        System.out.println("");
        System.out.println("No restart:");
        System.out.println("  - Job fails on first error");
        System.out.println("  - Good for debugging");
        System.out.println("  - Not recommended for production");

        System.out.println("\n--- Example ---");
        System.out.println("StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();");
        System.out.println("");
        System.out.println("// Fixed delay restart (recommended for production)");
        System.out.println("env.setRestartStrategy(RestartStrategies.fixedDelayRestart(");
        System.out.println("    3,  // max 3 restart attempts");
        System.out.println("    Time.of(10, TimeUnit.SECONDS)  // 10 second delay");
        System.out.println("));");
        System.out.println("");
        System.out.println("// Enable checkpointing");
        System.out.println("env.enableCheckpointing(5000);");

        System.out.println("\n--- Choosing Restart Strategy ---");
        System.out.println("Fixed delay:");
        System.out.println("  - Most common");
        System.out.println("  - Predictable behavior");
        System.out.println("  - Good for production");

        System.out.println("\nFailure rate:");
        System.out.println("  - Complex failure patterns");
        System.out.println("  - Bursty failures");
        System.out.println("  - Advanced use cases");

        System.out.println("\nNo restart:");
        System.out.println("  - Development, testing");
        System.out.println("  - Debugging");
        System.out.println("  - Not for production");
    }

    /**
     * Section 8: Exactly-Once Processing
     * Demonstrates exactly-once semantics
     */
    public static void section8_exactly_once_processing() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 8: EXACTLY-ONCE PROCESSING");
        System.out.println("================================================================================");

        System.out.println("\n--- Exactly-Once Semantics ---");
        System.out.println("Exactly-once: Each event processed exactly once");
        System.out.println("  - No data loss");
        System.out.println("  - No duplicates");
        System.out.println("  - Requires checkpointing");
        System.out.println("  - Requires transactional sinks");

        System.out.println("\n--- Requirements ---");
        System.out.println("1. Checkpointing enabled");
        System.out.println("2. Exactly-once checkpointing mode");
        System.out.println("3. Transactional or idempotent sinks");
        System.out.println("4. Stateful operators");

        System.out.println("\n--- Example Configuration ---");
        System.out.println("StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();");
        System.out.println("");
        System.out.println("// Enable checkpointing");
        System.out.println("env.enableCheckpointing(5000);");
        System.out.println("");
        System.out.println("// Exactly-once mode");
        System.out.println("env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);");
        System.out.println("");
        System.out.println("// Use transactional sink (e.g., Kafka with exactly-once)");
        System.out.println("FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(");
        System.out.println("    \"topic\",");
        System.out.println("    new SimpleStringSchema(),");
        System.out.println("    producerConfig,");
        System.out.println("    Semantic.EXACTLY_ONCE  // Exactly-once semantics");
        System.out.println(");");

        System.out.println("\n--- Sink Considerations ---");
        System.out.println("Transactional sinks:");
        System.out.println("  - Kafka (with exactly-once support)");
        System.out.println("  - Databases with transactions");
        System.out.println("  - File systems with two-phase commit");

        System.out.println("\nIdempotent sinks:");
        System.out.println("  - Upsert operations");
        System.out.println("  - Deduplication keys");
        System.out.println("  - Idempotent writes");

        System.out.println("\n--- Trade-offs ---");
        System.out.println("Exactly-once:");
        System.out.println("  - Slower due to alignment");
        System.out.println("  - Higher latency");
        System.out.println("  - More complex");
        System.out.println("  - Best for critical data");

        System.out.println("\nAt-least-once:");
        System.out.println("  - Faster");
        System.out.println("  - Lower latency");
        System.out.println("  - Possible duplicates");
        System.out.println("  - Good for non-critical data");
    }

    /**
     * Section 9: Checkpoint Tuning
     * Demonstrates checkpoint tuning best practices
     */
    public static void section9_checkpoint_tuning() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 9: CHECKPOINT TUNING");
        System.out.println("================================================================================");

        System.out.println("\n--- Tuning Guidelines ---");
        System.out.println("1. Checkpoint interval:");
        System.out.println("   - Too short: Overhead, performance impact");
        System.out.println("   - Too long: Long recovery time");
        System.out.println("   - Target: 1-5 minutes");

        System.out.println("\n2. Checkpoint timeout:");
        System.out.println("   - Too short: Frequent failures");
        System.out.println("   - Too long: Slow detection of issues");
        System.out.println("   - Target: 2-3x expected checkpoint time");

        System.out.println("\n3. State backend:");
        System.out.println("   - Memory: Fast, limited size");
        System.out.println("   - FS: Balanced, moderate size");
        System.out.println("   - RocksDB: Slower, unlimited size");

        System.out.println("\n--- Example Tuned Configuration ---");
        System.out.println("StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();");
        System.out.println("");
        System.out.println("// Checkpoint every 1 minute");
        System.out.println("env.enableCheckpointing(60000);");
        System.out.println("");
        System.out.println("// Exactly-once mode");
        System.out.println("env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);");
        System.out.println("");
        System.out.println("// 5 minute timeout");
        System.out.println("env.getCheckpointConfig().setCheckpointTimeout(300000);");
        System.out.println("");
        System.out.println("// Minimum 30 seconds between checkpoints");
        System.out.println("env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);");
        System.out.println("");
        System.out.println("// Retain checkpoints on cancellation");
        System.out.println("env.getCheckpointConfig().enableExternalizedCheckpoints(");
        System.out.println("    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);");
        System.out.println("");
        System.out.println("// Use RocksDB for large state");
        System.out.println("env.setStateBackend(new RocksDBStateBackend(\"file:///tmp/checkpoints\"));");
        System.out.println("");
        System.out.println("// Fixed delay restart");
        System.out.println("env.setRestartStrategy(RestartStrategies.fixedDelayRestart(");
        System.out.println("    3, Time.of(10, TimeUnit.SECONDS)");
        System.out.println("));");

        System.out.println("\n--- Performance Tips ---");
        System.out.println("1. Reduce state size:");
        System.out.println("   - Use state TTL");
        System.out.println("   - Clean up old state");
        System.out.println("   - Use efficient data structures");

        System.out.println("\n2. Optimize state backend:");
        System.out.println("   - Use RocksDB for large state");
        System.out.println("   - Use incremental checkpoints");
        System.out.println("   - Tune RocksDB options");

        System.out.println("\n3. Tune checkpoint interval:");
        System.out.println("   - Monitor checkpoint duration");
        System.out.println("   - Adjust based on workload");
        System.out.println("   - Balance overhead vs recovery time");

        System.out.println("\n--- Incremental Checkpoints ---");
        System.out.println("Incremental checkpoints: Only store changes");
        System.out.println("  - Faster for large state");
        System.out.println("  - Less storage");
        System.out.println("  - Supported by RocksDB");
        System.out.println("");
        System.out.println("env.getCheckpointConfig().enableUnalignedCheckpoints();");

        System.out.println("\n--- Unaligned Checkpoints ---");
        System.out.println("Unaligned checkpoints: Don't wait for barrier alignment");
        System.out.println("  - Faster checkpointing");
        System.out.println("  - Lower latency");
        System.out.println("  - Slightly more recovery time");
        System.out.println("");
        System.out.println("env.getCheckpointConfig().enableUnalignedCheckpoints();");

        System.out.println("\n--- Monitoring Checklist ---");
        System.out.println("[ ] Monitor checkpoint success rate");
        System.out.println("[ ] Track checkpoint duration");
        System.out.println("[ ] Monitor state size");
        System.out.println("[ ] Check alignment time");
        System.out.println("[ ] Tune based on metrics");
    }

    /**
     * Example: Stateful Process Function with Checkpointing
     */
    public static class CountingFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                "count",
                TypeInformation.of(Integer.class)
            );
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
            Tuple2<String, Integer> value,
            KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>::Context ctx,
            Collector<Tuple2<String, Integer>> out
        ) throws Exception {
            Integer currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0;
            }
            currentCount += value.f1;
            countState.update(currentCount);
            out.collect(new Tuple2<>(value.f0, currentCount));
        }
    }

    public static void main(String[] args) {
        System.out.println("================================================================================");
        System.out.println("APACHE FLINK CHECKPOINTING - CODE EXAMPLES");
        System.out.println("================================================================================");

        // Run all sections
        section1_understanding_checkpointing();
        section2_checkpoint_configuration();
        section3_checkpointing_modes();
        section4_state_backends();
        section5_savepoints();
        section6_checkpoint_monitoring();
        section7_restart_strategies();
        section8_exactly_once_processing();
        section9_checkpoint_tuning();

        System.out.println("\n================================================================================");
        System.out.println("ALL DEMONSTRATIONS COMPLETED");
        System.out.println("================================================================================");
        System.out.println("\nKey Takeaways:");
        System.out.println("1. Checkpointing enables fault tolerance and exactly-once semantics");
        System.out.println("2. Configure checkpoint interval based on recovery time requirements");
        System.out.println("3. Use exactly-once mode for critical data, at-least-once for lower latency");
        System.out.println("4. Choose state backend based on state size (Memory/FS/RocksDB)");
        System.out.println("5. Use savepoints for upgrades and maintenance");
        System.out.println("6. Monitor checkpoint metrics in Flink Web UI");
        System.out.println("7. Configure restart strategies for production");
        System.out.println("8. Use transactional sinks for exactly-once guarantees");
        System.out.println("9. Tune checkpointing based on workload and state size");
        System.out.println("\nCheck Flink Web UI for checkpoint metrics");
    }
}
