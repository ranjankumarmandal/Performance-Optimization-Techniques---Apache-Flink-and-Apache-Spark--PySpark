import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ================================================================================
 * APACHE FLINK STATE MANAGEMENT - CODE EXAMPLES
 * ================================================================================
 *
 * This file demonstrates state management mechanisms in Apache Flink with practical
 * examples for staff engineer preparation.
 *
 * Topics Covered:
 * 1. Understanding State
 * 2. Keyed State vs Operator State
 * 3. State Types
 * 4. State Time-to-Live (TTL)
 * 5. State Access Patterns
 * 6. State Best Practices
 * 7. State in Flink SQL
 * 8. State Monitoring
 * 9. State Tuning
 * ================================================================================
 */

public class StateManagement {

    /**
     * Section 1: Understanding State
     * Demonstrates what state is and why it's important
     */
    public static void section1_understanding_state() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 1: UNDERSTANDING STATE");
        System.out.println("================================================================================");

        System.out.println("\n--- What is State? ---");
        System.out.println("State: Data that operators remember across events");
        System.out.println("  - Maintains information from past events");
        System.out.println("  - Enables complex operations (aggregations, joins)");
        System.out.println("  - Required for stateful processing");
        System.out.println("  - Backed by state backend (checkpointing)");

        System.out.println("\n--- Why State is Important ---");
        System.out.println("1. Aggregations: Count, sum, average over time");
        System.out.println("2. Joins: Join streams with historical data");
        System.out.println("3. Pattern detection: Complex event processing");
        System.out.println("4. Machine learning: Model state");

        System.out.println("\n--- State Characteristics ---");
        System.out.println("1. Keyed state: Scoped to key");
        System.out.println("2. Operator state: Scoped to operator instance");
        System.out.println("3. Checkpointed: Backed by state backend");
        System.out.println("4. Recovered: Restored on failure");

        System.out.println("\n--- State Lifecycle ---");
        System.out.println("1. State initialized in open()");
        System.out.println("2. State updated in processElement()");
        System.out.println("3. State checkpointed periodically");
        System.out.println("4. State restored on failure");
        System.out.println("5. State cleared on close()");
    }

    /**
     * Section 2: Keyed State vs Operator State
     * Demonstrates different state scopes
     */
    public static void section2_keyed_vs_operator_state() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 2: KEYED STATE VS OPERATOR STATE");
        System.out.println("================================================================================");

        System.out.println("\n--- Keyed State ---");
        System.out.println("Keyed state: State scoped to a key");
        System.out.println("  - Available in KeyedProcessFunction");
        System.out.println("  - Each key has its own state");
        System.out.println("  - Automatically distributed");
        System.out.println("  - Example: Per-user counters");

        System.out.println("\n--- Operator State ---");
        System.out.println("Operator state: State scoped to operator instance");
        System.out.println("  - Available in non-keyed operators");
        System.out.println("  - Shared across all keys");
        System.out.println("  - List state or union list state");
        System.out.println("  - Example: Kafka offsets");

        System.out.println("\n--- Keyed State Example ---");
        System.out.println("public class MyFunction extends KeyedProcessFunction<String, Event, Result> {");
        System.out.println("    private ValueState<Integer> countState;");
        System.out.println("");
        System.out.println("    @Override");
        System.out.println("    public void open(Configuration parameters) {");
        System.out.println("        ValueStateDescriptor<Integer> descriptor =");
        System.out.println("            new ValueStateDescriptor<>(\"count\", Integer.class);");
        System.out.println("        countState = getRuntimeContext().getState(descriptor);");
        System.out.println("    }");
        System.out.println("}");

        System.out.println("\n--- Operator State Example ---");
        System.out.println("public class MySource extends RichSourceFunction<String> {");
        System.out.println("    private ListState<String> offsetsState;");
        System.out.println("");
        System.out.println("    @Override");
        System.out.println("    public void open(Configuration parameters) {");
        System.out.println("        ListStateDescriptor<String> descriptor =");
        System.out.println("            new ListStateDescriptor<>(\"offsets\", String.class);");
        System.out.println("        offsetsState = getRuntimeContext().getListState(descriptor);");
        System.out.println("    }");
        System.out.println("}");

        System.out.println("\n--- Choosing State Type ---");
        System.out.println("Use keyed state when:");
        System.out.println("  - Data is keyed (groupBy, keyBy)");
        System.out.println("  - State is per-key (user sessions)");
        System.out.println("  - Automatic distribution needed");

        System.out.println("\nUse operator state when:");
        System.out.println("  - Data is not keyed");
        System.out.println("  - State is shared (source offsets)");
        System.out.println("  - Custom distribution needed");
    }

    /**
     * Section 3: State Types
     * Demonstrates different state data structures
     */
    public static void section3_state_types() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 3: STATE TYPES");
        System.out.println("================================================================================");

        System.out.println("\n--- Value State ---");
        System.out.println("ValueState: Single value per key");
        System.out.println("  - get(): Retrieve value");
        System.out.println("  - update(value): Update value");
        System.out.println("  - clear(): Clear value");
        System.out.println("  - Use for: Counters, flags, last value");

        System.out.println("\n--- Example: Value State ---");
        System.out.println("ValueState<Integer> countState;");
        System.out.println("Integer count = countState.value();  // get");
        System.out.println("if (count == null) count = 0;");
        System.out.println("countState.update(count + 1);  // update");

        System.out.println("\n--- List State ---");
        System.out.println("ListState: List of values per key");
        System.out.println("  - add(value): Add to list");
        System.out.println("  - get(): Retrieve iterable");
        System.out.println("  - update(list): Replace entire list");
        System.out.println("  - Use for: Event history, buffers");

        System.out.println("\n--- Example: List State ---");
        System.out.println("ListState<String> eventsState;");
        System.out.println("eventsState.add(event);  // add");
        System.out.println("Iterable<String> events = eventsState.get();  // get");

        System.out.println("\n--- Map State ---");
        System.out.println("MapState: Map of key-value pairs");
        System.out.println("  - put(key, value): Add entry");
        System.out.println("  - get(key): Retrieve value");
        System.out.println("  - keys(): Get all keys");
        System.out.println("  - Use for: Lookups, nested state");

        System.out.println("\n--- Example: Map State ---");
        System.out.println("MapState<String, Integer> mapState;");
        System.out.println("mapState.put(\"key1\", 100);  // put");
        System.out.println("Integer value = mapState.get(\"key1\");  // get");

        System.out.println("\n--- Reducing State ---");
        System.out.println("ReducingState: Aggregated value");
        System.out.println("  - add(value): Add with reduce function");
        System.out.println("  - get(): Get aggregated value");
        System.out.println("  - Use for: Sum, max, min");

        System.out.println("\n--- Example: Reducing State ---");
        System.out.println("ReducingState<Integer> sumState;");
        System.out.println("sumState.add(value);  // add with reduce");

        System.out.println("\n--- Aggregating State ---");
        System.out.println("AggregatingState: Custom aggregation");
        System.out.println("  - add(value): Add with aggregate function");
        System.out.println("  - get(): Get aggregated value");
        System.out.println("  - Use for: Complex aggregations");

        System.out.println("\n--- State Type Selection ---");
        System.out.println("ValueState: Single value (counter, flag)");
        System.out.println("ListState: Multiple values (history, buffer)");
        System.out.println("MapState: Key-value pairs (lookup)");
        System.out.println("ReducingState: Simple aggregation (sum, max)");
        System.out.println("AggregatingState: Complex aggregation (average)");
    }

    /**
     * Section 4: State Time-to-Live (TTL)
     * Demonstrates state TTL configuration
     */
    public static void section4_state_ttl() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 4: STATE TIME-TO-LIVE (TTL)");
        System.out.println("================================================================================");

        System.out.println("\n--- What is State TTL? ---");
        System.out.println("TTL: Automatic expiration of old state");
        System.out.println("  - Reduces state size");
        System.out.println("  - Prevents unbounded growth");
        System.out.println("  - Configurable per state descriptor");
        System.out.println("  - Based on event time or processing time");

        System.out.println("\n--- TTL Configuration ---");
        System.out.println("StateTtlConfig ttlConfig = StateTtlConfig");
        System.out.println("    .newBuilder(Time.hours(24))  // TTL duration");
        System.out.println("    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)");
        System.out.println("    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)");
        System.out.println("    .cleanupInRocksdbCompactFilter(1000)  // Cleanup strategy");
        System.out.println("    .build();");

        System.out.println("\n--- Update Types ---");
        System.out.println("OnCreateAndWrite: Update TTL on create and write");
        System.out.println("OnReadAndWrite: Update TTL on read and write");
        System.out.println("Disabled: TTL never updated");

        System.out.println("\n--- State Visibility ---");
        System.out.println("NeverReturnExpired: Never return expired state");
        System.out.println("ReturnExpiredIfNotCleanedUp: Return expired if not cleaned");

        System.out.println("\n--- Cleanup Strategies ---");
        System.out.println("1. Full state scan: Expensive, thorough");
        System.out.println("2. Incremental cleanup: Background cleanup");
        System.out.println("3. RocksDB compaction: During compaction");
        System.out.println("4. Cleanup in snapshot: During checkpoint");

        System.out.println("\n--- Example: TTL Configuration ---");
        System.out.println("StateTtlConfig ttlConfig = StateTtlConfig");
        System.out.println("    .newBuilder(Time.hours(1))");
        System.out.println("    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)")
        .println("    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)")
        .println("    .cleanupInRocksdbCompactFilter(1000)")
        .println("    .build();");
        System.out.println("");
        System.out.println("ValueStateDescriptor<Integer> descriptor =");
        System.out.println("    new ValueStateDescriptor<>(\"count\", Integer.class);");
        System.out.println("descriptor.enableTimeToLive(ttlConfig);");

        System.out.println("\n--- TTL Best Practices ---");
        System.out.println("1. Use TTL for unbounded state");
        System.out.println("2. Choose appropriate TTL duration");
        System.out.println("3. Select efficient cleanup strategy");
        System.out.println("4. Monitor state size with TTL");
    }

    /**
     * Section 5: State Access Patterns
     * Demonstrates efficient state access patterns
     */
    public static void section5_state_access_patterns() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 5: STATE ACCESS PATTERNS");
        System.out.println("================================================================================");

        System.out.println("\n--- Efficient State Access ---");
        System.out.println("1. Minimize state access");
        System.out.println("2. Batch state updates");
        System.out.println("3. Use appropriate state type");
        System.out.println("4. Avoid state in hot path");

        System.out.println("\n--- Pattern 1: Check Before Update ---");
        System.out.println("Good: Check if update needed");
        System.out.println("Integer current = state.value();");
        System.out.println("if (current == null || current != newValue) {");
        System.out.println("    state.update(newValue);");
        System.out.println("}");

        System.out.println("\nBad: Always update");
        System.out.println("state.update(newValue);  // Unnecessary update");

        System.out.println("\n--- Pattern 2: Batch Updates ---");
        System.out.println("Good: Batch updates");
        System.out.println("ListState<String> buffer;");
        System.out.println("buffer.add(event);");
        System.out.println("if (buffer.get().spliterator().estimateSize() > 1000) {");
        System.out.println("    processBuffer(buffer);");
        System.out.println("    buffer.clear();");
        System.out.println("}");

        System.out.println("\n--- Pattern 3: Use Map for Lookups ---");
        System.out.println("Good: Use MapState for lookups");
        System.out.println("MapState<String, User> userState;");
        System.out.println("User user = userState.get(userId);");

        System.out.println("\nBad: Use ListState for lookups");
        System.out.println("ListState<User> userState;");
        System.out.println("// O(n) lookup");
        System.out.println("for (User user : userState.get()) {");
        System.out.println("    if (user.getId().equals(userId)) break;");
        System.out.println("}");

        System.out.println("\n--- Pattern 4: Lazy State Initialization ---");
        System.out.println("Good: Initialize state in open()");
        System.out.println("@Override");
        System.out.println("public void open(Configuration parameters) {");
        System.out.println("    state = getRuntimeContext().getState(descriptor);");
        System.out.println("}");

        System.out.println("\nBad: Initialize in processElement()");
        System.out.println("// Repeated initialization");
        System.out.println("if (state == null) {");
        System.out.println("    state = getRuntimeContext().getState(descriptor);");
        System.out.println("}");

        System.out.println("\n--- Access Pattern Guidelines ---");
        System.out.println("1. Minimize state access frequency");
        System.out.println("2. Use appropriate state type for access pattern");
        System.out.println("3. Batch updates when possible");
        System.out.println("4. Initialize state in open()");
    }

    /**
     * Section 6: State Best Practices
     * Demonstrates state management best practices
     */
    public static void section6_state_best_practices() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 6: STATE BEST PRACTICES");
        System.out.println("================================================================================");

        System.out.println("\n--- Best Practice 1: Use TTL ---");
        System.out.println("Always configure TTL for unbounded state");
        System.out.println("StateTtlConfig ttlConfig = StateTtlConfig");
        System.out.println("    .newBuilder(Time.hours(24)).build();");
        System.out.println("descriptor.enableTimeToLive(ttlConfig);");

        System.out.println("\n--- Best Practice 2: Choose Right State Type ---");
        System.out.println("ValueState: Single value");
        System.out.println("ListState: Multiple values");
        System.out.println("MapState: Key-value lookups");
        System.out.println("ReducingState: Simple aggregation");

        System.out.println("\n--- Best Practice 3: Monitor State Size ---");
        System.out.println("Check Flink Web UI → Checkpoints");
        System.out.println("  - State size per operator");
        System.out.println("  - State size per subtask");
        System.out.println("  - State backend metrics");

        System.out.println("\n--- Best Practice 4: Use RocksDB for Large State ---");
        System.out.println("env.setStateBackend(new RocksDBStateBackend(\"hdfs:///checkpoints\"));");
        System.out.println("  - Supports very large state");
        System.out.println("  - State on disk");
        System.out.println("  - Slower than memory");

        System.out.println("\n--- Best Practice 5: Clear Unused State ---");
        System.out.println("state.clear();  // Clear when no longer needed");
        System.out.println("  - Frees memory");
        System.out.println("  - Reduces checkpoint size");

        System.out.println("\n--- Best Practice 6: Use Incremental Checkpoints ---");
        System.out.println("env.getCheckpointConfig().enableUnalignedCheckpoints();");
        System.out.println("  - Faster for large state");
        System.out.println("  - Less storage");

        System.out.println("\n--- Anti-Patterns ---");
        System.out.println("Anti-pattern 1: Unbounded state without TTL");
        System.out.println("  - Problem: State grows indefinitely");
        System.out.println("  - Solution: Configure TTL");

        System.out.println("\nAnti-pattern 2: State in hot path");
        System.out.println("  - Problem: Performance degradation");
        System.out.println("  - Solution: Minimize state access");

        System.out.println("\nAnti-pattern 3: Wrong state type");
        System.out.println("  - Problem: Inefficient access");
        System.out.println("  - Solution: Use MapState for lookups");
    }

    /**
     * Section 7: State in Flink SQL
     * Demonstrates state management in SQL
     */
    public static void section7_state_in_sql() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 7: STATE IN FLINK SQL");
        System.out.println("================================================================================");

        System.out.println("\n--- State in SQL ---");
        System.out.println("Flink SQL automatically manages state");
        System.out.println("  - GROUP BY: Aggregation state");
        System.out.println("  - JOIN: Join state");
        System.out.println("  - OVER: Window state");
        System.out.println("  - MATCH_RECOGNIZE: Pattern state");

        System.out.println("\n--- GROUP BY State ---");
        System.out.println("SELECT userId, COUNT(*) FROM events GROUP BY userId");
        System.out.println("  - State: Count per userId");
        System.out.println("  - TTL: Configured via table config");

        System.out.println("\n--- JOIN State ---");
        System.out.println("SELECT * FROM orders JOIN users ON orders.userId = users.id");
        System.out.println("  - State: User table for join");
        System.out.println("  - TTL: Configured via join hint");

        System.out.println("\n--- Configuring State TTL in SQL ---");
        System.out.println("TableConfig tableConfig = tableEnv.getConfig();");
        System.out.println("tableConfig.setIdleStateRetentionTime(");
        System.out.println("    Time.hours(1),  // min TTL");
        System.out.println("    Time.hours(24)   // max TTL");
        System.out.println(");");

        System.out.println("\n--- State Backend Configuration for SQL ---");
        System.out.println("tableEnv.getConfig().set(\"state.backend\", \"rocksdb\");");
        System.out.println("tableEnv.getConfig().set(\"state.checkpoints.dir\", \"hdfs:///checkpoints\");");

        System.out.println("\n--- Monitoring SQL State ---");
        System.out.println("Flink Web UI → Checkpoints");
        System.out.println("  - State size per operator");
        System.out.println("  - Identify stateful SQL operators");

        System.out.println("\n--- SQL State Best Practices ---");
        System.out.println("1. Configure idle state retention");
        System.out.println("2. Use appropriate state backend");
        System.out.println("3. Monitor state size");
        System.out.println("4. Use window functions for bounded state");
    }

    /**
     * Section 8: State Monitoring
     * Demonstrates how to monitor state
     */
    public static void section8_state_monitoring() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 8: STATE MONITORING");
        System.out.println("================================================================================");

        System.out.println("\n--- Monitoring Methods ---");
        System.out.println("1. Flink Web UI → Checkpoints");
        System.out.println("2. Flink Web UI → State Backends");
        System.out.println("3. Metrics system");
        System.out.println("4. REST API");

        System.out.println("\n--- Web UI Metrics ---");
        System.out.println("State size:");
        System.out.println("  - Total state size");
        System.out.println("  - State size per operator");
        System.out.println("  - State size per subtask");

        System.out.println("\n--- Key Metrics ---");
        System.out.println("numStateEntriesPerKeyedStateGroup: Number of state entries");
        System.out.println("lastCheckpointSize: Size of last checkpoint");
        System.out.println("lastCheckpointDuration: Time to complete checkpoint");
        System.out.println("numBytesInLocalState: Local state size");

        System.out.println("\n--- Common Issues ---");
        System.out.println("Issue 1: State growing indefinitely");
        System.out.println("  - Symptom: Increasing state size");
        System.out.println("  - Cause: No TTL, unbounded state");
        System.out.println("  - Solution: Configure TTL");

        System.out.println("\nIssue 2: Slow checkpoints");
        System.out.println("  - Symptom: Long checkpoint duration");
        System.out.println("  - Cause: Large state, slow backend");
        System.out.println("  - Solution: Use RocksDB, incremental checkpoints");

        System.out.println("\nIssue 3: High memory usage");
        System.out.println("  - Symptom: OOM errors");
        System.out.println("  - Cause: State in memory");
        System.out.println("  - Solution: Use RocksDB, reduce state");

        System.out.println("\n--- Monitoring Checklist ---");
        System.out.println("[ ] Monitor state size");
        System.out.println("[ ] Check checkpoint duration");
        System.out.println("[ ] Monitor state backend metrics");
        System.out.println("[ ] Alert on state growth");
    }

    /**
     * Section 9: State Tuning
     * Demonstrates state tuning best practices
     */
    public static void section9_state_tuning() {
        System.out.println("\n================================================================================");
        System.out.println("SECTION 9: STATE TUNING");
        System.out.println("================================================================================");

        System.out.println("\n--- Tuning Guidelines ---");
        System.out.println("1. State backend selection");
        System.out.println("2. TTL configuration");
        System.out.println("3. Checkpoint tuning");
        System.out.println("4. State access optimization");

        System.out.println("\n--- State Backend Selection ---");
        System.out.println("MemoryStateBackend:");
        System.out.println("  - Small state (< few GB)");
        System.out.println("  - Fast access");
        System.out.println("  - Testing only");

        System.out.println("\nFsStateBackend:");
        System.out.println("  - Medium state (< tens of GB)");
        System.out.println("  - Balanced performance");
        System.out.println("  - Production");

        System.out.println("\nRocksDBStateBackend:");
        System.out.println("  - Large state (> tens of GB)");
        System.out.println("  - Slower but scalable");
        System.out.println("  - Production with large state");

        System.out.println("\n--- TTL Tuning ---");
        System.out.println("StateTtlConfig ttlConfig = StateTtlConfig");
        System.out.println("    .newBuilder(Time.hours(24))");
        System.out.println("    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)")
        .println("    .cleanupInRocksdbCompactFilter(1000)")
        .println("    .build();");

        System.out.println("\n--- Checkpoint Tuning ---");
        System.out.println("env.enableCheckpointing(60000);  // 1 minute interval");
        System.out.println("env.getCheckpointConfig().setCheckpointTimeout(300000);");
        System.out.println("env.getCheckpointConfig().enableUnalignedCheckpoints();");

        System.out.println("\n--- RocksDB Tuning ---");
        System.out.println("env.setStateBackend(new RocksDBStateBackend(\"hdfs:///checkpoints\"));");
        System.out.println("// Tune RocksDB options:");
        System.out.println("// - Block cache size");
        System.out.println("// - Write buffer size");
        System.out.println("// - Compression");

        System.out.println("\n--- Tuning Checklist ---");
        System.out.println("[ ] Choose appropriate state backend");
        System.out.println("[ ] Configure TTL for unbounded state");
        System.out.println("[ ] Tune checkpoint interval");
        System.out.println("[ ] Monitor state size");
        System.out.println("[ ] Optimize state access patterns");
    }

    /**
     * Example: Stateful Process Function with Multiple State Types
     */
    public static class MultiStateFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private ValueState<Integer> countState;
        private ListState<String> historyState;
        private MapState<String, Integer> mapState;

        @Override
        public void open(Configuration parameters) {
            // Value state for counter
            ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
                "count",
                TypeInformation.of(Integer.class)
            );
            countState = getRuntimeContext().getState(countDescriptor);

            // List state for history
            ListStateDescriptor<String> historyDescriptor = new ListStateDescriptor<>(
                "history",
                TypeInformation.of(String.class)
            );
            historyState = getRuntimeContext().getListState(historyDescriptor);

            // Map state for lookups
            MapStateDescriptor<String, Integer> mapDescriptor = new MapStateDescriptor<>(
                "map",
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class)
            );
            mapState = getRuntimeContext().getMapState(mapDescriptor);
        }

        @Override
        public void processElement(
            Tuple2<String, Integer> value,
            KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>::Context ctx,
            Collector<Tuple2<String, Integer>> out
        ) throws Exception {
            // Update value state
            Integer currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0;
            }
            currentCount += value.f1;
            countState.update(currentCount);

            // Add to history
            historyState.add(value.f0 + ":" + value.f1);

            // Update map state
            mapState.put(value.f0, value.f1);

            out.collect(new Tuple2<>(value.f0, currentCount));
        }
    }

    public static void main(String[] args) {
        System.out.println("================================================================================");
        System.out.println("APACHE FLINK STATE MANAGEMENT - CODE EXAMPLES");
        System.out.println("================================================================================");

        // Run all sections
        section1_understanding_state();
        section2_keyed_vs_operator_state();
        section3_state_types();
        section4_state_ttl();
        section5_state_access_patterns();
        section6_state_best_practices();
        section7_state_in_sql();
        section8_state_monitoring();
        section9_state_tuning();

        System.out.println("\n================================================================================");
        System.out.println("ALL DEMONSTRATIONS COMPLETED");
        System.out.println("================================================================================");
        System.out.println("\nKey Takeaways:");
        System.out.println("1. State enables complex operations (aggregations, joins)");
        System.out.println("2. Keyed state is per-key, operator state is per-operator");
        System.out.println("3. Choose appropriate state type (Value, List, Map, Reducing)");
        System.out.println("4. Configure TTL to prevent unbounded state growth");
        System.out.println("5. Optimize state access patterns for performance");
        System.out.println("6. Use RocksDB for large state in production");
        System.out.println("7. Flink SQL automatically manages state");
        System.out.println("8. Monitor state size in Flink Web UI");
        System.out.println("9. Tune state backend and checkpointing for performance");
        System.out.println("\nCheck Flink Web UI for state metrics");
    }
}
