import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class StateManagementAndBackends {

    public static void section1_stateTypes() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("key1", 1));
        data.add(new Tuple2<>("key1", 2));
        data.add(new Tuple2<>("key2", 3));
        data.add(new Tuple2<>("key1", 4));

        DataStream<Tuple2<String, Integer>> stream = env.fromCollection(data);

        stream.keyBy(value -> value.f0)
              .process(new KeyedStateExample())
              .print();

        env.execute("StateTypesDemo");
    }

    public static void section2_valueState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> data = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple2<>("key" + (i % 3), i));
        }

        DataStream<Tuple2<String, Integer>> stream = env.fromCollection(data);

        stream.keyBy(value -> value.f0)
              .process(new ValueStateExample())
              .print();

        env.execute("ValueStateDemo");
    }

    public static void section3_listState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> data = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple2<>("key" + (i % 3), i));
        }

        DataStream<Tuple2<String, Integer>> stream = env.fromCollection(data);

        stream.keyBy(value -> value.f0)
              .process(new ListStateExample())
              .print();

        env.execute("ListStateDemo");
    }

    public static void section4_mapState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<String, String, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>("user1", "productA", 100));
        data.add(new Tuple3<>("user1", "productB", 200));
        data.add(new Tuple3<>("user2", "productA", 150));
        data.add(new Tuple3<>("user1", "productA", 50));

        DataStream<Tuple3<String, String, Integer>> stream = env.fromCollection(data);

        stream.keyBy(value -> value.f0)
              .process(new MapStateExample())
              .print();

        env.execute("MapStateDemo");
    }

    public static void section5_reducingState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> data = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple2<>("key" + (i % 3), i));
        }

        DataStream<Tuple2<String, Integer>> stream = env.fromCollection(data);

        stream.keyBy(value -> value.f0)
              .process(new ReducingStateExample())
              .print();

        env.execute("ReducingStateDemo");
    }

    public static void section6_aggregatingState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> data = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple2<>("key" + (i % 3), i));
        }

        DataStream<Tuple2<String, Integer>> stream = env.fromCollection(data);

        stream.keyBy(value -> value.f0)
              .process(new AggregatingStateExample())
              .print();

        env.execute("AggregatingStateDemo");
    }

    public static void section7_stateTTL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> data = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            data.add(new Tuple2<>("key" + (i % 3), i));
        }

        DataStream<Tuple2<String, Integer>> stream = env.fromCollection(data);

        stream.keyBy(value -> value.f0)
              .process(new StateTTLExample())
              .print();

        env.execute("StateTTLDemo");
    }

    public static void section8_stateBackendConfiguration() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new org.apache.flink.contrib.streaming.state.RocksDBStateBackend("file:///tmp/flink-rocksdb", true));

        env.getCheckpointConfig().setCheckpointInterval(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("key1", 1));
        data.add(new Tuple2<>("key1", 2));

        DataStream<Tuple2<String, Integer>> stream = env.fromCollection(data);

        stream.keyBy(value -> value.f0)
              .process(new ValueStateExample())
              .print();

        env.execute("StateBackendConfigDemo");
    }

    public static void main(String[] args) throws Exception {
        section1_stateTypes();
        section2_valueState();
        section3_listState();
        section4_mapState();
        section5_reducingState();
        section6_aggregatingState();
        section7_stateTTL();
        section8_stateBackendConfiguration();
    }

    public static class KeyedStateExample extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {
        private ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                "count",
                Types.INT
            );
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            Integer count = countState.value();
            if (count == null) {
                count = 0;
            }
            count++;
            countState.update(count);
            out.collect("Key: " + value.f0 + ", Count: " + count);
        }
    }

    public static class ValueStateExample extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {
        private ValueState<Integer> lastValue;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                "lastValue",
                Types.INT
            );
            lastValue = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            Integer last = lastValue.value();
            lastValue.update(value.f1);
            out.collect("Key: " + value.f0 + ", Current: " + value.f1 + ", Last: " + (last != null ? last : "null"));
        }
    }

    public static class ListStateExample extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {
        private ListState<Integer> valuesState;

        @Override
        public void open(Configuration parameters) {
            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>(
                "values",
                Types.INT
            );
            valuesState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            valuesState.add(value.f1);
            List<Integer> values = new ArrayList<>();
            for (Integer val : valuesState.get()) {
                values.add(val);
            }
            out.collect("Key: " + value.f0 + ", Values: " + values);
        }
    }

    public static class MapStateExample extends KeyedProcessFunction<String, Tuple3<String, String, Integer>, String> {
        private MapState<String, Integer> productState;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>(
                "products",
                Types.STRING,
                Types.INT
            );
            productState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Tuple3<String, String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            Integer current = productState.get(value.f1);
            if (current == null) {
                current = 0;
            }
            current += value.f2;
            productState.put(value.f1, current);
            out.collect("User: " + value.f0 + ", Product: " + value.f1 + ", Total: " + current);
        }
    }

    public static class ReducingStateExample extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {
        private ReducingState<Integer> sumState;

        @Override
        public void open(Configuration parameters) {
            ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<>(
                "sum",
                (v1, v2) -> v1 + v2,
                Types.INT
            );
            sumState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            sumState.add(value.f1);
            out.collect("Key: " + value.f0 + ", Sum: " + sumState.get());
        }
    }

    public static class AggregatingStateExample extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {
        private AggregatingState<Integer, String> avgState;

        @Override
        public void open(Configuration parameters) {
            AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, String> descriptor = 
                new AggregatingStateDescriptor<>(
                    "avg",
                    new AverageAggregate(),
                    Types.STRING
                );
            avgState = getRuntimeContext().getAggregatingState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            avgState.add(value.f1);
            out.collect("Key: " + value.f0 + ", Avg: " + avgState.get());
        }

        public static class AverageAggregate implements org.apache.flink.api.common.functions.AggregateFunction<Integer, Tuple2<Integer, Integer>, String> {
            @Override
            public Tuple2<Integer, Integer> createAccumulator() {
                return new Tuple2<>(0, 0);
            }

            @Override
            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                return new Tuple2<>(accumulator.f0 + value, accumulator.f1 + 1);
            }

            @Override
            public String getResult(Tuple2<Integer, Integer> accumulator) {
                return String.valueOf(accumulator.f0 / (double) accumulator.f1);
            }

            @Override
            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
            }
        }
    }

    public static class StateTTLExample extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {
        private ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();

            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                "valueWithTTL",
                Types.INT
            );
            descriptor.enableTimeToLive(ttlConfig);
            valueState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            valueState.update(value.f1);
            Integer stored = valueState.value();
            out.collect("Key: " + value.f0 + ", Value: " + value.f1 + ", Stored: " + stored);
        }
    }
}
