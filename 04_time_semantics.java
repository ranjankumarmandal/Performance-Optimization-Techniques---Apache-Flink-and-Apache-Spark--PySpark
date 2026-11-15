import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class TimeSemantics {

    public static void section1_processingTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic.ProcessingTime);

        List<Tuple3<Long, String, Integer>> data = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple3<>(now + i * 1000, "key" + (i % 3), i));
        }

        DataStream<Tuple3<Long, String, Integer>> stream = env.fromCollection(data);

        stream.print();

        env.execute("ProcessingTimeDemo");
    }

    public static void section2_ingestionTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic.IngestionTime);

        List<Tuple3<Long, String, Integer>> data = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple3<>(now + i * 1000, "key" + (i % 3), i));
        }

        DataStream<Tuple3<Long, String, Integer>> stream = env.fromCollection(data);

        stream.print();

        env.execute("IngestionTimeDemo");
    }

    public static void section3_eventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic.EventTime);

        List<Tuple3<Long, String, Integer>> data = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple3<>(now + i * 1000, "key" + (i % 3), i));
        }

        DataStream<Tuple3<Long, String, Integer>> stream = env.fromCollection(data);

        WatermarkStrategy<Tuple3<Long, String, Integer>> watermarkStrategy = WatermarkStrategy
            .<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.f0);

        DataStream<Tuple3<Long, String, Integer>> withWatermarks = stream
            .assignTimestampsAndWatermarks(watermarkStrategy);

        withWatermarks.print();

        env.execute("EventTimeDemo");
    }

    public static void section4_watermarkStrategy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<Long, String, Integer>> data = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple3<>(now + i * 1000, "key" + (i % 3), i));
        }

        DataStream<Tuple3<Long, String, Integer>> stream = env.fromCollection(data);

        WatermarkStrategy<Tuple3<Long, String, Integer>> watermarkStrategy = WatermarkStrategy
            .<Tuple3<Long, String, Integer>>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.f0);

        DataStream<Tuple3<Long, String, Integer>> withWatermarks = stream
            .assignTimestampsAndWatermarks(watermarkStrategy);

        withWatermarks.print();

        env.execute("WatermarkStrategyDemo");
    }

    public static void section5_boundedOutOfOrderness() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<Long, String, Integer>> data = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple3<>(now + i * 1000, "key" + (i % 3), i));
        }

        DataStream<Tuple3<Long, String, Integer>> stream = env.fromCollection(data);

        WatermarkStrategy<Tuple3<Long, String, Integer>> watermarkStrategy = WatermarkStrategy
            .<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.f0);

        DataStream<Tuple3<Long, String, Integer>> withWatermarks = stream
            .assignTimestampsAndWatermarks(watermarkStrategy);

        withWatermarks.print();

        env.execute("BoundedOutOfOrdernessDemo");
    }

    public static void section6_customWatermarkGenerator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<Long, String, Integer>> data = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple3<>(now + i * 1000, "key" + (i % 3), i));
        }

        DataStream<Tuple3<Long, String, Integer>> stream = env.fromCollection(data);

        WatermarkStrategy<Tuple3<Long, String, Integer>> watermarkStrategy = WatermarkStrategy
            .<Tuple3<Long, String, Integer>>forGenerator(ctx -> new CustomWatermarkGenerator())
            .withTimestampAssigner((event, timestamp) -> event.f0);

        DataStream<Tuple3<Long, String, Integer>> withWatermarks = stream
            .assignTimestampsAndWatermarks(watermarkStrategy);

        withWatermarks.print();

        env.execute("CustomWatermarkGeneratorDemo");
    }

    public static void section7_idleSources() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<Long, String, Integer>> data = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 1; i <= 10; i++) {
            data.add(new Tuple3<>(now + i * 1000, "key" + (i % 3), i));
        }

        DataStream<Tuple3<Long, String, Integer>> stream = env.fromCollection(data);

        WatermarkStrategy<Tuple3<Long, String, Integer>> watermarkStrategy = WatermarkStrategy
            .<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.f0)
            .withIdleness(Duration.ofMinutes(1));

        DataStream<Tuple3<Long, String, Integer>> withWatermarks = stream
            .assignTimestampsAndWatermarks(watermarkStrategy);

        withWatermarks.print();

        env.execute("IdleSourcesDemo");
    }

    public static void main(String[] args) throws Exception {
        section1_processingTime();
        section2_ingestionTime();
        section3_eventTime();
        section4_watermarkStrategy();
        section5_boundedOutOfOrderness();
        section6_customWatermarkGenerator();
        section7_idleSources();
    }

    public static class CustomWatermarkGenerator implements WatermarkGenerator<Tuple3<Long, String, Integer>> {
        private long maxTimestamp;

        @Override
        public void onEvent(Tuple3<Long, String, Integer> event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, event.f0);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTimestamp - 1000));
        }
    }
}
