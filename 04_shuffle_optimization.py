"""
================================================================================
PYSPARK SHUFFLE OPTIMIZATION - CODE EXAMPLES
================================================================================

This file demonstrates shuffle optimization strategies with practical
examples for staff engineer preparation.

Topics Covered:
1. Understanding Shuffle Operations
2. Reduce Shuffle Operations
3. Shuffle File Handling
4. Shuffle Consolidation
5. Shuffle Sort vs Hash
6. Broadcast Hash Join
7. Shuffle Partition Count
8. Shuffle Read/Write Optimization
9. External Shuffle Service
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when, broadcast, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time


def create_spark_session():
    """
    Create and return a SparkSession with appropriate configuration.
    """
    spark = SparkSession.builder \
        .appName("ShuffleOptimizationDemo") \
        .master("local[4]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
        .getOrCreate()
    
    print("=" * 80)
    print("SPARK SESSION CREATED")
    print("=" * 80)
    print(f"Spark Version: {spark.version}")
    print(f"Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
    print(f"Broadcast Join Threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")
    print("=" * 80)
    
    return spark


def section_1_understanding_shuffle_operations(spark):
    """
    Section 1: Understanding Shuffle Operations
    Demonstrates what shuffle operations are and why they're expensive
    """
    print("\n" + "=" * 80)
    print("SECTION 1: UNDERSTANDING SHUFFLE OPERATIONS")
    print("=" * 80)
    
    print("\n--- What is Shuffle? ---")
    print("Shuffle: Redistributing data across partitions")
    print("  - Wide dependencies trigger shuffle")
    print("  - Expensive: disk I/O, network I/O, serialization")
    print("  - Stage boundary in execution plan")
    
    print("\n--- Common Shuffle Operations ---")
    shuffle_ops = [
        "groupByKey",
        "reduceByKey",
        "join",
        "distinct",
        "repartition",
        "orderBy",
        "intersect",
        "except"
    ]
    print("Operations that trigger shuffle:")
    for op in shuffle_ops:
        print(f"  - {op}")
    
    print("\n--- Narrow vs Wide Dependencies ---")
    print("Narrow dependencies (no shuffle):")
    print("  - map, filter, union")
    print("  - Data stays on same partition")
    print("  - Can be pipelined")
    
    print("\nWide dependencies (shuffle):")
    print("  - groupByKey, join, distinct")
    print("  - Data redistributed across partitions")
    print("  - Stage boundary")
    
    print("\n--- Shuffle Cost Components ---")
    print("1. Disk I/O: writing shuffle files")
    print("2. Network I/O: transferring data between executors")
    print("3. Serialization: converting data to bytes")
    print("4. Deserialization: converting bytes back to objects")
    print("5. Memory: buffering shuffle data")
    
    print("\n--- Example: Shuffle vs No Shuffle ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 10) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "group_id"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # No shuffle operation
    print("\n--- No Shuffle (filter) ---")
    df_filtered = df.filter(col("group_id") > 5)
    print(f"filter: {df_filtered.count()} rows (no shuffle)")
    
    # Shuffle operation
    print("\n--- With Shuffle (groupBy) ---")
    df_grouped = df.groupBy("group_id").count()
    print(f"groupBy: {df_grouped.count()} groups (triggers shuffle)")
    
    print("\n--- Minimizing Shuffles ---")
    print("Strategy: Minimize shuffles in job logic")
    print("  - Filter before shuffle")
    print("  - Combine multiple shuffles")
    print("  - Use reduceByKey over groupByKey")
    print("  - Use broadcast joins when possible")


def section_2_reduce_shuffle_operations(spark):
    """
    Section 2: Reduce Shuffle Operations
    Demonstrates techniques to minimize shuffle operations
    """
    print("\n" + "=" * 80)
    print("SECTION 2: REDUCE SHUFFLE OPERATIONS")
    print("=" * 80)
    
    print("\n--- Technique 1: Filter Before Shuffle ---")
    print("Filter data before expensive shuffle operations")
    
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Bad: Shuffle all data then filter
    print("\n--- Bad: Shuffle Then Filter ---")
    start = time.time()
    result_bad = df.groupBy("group_id").agg({"value": "sum"}).filter(col("group_id") < 10)
    count_bad = result_bad.count()
    time_bad = time.time() - start
    print(f"Result: {count_bad} rows")
    print(f"Time: {time_bad:.4f} seconds")
    
    # Good: Filter then shuffle
    print("\n--- Good: Filter Then Shuffle ---")
    start = time.time()
    result_good = df.filter(col("group_id") < 10).groupBy("group_id").agg({"value": "sum"})
    count_good = result_good.count()
    time_good = time.time() - start
    print(f"Result: {count_good} rows")
    print(f"Time: {time_good:.4f} seconds")
    print(f"Speedup: {time_bad/time_good:.2f}x")
    
    print("\n--- Technique 2: reduceByKey vs groupByKey ---")
    print("reduceByKey: map-side combine reduces shuffle data")
    print("groupByKey: no map-side combine, more shuffle")
    
    # RDD example (since reduceByKey is RDD API)
    rdd = spark.sparkContext.parallelize([(i, i * 10) for i in range(1, 1001)])
    
    print("\n--- reduceByKey (with map-side combine) ---")
    start = time.time()
    result_reduce = rdd.reduceByKey(lambda x, y: x + y)
    count_reduce = result_reduce.count()
    time_reduce = time.time() - start
    print(f"Result: {count_reduce} keys")
    print(f"Time: {time_reduce:.4f} seconds")
    
    print("\n--- groupByKey (no map-side combine) ---")
    start = time.time()
    result_group = rdd.groupByKey().mapValues(sum)
    count_group = result_group.count()
    time_group = time.time() - start
    print(f"Result: {count_group} keys")
    print(f"Time: {time_group:.4f} seconds")
    print(f"reduceByKey is {time_group/time_reduce:.2f}x faster")
    
    print("\n--- Technique 3: Combine Multiple Shuffles ---")
    print("Combine operations to reduce number of shuffles")
    
    # Bad: Multiple shuffles
    print("\n--- Bad: Multiple Shuffles ---")
    df1 = df.groupBy("group_id").agg({"value": "sum"})
    df2 = df.groupBy("group_id").agg({"value": "avg"})
    df3 = df1.join(df2, "group_id")
    
    # Good: Single shuffle
    print("\n--- Good: Single Shuffle ---")
    df_combined = df.groupBy("group_id").agg(
        _sum("value").alias("total"),
        (_sum("value") / _count("value")).alias("average")
    )
    
    print("Combined aggregation in single groupBy operation")


def section_3_shuffle_file_handling(spark):
    """
    Section 3: Shuffle File Handling
    Demonstrates shuffle file configuration
    """
    print("\n" + "=" * 80)
    print("SECTION 3: SHUFFLE FILE HANDLING")
    print("=" * 80)
    
    print("\n--- Shuffle File Configuration ---")
    configs = [
        ("spark.shuffle.file.buffer", "Buffer size for shuffle writes (default 32KB)"),
        ("spark.shuffle.io.maxRetries", "Max retries for fetch failures (default 3)"),
        ("spark.shuffle.io.retryWait", "Wait between retries (default 5s)"),
        ("spark.shuffle.compress", "Compress shuffle data (default true)"),
        ("spark.shuffle.spill.compress", "Compress spill data (default true)"),
    ]
    
    for config, description in configs:
        value = spark.conf.get(config, "default")
        print(f"{config}: {value}")
        print(f"  {description}")
    
    print("\n--- Tuning Shuffle File Buffer ---")
    print("spark.shuffle.file.buffer:")
    print("  - Default: 32KB")
    print("  - Increase for large shuffles (e.g., 64KB, 128KB)")
    print("  - Trade-off: memory vs I/O")
    
    print("\n--- Compression Configuration ---")
    print("Compression codecs:")
    print("  - spark.shuffle.compression.codec: lz4 (default), lzf, snappy, zstd")
    print("  - LZ4: fast, good compression")
    print("  - ZSTD: best compression, slower")
    print("  - Snappy: fast, moderate compression")
    
    print("\n--- Setting Compression Codec ---")
    print("spark.shuffle.compression.codec: zstd  # Better compression")
    print("spark.shuffle.compress: true  # Enable compression")
    
    print("\n--- Retry Configuration ---")
    print("For unreliable networks:")
    print("spark.shuffle.io.maxRetries: 5  # More retries")
    print("spark.shuffle.io.retryWait: 10s  # Longer wait")
    
    print("\n--- Monitoring Shuffle Files ---")
    print("Check Spark UI:")
    print("  - Stages tab: Shuffle Read/Write metrics")
    print("  - Executors tab: Shuffle files on disk")


def section_4_shuffle_consolidation(spark):
    """
    Section 4: Shuffle Consolidation
    Demonstrates shuffle file consolidation
    """
    print("\n" + "=" * 80)
    print("SECTION 4: SHUFFLE CONSOLIDATION")
    print("=" * 80)
    
    print("\n--- Understanding Shuffle Consolidation ---")
    print("Shuffle consolidation: Combine shuffle files")
    print("  - Reduces number of open files")
    print("  - Important for large number of partitions")
    print("  - Enabled by default in Spark 1.6+")
    
    print("\n--- Configuration ---")
    print("spark.shuffle.consolidateFiles: true (default in Spark 1.6+)")
    
    print("\n--- Without Consolidation ---")
    print("Each task creates one file per reducer")
    print("Example: 100 tasks x 200 partitions = 20,000 files")
    
    print("\n--- With Consolidation ---")
    print("Files are consolidated within executor")
    print("Example: 4 executors x 200 partitions = 800 files")
    
    print("\n--- When Consolidation Matters ---")
    print("Useful when:")
    print("  - Many tasks (high parallelism)")
    print("  - Many partitions")
    print("  - File system has file handle limits")
    
    print("\n--- Current Status ---")
    consolidate = spark.conf.get("spark.shuffle.consolidateFiles", "true")
    print(f"Shuffle consolidation: {consolidate}")
    
    print("\n--- Monitoring File Count ---")
    print("Check number of shuffle files:")
    print("  - Spark UI: Executors tab")
    print("  - File system: ls shuffle directory")
    print("  - Too many files: enable consolidation")


def section_5_shuffle_sort_vs_hash(spark):
    """
    Section 5: Shuffle Sort vs Hash
    Demonstrates different shuffle managers
    """
    print("\n" + "=" * 80)
    print("SECTION 5: SHUFFLE SORT VS HASH")
    print("=" * 80)
    
    print("\n--- Shuffle Managers ---")
    print("1. Sort Shuffle Manager (default)")
    print("   - Sort-based shuffle")
    print("   - Better for large data")
    print("   - Supports all operations")
    
    print("\n2. Hash Shuffle Manager (deprecated)")
    print("   - Hash-based shuffle")
    print("   - Faster for small data")
    print("   - Many files")
    
    print("\n3. Tungsten-Sort Shuffle")
    print("   - Optimized for primitive types")
    print("   - Uses off-heap memory")
    print("   - Auto-selected by Spark")
    
    print("\n--- Current Shuffle Manager ---")
    shuffle_manager = spark.conf.get("spark.shuffle.manager", "sort")
    print(f"Shuffle manager: {shuffle_manager}")
    
    print("\n--- Bypass Merge Threshold ---")
    print("spark.shuffle.sort.bypassMergeThreshold:")
    print("  - Use hash shuffle for small reduces")
    print("  - Default: 200")
    print("  - Set higher to use hash shuffle more")
    
    bypass_threshold = spark.conf.get("spark.shuffle.sort.bypassMergeThreshold", "200")
    print(f"Bypass merge threshold: {bypass_threshold}")
    
    print("\n--- When Each Manager is Used ---")
    print("Sort shuffle:")
    print("  - Default for most operations")
    print("  - Large datasets")
    print("  - Complex operations")
    
    print("\nTungsten-sort:")
    print("  - Primitive types (int, long, etc.)")
    print("  - Simple aggregations")
    print("  - Auto-selected by Spark")
    
    print("\nHash shuffle (bypass):")
    print("  - Small number of reduce partitions")
    print("  - Below bypass merge threshold")
    print("  - Faster for small data")
    
    print("\n--- Recommendation ---")
    print("Use default (sort shuffle)")
    print("  - Spark auto-selects optimal manager")
    print("  - Sort shuffle is most versatile")
    print("  - Tungsten-sort used when beneficial")


def section_6_broadcast_hash_join(spark):
    """
    Section 6: Broadcast Hash Join
    Demonstrates broadcast join optimization
    """
    print("\n" + "=" * 80)
    print("SECTION 6: BROADCAST HASH JOIN")
    print("=" * 80)
    
    print("\n--- Understanding Broadcast Join ---")
    print("Broadcast join: Send small table to all executors")
    print("  - Eliminates shuffle for small table")
    print("  - Fast: hash join on local data")
    print("  - Automatic for tables below threshold")
    
    print("\n--- Configuration ---")
    broadcast_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print(f"Broadcast threshold: {broadcast_threshold}")
    print("Default: 10MB")
    print("-1: disable broadcast")
    print("Higher value: enable for larger tables")
    
    print("\n--- Example: Broadcast Join ---")
    # Create large table
    large_data = [(i, f"user_{i}", i % 100) for i in range(1, 10001)]
    large_df = spark.createDataFrame(large_data, ["id", "name", "region"])
    
    # Create small table
    small_data = [(i, f"region_{i}") for i in range(1, 101)]
    small_df = spark.createDataFrame(small_data, ["region", "region_name"])
    
    print(f"\nLarge table: {large_df.count()} rows")
    print(f"Small table: {small_df.count()} rows")
    
    # Automatic broadcast join
    print("\n--- Automatic Broadcast Join ---")
    start = time.time()
    result_auto = large_df.join(small_df, "region")
    count_auto = result_auto.count()
    time_auto = time.time() - start
    print(f"Result: {count_auto} rows")
    print(f"Time: {time_auto:.4f} seconds")
    
    # Manual broadcast join
    print("\n--- Manual Broadcast Join ---")
    start = time.time()
    result_manual = large_df.join(broadcast(small_df), "region")
    count_manual = result_manual.count()
    time_manual = time.time() - start
    print(f"Result: {count_manual} rows")
    print(f"Time: {time_manual:.4f} seconds")
    
    print("\n--- Check Query Plan ---")
    print("result_auto.explain()")
    print("Look for 'BroadcastHashJoin' in plan")
    
    print("\n--- When to Use Broadcast Join ---")
    print("Use when:")
    print("  - One table is small (< 100MB)")
    print("  - Joining dimension table to fact table")
    print("  - Star schema queries")
    print("  - Lookup tables")
    
    print("\n--- When NOT to Use Broadcast Join ---")
    print("Avoid when:")
    print("  - Both tables are large")
    print("  - Table exceeds memory")
    print("  - Many broadcast joins in same query")
    
    print("\n--- Tuning Broadcast Threshold ---")
    print("Increase threshold:")
    print("spark.sql.autoBroadcastJoinThreshold: 100MB")
    print("\nDisable broadcast:")
    print("spark.sql.autoBroadcastJoinThreshold: -1")


def section_7_shuffle_partition_count(spark):
    """
    Section 7: Shuffle Partition Count
    Demonstrates tuning shuffle partition count
    """
    print("\n" + "=" * 80)
    print("SECTION 7: SHUFFLE PARTITION COUNT")
    print("=" * 80)
    
    print("\n--- Understanding Shuffle Partitions ---")
    print("spark.sql.shuffle.partitions:")
    print("  - Default: 200")
    print("  - Number of partitions after shuffle")
    print("  - Affects parallelism and task overhead")
    
    print("\n--- Current Configuration ---")
    shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    print(f"Shuffle partitions: {shuffle_partitions}")
    
    print("\n--- Too Many Partitions ---")
    print("Problems:")
    print("  - Task overhead (scheduling, serialization)")
    print("  - Small files problem")
    print("  - Increased metadata")
    
    print("\n--- Too Few Partitions ---")
    print("Problems:")
    print("  - OOM on executors")
    print("  - Long task duration")
    print("  - Underutilized cluster")
    
    print("\n--- Optimal Partition Count ---")
    print("Formula: total_data_size / target_partition_size")
    print("Target partition size: 128MB-256MB")
    
    print("\nExample calculations:")
    print("  - 10GB data / 128MB = ~80 partitions")
    print("  - 100GB data / 128MB = ~800 partitions")
    print("  - 1TB data / 128MB = ~8000 partitions")
    
    print("\n--- Demonstration ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100) for i in range(1, 100001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Test with different partition counts
    for partitions in [4, 8, 16]:
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        df_grouped = df.groupBy("group_id").count()
        print(f"Partitions: {partitions}, Result: {df_grouped.rdd.getNumPartitions()} partitions")
    
    # Reset
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    
    print("\n--- Tuning Guidelines ---")
    print("1. Start with default (200)")
    print("2. Check Spark UI for task duration")
    print("3. Reduce if tasks are fast (< 1s)")
    print("4. Increase if tasks are slow (> 5min) or OOM")
    print("5. Target: 1-5 minutes per task")
    
    print("\n--- Dynamic Partition Tuning ---")
    print("Spark 3.0+ AQE:")
    print("spark.sql.adaptive.enabled: true")
    print("spark.sql.adaptive.coalescePartitions.enabled: true")
    print("Automatically coalesces partitions at runtime")


def section_8_shuffle_read_write_optimization(spark):
    """
    Section 8: Shuffle Read/Write Optimization
    Demonstrates shuffle read/write optimization techniques
    """
    print("\n" + "=" * 80)
    print("SECTION 8: SHUFFLE READ/WRITE OPTIMIZATION")
    print("=" * 80)
    
    print("\n--- Compression ---")
    print("spark.shuffle.compress: true (default)")
    print("  - Compress shuffle data")
    print("  - Reduces disk I/O and network I/O")
    print("  - Trade-off: CPU vs I/O")
    
    print("\nspark.shuffle.spill.compress: true (default)")
    print("  - Compress spill data")
    print("  - Reduces disk I/O")
    
    print("\n--- Compression Codecs ---")
    codecs = [
        ("lz4", "Fast, good compression (default)"),
        ("lzf", "Fast, moderate compression"),
        ("snappy", "Fast, compatible"),
        ("zstd", "Best compression, slower"),
    ]
    
    print("Available codecs:")
    for codec, description in codecs:
        print(f"  {codec}: {description}")
    
    print("\n--- Setting Compression ---")
    print("spark.shuffle.compression.codec: lz4")
    print("spark.io.compression.codec: lz4")
    
    print("\n--- Block Size ---")
    print("spark.shuffle.file.buffer: 32KB (default)")
    print("  - Buffer size for shuffle writes")
    print("  - Increase for large shuffles")
    print("  - Typical: 32KB - 128KB")
    
    print("\n--- Network Optimization ---")
    print("spark.shuffle.io.maxRetries: 3 (default)")
    print("  - Max retries for fetch failures")
    
    print("\nspark.shuffle.io.retryWait: 5s (default)")
    print("  - Wait between retries")
    
    print("\nspark.network.timeout: 120s (default)")
    print("  - Network timeout")
    
    print("\n--- Monitoring Shuffle Metrics ---")
    print("Check Spark UI Stages tab:")
    print("  - Shuffle Read Size")
    print("  - Shuffle Write Size")
    print("  - Shuffle Read Records")
    print("  - Shuffle Write Records")
    
    print("\n--- Optimization Checklist ---")
    print("[ ] Enable compression (default)")
    print("[ ] Use LZ4 codec (default)")
    print("[ ] Tune buffer size for large shuffles")
    print("[ ] Increase retries for unreliable networks")
    print("[ ] Monitor shuffle metrics in Spark UI")


def section_9_external_shuffle_service(spark):
    """
    Section 9: External Shuffle Service
    Demonstrates external shuffle service configuration
    """
    print("\n" + "=" * 80)
    print("SECTION 9: EXTERNAL SHUFFLE SERVICE")
    print("=" * 80)
    
    print("\n--- Understanding External Shuffle Service ---")
    print("External shuffle service: Separate service for shuffle files")
    print("  - Allows executors to be removed without losing shuffle files")
    print("  - Required for dynamic allocation")
    print("  - Improves cluster utilization")
    
    print("\n--- Benefits ---")
    print("1. Dynamic allocation support")
    print("  - Executors can be removed when idle")
    print("  - Shuffle files preserved")
    print("  - Better resource utilization")
    
    print("\n2. Improved fault tolerance")
    print("  - Shuffle files survive executor failure")
    print("  - No need to recompute shuffle")
    
    print("\n3. Better cluster utilization")
    print("  - Executors can be reused")
    print("  - Reduced resource waste")
    
    print("\n--- Configuration ---")
    print("spark.shuffle.service.enabled: true")
    print("spark.shuffle.service.port: 7337 (default)")
    
    print("\n--- Current Status ---")
    shuffle_service = spark.conf.get("spark.shuffle.service.enabled", "false")
    print(f"External shuffle service: {shuffle_service}")
    
    print("\n--- Dynamic Allocation ---")
    print("Requires external shuffle service:")
    print("spark.dynamicAllocation.enabled: true")
    print("spark.dynamicAllocation.minExecutors: 2")
    print("spark.dynamicAllocation.maxExecutors: 10")
    
    print("\n--- When to Enable ---")
    print("Enable when:")
    print("  - Using dynamic allocation")
    print("  - Running on YARN")
    print("  - Running on Kubernetes")
    print("  - Need executor removal without recomputation")
    
    print("\n--- When Not Needed ---")
    print("Not needed when:")
    print("  - Standalone cluster without dynamic allocation")
    print("  - Static executor allocation")
    print("  - Simple workloads")
    
    print("\n--- Setup ---")
    print("YARN:")
    print("  - Start shuffle service on NodeManagers")
    print("  - yarn-daemon.sh start shuffleservice")
    
    print("\nKubernetes:")
    print("  - Deploy shuffle service as sidecar")
    print("  - Configure in spark-submit")
    
    print("\n--- Monitoring ---")
    print("Check Spark UI:")
    print("  - Executors tab: shuffle service status")
    print("  - Check for shuffle service errors")


def main():
    """
    Main function to run all demonstrations
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run all sections
        section_1_understanding_shuffle_operations(spark)
        section_2_reduce_shuffle_operations(spark)
        section_3_shuffle_file_handling(spark)
        section_4_shuffle_consolidation(spark)
        section_5_shuffle_sort_vs_hash(spark)
        section_6_broadcast_hash_join(spark)
        section_7_shuffle_partition_count(spark)
        section_8_shuffle_read_write_optimization(spark)
        section_9_external_shuffle_service(spark)
        
        print("\n" + "=" * 80)
        print("ALL DEMONSTRATIONS COMPLETED")
        print("=" * 80)
        print("\nKey Takeaways:")
        print("1. Shuffle is expensive (disk I/O, network I/O, serialization)")
        print("2. Minimize shuffles: filter before shuffle, combine operations")
        print("3. Use reduceByKey over groupByKey (map-side combine)")
        print("4. Use broadcast joins for small tables")
        print("5. Tune spark.sql.shuffle.partitions based on data size")
        print("6. Enable compression (LZ4) for shuffle data")
        print("7. Enable external shuffle service for dynamic allocation")
        print("8. Monitor shuffle metrics in Spark UI")
        print("\nCheck Spark UI at http://localhost:4040 for shuffle metrics")
        
    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()
