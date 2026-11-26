"""
================================================================================
PYSPARK CACHING AND PERSISTENCE STRATEGIES - CODE EXAMPLES
================================================================================

This file demonstrates caching and persistence strategies with practical
examples for staff engineer preparation.

Topics Covered:
1. Understanding Caching
2. Storage Levels
3. When to Cache
4. Cache vs Persist
5. Unpersisting Data
6. Monitoring Cache Usage
7. Cache Best Practices
8. Cache in SQL
9. Cache in Iterative Algorithms
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when, sum as _sum, count as _count, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time


def create_spark_session():
    """
    Create and return a SparkSession with appropriate configuration.
    """
    spark = SparkSession.builder \
        .appName("CachingPersistenceStrategiesDemo") \
        .master("local[4]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    print("=" * 80)
    print("SPARK SESSION CREATED")
    print("=" * 80)
    print(f"Spark Version: {spark.version}")
    print("=" * 80)
    
    return spark


def section_1_understanding_caching(spark):
    """
    Section 1: Understanding Caching
    Demonstrates what caching is and why it's important
    """
    print("\n" + "=" * 80)
    print("SECTION 1: UNDERSTANDING CACHING")
    print("=" * 80)
    
    print("\n--- What is Caching? ---")
    print("Caching: Store DataFrame in memory for reuse")
    print("  - Avoids recomputation")
    print("  - Faster access")
    print("  - Useful for iterative algorithms")
    print("  - Useful for repeated queries")
    
    print("\n--- How Caching Works ---")
    print("1. DataFrame is computed")
    print("2. Result stored in memory/disk")
    print("3. Subsequent actions use cached data")
    print("4. No recomputation needed")
    
    print("\n--- Benefits ---")
    print("1. Performance: Faster access to data")
    print("2. Efficiency: Avoid recomputation")
    print("3. Iterative algorithms: Machine learning, graph processing")
    print("4. Repeated queries: Dashboard, analytics")
    
    print("\n--- Costs ---")
    print("1. Memory: Cached data uses memory")
    print("2. Storage: May spill to disk if memory insufficient")
    print("3. Staleness: Need to refresh if source changes")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Without cache
    print("\n--- Without Cache ---")
    start = time.time()
    result1 = df.filter(col("group_id") > 50).count()
    time1 = time.time() - start
    print(f"First query: {time1:.4f} seconds")
    
    start = time.time()
    result2 = df.filter(col("group_id") > 50).count()
    time2 = time.time() - start
    print(f"Second query: {time2:.4f} seconds (recomputed)")
    
    # With cache
    print("\n--- With Cache ---")
    df_filtered = df.filter(col("group_id") > 50).cache()
    
    start = time.time()
    result3 = df_filtered.count()
    time3 = time.time() - start
    print(f"First query (with cache): {time3:.4f} seconds (computes and caches)")
    
    start = time.time()
    result4 = df_filtered.count()
    time4 = time.time() - start
    print(f"Second query (with cache): {time4:.4f} seconds (uses cache)")
    print(f"Speedup: {time2/time4:.1f}x")
    
    df_filtered.unpersist()


def section_2_storage_levels(spark):
    """
    Section 2: Storage Levels
    Demonstrates different storage levels
    """
    print("\n" + "=" * 80)
    print("SECTION 2: STORAGE LEVELS")
    print("=" * 80)
    
    print("\n--- Storage Levels ---")
    from pyspark.storagelevel import StorageLevel
    
    storage_levels = [
        ("MEMORY_ONLY", "Store in memory only, deserialized"),
        ("MEMORY_ONLY_SER", "Store in memory only, serialized"),
        ("MEMORY_AND_DISK", "Store in memory, spill to disk if needed"),
        ("MEMORY_AND_DISK_SER", "Store in memory (serialized), spill to disk"),
        ("DISK_ONLY", "Store on disk only"),
        ("OFF_HEAP", "Store off-heap (serialized)"),
    ]
    
    print("Available storage levels:")
    for level, description in storage_levels:
        print(f"  {level}: {description}")
    
    print("\n--- Storage Level Trade-offs ---")
    print("MEMORY_ONLY:")
    print("  - Fastest access")
    print("  - Uses most memory")
    print("  - Loses data if memory insufficient")
    
    print("\nMEMORY_ONLY_SER:")
    print("  - Fast access")
    print("  - Uses less memory (serialized)")
    print("  - CPU overhead for deserialization")
    
    print("\nMEMORY_AND_DISK:")
    print("  - Fast access (memory)")
    print("  - Fallback to disk")
    print("  - No data loss")
    print("  - Slower if spilled to disk")
    
    print("\nDISK_ONLY:")
    print("  - Slowest access")
    print("  - Uses no memory")
    print("  - Good for very large datasets")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print("\n--- MEMORY_ONLY ---")
    df_memory_only = df.cache()  # Default: MEMORY_ONLY
    df_memory_only.count()
    print("Cached with MEMORY_ONLY (default)")
    
    print("\n--- MEMORY_AND_DISK ---")
    from pyspark.storagelevel import StorageLevel
    df_memory_disk = df.persist(StorageLevel.MEMORY_AND_DISK)
    df_memory_disk.count()
    print("Cached with MEMORY_AND_DISK")
    
    print("\n--- DISK_ONLY ---")
    df_disk = df.persist(StorageLevel.DISK_ONLY)
    df_disk.count()
    print("Cached with DISK_ONLY")
    
    # Cleanup
    df_memory_only.unpersist()
    df_memory_disk.unpersist()
    df_disk.unpersist()
    
    print("\n--- Choosing Storage Level ---")
    print("MEMORY_ONLY:")
    print("  - Data fits in memory")
    print("  - Fast access needed")
    
    print("\nMEMORY_AND_DISK:")
    print("  - Data may not fit in memory")
    print("  - Want to avoid recomputation")
    
    print("\nDISK_ONLY:")
    print("  - Data too large for memory")
    print("  - Still want to avoid recomputation")


def section_3_when_to_cache(spark):
    """
    Section 3: When to Cache
    Demonstrates scenarios where caching is beneficial
    """
    print("\n" + "=" * 80)
    print("SECTION 3: WHEN TO CACHE")
    print("=" * 80)
    
    print("\n--- Good Caching Scenarios ---")
    scenarios = [
        ("Iterative algorithms", "Machine learning, graph processing"),
        ("Repeated queries", "Dashboard, analytics"),
        ("Expensive transformations", "Complex joins, aggregations"),
        ("Small lookup tables", "Broadcast joins"),
    ]
    
    print("When to cache:")
    for scenario, description in scenarios:
        print(f"  {scenario}: {description}")
    
    print("\n--- Bad Caching Scenarios ---")
    print("Don't cache when:")
    print("  - Data is used only once")
    print("  - Data is very large and memory constrained")
    print("  - Data changes frequently")
    print("  - Cache overhead > recomputation cost")
    
    print("\n--- Example: Repeated Queries ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Expensive transformation
    df_expensive = df.filter(col("group_id") > 50) \
                     .groupBy("group_id") \
                     .agg({"value": "sum", "id": "count"})
    
    print("\n--- Without Cache ---")
    start = time.time()
    result1 = df_expensive.filter(col("group_id") > 70).count()
    time1 = time.time() - start
    print(f"Query 1: {time1:.4f} seconds")
    
    start = time.time()
    result2 = df_expensive.filter(col("group_id") < 80).count()
    time2 = time.time() - start
    print(f"Query 2: {time2:.4f} seconds (recomputed)")
    
    print("\n--- With Cache ---")
    df_expensive_cached = df.filter(col("group_id") > 50) \
                             .groupBy("group_id") \
                             .agg({"value": "sum", "id": "count"}) \
                             .cache()
    
    start = time.time()
    result3 = df_expensive_cached.filter(col("group_id") > 70).count()
    time3 = time.time() - start
    print(f"Query 1 (cached): {time3:.4f} seconds (computes and caches)")
    
    start = time.time()
    result4 = df_expensive_cached.filter(col("group_id") < 80).count()
    time4 = time.time() - start
    print(f"Query 2 (cached): {time4:.4f} seconds (uses cache)")
    print(f"Speedup: {time2/time4:.1f}x")
    
    df_expensive_cached.unpersist()
    
    print("\n--- Decision Framework ---")
    print("Cache if:")
    print("  - Data reused multiple times")
    print("  - Computation is expensive")
    print("  - Memory is available")
    print("  - Data fits in memory")
    
    print("\nDon't cache if:")
    print("  - Data used once")
    print("  - Computation is cheap")
    print("  - Memory is constrained")
    print("  - Data too large")


def section_4_cache_vs_persist(spark):
    """
    Section 4: Cache vs Persist
    Demonstrates difference between cache() and persist()
    """
    print("\n" + "=" * 80)
    print("SECTION 4: CACHE VS PERSIST")
    print("=" * 80)
    
    print("\n--- cache() ---")
    print("df.cache()")
    print("  - Shorthand for persist(MEMORY_ONLY)")
    print("  - Default storage level")
    print("  - Simple to use")
    
    print("\n--- persist() ---")
    print("df.persist(storage_level)")
    print("  - Specify storage level")
    print("  - More control")
    print("  - Flexible")
    
    print("\n--- Example ---")
    from pyspark.storagelevel import StorageLevel
    
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print("\n--- cache() ---")
    df_cached = df.cache()
    df_cached.count()
    print("Cached with default (MEMORY_ONLY)")
    
    print("\n--- persist() ---")
    df_persisted = df.persist(StorageLevel.MEMORY_AND_DISK)
    df_persisted.count()
    print("Persisted with MEMORY_AND_DISK")
    
    # Cleanup
    df_cached.unpersist()
    df_persisted.unpersist()
    
    print("\n--- When to Use Each ---")
    print("Use cache() when:")
    print("  - Default storage level is fine")
    print("  - Data fits in memory")
    print("  - Simple use case")
    
    print("\nUse persist() when:")
    print("  - Need specific storage level")
    print("  - Data may not fit in memory")
    print("  - Need more control")
    
    print("\n--- Recommendation ---")
    print("Start with cache()")
    print("  - Simple, works for most cases")
    print("  - Switch to persist() if needed")
    print("  - Tune storage level based on data size")


def section_5_unpersisting_data(spark):
    """
    Section 5: Unpersisting Data
    Demonstrates how to unpersist cached data
    """
    print("\n" + "=" * 80)
    print("SECTION 5: UNPERSISTING DATA")
    print("=" * 80)
    
    print("\n--- Why Unpersist? ---")
    print("Unpersist: Remove cached data from memory")
    print("  - Free up memory")
    print("  - Avoid memory pressure")
    print("  - Clean up after use")
    
    print("\n--- Unpersist Methods ---")
    print("df.unpersist()")
    print("  - Remove from memory immediately")
    print("  - Blocking operation")
    
    print("\ndf.unpersist(blocking=False)")
    print("  - Remove from memory asynchronously")
    print("  - Non-blocking")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print("\n--- Cache and Unpersist ---")
    df_cached = df.cache()
    df_cached.count()
    print("DataFrame cached")
    
    df_cached.unpersist()
    print("DataFrame unpersisted")
    
    print("\n--- Automatic Unpersist ---")
    print("Spark automatically unpurges:")
    print("  - When SparkContext stops")
    print("  - When memory is needed (LRU eviction)")
    print("  - But manual unpersist is recommended")
    
    print("\n--- Best Practices ---")
    print("1. Unpersist when done with data")
    print("2. Use unpersist(blocking=False) for non-blocking")
    print("3. Unpersist in finally blocks for cleanup")
    print("4. Monitor memory usage")
    
    print("\n--- Example with Cleanup ---")
    print("try:")
    print("    df_cached = df.cache()")
    print("    df_cached.count()")
    print("    # Use df_cached")
    print("finally:")
    print("    df_cached.unpersist()")


def section_6_monitoring_cache_usage(spark):
    """
    Section 6: Monitoring Cache Usage
    Demonstrates how to monitor cached data
    """
    print("\n" + "=" * 80)
    print("SECTION 6: MONITORING CACHE USAGE")
    print("=" * 80)
    
    print("\n--- Monitoring Methods ---")
    print("1. Spark UI Storage tab")
    print("2. df.storageLevel")
    print("3. df.is_cached")
    print("4. SparkContext.getRDDStorageInfo()")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print("\n--- Before Caching ---")
    print(f"Is cached: {df.is_cached}")
    print(f"Storage level: {df.storageLevel}")
    
    print("\n--- After Caching ---")
    df_cached = df.cache()
    df_cached.count()
    print(f"Is cached: {df_cached.is_cached}")
    print(f"Storage level: {df_cached.storageLevel}")
    
    print("\n--- Spark UI ---")
    print("Check Spark UI → Storage tab:")
    print("  - Cached DataFrames")
    print("  - Fraction cached")
    print("  - Size in memory")
    print("  - Size on disk")
    
    print("\n--- Monitoring Metrics ---")
    print("Key metrics:")
    print("  - Memory used")
    print("  - Disk used")
    print("  - Fraction cached")
    print("  - Number of cached partitions")
    
    print("\n--- Common Issues ---")
    print("Issue 1: Cache not used")
    print("  - Symptom: Data recomputed")
    print("  - Cause: Not triggered with action")
    print("  - Solution: Call action after cache()")
    
    print("\nIssue 2: Out of memory")
    print("  - Symptom: OOM errors")
    print("  - Cause: Too much cached data")
    print("  - Solution: Unpersist, use MEMORY_AND_DISK")
    
    print("\nIssue 3: Cache eviction")
    print("  - Symptom: Cache evicted")
    print("  - Cause: Memory pressure")
    print("  - Solution: Increase memory, unpersist unused data")
    
    # Cleanup
    df_cached.unpersist()
    
    print("\n--- Monitoring Checklist ---")
    print("[ ] Check Spark UI Storage tab")
    print("[ ] Verify cache is used")
    print("[ ] Monitor memory usage")
    print("[ ] Unpersist when done")


def section_7_cache_best_practices(spark):
    """
    Section 7: Cache Best Practices
    Demonstrates caching best practices
    """
    print("\n" + "=" * 80)
    print("SECTION 7: CACHE BEST PRACTICES")
    print("=" * 80)
    
    print("\n--- Best Practice 1: Cache After Transformation ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print("Good: Cache after transformation")
    df_cached = df.filter(col("group_id") > 50).cache()
    df_cached.count()
    print("Cached filtered data")
    
    print("\nBad: Cache before transformation")
    print("# df.cache()")
    print("# df.filter(col('group_id') > 50).count()")
    print("# Caches all data, then filters")
    
    # Cleanup
    df_cached.unpersist()
    
    print("\n--- Best Practice 2: Cache Small Lookup Tables ---")
    print("Cache small tables for broadcast joins:")
    print("small_df.cache()")
    print("large_df.join(broadcast(small_df), 'key')")
    
    print("\n--- Best Practice 3: Unpersist When Done ---")
    print("Always unpersist when done:")
    print("try:")
    print("    df_cached = df.cache()")
    print("    # Use df_cached")
    print("finally:")
    print("    df_cached.unpersist()")
    
    print("\n--- Best Practice 4: Use Appropriate Storage Level ---")
    from pyspark.storagelevel import StorageLevel
    
    print("Choose storage level based on data size:")
    print("  - Small data: MEMORY_ONLY")
    print("  - Medium data: MEMORY_AND_DISK")
    print("  - Large data: DISK_ONLY")
    
    print("\n--- Best Practice 5: Monitor Cache Usage ---")
    print("Regularly check Spark UI:")
    print("  - Storage tab")
    print("  - Memory usage")
    print("  - Cache hit rate")
    
    print("\n--- Best Practice 6: Cache in Right Place ---")
    print("Cache at the right point in pipeline:")
    print("  - After expensive transformations")
    print("  - Before repeated operations")
    print("  - Not too early (caches too much)")
    print("  - Not too late (misses opportunity)")
    
    print("\n--- Anti-Patterns ---")
    print("Anti-pattern 1: Cache everything")
    print("  - Wastes memory")
    print("  - May cause OOM")
    
    print("\nAnti-pattern 2: Cache once, never unpersist")
    print("  - Memory leak")
    print("  - Memory pressure")
    
    print("\nAnti-pattern 3: Cache too early")
    print("  - Caches unnecessary data")
    print("  - Wastes memory")


def section_8_cache_in_sql(spark):
    """
    Section 8: Cache in SQL
    Demonstrates caching in SQL queries
    """
    print("\n" + "=" * 80)
    print("SECTION 8: CACHE IN SQL")
    print("=" * 80)
    
    print("\n--- Caching Tables ---")
    print("CACHE TABLE table_name")
    print("  - Cache table in memory")
    print("  - Available for SQL queries")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    df.createOrReplaceTempView("users")
    
    print("\n--- Cache Table ---")
    spark.sql("CACHE TABLE users")
    print("Table 'users' cached")
    
    print("\n--- Query Cached Table ---")
    result = spark.sql("SELECT COUNT(*) FROM users WHERE group_id > 50")
    print(f"Result: {result.collect()[0][0]}")
    
    print("\n--- Uncache Table ---")
    spark.sql("UNCACHE TABLE users")
    print("Table 'users' uncached")
    
    print("\n--- Cache Lazy Table ---")
    print("CACHE LAZY TABLE table_name")
    print("  - Cache on first use")
    print("  - Lazy evaluation")
    
    print("\n--- Cache with Options ---")
    print("CACHE TABLE table_name OPTIONS ('storageLevel' 'MEMORY_AND_DISK')")
    
    print("\n--- Check Cached Tables ---")
    spark.sql("CACHE TABLE users")
    cached_tables = spark.sql("SHOW TABLES").filter(col("isCached") == "true")
    print("Cached tables:")
    cached_tables.show()
    
    spark.sql("UNCACHE TABLE users")
    
    print("\n--- Best Practices ---")
    print("1. Cache frequently accessed tables")
    print("2. Uncache when done")
    print("3. Use LAZY for infrequent access")
    print("4. Monitor cache usage")


def section_9_cache_in_iterative_algorithms(spark):
    """
    Section 9: Cache in Iterative Algorithms
    Demonstrates caching for iterative algorithms
    """
    print("\n" + "=" * 80)
    print("SECTION 9: CACHE IN ITERATIVE ALGORITHMS")
    print("=" * 80)
    
    print("\n--- Why Cache in Iterative Algorithms? ---")
    print("Iterative algorithms:")
    print("  - Machine learning (training)")
    print("  - Graph processing")
    print("  - PageRank, connected components")
    print("  - Multiple iterations over same data")
    
    print("\n--- Benefits ---")
    print("1. Avoid recomputation each iteration")
    print("2. Significant performance improvement")
    print("  - 10-100x speedup common")
    
    print("\n--- Example: Simple Iterative Algorithm ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print("\n--- Without Cache ---")
    start = time.time()
    for i in range(5):
        result = df.filter(col("group_id") > 50).count()
    time_without_cache = time.time() - start
    print(f"5 iterations without cache: {time_without_cache:.4f} seconds")
    
    print("\n--- With Cache ---")
    df_cached = df.filter(col("group_id") > 50).cache()
    df_cached.count()  # Trigger cache
    
    start = time.time()
    for i in range(5):
        result = df_cached.count()
    time_with_cache = time.time() - start
    print(f"5 iterations with cache: {time_with_cache:.4f} seconds")
    print(f"Speedup: {time_without_cache/time_with_cache:.1f}x")
    
    df_cached.unpersist()
    
    print("\n--- Machine Learning Example ---")
    print("MLlib automatically caches training data:")
    print("from pyspark.ml.classification import LogisticRegression")
    print("lr = LogisticRegression()")
    print("model = lr.fit(training_data)")
    print("# training_data is automatically cached")
    
    print("\n--- Graph Processing Example ---")
    print("GraphX automatically caches graph data:")
    print("# In GraphX (Scala/Java)")
    print("# graph.cache()")
    print("# PageRank on cached graph")
    
    print("\n--- Best Practices ---")
    print("1. Cache data before iterative algorithm")
    print("2. Unpersist after algorithm completes")
    print("3. Use appropriate storage level")
    print("4. Monitor memory usage")
    
    print("\n--- Example Pattern ---")
    print("# Cache data")
    print("data_cached = data.cache()")
    print("data_cached.count()  # Trigger cache")
    print("")
    print("# Run iterative algorithm")
    print("for i in range(iterations):")
    print("    result = process(data_cached)")
    print("")
    print("# Cleanup")
    print("data_cached.unpersist()")


def main():
    """
    Main function to run all demonstrations
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run all sections
        section_1_understanding_caching(spark)
        section_2_storage_levels(spark)
        section_3_when_to_cache(spark)
        section_4_cache_vs_persist(spark)
        section_5_unpersisting_data(spark)
        section_6_monitoring_cache_usage(spark)
        section_7_cache_best_practices(spark)
        section_8_cache_in_sql(spark)
        section_9_cache_in_iterative_algorithms(spark)
        
        print("\n" + "=" * 80)
        print("ALL DEMONSTRATIONS COMPLETED")
        print("=" * 80)
        print("\nKey Takeaways:")
        print("1. Cache data that is reused multiple times")
        print("2. Choose appropriate storage level (MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY)")
        print("3. Cache after expensive transformations")
        print("4. Use cache() for simple cases, persist() for control")
        print("5. Always unpersist when done with cached data")
        print("6. Monitor cache usage in Spark UI Storage tab")
        print("7. Cache small lookup tables for broadcast joins")
        print("8. Use CACHE TABLE in SQL for frequently accessed tables")
        print("9. Cache data before iterative algorithms for 10-100x speedup")
        print("\nCheck Spark UI at http://localhost:4040 for cache metrics")
        
    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()
