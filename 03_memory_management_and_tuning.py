"""
================================================================================
PYSPARK MEMORY MANAGEMENT AND TUNING - CODE EXAMPLES
================================================================================

This file demonstrates memory management and tuning strategies with practical
examples for staff engineer preparation.

Topics Covered:
1. Spark Memory Model
2. Memory Configuration
3. Memory Tuning Guidelines
4. Garbage Collection Tuning
5. Off-Heap Memory
6. Tungsten Memory Management
7. Spill to Disk
8. Memory Profiling
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time


def create_spark_session():
    """
    Create and return a SparkSession with appropriate configuration.
    """
    spark = SparkSession.builder \
        .appName("MemoryManagementTuningDemo") \
        .master("local[4]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.5") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    print("=" * 80)
    print("SPARK SESSION CREATED")
    print("=" * 80)
    print(f"Spark Version: {spark.version}")
    print(f"Executor Memory: {spark.conf.get('spark.executor.memory')}")
    print(f"Memory Fraction: {spark.conf.get('spark.memory.fraction')}")
    print(f"Storage Fraction: {spark.conf.get('spark.memory.storageFraction')}")
    print("=" * 80)
    
    return spark


def section_1_spark_memory_model(spark):
    """
    Section 1: Spark Memory Model
    Demonstrates the Spark memory model components
    """
    print("\n" + "=" * 80)
    print("SECTION 1: SPARK MEMORY MODEL")
    print("=" * 80)
    
    print("\n--- Memory Components ---")
    print("On-heap memory: managed by JVM")
    print("  - Execution memory: shuffles, joins, sorts")
    print("  - Storage memory: caching/broadcasting")
    print("  - User memory: user data structures, UDFs")
    print("  - Reserved memory: system overhead (300MB)")
    
    print("\nOff-heap memory: managed by Spark (Tungsten)")
    print("  - Used for binary data storage")
    print("  - Reduces JVM GC pressure")
    print("  - Configured via spark.memory.offHeap.size")
    
    print("\n--- Memory Allocation Formula ---")
    print("Total Heap Memory = Reserved + (Execution + Storage) + User")
    print("Execution + Storage = Heap * spark.memory.fraction")
    print("Storage = (Execution + Storage) * spark.memory.storageFraction")
    print("Execution = (Execution + Storage) - Storage")
    
    print("\n--- Current Configuration ---")
    executor_memory = int(spark.conf.get("spark.executor.memory").replace("g", "")) * 1024 * 1024 * 1024
    memory_fraction = float(spark.conf.get("spark.memory.fraction"))
    storage_fraction = float(spark.conf.get("spark.memory.storageFraction"))
    
    reserved = 300 * 1024 * 1024  # 300MB
    execution_storage = int((executor_memory - reserved) * memory_fraction)
    storage = int(execution_storage * storage_fraction)
    execution = execution_storage - storage
    user = executor_memory - reserved - execution_storage
    
    print(f"Total Heap: {executor_memory / (1024**3):.2f} GB")
    print(f"Reserved: {reserved / (1024**2):.0f} MB")
    print(f"Execution + Storage: {execution_storage / (1024**2):.0f} MB")
    print(f"  - Execution: {execution / (1024**2):.0f} MB")
    print(f"  - Storage: {storage / (1024**2):.0f} MB")
    print(f"User Memory: {user / (1024**2):.0f} MB")


def section_2_memory_configuration(spark):
    """
    Section 2: Memory Configuration
    Demonstrates key memory configuration parameters
    """
    print("\n" + "=" * 80)
    print("SECTION 2: MEMORY CONFIGURATION")
    print("=" * 80)
    
    print("\n--- Key Memory Configurations ---")
    
    configs = [
        ("spark.executor.memory", "Total memory per executor"),
        ("spark.driver.memory", "Driver memory"),
        ("spark.memory.fraction", "Fraction for execution+storage (default 0.6)"),
        ("spark.memory.storageFraction", "Fraction of execution+storage for storage (default 0.5)"),
        ("spark.executor.memoryOverhead", "Off-heap memory for system overhead"),
        ("spark.memory.offHeap.enabled", "Enable off-heap memory"),
        ("spark.memory.offHeap.size", "Off-heap memory size"),
    ]
    
    for config, description in configs:
        value = spark.conf.get(config, "not set")
        print(f"{config}: {value}")
        print(f"  {description}")
    
    print("\n--- Setting Memory Configuration ---")
    print("Example configurations:")
    print("spark.executor.memory: 4g")
    print("spark.memory.fraction: 0.75  # Increase if OOM on shuffles")
    print("spark.memory.storageFraction: 0.3  # Decrease if caching causes OOM")
    
    print("\n--- Memory Overhead ---")
    print("spark.executor.memoryOverhead: off-heap for system overhead")
    print("  - YARN: container overhead")
    print("  - Kubernetes: pod overhead")
    print("  - Typically 10-20% of executor memory")


def section_3_memory_tuning_guidelines(spark):
    """
    Section 3: Memory Tuning Guidelines
    Demonstrates memory tuning best practices
    """
    print("\n" + "=" * 80)
    print("SECTION 3: MEMORY TUNING GUIDELINES")
    print("=" * 80)
    
    print("\n--- General Tuning Guidelines ---")
    print("1. Leave 20-30% for OS and overhead")
    print("2. spark.memory.fraction: increase if OOM on shuffles")
    print("3. spark.memory.storageFraction: decrease if caching causes OOM")
    print("4. Monitor GC logs to tune")
    print("5. Target: <10% of task time in GC")
    
    print("\n--- Scenario 1: OOM on Shuffles ---")
    print("Problem: OutOfMemoryError during shuffle operations")
    print("Solution:")
    print("  - Increase spark.memory.fraction (e.g., 0.6 -> 0.75)")
    print("  - Decrease spark.memory.storageFraction (e.g., 0.5 -> 0.3)")
    print("  - Increase spark.executor.memory")
    print("  - Reduce spark.sql.shuffle.partitions")
    
    print("\n--- Scenario 2: OOM on Caching ---")
    print("Problem: OutOfMemoryError when caching large datasets")
    print("Solution:")
    print("  - Increase spark.memory.storageFraction (e.g., 0.5 -> 0.7)")
    print("  - Use MEMORY_AND_DISK storage level")
    print("  - Cache less data or unpersist when done")
    
    print("\n--- Scenario 3: High GC Time ---")
    print("Problem: Tasks spending >10% time in GC")
    print("Solution:")
    print("  - Use G1GC for large heaps (>4GB)")
    print("  - Enable off-heap memory")
    print("  - Reduce object creation in UDFs")
    print("  - Use DataFrame API over RDD")
    
    print("\n--- Executor Sizing Guidelines ---")
    print("Cores per executor: 5-6 (leave 1 for OS/Hadoop)")
    print("Memory per executor: 30-60GB (avoid >64GB)")
    print("More executors with smaller memory is often better")
    print("Consider YARN container overhead")
    
    print("\n--- Example Calculation ---")
    print("Node with 128GB RAM, 16 cores:")
    print("  - Option 1: 2 executors x 8 cores x 60GB = 120GB")
    print("  - Option 2: 3 executors x 5 cores x 40GB = 120GB (better)")
    print("  - Option 2 provides better parallelism and smaller heaps")


def section_4_garbage_collection_tuning(spark):
    """
    Section 4: Garbage Collection Tuning
    Demonstrates GC tuning strategies
    """
    print("\n" + "=" * 80)
    print("SECTION 4: GARBAGE COLLECTION TUNING")
    print("=" * 80)
    
    print("\n--- GC Options ---")
    print("1. G1GC (Garbage First): Recommended for large heaps (>4GB)")
    print("2. Parallel GC: Good for smaller heaps")
    print("3. CMS (Concurrent Mark Sweep): Deprecated in Java 9+")
    
    print("\n--- G1GC Configuration ---")
    print("Enable G1GC:")
    print("  --conf spark.executor.extraJavaOptions='-XX:+UseG1GC'")
    print("  --conf spark.driver.extraJavaOptions='-XX:+UseG1GC'")
    
    print("\nG1GC Tuning Parameters:")
    print("  -XX:MaxGCPauseMillis=200  # Target max GC pause")
    print("  -XX:InitiatingHeapOccupancyPercent=35  # Start GC at 35% heap")
    print("  -XX:+PrintGCDetails  # Print GC details")
    print("  -XX:+PrintGCDateStamps  # Print GC timestamps")
    
    print("\n--- Monitoring GC ---")
    print("Check Spark UI Executors tab:")
    print("  - GC Time")
    print("  - Target: <10% of task time")
    
    print("\n--- GC Performance Indicators ---")
    print("Good GC:")
    print("  - GC time < 10% of task time")
    print("  - Few full GC cycles")
    print("  - Consistent pause times")
    
    print("\nPoor GC:")
    print("  - GC time > 20% of task time")
    print("  - Frequent full GC cycles")
    print("  - Long pause times")
    
    print("\n--- GC Tuning Example ---")
    print("For 60GB heap with high GC time:")
    print("spark.executor.extraJavaOptions='")
    print("  -XX:+UseG1GC")
    print("  -XX:MaxGCPauseMillis=200")
    print("  -XX:InitiatingHeapOccupancyPercent=40")
    print("  -XX:+PrintGCDetails")
    print("  -XX:+PrintGCDateStamps")
    print("  -Xlog:gc*:file=/tmp/gc.log'")
    
    print("\n--- Alternative: Reduce Heap Size ---")
    print("Instead of tuning GC, consider:")
    print("  - More executors with smaller heaps")
    print("  - Enable off-heap memory")
    print("  - Reduce cached data")


def section_5_off_heap_memory(spark):
    """
    Section 5: Off-Heap Memory
    Demonstrates off-heap memory configuration and benefits
    """
    print("\n" + "=" * 80)
    print("SECTION 5: OFF-HEAP MEMORY")
    print("=" * 80)
    
    print("\n--- Understanding Off-Heap Memory ---")
    print("Off-heap memory: memory outside JVM heap")
    print("  - Managed by Spark, not JVM")
    print("  - Reduces GC pressure")
    print("  - Used by Tungsten for binary format")
    
    print("\n--- Benefits of Off-Heap Memory ---")
    print("1. Reduced GC overhead")
    print("2. Larger memory capacity")
    print("3. Better for binary data")
    print("4. Shared between executors (with shuffle service)")
    
    print("\n--- Configuration ---")
    print("Enable off-heap:")
    print("spark.memory.offHeap.enabled: true")
    print("spark.memory.offHeap.size: 2g  # Size of off-heap memory")
    
    print("\n--- Current Off-Heap Status ---")
    off_heap_enabled = spark.conf.get("spark.memory.offHeap.enabled", "false")
    off_heap_size = spark.conf.get("spark.memory.offHeap.size", "not set")
    print(f"Off-heap enabled: {off_heap_enabled}")
    print(f"Off-heap size: {off_heap_size}")
    
    print("\n--- When to Use Off-Heap ---")
    print("Use off-heap when:")
    print("  - High GC time (>10%)")
    print("  - Large heaps (>4GB)")
    print("  - Memory-intensive operations")
    print("  - Using Tungsten (default in Spark 2.0+)")
    
    print("\n--- Off-Heap Limitations ---")
    print("  - Not all data can be stored off-heap")
    print("  - UDFs still use on-heap")
    print("  - Requires careful sizing")
    print("  - May not work with all cluster managers")


def section_6_tungsten_memory_management(spark):
    """
    Section 6: Tungsten Memory Management
    Demonstrates Tungsten project optimizations
    """
    print("\n" + "=" * 80)
    print("SECTION 6: TUNGSTEN MEMORY MANAGEMENT")
    print("=" * 80)
    
    print("\n--- Tungsten Project ---")
    print("Tungsten: Spark's memory management and CPU optimization project")
    print("  - Binary in-memory format")
    print("  - Cache-aware computation")
    print("  - Code generation")
    print("  - Off-heap memory management")
    
    print("\n--- Binary In-Memory Format ---")
    print("Traditional Java objects:")
    print("  - High overhead (object header, pointers)")
    print("  - Poor cache locality")
    print("  - High GC pressure")
    
    print("\nTungsten binary format:")
    print("  - Compact representation")
    print("  - Cache-friendly")
    print("  - No Java object overhead")
    print("  - Enabled by default in Spark 2.0+")
    
    print("\n--- Cache-Aware Computation ---")
    print("Tungsten optimizes for CPU cache:")
    print("  - Processes data in cache-friendly batches")
    print("  - Reduces cache misses")
    print("  - Improves CPU utilization")
    
    print("\n--- Code Generation ---")
    print("Whole-stage code generation:")
    print("  - Generates optimized bytecode")
    print("  - Reduces virtual function calls")
    print("  - Better performance")
    
    print("\n--- Tungsten Configuration ---")
    print("spark.sql.codegen.wholeStage: true (default)")
    print("spark.sql.codegen.factoryMode: CODEGEN_ONLY (default)")
    print("spark.sql.inMemoryColumnarStorage.compressed: true")
    
    print("\n--- Current Tungsten Status ---")
    codegen_enabled = spark.conf.get("spark.sql.codegen.wholeStage", "true")
    print(f"Whole-stage codegen: {codegen_enabled}")
    
    print("\n--- Tungsten Benefits ---")
    print("Performance improvements:")
    print("  - 2-10x faster for aggregations")
    print("  - Reduced memory usage")
    print("  - Lower GC pressure")
    print("  - Better CPU utilization")


def section_7_spill_to_disk(spark):
    """
    Section 7: Spill to Disk
    Demonstrates spill behavior and configuration
    """
    print("\n" + "=" * 80)
    print("SECTION 7: SPILL TO DISK")
    print("=" * 80)
    
    print("\n--- Understanding Spill ---")
    print("Spill: When memory insufficient, data spills to disk")
    print("  - Happens during shuffles, joins, sorts")
    print("  - Slows down operations (disk I/O)")
    print("  - Indicates memory pressure")
    
    print("\n--- Spill Configuration ---")
    configs = [
        ("spark.shuffle.spill", "Enable/disable spilling (default true)"),
        ("spark.shuffle.spill.numElementsForceSpillThreshold", "Elements before forced spill"),
        ("spark.shuffle.sort.bypassMergeThreshold", "Use hash shuffle for small reduces"),
    ]
    
    for config, description in configs:
        value = spark.conf.get(config, "default")
        print(f"{config}: {value}")
        print(f"  {description}")
    
    print("\n--- Monitoring Spill ---")
    print("Check Spark UI:")
    print("  - Stages tab: Shuffle Spill (Memory) and Shuffle Spill (Disk)")
    print("  - Executors tab: Disk Spilled and Memory Spilled metrics")
    
    print("\n--- Creating Spill Scenario ---")
    print("Creating a scenario that may cause spill:")
    
    # Create large dataset that may cause spill
    large_data = [(i, f"user_{i}", i * 1000) for i in range(1, 100001)]
    df = spark.createDataFrame(large_data, ["id", "name", "value"])
    
    print(f"Created DataFrame with {df.count()} rows")
    
    # Perform aggregation that may cause spill
    print("\nPerforming aggregation (may cause spill on small memory):")
    start = time.time()
    result = df.groupBy("id").agg({"value": "sum"})
    result_count = result.count()
    elapsed = time.time() - start
    
    print(f"Aggregation result: {result_count} rows")
    print(f"Time: {elapsed:.4f} seconds")
    print("Check Spark UI for spill metrics")
    
    print("\n--- Reducing Spill ---")
    print("To reduce spill:")
    print("  - Increase executor memory")
    print("  - Increase spark.memory.fraction")
    print("  - Reduce spark.sql.shuffle.partitions")
    print("  - Use broadcast joins for small tables")
    print("  - Filter data before shuffle")
    
    print("\n--- Spill vs OOM ---")
    print("Spill is better than OOM:")
    print("  - Job completes (slower)")
    print("  - No failure")
    print("  - But indicates need for tuning")
    
    print("\n--- Optimal: No Spill, No OOM ---")
    print("Goal: Configure memory to avoid both spill and OOM")
    print("  - Right-sized executors")
    print("  - Proper partitioning")
    print("  - Efficient operations")


def section_8_memory_profiling(spark):
    """
    Section 8: Memory Profiling
    Demonstrates memory profiling techniques
    """
    print("\n" + "=" * 80)
    print("SECTION 8: MEMORY PROFILING")
    print("=" * 80)
    
    print("\n--- Spark UI Memory Metrics ---")
    print("Storage tab:")
    print("  - Fraction cached")
    print("  - Memory used")
    print("  - Disk used")
    
    print("\nExecutors tab:")
    print("  - Memory used")
    print("  - Disk Spilled")
    print("  - Memory Spilled")
    print("  - GC Time")
    
    print("\n--- Profiling with Spark UI ---")
    print("Steps:")
    print("1. Run your job")
    print("2. Open Spark UI (http://localhost:4040)")
    print("3. Check Executors tab for memory metrics")
    print("4. Check Stages tab for spill metrics")
    print("5. Identify bottlenecks")
    
    print("\n--- GC Log Analysis ---")
    print("Enable GC logging:")
    print("spark.executor.extraJavaOptions='-XX:+PrintGCDetails -XX:+PrintGCDateStamps'")
    
    print("\nAnalyze GC logs:")
    print("  - GC frequency")
    print("  - GC duration")
    print("  - Heap usage before/after GC")
    print("  - Full GC vs young GC")
    
    print("\n--- Java Flight Recorder ---")
    print("For deep profiling:")
    print("  - Enable JFR in JVM")
    print("  - Record memory allocations")
    print("  - Analyze hot methods")
    print("  - Identify memory leaks")
    
    print("\n--- Memory Profiling Tools ---")
    print("1. Spark UI: Built-in, easy to use")
    print("2. GC logs: Detailed GC information")
    print("3. JFR: Deep JVM profiling")
    print("4. YourKit/JProfiler: Commercial profilers")
    
    print("\n--- Common Memory Issues ---")
    print("Issue 1: High GC time")
    print("  - Symptom: GC time > 10% of task time")
    print("  - Cause: Large heaps, many objects")
    print("  - Solution: G1GC, off-heap, reduce heap size")
    
    print("\nIssue 2: Frequent Spill")
    print("  - Symptom: High disk spill metrics")
    print("  - Cause: Insufficient memory for operations")
    print("  - Solution: Increase memory, reduce partitions")
    
    print("\nIssue 3: OOM Errors")
    print("  - Symptom: OutOfMemoryError in logs")
    print("  - Cause: Memory exhausted")
    print("  - Solution: Increase memory, reduce data size")
    
    print("\n--- Profiling Checklist ---")
    print("[ ] Check Spark UI for memory usage")
    print("[ ] Check GC time in Spark UI")
    print("[ ] Check spill metrics in Spark UI")
    print("[ ] Enable and analyze GC logs")
    print("[ ] Consider JFR for deep profiling")
    print("[ ] Tune configuration based on findings")


def main():
    """
    Main function to run all demonstrations
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run all sections
        section_1_spark_memory_model(spark)
        section_2_memory_configuration(spark)
        section_3_memory_tuning_guidelines(spark)
        section_4_garbage_collection_tuning(spark)
        section_5_off_heap_memory(spark)
        section_6_tungsten_memory_management(spark)
        section_7_spill_to_disk(spark)
        section_8_memory_profiling(spark)
        
        print("\n" + "=" * 80)
        print("ALL DEMONSTRATIONS COMPLETED")
        print("=" * 80)
        print("\nKey Takeaways:")
        print("1. Understand Spark memory model (execution vs storage)")
        print("2. Leave 20-30% for OS and overhead")
        print("3. Use G1GC for large heaps (>4GB)")
        print("4. Enable off-heap memory to reduce GC pressure")
        print("5. Monitor GC time (target <10%)")
        print("6. Monitor spill metrics in Spark UI")
        print("7. Tune spark.memory.fraction based on workload")
        print("8. Profile memory usage regularly")
        print("\nCheck Spark UI at http://localhost:4040 for memory metrics")
        
    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()
