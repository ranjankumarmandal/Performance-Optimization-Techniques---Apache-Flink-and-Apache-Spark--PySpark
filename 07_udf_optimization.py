"""
================================================================================
PYSPARK UDF OPTIMIZATION - CODE EXAMPLES
================================================================================

This file demonstrates UDF optimization strategies with practical
examples for staff engineer preparation.

Topics Covered:
1. Understand UDF Performance Impact
2. Use Pandas UDFs (Vectorized UDFs)
3. Pandas UDF Types
4. Pandas UDF Best Practices
5. Regular UDF Optimization
6. UDF Serialization
7. UDF Registration
8. Avoid Python UDFs in Joins
9. Monitor UDF Performance
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when, pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
import pandas as pd
import time


def create_spark_session():
    """
    Create and return a SparkSession with appropriate configuration.
    """
    spark = SparkSession.builder \
        .appName("UDFOptimizationDemo") \
        .master("local[4]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    print("=" * 80)
    print("SPARK SESSION CREATED")
    print("=" * 80)
    print(f"Spark Version: {spark.version}")
    print(f"Arrow Enabled: {spark.conf.get('spark.sql.execution.arrow.pyspark.enabled')}")
    print("=" * 80)
    
    return spark


def section_1_understand_udf_performance_impact(spark):
    """
    Section 1: Understand UDF Performance Impact
    Demonstrates why UDFs are slow
    """
    print("\n" + "=" * 80)
    print("SECTION 1: UNDERSTAND UDF PERFORMANCE IMPACT")
    print("=" * 80)
    
    print("\n--- Why UDFs Are Slow ---")
    print("UDF performance issues:")
    print("  - Row-by-row processing")
    print("  - Serialization/deserialization overhead")
    print("  - No Catalyst optimization")
    print("  - Python-JVM boundary crossing")
    print("  - 10-100x slower than built-in functions")
    
    print("\n--- UDF Execution Flow ---")
    print("1. JVM: Row data serialized")
    print("2. Transfer to Python")
    print("3. Python: Deserialize, process, serialize")
    print("4. Transfer back to JVM")
    print("5. JVM: Deserialize result")
    print("This happens for EVERY row!")
    
    print("\n--- Comparison ---")
    print("Built-in function:")
    print("  - Optimized by Catalyst")
    print("  - Vectorized operations")
    print("  - No serialization overhead")
    print("  - Fast")
    
    print("\nRegular UDF:")
    print("  - Row-by-row processing")
    print("  - Serialization overhead")
    print("  - No Catalyst optimization")
    print("  - Slow")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Built-in function
    print("\n--- Built-in Function (Fast) ---")
    start = time.time()
    result_builtin = df.withColumn("value_doubled", col("value") * 2)
    count_builtin = result_builtin.count()
    time_builtin = time.time() - start
    print(f"Built-in: {time_builtin:.4f} seconds")
    
    print("\n--- Regular UDF (Slow) ---")
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    
    @udf(IntegerType())
    def double_value_udf(x):
        return x * 2
    
    start = time.time()
    result_udf = df.withColumn("value_doubled", double_value_udf(col("value")))
    count_udf = result_udf.count()
    time_udf = time.time() - start
    print(f"Regular UDF: {time_udf:.4f} seconds")
    print(f"UDF is {time_udf/time_builtin:.1f}x slower")
    
    print("\n--- Recommendation ---")
    print("Always prefer built-in functions")
    print("  - Check pyspark.sql.functions first")
    print("  - Use SQL expressions")
    print("  - Only use UDF when necessary")


def section_2_use_pandas_udfs(spark):
    """
    Section 2: Use Pandas UDFs (Vectorized UDFs)
    Demonstrates pandas UDF performance
    """
    print("\n" + "=" * 80)
    print("SECTION 2: USE PANDAS UDFS (VECTORIZED UDFS)")
    print("=" * 80)
    
    print("\n--- What are Pandas UDFs? ---")
    print("Pandas UDFs (Vectorized UDFs):")
    print("  - Process batches as pandas Series")
    print("  - Arrow serialization (faster)")
    print("  - Vectorized operations")
    print("  - 10-100x faster than regular UDFs")
    
    print("\n--- Configuration ---")
    arrow_enabled = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")
    print(f"Arrow enabled: {arrow_enabled}")
    print("spark.sql.execution.arrow.pyspark.enabled: true")
    
    print("\n--- Arrow Serialization ---")
    print("Arrow benefits:")
    print("  - Zero-copy serialization")
    print("  - Columnar format")
    print("  - Cross-language compatible")
    print("  - Much faster than pickle")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Regular UDF
    print("\n--- Regular UDF ---")
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    
    @udf(IntegerType())
    def double_value_regular(x):
        return x * 2
    
    start = time.time()
    result_regular = df.withColumn("value_doubled", double_value_regular(col("value")))
    count_regular = result_regular.count()
    time_regular = time.time() - start
    print(f"Regular UDF: {time_regular:.4f} seconds")
    
    # Pandas UDF
    print("\n--- Pandas UDF ---")
    @pandas_udf(IntegerType())
    def double_value_pandas(series: pd.Series) -> pd.Series:
        return series * 2
    
    start = time.time()
    result_pandas = df.withColumn("value_doubled", double_value_pandas(col("value")))
    count_pandas = result_pandas.count()
    time_pandas = time.time() - start
    print(f"Pandas UDF: {time_pandas:.4f} seconds")
    print(f"Pandas UDF is {time_regular/time_pandas:.1f}x faster")
    
    print("\n--- When to Use Pandas UDFs ---")
    print("Use pandas UDFs when:")
    print("  - UDF is necessary")
    print("  - Processing large datasets")
    print("  - Performance is critical")
    print("  - Logic can be vectorized")


def section_3_pandas_udf_types(spark):
    """
    Section 3: Pandas UDF Types
    Demonstrates different types of pandas UDFs
    """
    print("\n" + "=" * 80)
    print("SECTION 3: PANDAS UDF TYPES")
    print("=" * 80)
    
    print("\n--- Pandas UDF Types ---")
    udf_types = [
        ("Scalar", "Input/output scalar, batch processing"),
        ("Grouped Map", "groupBy().apply(), custom transformations"),
        ("Grouped Aggregate", "Custom aggregations"),
        ("Window", "Window operations"),
    ]
    
    print("Pandas UDF types:")
    for udf_type, description in udf_types:
        print(f"  {udf_type}: {description}")
    
    print("\n--- Scalar Pandas UDF ---")
    # Create sample data
    data = [(i, f"user_{i}", i * 100) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    @pandas_udf(IntegerType())
    def double_value(series: pd.Series) -> pd.Series:
        return series * 2
    
    df_scalar = df.withColumn("value_doubled", double_value(col("value")))
    print("Scalar UDF result:")
    df_scalar.show(5)
    
    print("\n--- Grouped Map Pandas UDF ---")
    @pandas_udf(schema="id long, name string, value double")
    def normalize_group(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.copy()
        pdf['value'] = pdf['value'] / pdf['value'].max()
        return pdf
    
    print("\nGrouped Map UDF:")
    print("df.groupby('group_id').apply(normalize_group)")
    print("Custom transformation per group")
    
    print("\n--- Grouped Aggregate Pandas UDF ---")
    @pandas_udf(DoubleType())
    def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
        return (values * weights).sum() / weights.sum()
    
    print("\nGrouped Aggregate UDF:")
    print("Custom aggregation function")
    print("df.groupby('group_id').agg(weighted_mean('value', 'weight'))")
    
    print("\n--- Window Pandas UDF ---")
    from pyspark.sql.window import Window
    
    @pandas_udf(DoubleType())
    def rolling_mean(series: pd.Series) -> pd.Series:
        return series.rolling(3).mean()
    
    print("\nWindow UDF:")
    print("Custom window function")
    print("df.withColumn('rolling_avg', rolling_mean('value').over(window))")
    
    print("\n--- Choosing the Right Type ---")
    print("Scalar: Simple transformations")
    print("Grouped Map: Complex per-group transformations")
    print("Grouped Aggregate: Custom aggregations")
    print("Window: Custom window functions")


def section_4_pandas_udf_best_practices(spark):
    """
    Section 4: Pandas UDF Best Practices
    Demonstrates best practices for pandas UDFs
    """
    print("\n" + "=" * 80)
    print("SECTION 4: PANDAS UDF BEST PRACTICES")
    print("=" * 80)
    
    print("\n--- Best Practice 1: Use Vectorized Operations ---")
    print("Bad: Row-by-row iteration")
    print("@pandas_udf(IntegerType())")
    print("def slow_func(series):")
    print("    return [x * 2 for x in series]  # Slow")
    
    print("\nGood: Vectorized operations")
    print("@pandas_udf(IntegerType())")
    print("def fast_func(series):")
    print("    return series * 2  # Fast")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    @pandas_udf(IntegerType())
    def vectorized_double(series: pd.Series) -> pd.Series:
        return series * 2
    
    result = df.withColumn("value_doubled", vectorized_double(col("value")))
    print("Vectorized operation result:")
    result.show(5)
    
    print("\n--- Best Practice 2: Use Pandas/Numpy Built-ins ---")
    print("Use pandas and numpy functions:")
    print("  - series.mean()")
    print("  - series.std()")
    print("  - np.log()")
    print("  - np.exp()")
    
    print("\n--- Best Practice 3: Minimize Data Transfer ---")
    print("Keep data in pandas:")
    print("  - Process in pandas as much as possible")
    print("  - Avoid transferring back and forth")
    print("  - Combine operations in single UDF")
    
    print("\n--- Best Practice 4: Handle Nulls ---")
    @pandas_udf(IntegerType())
    def handle_nulls(series: pd.Series) -> pd.Series:
        return series.fillna(0) * 2
    
    print("Handle nulls in pandas:")
    print("  - series.fillna()")
    print("  - series.dropna()")
    print("  - series.isna()")
    
    print("\n--- Best Practice 5: Type Hints ---")
    print("Use type hints for clarity:")
    print("@pandas_udf(IntegerType())")
    print("def func(series: pd.Series) -> pd.Series:")
    print("    return series * 2")
    
    print("\n--- Performance Tips ---")
    print("1. Use vectorized operations")
    print("2. Minimize data transfer")
    print("3. Handle nulls properly")
    print("4. Use type hints")
    print("5. Profile performance")


def section_5_regular_udf_optimization(spark):
    """
    Section 5: Regular UDF Optimization
    Demonstrates optimizing regular UDFs
    """
    print("\n" + "=" * 80)
    print("SECTION 5: REGULAR UDF OPTIMIZATION")
    print("=" * 80)
    
    print("\n--- When to Use Regular UDFs ---")
    print("Use regular UDFs when:")
    print("  - Pandas UDF not available (Spark < 2.3)")
    print("  - Logic cannot be vectorized")
    print("  - Complex stateful operations")
    
    print("\n--- Optimization 1: Cache Results ---")
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    
    @udf(IntegerType())
    def expensive_computation(x):
        # Simulate expensive computation
        time.sleep(0.001)
        return x * 2
    
    print("Cache UDF results if reused:")
    print("df_cached = df.withColumn('result', expensive_computation(col('value'))).cache()")
    print("df_cached.filter(col('result') > 1000)")
    
    print("\n--- Optimization 2: Keep UDF Logic Simple ---")
    print("Bad: Complex UDF")
    print("@udf(IntegerType())")
    print("def complex_udf(x):")
    print("    # Complex logic with multiple operations")
    print("    result = x")
    print("    for i in range(100):")
    print("        result = complex_operation(result)")
    print("    return result")
    
    print("\nGood: Simple UDF")
    print("@udf(IntegerType())")
    print("def simple_udf(x):")
    print("    return x * 2")
    
    print("\n--- Optimization 3: Use Deterministic UDFs ---")
    print("Mark UDF as deterministic for caching:")
    print("@udf(IntegerType())")
    print("@pandas_udf(IntegerType())")
    print("spark.udf.register('my_udf', my_udf)")
    
    print("\n--- Optimization 4: Avoid Heavy Computation ---")
    print("Move heavy computation outside UDF:")
    print("  - Pre-compute lookup tables")
    print("  - Use broadcast variables")
    print("  - Cache reference data")
    
    print("\n--- Example: Broadcast Variable ---")
    lookup_data = {1: "A", 2: "B", 3: "C"}
    broadcast_lookup = spark.sparkContext.broadcast(lookup_data)
    
    @udf(StringType())
    def lookup_category(x):
        return broadcast_lookup.value.get(x, "Unknown")
    
    print("Use broadcast variables for lookups:")
    print("Reduces data transfer to executors")
    
    print("\n--- Recommendation ---")
    print("Prefer pandas UDFs over regular UDFs")
    print("  - 10-100x faster")
    print("  - Vectorized operations")
    print("  - Arrow serialization")


def section_6_udf_serialization(spark):
    """
    Section 6: UDF Serialization
    Demonstrates UDF serialization configuration
    """
    print("\n" + "=" * 80)
    print("SECTION 6: UDF SERIALIZATION")
    print("=" * 80)
    
    print("\n--- Serialization Methods ---")
    print("Regular UDF:")
    print("  - Pickle serialization")
    print("  - Slow")
    print("  - Row-by-row")
    
    print("\nPandas UDF:")
    print("  - Arrow serialization")
    print("  - Fast")
    print("  - Batch processing")
    
    print("\n--- Arrow Configuration ---")
    print("spark.sql.execution.arrow.pyspark.enabled: true")
    print("spark.sql.execution.arrow.maxRecordsPerBatch: 10000")
    
    print("\n--- Current Configuration ---")
    arrow_enabled = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")
    max_records = spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch", "default")
    print(f"Arrow enabled: {arrow_enabled}")
    print(f"Max records per batch: {max_records}")
    
    print("\n--- Tuning Batch Size ---")
    print("spark.sql.execution.arrow.maxRecordsPerBatch:")
    print("  - Default: 10000")
    print("  - Larger batch = better performance")
    print("  - Larger batch = more memory")
    print("  - Tune based on data size")
    
    print("\n--- Trade-offs ---")
    print("Small batch size:")
    print("  - Less memory")
    print("  - More overhead")
    print("  - Slower")
    
    print("\nLarge batch size:")
    print("  - More memory")
    print("  - Less overhead")
    print("  - Faster")
    
    print("\n--- Example Configuration ---")
    print("For large datasets:")
    print("spark.sql.execution.arrow.maxRecordsPerBatch: 50000")
    
    print("\nFor memory-constrained:")
    print("spark.sql.execution.arrow.maxRecordsPerBatch: 1000")
    
    print("\n--- Monitoring ---")
    print("Check Spark UI:")
    print("  - Task duration")
    print("  - Memory usage")
    print("  - Serialization time")


def section_7_udf_registration(spark):
    """
    Section 7: UDF Registration
    Demonstrates registering UDFs for SQL
    """
    print("\n" + "=" * 80)
    print("SECTION 7: UDF REGISTRATION")
    print("=" * 80)
    
    print("\n--- Why Register UDFs? ---")
    print("UDF registration benefits:")
    print("  - Use in SQL queries")
    print("  - Reuse across sessions")
    print("  - Share with team")
    
    print("\n--- Registration Methods ---")
    print("1. spark.udf.register()")
    print("2. @udf decorator")
    
    print("\n--- Example: Register for SQL ---")
    # Create sample data
    data = [(i, f"user_{i}", i * 100) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    df.createOrReplaceTempView("users")
    
    # Register UDF
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    
    @udf(IntegerType())
    def double_value(x):
        return x * 2
    
    spark.udf.register("double_value", double_value)
    
    print("\nUDF registered as 'double_value'")
    
    # Use in SQL
    result = spark.sql("SELECT id, name, double_value(value) as doubled FROM users")
    print("SQL result:")
    result.show(5)
    
    print("\n--- Example: Pandas UDF Registration ---")
    @pandas_udf(IntegerType())
    def double_value_pandas(series: pd.Series) -> pd.Series:
        return series * 2
    
    spark.udf.register("double_value_pandas", double_value_pandas)
    
    print("\nPandas UDF registered as 'double_value_pandas'")
    
    # Use in SQL
    result_pandas = spark.sql("SELECT id, name, double_value_pandas(value) as doubled FROM users")
    print("SQL result with pandas UDF:")
    result_pandas.show(5)
    
    print("\n--- Return Type Specification ---")
    print("Specify return type explicitly:")
    print("@udf('integer')")
    print("@udf(IntegerType())")
    print("@pandas_udf('integer')")
    print("@pandas_udf(IntegerType())")
    
    print("\n--- Best Practices ---")
    print("1. Specify return type explicitly")
    print("2. Use descriptive names")
    print("3. Document UDF purpose")
    print("4. Test before registration")


def section_8_avoid_python_udfs_in_joins(spark):
    """
    Section 8: Avoid Python UDFs in Joins
    Demonstrates why UDFs in joins are problematic
    """
    print("\n" + "=" * 80)
    print("SECTION 8: AVOID PYTHON UDFS IN JOINS")
    print("=" * 80)
    
    print("\n--- Why Avoid UDFs in Joins? ---")
    print("UDFs in join problems:")
    print("  - Disable join optimizations")
    print("  - Prevent broadcast join")
    print("  - Force shuffle join")
    print("  - Slow execution")
    
    print("\n--- Example: UDF in Join Condition ---")
    # Create sample data
    data1 = [(i, f"user_{i}", i % 100) for i in range(1, 101)]
    data2 = [(i, f"other_{i}", i % 100) for i in range(1, 101)]
    
    df1 = spark.createDataFrame(data1, ["id", "name", "group_id"])
    df2 = spark.createDataFrame(data2, ["id", "other_name", "group_id"])
    
    print("\n--- Bad: UDF in Join Condition ---")
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    
    @udf(IntegerType())
    def normalize_group(x):
        return x % 50
    
    print("# Bad: UDF in join condition")
    print("# result = df1.join(df2, normalize_group(df1.group_id) == normalize_group(df2.group_id))")
    print("# Disables optimizations, forces shuffle")
    
    print("\n--- Good: Materialize Before Join ---")
    print("\nMaterialize UDF results before join:")
    df1_normalized = df1.withColumn("group_normalized", col("group_id") % 50)
    df2_normalized = df2.withColumn("group_normalized", col("group_id") % 50)
    
    result = df1_normalized.join(df2_normalized, "group_normalized")
    print("Join on normalized column (no UDF)")
    print(f"Result: {result.count()} rows")
    
    print("\n--- Alternative: Move UDF Outside Join ---")
    print("Apply UDF before or after join:")
    print("df1_transformed = df1.withColumn('new_key', udf(col('key')))")
    print("df2_transformed = df2.withColumn('new_key', udf(col('key')))")
    print("result = df1_transformed.join(df2_transformed, 'new_key')")
    
    print("\n--- Best Practices ---")
    print("1. Avoid UDFs in join conditions")
    print("2. Materialize UDF results before join")
    print("3. Use built-in functions in joins")
    print("4. Move UDFs outside joins when possible")
    
    print("\n--- Check Query Plan ---")
    print("Use explain() to check:")
    print("  - Join strategy")
    print("  - Broadcast join disabled?")
    print("  - UDF in join condition?")


def section_9_monitor_udf_performance(spark):
    """
    Section 9: Monitor UDF Performance
    Demonstrates monitoring UDF performance
    """
    print("\n" + "=" * 80)
    print("SECTION 9: MONITOR UDF PERFORMANCE")
    print("=" * 80)
    
    print("\n--- Monitoring Methods ---")
    print("1. Spark UI")
    print("2. Task duration comparison")
    print("3. Python profilers")
    print("4. Benchmarking")
    
    print("\n--- Spark UI Metrics ---")
    print("Check Spark UI:")
    print("  - Jobs tab: Overall job time")
    print("  - Stages tab: Stage duration")
    print("  - Tasks tab: Task duration distribution")
    print("  - SQL tab: Query execution time")
    
    print("\n--- Benchmarking Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    print("\nBenchmark: Built-in vs UDF vs Pandas UDF")
    
    # Built-in
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    
    start = time.time()
    result_builtin = df.withColumn("doubled", col("value") * 2)
    result_builtin.count()
    time_builtin = time.time() - start
    print(f"Built-in: {time_builtin:.4f}s")
    
    # Regular UDF
    @udf(IntegerType())
    def double_udf(x):
        return x * 2
    
    start = time.time()
    result_udf = df.withColumn("doubled", double_udf(col("value")))
    result_udf.count()
    time_udf = time.time() - start
    print(f"Regular UDF: {time_udf:.4f}s ({time_udf/time_builtin:.1f}x slower)")
    
    # Pandas UDF
    @pandas_udf(IntegerType())
    def double_pandas(series: pd.Series) -> pd.Series:
        return series * 2
    
    start = time.time()
    result_pandas = df.withColumn("doubled", double_pandas(col("value")))
    result_pandas.count()
    time_pandas = time.time() - start
    print(f"Pandas UDF: {time_pandas:.4f}s ({time_pandas/time_builtin:.1f}x slower)")
    
    print("\n--- Python Profiling ---")
    print("Profile UDF with Python profilers:")
    print("import cProfile")
    print("cProfile.run('my_udf_function')")
    
    print("\n--- Performance Indicators ---")
    print("Good performance:")
    print("  - UDF time < 2x built-in")
    print("  - Consistent task duration")
    print("  - No stragglers")
    
    print("\nPoor performance:")
    print("  - UDF time > 10x built-in")
    print("  - High variance in task duration")
    print("  - Stragglers present")
    
    print("\n--- Optimization Checklist ---")
    print("[ ] Use built-in functions first")
    print("[ ] Use pandas UDF over regular UDF")
    print("[ ] Enable Arrow serialization")
    print("[ ] Use vectorized operations")
    print("[ ] Monitor Spark UI")
    print("[ ] Benchmark alternatives")
    print("[ ] Consider rewriting in Scala for critical paths")


def main():
    """
    Main function to run all demonstrations
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run all sections
        section_1_understand_udf_performance_impact(spark)
        section_2_use_pandas_udfs(spark)
        section_3_pandas_udf_types(spark)
        section_4_pandas_udf_best_practices(spark)
        section_5_regular_udf_optimization(spark)
        section_6_udf_serialization(spark)
        section_7_udf_registration(spark)
        section_8_avoid_python_udfs_in_joins(spark)
        section_9_monitor_udf_performance(spark)
        
        print("\n" + "=" * 80)
        print("ALL DEMONSTRATIONS COMPLETED")
        print("=" * 80)
        print("\nKey Takeaways:")
        print("1. UDFs are 10-100x slower than built-in functions")
        print("2. Use pandas UDFs for better performance (10-100x faster than regular UDF)")
        print("3. Enable Arrow serialization for pandas UDFs")
        print("4. Use vectorized operations in pandas UDFs")
        print("5. Cache UDF results if reused")
        print("6. Tune batch size for Arrow serialization")
        print("7. Register UDFs for SQL usage")
        print("8. Avoid UDFs in join conditions")
        print("9. Monitor UDF performance in Spark UI")
        print("10. Consider rewriting in Scala for critical paths")
        print("\nCheck Spark UI at http://localhost:4040 for UDF metrics")
        
    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()
