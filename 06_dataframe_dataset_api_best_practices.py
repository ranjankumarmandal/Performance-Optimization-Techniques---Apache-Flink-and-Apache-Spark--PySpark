"""
================================================================================
PYSPARK DATAFRAME/DATASET API BEST PRACTICES - CODE EXAMPLES
================================================================================

This file demonstrates DataFrame/Dataset API best practices with practical
examples for staff engineer preparation.

Topics Covered:
1. Use DataFrame API over RDD
2. Use Built-in Functions
3. Avoid collect() on Large Data
4. Use SQL for Complex Queries
5. Chain Operations
6. Select Columns Explicitly
7. Use withColumn for Multiple Transformations
8. Avoid UDFs When Possible
9. Use Typed Operations
10. Handle Null Values Properly
11. Use Window Functions Efficiently
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when, sum as _sum, count as _count, avg, upper, lower, trim, split, explode, isnull, isnotnull, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.window import Window
import time


def create_spark_session():
    """
    Create and return a SparkSession with appropriate configuration.
    """
    spark = SparkSession.builder \
        .appName("DataFrameDatasetBestPracticesDemo") \
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


def section_1_use_dataframe_api_over_rdd(spark):
    """
    Section 1: Use DataFrame API over RDD
    Demonstrates why DataFrame API is preferred over RDD
    """
    print("\n" + "=" * 80)
    print("SECTION 1: USE DATAFRAME API OVER RDD")
    print("=" * 80)
    
    print("\n--- Why DataFrame API? ---")
    print("DataFrame API benefits:")
    print("  - Catalyst optimizer optimizations")
    print("  - Tungsten execution engine")
    print("  - Better memory management")
    print("  - 10-100x faster than RDDs")
    
    print("\n--- RDD vs DataFrame ---")
    print("RDD:")
    print("  - Low-level, untyped")
    print("  - No Catalyst optimization")
    print("  - More boilerplate")
    print("  - Slower execution")
    
    print("\nDataFrame:")
    print("  - Typed with schema")
    print("  - Catalyst optimizer")
    print("  - Less boilerplate")
    print("  - Faster execution")
    
    print("\n--- Example: RDD Approach ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print("\nRDD approach (not recommended):")
    print("rdd = df.rdd")
    print("filtered = rdd.filter(lambda x: x[2] > 50)")
    print("result = filtered.map(lambda x: (x[2], x[3])).reduceByKey(lambda a, b: a + b)")
    
    print("\n--- Example: DataFrame Approach ---")
    print("DataFrame approach (recommended):")
    print("result = df.filter(col('group_id') > 50).groupBy('group_id').agg({'value': 'sum'})")
    
    # Demonstrate DataFrame approach
    result = df.filter(col("group_id") > 50).groupBy("group_id").agg({"value": "sum"})
    print(f"\nDataFrame result: {result.count()} groups")
    
    print("\n--- When to Use RDD ---")
    print("Use RDD only when:")
    print("  - Custom data types not supported by DataFrame")
    print("  - Low-level control needed")
    print("  - Python-specific operations required")
    
    print("\n--- Recommendation ---")
    print("Always prefer DataFrame API")
    print("  - Better performance")
    print("  - Less code")
    print("  - Automatic optimization")


def section_2_use_builtin_functions(spark):
    """
    Section 2: Use Built-in Functions
    Demonstrates using built-in functions over UDFs
    """
    print("\n" + "=" * 80)
    print("SECTION 2: USE BUILT-IN FUNCTIONS")
    print("=" * 80)
    
    print("\n--- Why Built-in Functions? ---")
    print("Built-in functions benefits:")
    print("  - Vectorized operations")
    print("  - Catalyst optimizations")
    print("  - Faster than UDFs")
    print("  - No serialization overhead")
    
    print("\n--- Common Built-in Functions ---")
    builtin_funcs = [
        ("col", "Column reference"),
        ("lit", "Literal value"),
        ("sum, avg, count", "Aggregations"),
        ("upper, lower", "String functions"),
        ("when, otherwise", "Conditional expressions"),
        ("split, explode", "Array functions"),
    ]
    
    print("Common built-in functions:")
    for func, description in builtin_funcs:
        print(f"  {func}: {description}")
    
    print("\n--- Example: String Functions ---")
    # Create sample data
    data = [(i, f"  User_{i}  ", i % 100) for i in range(1, 11)]
    df = spark.createDataFrame(data, ["id", "name", "group_id"])
    
    print(f"\nOriginal data:")
    df.show(5, truncate=False)
    
    # Use built-in functions
    df_cleaned = df.withColumn("name_clean", trim(col("name"))) \
                   .withColumn("name_upper", upper(col("name_clean"))) \
                   .withColumn("name_lower", lower(col("name_clean")))
    
    print("\nAfter built-in transformations:")
    df_cleaned.show(5, truncate=False)
    
    print("\n--- Example: Conditional Expressions ---")
    df_conditional = df.withColumn("category", 
                                   when(col("group_id") < 30, "Low")
                                   .when(col("group_id") < 70, "Medium")
                                   .otherwise("High"))
    
    print("\nWith conditional expression:")
    df_conditional.show(5, truncate=False)
    
    print("\n--- Example: Array Functions ---")
    # Create data with array
    array_data = [(i, f"user_{i}", ["tag1", "tag2", "tag3"]) for i in range(1, 6)]
    df_array = spark.createDataFrame(array_data, ["id", "name", "tags"])
    
    print("\nData with array:")
    df_array.show(truncate=False)
    
    # Explode array
    df_exploded = df_array.withColumn("tag", explode(col("tags")))
    print("\nAfter explode:")
    df_exploded.show(truncate=False)
    
    print("\n--- Recommendation ---")
    print("Always use built-in functions first")
    print("  - Check pyspark.sql.functions")
    print("  - Only use UDF when built-in not available")


def section_3_avoid_collect_on_large_data(spark):
    """
    Section 3: Avoid collect() on Large Data
    Demonstrates why collect() is dangerous and alternatives
    """
    print("\n" + "=" * 80)
    print("SECTION 3: AVOID COLLECT() ON LARGE DATA")
    print("=" * 80)
    
    print("\n--- Why Avoid collect()? ---")
    print("collect() problems:")
    print("  - Brings all data to driver")
    print("  - Can cause OOM on driver")
    print("  - Network bottleneck")
    print("  - Not scalable")
    
    print("\n--- Alternatives to collect() ---")
    alternatives = [
        ("take(n)", "Get first n rows"),
        ("head(n)", "Get first n rows (alias for take)"),
        ("limit(n)", "Limit DataFrame to n rows"),
        ("show(n)", "Display first n rows"),
        ("first()", "Get first row"),
        ("count()", "Count rows"),
        ("write()", "Write to storage"),
    ]
    
    print("Alternatives:")
    for alt, description in alternatives:
        print(f"  {alt}: {description}")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Bad: collect all data
    print("\n--- Bad: collect() ---")
    print("# DON'T DO THIS for large data:")
    print("# all_data = df.collect()  # OOM risk!")
    
    # Good: use take()
    print("\n--- Good: take() ---")
    sample = df.take(5)
    print(f"First 5 rows: {len(sample)} rows")
    
    # Good: use show()
    print("\n--- Good: show() ---")
    df.show(5)
    
    # Good: use count()
    print("\n--- Good: count() ---")
    row_count = df.count()
    print(f"Total rows: {row_count}")
    
    # Good: use write()
    print("\n--- Good: write() ---")
    print("# df.write.parquet('/path/to/output')")
    print("# Write to storage instead of collecting")
    
    print("\n--- When to Use collect() ---")
    print("Only use collect() when:")
    print("  - Data is small (< 100MB)")
    print("  - Need all data on driver")
    print("  - Data fits in driver memory")
    
    print("\n--- Recommendation ---")
    print("Avoid collect() on large data")
    print("  - Use take(), head(), show() for sampling")
    print("  - Use write() for saving results")
    print("  - Use count() for counts")


def section_4_use_sql_for_complex_queries(spark):
    """
    Section 4: Use SQL for Complex Queries
    Demonstrates using SQL for complex logic
    """
    print("\n" + "=" * 80)
    print("SECTION 4: USE SQL FOR COMPLEX QUERIES")
    print("=" * 80)
    
    print("\n--- Why Use SQL? ---")
    print("SQL benefits:")
    print("  - More readable for complex logic")
    print("  - Same optimizations as DataFrame API")
    print("  - Familiar syntax")
    print("  - Easier to share with analysts")
    
    print("\n--- DataFrame API vs SQL ---")
    print("DataFrame API:")
    print("  - Programmatic")
    print("  - Type-safe")
    print("  - Good for simple to medium complexity")
    
    print("\nSQL:")
    print("  - Declarative")
    print("  - More readable for complex queries")
    print("  - Good for complex aggregations, joins")
    
    print("\n--- Example: DataFrame API ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    df.createOrReplaceTempView("users")
    
    print("\nDataFrame API approach:")
    result_df = df.filter(col("group_id") > 50) \
                  .groupBy("group_id") \
                  .agg({"value": "sum", "id": "count"}) \
                  .orderBy(col("group_id"))
    
    print("Result:")
    result_df.show()
    
    print("\n--- Example: SQL ---")
    print("\nSQL approach:")
    query = """
    SELECT group_id, 
           SUM(value) as total_value, 
           COUNT(id) as user_count
    FROM users
    WHERE group_id > 50
    GROUP BY group_id
    ORDER BY group_id
    """
    
    result_sql = spark.sql(query)
    print("Result:")
    result_sql.show()
    
    print("\n--- When to Use SQL ---")
    print("Use SQL when:")
    print("  - Query is complex (multiple joins, aggregations)")
    print("  - Logic is easier to express in SQL")
    print("  - Sharing with SQL-savvy team members")
    print("  - Migrating from traditional SQL systems")
    
    print("\n--- When to Use DataFrame API ---")
    print("Use DataFrame API when:")
    print("  - Building programmatic pipelines")
    print("  - Need type safety")
    print("  - Query is simple to medium complexity")
    print("  - Mixing with Python code")
    
    print("\n--- Recommendation ---")
    print("Use SQL for complex queries")
    print("  - More readable")
    print("  - Same performance")
    print("  - Easier to maintain")


def section_5_chain_operations(spark):
    """
    Section 5: Chain Operations
    Demonstrates chaining DataFrame operations
    """
    print("\n" + "=" * 80)
    print("SECTION 5: CHAIN OPERATIONS")
    print("=" * 80)
    
    print("\n--- Why Chain Operations? ---")
    print("Chaining benefits:")
    print("  - Allows Catalyst to optimize entire pipeline")
    print("  - More readable")
    print("  - Avoids intermediate variables")
    print("  - Better performance")
    
    print("\n--- Example: Not Chained ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print("\nNot chained (less optimal):")
    print("df_filtered = df.filter(col('group_id') > 50)")
    print("df_selected = df_filtered.select('id', 'name', 'value')")
    print("df_grouped = df_selected.groupBy('group_id').agg({'value': 'sum'})")
    
    print("\n--- Example: Chained ---")
    print("\nChained (recommended):")
    result = df.filter(col("group_id") > 50) \
               .select("id", "name", "value") \
               .groupBy("group_id") \
               .agg({"value": "sum"})
    
    print("Result:")
    result.show()
    
    print("\n--- Benefits of Chaining ---")
    print("1. Catalyst optimization:")
    print("   - Sees entire pipeline")
    print("   - Can optimize across operations")
    
    print("\n2. Readability:")
    print("   - Clear data flow")
    print("   - Less code")
    
    print("\n3. Performance:")
    print("   - Fewer intermediate DataFrames")
    print("   - Less memory overhead")
    
    print("\n--- Best Practices ---")
    print("1. Chain transformations")
    print("2. Break at actions")
    print("3. Use line breaks for readability")
    print("4. Cache if intermediate results reused")
    
    print("\n--- Example with Cache ---")
    print("\nCache intermediate result:")
    print("df_cached = df.filter(col('group_id') > 50).cache()")
    print("result1 = df_cached.groupBy('group_id').agg({'value': 'sum'})")
    print("result2 = df_cached.agg({'value': 'avg'})")


def section_6_select_columns_explicitly(spark):
    """
    Section 6: Select Columns Explicitly
    Demonstrates selecting only needed columns
    """
    print("\n" + "=" * 80)
    print("SECTION 6: SELECT COLUMNS EXPLICITLY")
    print("=" * 80)
    
    print("\n--- Why Select Explicitly? ---")
    print("Explicit selection benefits:")
    print("  - Reduces data movement")
    print("  - Enables column pruning")
    print("  - Reduces memory usage")
    print("  - Faster operations")
    
    print("\n--- Example: SELECT * ---")
    # Create sample data with many columns
    data = [(i, f"user_{i}", i % 100, i * 100, f"email_{i}", f"phone_{i}", f"address_{i}") 
            for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value", "email", "phone", "address"])
    
    print(f"\nDataFrame with {df.count()} rows and 7 columns")
    
    print("\n--- Bad: SELECT * ---")
    print("# Bad: Select all columns")
    print("# df_all = df.select('*')")
    print("# Reads all columns even if not needed")
    
    print("\n--- Good: Select Explicitly ---")
    print("\nGood: Select only needed columns")
    result = df.select("id", "name", "value")
    print("Selected columns: id, name, value")
    result.show(5)
    
    print("\n--- Column Pruning ---")
    print("Catalyst automatically prunes columns:")
    print("  - Only selected columns are read")
    print("  - Reduces I/O")
    print("  - Reduces memory")
    
    print("\n--- Example in Pipeline ---")
    print("\nIn a pipeline:")
    result_pipeline = df.select("id", "name", "group_id", "value") \
                        .filter(col("group_id") > 50) \
                        .select("id", "name", "value")
    
    print("Only id, name, value in final result")
    result_pipeline.show(5)
    
    print("\n--- Best Practices ---")
    print("1. Select only needed columns")
    print("2. Avoid SELECT *")
    print("3. Select early in pipeline")
    print("4. Use column pruning")


def section_7_use_withcolumn_for_transformations(spark):
    """
    Section 7: Use withColumn for Multiple Transformations
    Demonstrates using withColumn for transformations
    """
    print("\n" + "=" * 80)
    print("SECTION 7: USE WITHCOLUMN FOR TRANSFORMATIONS")
    print("=" * 80)
    
    print("\n--- Why withColumn? ---")
    print("withColumn benefits:")
    print("  - Add or replace columns")
    print("  - Chain multiple transformations")
    print("  - More efficient than creating new DataFrames")
    print("  - Clean syntax")
    
    print("\n--- Example: Single withColumn ---")
    # Create sample data
    data = [(i, f"user_{i}", i * 100) for i in range(1, 11)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    print("\nOriginal data:")
    df.show()
    
    # Add single column
    df_with_one = df.withColumn("value_doubled", col("value") * 2)
    print("\nAfter adding one column:")
    df_with_one.show()
    
    print("\n--- Example: Multiple withColumn ---")
    print("\nChain multiple withColumn calls:")
    df_transformed = df.withColumn("value_doubled", col("value") * 2) \
                       .withColumn("value_squared", col("value") * col("value")) \
                       .withColumn("category", when(col("value") > 500, "High").otherwise("Low"))
    
    print("After multiple transformations:")
    df_transformed.show()
    
    print("\n--- Benefits ---")
    print("1. Efficient:")
    print("   - Single DataFrame modified")
    print("   - Less memory overhead")
    
    print("\n2. Readable:")
    print("   - Clear transformation chain")
    print("   - Easy to understand")
    
    print("\n3. Flexible:")
    print("   - Add or replace columns")
    print("   - Complex expressions")
    
    print("\n--- Best Practices ---")
    print("1. Chain withColumn calls")
    print("2. Use descriptive column names")
    print("3. Use built-in functions in expressions")
    print("4. Avoid UDFs when possible")


def section_8_avoid_udfs_when_possible(spark):
    """
    Section 8: Avoid UDFs When Possible
    Demonstrates why UDFs are slow and alternatives
    """
    print("\n" + "=" * 80)
    print("SECTION 8: AVOID UDFS WHEN POSSIBLE")
    print("=" * 80)
    
    print("\n--- Why Avoid UDFs? ---")
    print("UDF problems:")
    print("  - Row-by-row processing")
    print("  - Serialization/deserialization overhead")
    print("  - No Catalyst optimization")
    print("  - 10-100x slower than built-in functions")
    
    print("\n--- Example: UDF (Slow) ---")
    # Create sample data
    data = [(i, f"user_{i}", i * 100) for i in range(1, 1001)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    print("\n--- UDF Approach (Slow) ---")
    print("# from pyspark.sql.functions import udf")
    print("# from pyspark.sql.types import IntegerType")
    print("#")
    print("# @udf(IntegerType())")
    print("# def double_value(x):")
    print("#     return x * 2")
    print("#")
    print("# df_udf = df.withColumn('value_doubled', double_value(col('value')))")
    print("# # Slow due to serialization overhead")
    
    print("\n--- Built-in Function (Fast) ---")
    print("\nBuilt-in function approach (fast):")
    df_builtin = df.withColumn("value_doubled", col("value") * 2)
    print("Result:")
    df_builtin.show(5)
    
    print("\n--- Performance Comparison ---")
    print("UDF: ~10-100x slower")
    print("Built-in: Fast, optimized")
    
    print("\n--- When to Use UDFs ---")
    print("Use UDFs only when:")
    print("  - Built-in function not available")
    print("  - Complex custom logic needed")
    print("  - Consider pandas_udf for better performance")
    
    print("\n--- Alternative: pandas_udf ---")
    print("pandas_udf (vectorized UDF):")
    print("  - Process batches as pandas Series")
    print("  - Arrow serialization")
    print("  - 10-100x faster than regular UDF")
    
    print("\n--- Recommendation ---")
    print("Always prefer built-in functions")
    print("  - Check pyspark.sql.functions first")
    print("  - Use SQL expressions")
    print("  - Only use UDF when necessary")
    print("  - Use pandas_udf if UDF needed")


def section_9_use_typed_operations(spark):
    """
    Section 9: Use Typed Operations
    Demonstrates using typed Column expressions
    """
    print("\n" + "=" * 80)
    print("SECTION 9: USE TYPED OPERATIONS")
    print("=" * 80)
    
    print("\n--- Why Typed Operations? ---")
    print("Typed operations benefits:")
    print("  - Catalyst optimizations")
    print("  - Type safety")
    print("  - Better performance")
    print("  - More readable")
    
    print("\n--- Example: Python Lanbda (Slow) ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print("\n--- Python Lambda (Not Recommended) ---")
    print("# df.filter(lambda x: x.group_id > 50)")
    print("# Slow, no optimization")
    
    print("\n--- Column Expression (Recommended) ---")
    print("\nColumn expression approach (recommended):")
    result = df.filter(col("group_id") > 50)
    print("Result:")
    result.show(5)
    
    print("\n--- Comparison ---")
    print("Python lambda:")
    print("  - Row-by-row processing")
    print("  - No Catalyst optimization")
    print("  - Slow")
    
    print("\nColumn expression:")
    print("  - Optimized by Catalyst")
    print("  - Type-safe")
    print("  - Fast")
    
    print("\n--- Examples ---")
    print("Filter:")
    print("  df.filter(col('age') > 25)")
    
    print("\nSelect:")
    print("  df.select(col('name'), col('age'))")
    
    print("\nWithColumn:")
    print("  df.withColumn('doubled', col('value') * 2)")
    
    print("\n--- Best Practices ---")
    print("1. Use col() for column references")
    print("2. Use Column expressions")
    print("3. Avoid Python lambdas")
    print("4. Use built-in functions")


def section_10_handle_null_values_properly(spark):
    """
    Section 10: Handle Null Values Properly
    Demonstrates null value handling
    """
    print("\n" + "=" * 80)
    print("SECTION 10: HANDLE NULL VALUES PROPERLY")
    print("=" * 80)
    
    print("\n--- Null Handling Functions ---")
    null_funcs = [
        ("isnull()", "Check if null"),
        ("isnotnull()", "Check if not null"),
        ("coalesce()", "Return first non-null value"),
        ("na.fill()", "Fill null values"),
        ("na.drop()", "Drop rows with null values"),
    ]
    
    print("Null handling functions:")
    for func, description in null_funcs:
        print(f"  {func}: {description}")
    
    print("\n--- Example: Data with Nulls ---")
    # Create sample data with nulls
    data = [
        (1, "user_1", 100),
        (2, None, 200),
        (3, "user_3", None),
        (4, "user_4", 400),
        (5, None, None),
    ]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    print("\nData with nulls:")
    df.show()
    
    print("\n--- isnull() ---")
    df_null_check = df.filter(isnull(col("name")))
    print("Rows where name is null:")
    df_null_check.show()
    
    print("\n--- isnotnull() ---")
    df_notnull_check = df.filter(isnotnull(col("name")))
    print("Rows where name is not null:")
    df_notnull_check.show()
    
    print("\n--- coalesce() ---")
    df_coalesce = df.withColumn("value_clean", coalesce(col("value"), lit(0)))
    print("Replace null value with 0:")
    df_coalesce.show()
    
    print("\n--- na.fill() ---")
    df_filled = df.na.fill({"name": "unknown", "value": 0})
    print("Fill null values:")
    df_filled.show()
    
    print("\n--- na.drop() ---")
    df_dropped = df.na.drop()
    print("Drop rows with any null:")
    df_dropped.show()
    
    print("\n--- Best Practices ---")
    print("1. Check for nulls early")
    print("2. Handle nulls appropriately")
    print("3. Use coalesce() for default values")
    print("4. Use na.fill() for bulk filling")
    print("5. Use na.drop() if nulls not acceptable")


def section_11_use_window_functions_efficiently(spark):
    """
    Section 11: Use Window Functions Efficiently
    Demonstrates window function usage
    """
    print("\n" + "=" * 80)
    print("SECTION 11: USE WINDOW FUNCTIONS EFFICIENTLY")
    print("=" * 80)
    
    print("\n--- What are Window Functions? ---")
    print("Window functions:")
    print("  - Perform calculations across rows")
    print("  - Related to current row")
    print("  - Powerful for analytics")
    
    print("\n--- Common Window Functions ---")
    window_funcs = [
        ("row_number()", "Row number in window"),
        ("rank()", "Rank with gaps"),
        ("dense_rank()", "Rank without gaps"),
        ("lag()", "Value from previous row"),
        ("lead()", "Value from next row"),
        ("sum() over window", "Running total"),
    ]
    
    print("Common window functions:")
    for func, description in window_funcs:
        print(f"  {func}: {description}")
    
    print("\n--- Example ---")
    # Create sample data
    data = [
        (1, "user_1", "A", 100),
        (2, "user_2", "A", 200),
        (3, "user_3", "A", 150),
        (4, "user_4", "B", 300),
        (5, "user_5", "B", 250),
    ]
    df = spark.createDataFrame(data, ["id", "name", "group", "value"])
    
    print("\nOriginal data:")
    df.show()
    
    print("\n--- Window Specification ---")
    window_spec = Window.partitionBy("group").orderBy("value")
    
    print("\n--- row_number() ---")
    from pyspark.sql.functions import row_number
    
    df_rownum = df.withColumn("row_num", row_number().over(window_spec))
    print("Row number within group:")
    df_rownum.show()
    
    print("\n--- lag() ---")
    from pyspark.sql.functions import lag
    
    df_lag = df.withColumn("prev_value", lag("value", 1).over(window_spec))
    print("Previous value:")
    df_lag.show()
    
    print("\n--- Running Total ---")
    df_running = df.withColumn("running_total", _sum("value").over(window_spec))
    print("Running total:")
    df_running.show()
    
    print("\n--- Performance Tips ---")
    print("1. Cache before complex window operations")
    print("2. Avoid multiple window operations if possible")
    print("3. Use appropriate partitioning")
    print("4. Consider window size")
    
    print("\n--- Example with Cache ---")
    print("\nCache before window operations:")
    print("df_cached = df.cache()")
    print("df_cached.withColumn('rank', row_number().over(window_spec))")
    print("df_cached.withColumn('lag', lag('value').over(window_spec))")


def main():
    """
    Main function to run all demonstrations
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run all sections
        section_1_use_dataframe_api_over_rdd(spark)
        section_2_use_builtin_functions(spark)
        section_3_avoid_collect_on_large_data(spark)
        section_4_use_sql_for_complex_queries(spark)
        section_5_chain_operations(spark)
        section_6_select_columns_explicitly(spark)
        section_7_use_withcolumn_for_transformations(spark)
        section_8_avoid_udfs_when_possible(spark)
        section_9_use_typed_operations(spark)
        section_10_handle_null_values_properly(spark)
        section_11_use_window_functions_efficiently(spark)
        
        print("\n" + "=" * 80)
        print("ALL DEMONSTRATIONS COMPLETED")
        print("=" * 80)
        print("\nKey Takeaways:")
        print("1. Use DataFrame API over RDD (Catalyst optimization)")
        print("2. Use built-in functions over UDFs (10-100x faster)")
        print("3. Avoid collect() on large data (use take, show, write)")
        print("4. Use SQL for complex queries (more readable)")
        print("5. Chain operations (Catalyst can optimize entire pipeline)")
        print("6. Select columns explicitly (enables column pruning)")
        print("7. Use withColumn for transformations (efficient)")
        print("8. Avoid UDFs when possible (use built-in functions)")
        print("9. Use typed operations (Column expressions)")
        print("10. Handle null values properly (isnull, coalesce, fill)")
        print("11. Use window functions efficiently (cache before complex ops)")
        print("\nCheck Spark UI at http://localhost:4040 for metrics")
        
    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()
