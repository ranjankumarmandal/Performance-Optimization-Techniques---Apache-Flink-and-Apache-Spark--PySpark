"""
================================================================================
PYSPARK CATALYST OPTIMIZER AND QUERY PLANNING - CODE EXAMPLES
================================================================================

This file demonstrates Catalyst optimizer and query planning with practical
examples for staff engineer preparation.

Topics Covered:
1. Understanding Catalyst Optimizer
2. Query Plan Analysis
3. Predicate Pushdown
4. Column Pruning
5. Constant Folding
6. Operator Reordering
7. Join Reordering
8. Cost-Based Optimization (CBO)
9. Adaptive Query Execution (AQE)
10. Subquery Optimization
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when, sum as _sum, count as _count, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType


def create_spark_session():
    """
    Create and return a SparkSession with appropriate configuration.
    """
    spark = SparkSession.builder \
        .appName("CatalystOptimizerDemo") \
        .master("local[4]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.cbo.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("=" * 80)
    print("SPARK SESSION CREATED")
    print("=" * 80)
    print(f"Spark Version: {spark.version}")
    print(f"CBO Enabled: {spark.conf.get('spark.sql.cbo.enabled')}")
    print(f"AQE Enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
    print("=" * 80)
    
    return spark


def section_1_understanding_catalyst_optimizer(spark):
    """
    Section 1: Understanding Catalyst Optimizer
    Demonstrates how Catalyst optimizer works
    """
    print("\n" + "=" * 80)
    print("SECTION 1: UNDERSTANDING CATALYST OPTIMIZER")
    print("=" * 80)
    
    print("\n--- What is Catalyst Optimizer? ---")
    print("Catalyst: Spark's extensible query optimizer")
    print("  - Rule-based optimization")
    print("  - Cost-based optimization (CBO)")
    print("  - Logical plan → Optimized logical plan → Physical plan")
    
    print("\n--- Optimization Phases ---")
    print("1. Analysis: Resolve references, check types")
    print("2. Logical Optimization: Apply rule-based optimizations")
    print("3. Physical Planning: Choose physical implementations")
    print("4. Code Generation: Generate Java bytecode")
    
    print("\n--- Rule-Based Optimizations ---")
    rule_optimizations = [
        "Predicate pushdown",
        "Column pruning",
        "Constant folding",
        "Operator reordering",
        "Join reordering",
        "Boolean simplification",
    ]
    print("Rule-based optimizations:")
    for opt in rule_optimizations:
        print(f"  - {opt}")
    
    print("\n--- Cost-Based Optimizations ---")
    print("Cost-based optimization (CBO):")
    print("  - Uses table statistics")
    print("  - Estimates cost of different plans")
    print("  - Chooses cheapest plan")
    print("  - Requires ANALYZE TABLE")
    
    print("\n--- Benefits ---")
    print("Why Catalyst matters:")
    print("  - Automatic optimization")
    print("  - No manual tuning needed")
    print("  - 10-100x performance improvement")
    print("  - Works for SQL and DataFrame API")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Simple query
    result = df.filter(col("group_id") > 50).groupBy("group_id").agg({"value": "sum"})
    print("Query: filter → groupBy → agg")
    print("Catalyst automatically optimizes this query")


def section_2_query_plan_analysis(spark):
    """
    Section 2: Query Plan Analysis
    Demonstrates how to analyze query plans
    """
    print("\n" + "=" * 80)
    print("SECTION 2: QUERY PLAN ANALYSIS")
    print("=" * 80)
    
    print("\n--- explain() Method ---")
    print("df.explain(): View physical plan")
    print("df.explain(extended=True): View logical and physical plans")
    print("df.explain(mode='cost'): View cost-based plan")
    
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print("\n--- Simple Query Plan ---")
    result = df.filter(col("group_id") > 50).groupBy("group_id").agg({"value": "sum"})
    
    print("\nPhysical Plan:")
    result.explain()
    
    print("\n--- Extended Plan ---")
    print("\nLogical and Physical Plan:")
    result.explain(extended=True)
    
    print("\n--- Reading the Plan ---")
    print("Key elements to look for:")
    print("  - Number of stages (fewer is better)")
    print("  - Join strategies (BroadcastHashJoin vs SortMergeJoin)")
    print("  - Filter pushdown (PushedFilters)")
    print("  - Scan operations (FileScan vs InMemoryTableScan)")
    
    print("\n--- Common Plan Patterns ---")
    print("Good patterns:")
    print("  - BroadcastHashJoin for small tables")
    print("  - PushedFilters in scan")
    print("  - Few stages")
    
    print("\nBad patterns:")
    print("  - SortMergeJoin for small tables")
    print("  - No pushed filters")
    print("  - Many stages")
    
    print("\n--- Identifying Bottlenecks ---")
    print("Look for:")
    print("  - Exchange (shuffle operations)")
    print("  - Sort operations")
    print("  - Scan without filters")
    print("  - Cartesian products")


def section_3_predicate_pushdown(spark):
    """
    Section 3: Predicate Pushdown
    Demonstrates predicate pushdown optimization
    """
    print("\n" + "=" * 80)
    print("SECTION 3: PREDICATE PUSHDOWN")
    print("=" * 80)
    
    print("\n--- What is Predicate Pushdown? ---")
    print("Predicate pushdown: Push filters to data source")
    print("  - Reduces data read from storage")
    print("  - Automatic for DataFrames")
    print("  - Works with Parquet, ORC, JDBC")
    
    print("\n--- Benefits ---")
    print("1. Reduced I/O: Read less data")
    print("2. Reduced network: Transfer less data")
    print("3. Reduced memory: Process less data")
    print("4. Faster queries: Less data to process")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 1001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Query with filter
    result = df.filter(col("group_id") > 50).select("id", "name", "value")
    
    print("\n--- Query Plan ---")
    result.explain()
    
    print("\nLook for 'PushedFilters' in the plan")
    print("This indicates predicate pushdown")
    
    print("\n--- Manual vs Automatic ---")
    print("Automatic (recommended):")
    print("  df.filter(col('age') > 25)")
    print("  Catalyst pushes filter to data source")
    
    print("\nManual (not recommended):")
    print("  df.select('*').filter(col('age') > 25)")
    print("  May prevent pushdown")
    
    print("\n--- When Pushdown Works ---")
    print("Works with:")
    print("  - Parquet files")
    print("  - ORC files")
    print("  - JDBC sources")
    print("  - Delta Lake")
    
    print("\n--- When Pushdown Doesn't Work ---")
    print("Doesn't work with:")
    print("  - UDFs in filter")
    print("  - Complex expressions")
    print("  - Some file formats")
    
    print("\n--- Checking Pushdown ---")
    print("Check query plan for:")
    print("  - PushedFilters: [isnotnull(group_id), (group_id > 50)]")
    print("  - If missing, pushdown not applied")


def section_4_column_pruning(spark):
    """
    Section 4: Column Pruning
    Demonstrates column pruning optimization
    """
    print("\n" + "=" * 80)
    print("SECTION 4: COLUMN PRUNING")
    print("=" * 80)
    
    print("\n--- What is Column Pruning? ---")
    print("Column pruning: Only read required columns")
    print("  - Reduces I/O and memory")
    print("  - Automatic for DataFrames")
    print("  - Catalyst analyzes column usage")
    
    print("\n--- Benefits ---")
    print("1. Reduced I/O: Read less data from disk")
    print("2. Reduced memory: Store less data in memory")
    print("3. Faster serialization: Less data to serialize")
    
    print("\n--- Example ---")
    # Create sample data with many columns
    data = [(i, f"user_{i}", i % 100, i * 100, f"email_{i}", f"phone_{i}") 
            for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value", "email", "phone"])
    
    print(f"\nDataFrame with {df.count()} rows and 6 columns")
    
    # Select only needed columns
    result = df.select("id", "name", "value").filter(col("group_id") > 50)
    
    print("\n--- Query Plan ---")
    result.explain()
    
    print("\nLook for column list in scan")
    print("Only selected columns are read")
    
    print("\n--- Best Practices ---")
    print("1. Select only needed columns:")
    print("   df.select('col1', 'col2') over df.select('*')")
    
    print("\n2. Avoid SELECT *:")
    print("   Bad: df.select('*')")
    print("   Good: df.select('col1', 'col2')")
    
    print("\n3. Chain operations:")
    print("   df.filter().select().groupBy()")
    print("   Catalyst can prune across operations")
    
    print("\n--- Column Pruning in Joins ---")
    print("Catalyst prunes columns before join:")
    print("  - Only join columns needed for result")
    print("  - Reduces shuffle data")
    print("  - Faster joins")
    
    print("\n--- Checking Pruning ---")
    print("Check query plan for:")
    print("  - Column list in scan operations")
    print("  - Only used columns should be present")


def section_5_constant_folding(spark):
    """
    Section 5: Constant Folding
    Demonstrates constant folding optimization
    """
    print("\n" + "=" * 80)
    print("SECTION 5: CONSTANT FOLDING")
    print("=" * 80)
    
    print("\n--- What is Constant Folding? ---")
    print("Constant folding: Compute constant expressions at compile time")
    print("  - Reduces runtime computation")
    print("  - Automatic optimization")
    print("  - Works for arithmetic and logical expressions")
    
    print("\n--- Examples ---")
    print("Before: WHERE col > 10 + 5")
    print("After:  WHERE col > 15")
    
    print("\nBefore: WHERE col = 100 / 10")
    print("After:  WHERE col = 10")
    
    print("\nBefore: WHERE col = 'a' || 'b'")
    print("After:  WHERE col = 'ab'")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i * 100) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Query with constant expression
    result = df.filter(col("value") > 1000 + 500)
    
    print("\n--- Query Plan ---")
    result.explain()
    
    print("\nCheck plan for folded constant")
    print("Expression should be pre-computed")
    
    print("\n--- When Constant Folding Works ---")
    print("Works for:")
    print("  - Arithmetic: +, -, *, /")
    print("  - Logical: AND, OR, NOT")
    print("  - String concatenation")
    print("  - Type casts")
    
    print("\n--- When Constant Folding Doesn't Work ---")
    print("Doesn't work for:")
    print("  - Expressions with columns")
    print("  - UDFs")
    print("  - Non-deterministic functions (rand(), now())")
    
    print("\n--- Benefits ---")
    print("1. Reduced computation: Constants computed once")
    print("2. Better optimization: Enables other optimizations")
    print("3. Faster execution: Less runtime work")


def section_6_operator_reordering(spark):
    """
    Section 6: Operator Reordering
    Demonstrates operator reordering optimization
    """
    print("\n" + "=" * 80)
    print("SECTION 6: OPERATOR REORDERING")
    print("=" * 80)
    
    print("\n--- What is Operator Reordering? ---")
    print("Operator reordering: Catalyst reorders operations for efficiency")
    print("  - Filters pushed before joins")
    print("  - Projections pushed after joins")
    print("  - Automatic optimization")
    
    print("\n--- Example: Filter Before Join ---")
    print("User writes:")
    print("  df1.join(df2, 'key').filter(df1.col > 10)")
    
    print("\nCatalyst rewrites to:")
    print("  df1.filter(col > 10).join(df2, 'key')")
    print("  (Filter pushed before join)")
    
    print("\n--- Example ---")
    # Create sample data
    data1 = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 101)]
    data2 = [(i, f"other_{i}", i % 50) for i in range(1, 51)]
    
    df1 = spark.createDataFrame(data1, ["id", "name", "group_id", "value"])
    df2 = spark.createDataFrame(data2, ["group_id", "other_name", "category"])
    
    print(f"\nDataFrame1: {df1.count()} rows")
    print(f"DataFrame2: {df2.count()} rows")
    
    # Join with filter after
    result = df1.join(df2, "group_id").filter(col("value") > 5000)
    
    print("\n--- Query Plan ---")
    result.explain()
    
    print("\nCheck if filter is pushed before join")
    print("Look for Filter in plan before Join")
    
    print("\n--- Projection Pushdown ---")
    print("Projections pushed after joins:")
    print("  - Only needed columns computed")
    print("  - Reduces intermediate data")
    
    print("\n--- Benefits ---")
    print("1. Reduced data: Less data shuffled")
    print("2. Faster joins: Smaller join inputs")
    print("3. Less memory: Less intermediate data")
    
    print("\n--- Checking Reordering ---")
    print("Check query plan for:")
    print("  - Filter before Join")
    print("  - Project after Join")
    print("  - Optimal operation order")


def section_7_join_reordering(spark):
    """
    Section 7: Join Reordering
    Demonstrates join reordering optimization
    """
    print("\n" + "=" * 80)
    print("SECTION 7: JOIN REORDERING")
    print("=" * 80)
    
    print("\n--- What is Join Reordering? ---")
    print("Join reordering: Catalyst reorders joins based on table sizes")
    print("  - Star schema detection")
    print("  - Broadcast joins for small tables")
    print("  - Sort-merge joins for large tables")
    
    print("\n--- Star Schema Detection ---")
    print("Star schema: Fact table joins multiple dimension tables")
    print("  - Fact table: Large, contains metrics")
    print("  - Dimension tables: Small, contain attributes")
    
    print("\nCatalyst optimizes:")
    print("  - Broadcast dimension tables")
    print("  - Join fact with dimensions efficiently")
    
    print("\n--- Example ---")
    # Create star schema
    fact_data = [(i, i % 10, i % 5, i * 100) for i in range(1, 1001)]
    dim1_data = [(i, f"dim1_{i}") for i in range(10)]
    dim2_data = [(i, f"dim2_{i}") for i in range(5)]
    
    fact_df = spark.createDataFrame(fact_data, ["id", "dim1_id", "dim2_id", "value"])
    dim1_df = spark.createDataFrame(dim1_data, ["dim1_id", "dim1_name"])
    dim2_df = spark.createDataFrame(dim2_data, ["dim2_id", "dim2_name"])
    
    print(f"\nFact table: {fact_df.count()} rows")
    print(f"Dimension1: {dim1_df.count()} rows")
    print(f"Dimension2: {dim2_df.count()} rows")
    
    # Star schema join
    result = fact_df.join(dim1_df, "dim1_id").join(dim2_df, "dim2_id")
    
    print("\n--- Query Plan ---")
    result.explain()
    
    print("\nLook for:")
    print("  - BroadcastHashJoin for dimension tables")
    print("  - SortMergeJoin for fact table")
    
    print("\n--- Join Strategies ---")
    strategies = [
        ("BroadcastHashJoin", "Small table broadcasted to all executors"),
        ("SortMergeJoin", "Large tables sorted and merged"),
        ("ShuffleHashJoin", "Hash-based shuffle join"),
    ]
    
    print("Join strategies:")
    for strategy, description in strategies:
        print(f"  {strategy}: {description}")
    
    print("\n--- Cost-Based Join Reordering ---")
    print("With CBO enabled:")
    print("  - Uses table statistics")
    print("  - Estimates join costs")
    print("  - Chooses optimal order")
    
    print("\n--- Manual Join Hints ---")
    print("Force join strategy:")
    print("  df1.join(broadcast(df2), 'key')  # Force broadcast")
    print("  df1.join(df2.hint('merge'), 'key')  # Force sort-merge")
    
    print("\n--- Benefits ---")
    print("1. Faster joins: Optimal join strategies")
    print("2. Less shuffle: Broadcast when possible")
    print("3. Better parallelism: Optimal join order")


def section_8_cost_based_optimization(spark):
    """
    Section 8: Cost-Based Optimization (CBO)
    Demonstrates cost-based optimization
    """
    print("\n" + "=" * 80)
    print("SECTION 8: COST-BASED OPTIMIZATION (CBO)")
    print("=" * 80)
    
    print("\n--- What is CBO? ---")
    print("Cost-Based Optimization: Uses statistics to estimate costs")
    print("  - Analyzes table statistics")
    print("  - Estimates cardinality")
    print("  - Chooses cheapest plan")
    
    print("\n--- Configuration ---")
    cbo_enabled = spark.conf.get("spark.sql.cbo.enabled")
    histogram_enabled = spark.conf.get("spark.sql.statistics.histogram.enabled")
    
    print(f"CBO Enabled: {cbo_enabled}")
    print(f"Histogram Enabled: {histogram_enabled}")
    
    print("\n--- Collecting Statistics ---")
    print("ANALYZE TABLE COMPUTE STATISTICS:")
    print("  - Collects table-level statistics")
    print("  - Row count, size in bytes")
    
    print("\nANALYZE TABLE COMPUTE STATISTICS FOR COLUMNS:")
    print("  - Collects column-level statistics")
    print("  - NDV (number of distinct values)")
    print("  - Null count, min/max values")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 1001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    # Save as table
    df.write.mode("overwrite").saveAsTable("users")
    
    print("\nTable 'users' created")
    
    # Collect statistics
    print("\n--- Collecting Statistics ---")
    spark.sql("ANALYZE TABLE users COMPUTE STATISTICS")
    print("Table statistics collected")
    
    spark.sql("ANALYZE TABLE users COMPUTE STATISTICS FOR COLUMNS id, group_id, value")
    print("Column statistics collected")
    
    # View statistics
    print("\n--- Viewing Statistics ---")
    stats = spark.sql("DESCRIBE EXTENDED users")
    print("Table statistics:")
    stats.show(truncate=False)
    
    print("\n--- When CBO Helps ---")
    print("CBO helps with:")
    print("  - Join reordering")
    print("  - Join strategy selection")
    print("  - Multi-way joins")
    print("  - Complex queries")
    
    print("\n--- When CBO Doesn't Help ---")
    print("CBO less useful for:")
    print("  - Simple queries")
    print("  - Small tables")
    print("  - Single-table operations")
    
    print("\n--- Best Practices ---")
    print("1. Enable CBO for production")
    print("2. Collect statistics regularly")
    print("3. Update statistics after data changes")
    print("4. Use for large, complex queries")
    
    # Cleanup
    spark.sql("DROP TABLE IF EXISTS users")


def section_9_adaptive_query_execution(spark):
    """
    Section 9: Adaptive Query Execution (AQE)
    Demonstrates AQE features
    """
    print("\n" + "=" * 80)
    print("SECTION 9: ADAPTIVE QUERY EXECUTION (AQE)")
    print("=" * 80)
    
    print("\n--- What is AQE? ---")
    print("Adaptive Query Execution: Optimizes at runtime based on statistics")
    print("  - Spark 3.0+ feature")
    print("  - Runtime optimization")
    print("  - Uses shuffle statistics")
    
    print("\n--- AQE Features ---")
    aqe_features = [
        ("Dynamic coalescing", "Merge shuffle partitions at runtime"),
        ("Skew join handling", "Handle skewed joins automatically"),
        ("Dynamic switch join strategies", "Switch join strategies based on size"),
    ]
    
    print("AQE features:")
    for feature, description in aqe_features:
        print(f"  {feature}: {description}")
    
    print("\n--- Configuration ---")
    aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
    coalesce_enabled = spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")
    skew_enabled = spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
    
    print(f"AQE Enabled: {aqe_enabled}")
    print(f"Dynamic Coalesce: {coalesce_enabled}")
    print(f"Skew Join: {skew_enabled}")
    
    print("\n--- Dynamic Coalescing ---")
    print("Problem: Too many small partitions")
    print("Solution: AQE merges partitions at runtime")
    print("Benefit: Reduces task overhead")
    
    print("\nConfiguration:")
    print("spark.sql.adaptive.coalescePartitions.enabled: true")
    print("spark.sql.adaptive.coalescePartitions.minPartitionNum: 1")
    
    print("\n--- Skew Join Handling ---")
    print("Problem: Skewed data causes stragglers")
    print("Solution: AQE splits skewed partitions")
    print("Benefit: Faster joins, no stragglers")
    
    print("\nConfiguration:")
    print("spark.sql.adaptive.skewJoin.enabled: true")
    print("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes: 256MB")
    
    print("\n--- Example ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    print(f"\nDataFrame with {df.count()} rows")
    
    # Query that benefits from AQE
    result = df.groupBy("group_id").agg({"value": "sum"})
    
    print("\n--- Query Plan ---")
    result.explain()
    
    print("\nLook for 'AQEShuffleRead' in plan")
    print("Indicates AQE is active")
    
    print("\n--- When to Use AQE ---")
    print("Use AQE when:")
    print("  - Data size is unpredictable")
    print("  - Skewed data is common")
    print("  - Partition count is uncertain")
    print("  - Running on Spark 3.0+")
    
    print("\n--- Benefits ---")
    print("1. Automatic tuning: No manual partition tuning")
    print("2. Better performance: Runtime optimization")
    print("3. Handles skew: Automatic skew handling")
    print("4. Reduced overhead: Fewer small tasks")


def section_10_subquery_optimization(spark):
    """
    Section 10: Subquery Optimization
    Demonstrates subquery optimization
    """
    print("\n" + "=" * 80)
    print("SECTION 10: SUBQUERY OPTIMIZATION")
    print("=" * 80)
    
    print("\n--- Subquery Types ---")
    subquery_types = [
        ("Scalar subquery", "Returns single value"),
        ("EXISTS subquery", "Checks existence"),
        ("IN subquery", "Checks membership"),
        ("Correlated subquery", "References outer query"),
    ]
    
    print("Subquery types:")
    for stype, description in subquery_types:
        print(f"  {stype}: {description}")
    
    print("\n--- Subquery Rewriting ---")
    print("Catalyst rewrites subqueries to joins:")
    print("  - WHERE col IN (SELECT ...) → JOIN")
    print("  - WHERE EXISTS (SELECT ...) → SEMI JOIN")
    print("  - Scalar subquery → Aggregation")
    
    print("\n--- Example: Scalar Subquery ---")
    # Create sample data
    data = [(i, f"user_{i}", i % 100, i * 100) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "value"])
    
    df.createOrReplaceTempView("users")
    
    print(f"\nView 'users' created with {df.count()} rows")
    
    # Scalar subquery
    query = """
    SELECT id, name, value,
           (SELECT AVG(value) FROM users) as avg_value
    FROM users
    WHERE value > (SELECT AVG(value) FROM users)
    """
    
    print("\n--- Query with Scalar Subquery ---")
    print(query)
    
    result = spark.sql(query)
    print(f"\nResult: {result.count()} rows")
    
    print("\n--- Query Plan ---")
    result.explain()
    
    print("\nCheck if subquery is rewritten")
    print("Look for rewritten plan structure")
    
    print("\n--- Example: IN Subquery ---")
    query_in = """
    SELECT * FROM users
    WHERE group_id IN (SELECT DISTINCT group_id FROM users WHERE value > 5000)
    """
    
    print("\n--- Query with IN Subquery ---")
    print(query_in)
    
    result_in = spark.sql(query_in)
    print(f"\nResult: {result_in.count()} rows")
    
    print("\n--- Query Plan ---")
    result_in.explain()
    
    print("\nCheck if IN is rewritten to SEMI JOIN")
    
    print("\n--- Correlated Subquery ---")
    query_correlated = """
    SELECT u1.id, u1.name, u1.value
    FROM users u1
    WHERE u1.value > (SELECT AVG(u2.value) FROM users u2 WHERE u2.group_id = u1.group_id)
    """
    
    print("\n--- Query with Correlated Subquery ---")
    print(query_correlated)
    
    result_correlated = spark.sql(query_correlated)
    print(f"\nResult: {result_correlated.count()} rows")
    
    print("\n--- Query Plan ---")
    result_correlated.explain()
    
    print("\n--- Optimization Tips ---")
    print("1. Prefer joins over subqueries when possible")
    print("2. Use EXISTS instead of IN for large datasets")
    print("3. Avoid deeply nested subqueries")
    print("4. Check query plan for rewriting")


def main():
    """
    Main function to run all demonstrations
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run all sections
        section_1_understanding_catalyst_optimizer(spark)
        section_2_query_plan_analysis(spark)
        section_3_predicate_pushdown(spark)
        section_4_column_pruning(spark)
        section_5_constant_folding(spark)
        section_6_operator_reordering(spark)
        section_7_join_reordering(spark)
        section_8_cost_based_optimization(spark)
        section_9_adaptive_query_execution(spark)
        section_10_subquery_optimization(spark)
        
        print("\n" + "=" * 80)
        print("ALL DEMONSTRATIONS COMPLETED")
        print("=" * 80)
        print("\nKey Takeaways:")
        print("1. Catalyst automatically optimizes queries")
        print("2. Use explain() to analyze query plans")
        print("3. Predicate pushdown reduces I/O")
        print("4. Column pruning reduces memory usage")
        print("5. Constant folding reduces computation")
        print("6. Operator reordering improves efficiency")
        print("7. Join reordering chooses optimal join strategies")
        print("8. CBO uses statistics for optimization")
        print("9. AQE optimizes at runtime (Spark 3.0+)")
        print("10. Subqueries are rewritten to joins")
        print("\nCheck Spark UI at http://localhost:4040 for query metrics")
        
    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()
