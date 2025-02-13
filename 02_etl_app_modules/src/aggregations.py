import pyspark.sql.functions as F

def perform_aggregations(spark, input_path, output_path):
    """
    Performs aggregations on the transformed data.
    """
    # Read transformed data
    df = spark.read.parquet(input_path)

    # Example aggregation: Total sales by product
    aggregated_data = df.groupBy("dept").agg(
        F.sum("salary").alias("total_salary_expense"),
        F.count("id").alias("employee_count")
    )

    # Write aggregated data
    aggregated_data.write.mode('overwrite').parquet(output_path)
    print(f"Aggregations completed. Aggregated data saved to {output_path}")
