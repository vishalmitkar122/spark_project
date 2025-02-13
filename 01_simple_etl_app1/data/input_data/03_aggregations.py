from pyspark.sql import SparkSession
from pyspark.sql.functions import col ,sum as _sum, count



# initialize sparksession


spark =SparkSession.builder \
    .appName("employeeDataAggregation")\
    .getOrCreate()


# read transformed data

final_out_path   =r"C:\Users\Vishal\originalprojectpst\spark_project\spark_project\01_simple_etl_app1\data\input_data\employee_new.csv"
transformed_data = spark.read.csv(final_out_path,header=True,inferSchema=True)

aggregated_data = transformed_data.groupby('dept').agg(_sum('salary').alias('total_salary_expense'),count('id').alias('employee_count'))


aggregated_data.show()
