from pyspark.sql import  SparkSession
from pyspark.sql.functions import col , when, avg


# Initialize SparkSession

spark = SparkSession.builder \
    .appName('employeeDataTransformation') \
    .getOrCreate()

# read claned data

clean_data_path = r"C:\Users\Vishal\originalprojectpst\spark_project\spark_project\01_simple_etl_app1\data\input_data\employee_new.csv"
clean_data = spark.read.csv(clean_data_path,header=True,inferSchema=True)

avg_salary_per_dept = clean_data.groupby('dept').agg(avg('salary').alias('avg_salary'))
transformed_data = clean_data.join(avg_salary_per_dept, on='dept', how='inner').withColumn('salary_category',when(col('salary') > col('avg_salary'),'high').otherwise('low'))

transformed_data.show()
spark.stop()