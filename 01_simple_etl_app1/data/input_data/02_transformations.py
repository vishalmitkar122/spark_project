from pyspark.sql import  SparkSession
from pyspark.sql.functions import col,when


# Initialize SparkSession

spark = SparkSession.builder \
    .appName('employeeDataTransformation') \
    .getOrCreate()

# read claned data

clean_data_path = "data/clean_data"
clean_data = spark.read.parquet(clean_data_path)

avg_salary_per_dept = clean_data.groupby('dept').agg(avg('salary').alias('avg_salary'))
transformed_data = clean_data.join(avg_salary_per_dept,on='dept',how='inner') \
    .withColumn('salary_category',when(col('salary'),'high').otherwise('low'))


avg_salary_per_dept.show()