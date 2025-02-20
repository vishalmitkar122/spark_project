from pyspark.sql import SparkSession
from pyspark.sql.functions import trim


# initialize sparksession

spark = SparkSession.builder \
    .appName('EmployeeDataCleaning') \
    .getOrCreate()


# Read raw data

input_path = r"C:\Users\Vishal\originalprojectpst\spark_project\spark_project\01_simple_etl_app1\data\input_data\employee_new.csv"

raw_data = spark.read.csv(input_path,header=True,inferSchema=True)

# data cleaning

cleaned_data = raw_data.drop_duplicates() \
    .withColumn("first_name",trim(raw_data["first_name"]))\
    .withColumn("last_name",trim(raw_data['last_name'])) \
    .withColumn('email',trim(raw_data['email'])) \
    .withColumn('dept',trim(raw_data['dept']))

cleaned_data.show()

# cleaned_data_path = "data/clean_data/"
#
# cleaned_data.write.mode('overwrite').parquet(cleaned_data_path)

spark.stop()