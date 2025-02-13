from pyspark.sql import SparkSession
from src.data_cleaning import clean_data
from src.transformations import apply_transformations
from src.aggregations import perform_aggregations
from src.write_data import write_to_redshift
from src.utils import read_config

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Simple ETL App") \
        .getOrCreate()

    # Load configuration
    config = read_config("config/config.yaml")
    conf = config['paths']

    # Run ETL steps
    clean_data(spark, conf['input'], conf['clean_data'])

    # write_to_redshift(spark, conf['s3_output_path'], conf['redshift_jdbc_url'], conf['redshift_table'], conf['user'])
    apply_transformations(spark, conf['clean_data'], conf['transformed_data'])

    # Perform Aggregation on Final results
    perform_aggregations(spark, conf['transformed_data'], conf['aggregated_data'])

    spark.stop()

if __name__ == "__main__":
    main()
