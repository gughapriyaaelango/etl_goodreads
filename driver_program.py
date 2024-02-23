from pyspark.sql import SparkSession
from goodreads_transform import GoodreadsTransform
from s3_module import GoodReadsS3Module
from pathlib import Path
import logging
import logging.config
import configparser
from warehouse.goodreads_warehouse_driver import GoodReadsWarehouseDriver
import time

# Load configurations from config file
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

# Configure logging
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Initialize a Spark session
    """
    return SparkSession.builder.master('yarn').appName("goodreads") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
        .enableHiveSupport().getOrCreate()

def main():
    """
    Main entry point of the program
    """
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_spark_session()
    goodreads_transformer = GoodreadsTransform(spark)

    # Define transformation methods for each file
    transformation_methods = {
        "author.csv": goodreads_transformer.transform_author_dataset,
        "book.csv": goodreads_transformer.transform_books_dataset,
        "reviews.csv": goodreads_transformer.transform_reviews_dataset,
        "user.csv": goodreads_transformer.transform_users_dataset
    }

    logging.debug("\n\nCopying data from S3 landing zone...")
    s3_module = GoodReadsS3Module()
    s3_module.s3_move_data(source_bucket=config.get('BUCKET', 'LANDING_ZONE'), target_bucket=config.get('BUCKET', 'WORKING_ZONE'))

    files_in_working_zone = s3_module.get_files(config.get('BUCKET', 'WORKING_ZONE'))

    # Clean up processed zone if files are available in working zone
    if len(set(transformation_methods.keys()) & set(files_in_working_zone)) > 0:
        logging.info("Cleaning up processed zone.")
        s3_module.clean_bucket(config.get('BUCKET', 'PROCESSED_ZONE'))

    # Apply transformations for each file in the working zone
    for file in files_in_working_zone:
        if file in transformation_methods:
            transformation_methods[file]()

    logging.debug("Waiting before setting up Warehouse")
    time.sleep(5)

    # Start warehouse functionality
    warehouse_driver = GoodReadsWarehouseDriver()
    logging.debug("Setting up staging tables")
    warehouse_driver.setup_staging_tables()
    logging.debug("Populating staging tables")
    warehouse_driver.load_staging_tables()
    logging.debug("Setting up Warehouse tables")
    warehouse_driver.setup_warehouse_tables()
    logging.debug("Performing UPSERT")
    warehouse_driver.perform_upsert()

# Entry point for the pipeline
if __name__ == "__main__":
    main()
