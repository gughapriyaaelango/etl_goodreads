from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import goodreads_udf
import logging
import configparser
from pathlib import Path

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

class GoodreadsDataTransformer:
    """
    This class handles the transformation operations on the dataset.
    It includes methods to transform author, reviews, books, and users datasets.
    """

    def __init__(self, spark):
        self._spark = spark
        self._load_path = 's3a://' + config.get('BUCKET', 'WORKING_ZONE')
        self._save_path = 's3a://' + config.get('BUCKET', 'PROCESSED_ZONE')

    def transform_author_dataset(self):
        """
        Transforms the author dataset
        """
        logging.debug("Transforming author dataset...")
        author_df = self._spark.read.csv(
            self._load_path + '/author.csv', header=True, mode='PERMISSIVE', inferSchema=True)

        author_lookup_df = author_df.groupBy('author_id').agg(
            F.max('record_create_timestamp').alias('record_create_timestamp'))

        author_lookup_df.persist()
        F.broadcast(author_lookup_df)

        deduped_author_df = author_df.join(
            author_lookup_df, ['author_id', 'record_create_timestamp'], how='inner').select(author_df.columns)\
            .withColumn('name', goodreads_udf.remove_extra_spaces('name'))

        logging.debug(f"Writing data to {self._save_path + '/authors/'}")
        deduped_author_df.repartition(10).write.csv(path=self._save_path + '/authors/', sep='|', mode='overwrite',
                                                    compression='gzip', header=True, timestampFormat='yyyy-MM-dd HH:mm:ss.SSS',
                                                    quote='"', escape='"')

    def transform_reviews_dataset(self):
        """
        Transforms the reviews dataset
        """
        logging.debug("Transforming reviews dataset...")
        reviews_df = self._spark.read.csv(self._load_path + '/reviews.csv', header=True,
                                          mode='PERMISSIVE', inferSchema=True, quote="\"", escape="\"")

        reviews_lookup_df = reviews_df.groupBy('review_id').agg(
            F.max('record_create_timestamp').alias('record_create_timestamp'))

        reviews_lookup_df.persist()
        F.broadcast(reviews_lookup_df)

        deduped_reviews_df = reviews_df.join(
            reviews_lookup_df, ['review_id', 'record_create_timestamp'], how='inner').select(reviews_df.columns)

        deduped_reviews_df = deduped_reviews_df\
            .withColumn('review_added_date', goodreads_udf.stringtodatetime('review_added_date')) \
            .withColumn('review_updated_date', goodreads_udf.stringtodatetime('review_updated_date'))

        logging.debug(f"Writing data to {self._save_path + '/reviews/'}")
        deduped_reviews_df.repartition(10).write.csv(path=self._save_path + '/reviews/', sep='|', mode='overwrite',
                                                      compression='gzip', header=True, timestampFormat='yyyy-MM-dd HH:mm:ss.SSS',
                                                      quote='"', escape='"')

    def transform_books_dataset(self):
        """
        Transforms the books dataset
        """
        logging.debug("Transforming books dataset...")
        books_df = self._spark.read.csv(self._load_path + '/book.csv', header=True, mode='PERMISSIVE',
                                        inferSchema=True, quote="\"", escape="\"")

        books_lookup_df = books_df.groupBy('book_id').agg(
            F.max('record_create_timestamp').alias('record_create_timestamp'))
        books_lookup_df.persist()
        F.broadcast(books_lookup_df)

        deduped_books_df = books_df.join(
            books_lookup_df, ['book_id', 'record_create_timestamp'], how='inner').select(books_df.columns)

        logging.debug(f"Writing data to {self._save_path + '/books/'}")
        deduped_books_df.repartition(10).write.csv(path=self._save_path + '/books/', sep='|', mode='overwrite',
                                                    compression='gzip', header=True, timestampFormat='yyyy-MM-dd HH:mm:ss.SSS',
                                                    quote='"', escape='"')

    def transform_users_dataset(self):
        """
        Transforms the users dataset
        """
        logging.debug("Transforming users dataset...")
        users_df = self._spark.read.csv(self._load_path + '/user.csv', header=True, mode='PERMISSIVE',
                                        inferSchema=True, quote="\"", escape="\"")

        users_lookup_df = users_df.groupBy('user_id').agg(
            F.max('record_create_timestamp').alias('record_create_timestamp'))

        users_lookup_df.persist()
        F.broadcast(users_lookup_df)

        deduped_users_df = users_df.join(
            users_lookup_df, ['user_id', 'record_create_timestamp'], how='inner').select(users_df.columns)

        logging.debug(f"Writing data to {self._save_path + '/users/'}")
        deduped_users_df.repartition(10).write.csv(path=self._save_path + '/users/', sep='|', mode='overwrite',
                                                    compression='gzip', header=True, timestampFormat='yyyy-MM-dd HH:mm:ss.SSS',
                                                    quote='"', escape='"')
