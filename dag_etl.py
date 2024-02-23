from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.goodreads_plugin import DataQualityOperator
from airflow.operators.goodreads_plugin import LoadAnalyticsOperator
from helpers import AnalyticsQueries as AQ

# Define default arguments for the DAG
default_args = {
    'owner': 'goodreads',
    'depends_on_past': True,
    'start_date': datetime(2020, 2, 19, 0, 0, 0, 0),
    'end_date': datetime(2020, 2, 20, 0, 0, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': True
}

# Define the DAG name and its properties
dag_name = 'goodreads_data_pipeline'
dag = DAG(
    dag_name,
    default_args=default_args,
    description='Pipeline to load and transform data from landing zone to processed zone and populate data to goodreads warehouse.',
    schedule_interval='*/10 * * * *',  # Run every 10 minutes
    max_active_runs=1
)

# Define the beginning operator for the DAG
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# SSH Hook for EMR
emr_ssh_hook = SSHHook(ssh_conn_id='emr_ssh_connection')

# SSH Operator for executing ETL job
job_operator = SSHOperator(
    task_id="GoodReadsETLJob",
    command='cd /home/hadoop/goodreads_etl_pipeline/src;export PYSPARK_DRIVER_PYTHON=python3;export PYSPARK_PYTHON=python3;spark-submit --master yarn goodreads_driver.py;',
    ssh_hook=emr_ssh_hook,
    dag=dag)

# Data quality checks for warehouse
warehouse_data_quality_checks = DataQualityOperator(
    task_id='Warehouse_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["goodreads_warehouse.authors", "goodreads_warehouse.reviews", "goodreads_warehouse.books", "goodreads_warehouse.users"]
)

# Load analytics schema
create_analytics_schema = LoadAnalyticsOperator(
    task_id='Create_analytics_schema',
    redshift_conn_id='redshift',
    sql_query=[AQ.create_schema],
    dag=dag
)

# Define tasks for author analytics
create_author_analytics_table = LoadAnalyticsOperator(
    task_id='Create_author_analytics_table',
    redshift_conn_id='redshift',
    sql_query=[AQ.create_author_reviews, AQ.create_author_rating, AQ.create_best_authors],
    dag=dag
)

# Define tasks for book analytics
create_book_analytics_table = LoadAnalyticsOperator(
    task_id='Create_book_analytics_table',
    redshift_conn_id='redshift',
    sql_query=[AQ.create_book_reviews, AQ.create_book_rating, AQ.create_best_books],
    dag=dag
)

# Define tasks for loading author analytics data
load_author_table_reviews = LoadAnalyticsOperator(
    task_id='Load_author_table_reviews',
    redshift_conn_id='redshift',
    sql_query=[AQ.populate_authors_reviews.format('2020-02-01 00:00:00.000000', '2020-02-28 00:00:00.000000')],
    dag=dag
)

load_author_table_ratings = LoadAnalyticsOperator(
    task_id='Load_author_table_ratings',
    redshift_conn_id='redshift',
    sql_query=[AQ.populate_authors_ratings.format('2020-02-01 00:00:00.000000', '2020-02-28 00:00:00.000000')],
    dag=dag
)

load_best_author = LoadAnalyticsOperator(
    task_id='Load_best_author',
    redshift_conn_id='redshift',
    sql_query=[AQ.populate_best_authors],
    dag=dag
)

# Define tasks for loading book analytics data
load_book_table_reviews = LoadAnalyticsOperator(
    task_id='Load_book_table_reviews',
    redshift_conn_id='redshift',
    sql_query=[AQ.populate_books_reviews.format('2020-02-01 00:00:00.000000', '2020-02-28 00:00:00.000000')],
    dag=dag
)

load_book_table_ratings = LoadAnalyticsOperator(
    task_id='Load_book_table_ratings',
    redshift_conn_id='redshift',
    sql_query=[AQ.populate_books_ratings.format('2020-02-01 00:00:00.000000', '2020-02-28 00:00:00.000000')],
    dag=dag
)

load_best_book = LoadAnalyticsOperator(
    task_id='Load_best_books',
    redshift_conn_id='redshift',
    sql_query=[AQ.populate_best_books],
    dag=dag
)

# Data quality checks for authors and books
authors_data_quality_checks = DataQualityOperator(
    task_id='Authors_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["goodreads_analytics.popular_authors_average_rating", "goodreads_analytics.popular_authors_average_rating"]
)

books_data_quality_checks = DataQualityOperator(
    task_id='Books_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["goodreads_analytics.popular_books_average_rating", "goodreads_analytics.popular_books_review_count"]
)

# Define the end operator for the DAG
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Define the task dependencies
start_operator >> job_operator >> warehouse_data_quality_checks >> create_analytics_schema
create_analytics_schema >> [create_author_analytics_table, create_book_analytics_table]
create_author_analytics_table >> [load_author_table_reviews, load_author_table_ratings, load_best_author] >> authors_data_quality_checks
create_book_analytics_table >> [load_book_table_reviews, load_book_table_ratings, load_best_book] >> books_data_quality_checks
[authors_data_quality_checks, books_data_quality_checks] >> end_operator
