from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,
                                LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTableOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'joeyyoo',
    'start_date': datetime(2019, 1, 12),
    's3_bucket': "udacity-dend",
    'event_filename': "log_data",
    'song_filename': "song_data",
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_sparkify_airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table = CreateTableOperator(
    task_id = "Create_tables",
    redshift_conn_id = "redshift",
    dag = dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table= "event",
    s3_bucket=default_args["s3_bucket"],
    format="JSON",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table= "song",
    s3_bucket=default_args["s3_bucket"],
    format= "JSON"
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = "redshift",
    sql_query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id = "redshift",
    sql_query=SqlQueries.user_table_insert,
    table="users",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = "redshift",
    sql_query=SqlQueries.song_table_insert,
    table="songs",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = "redshift",
    sql_query=SqlQueries.artist_table_insert,
    table="artists",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = "redshift",
    sql_query=SqlQueries.time_table_insert,
    table="time",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables=["songplays","users","songs","artists","time"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# define graph relationships
start_operator >> create_table
create_table >> [stage_events_to_redshift, stage_songs_to_redshift] \
 >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
  >> run_quality_checks
  run_quality_checks >> end_operator
