import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Read songs JSON files from S3 and processes them with Spark
    
    Params:
        spark: Spark session connected using AWS
        input_data: Directory where to find the JSON input files
        output_data: Directory where to save parquet files
    """

    song_data = os.path.join(input_data + "song_data/*/*/*/*.json")

    df = spark.read.json(song_data)

    # Create the song table and save as parquet
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration'].dropDuplicates(["song_id"])
    songs_table.createOrReplaceTempView('songs')
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), mode='overwrite')

    # Create the artist table and save as parquet
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'].dropDuplicates(["artist_id"])
    artist_table.createOrReplaceTempView('artists') 
    artists_table.write.partiotionBy('artist_id').parquet(os.path.join(output_data, 'artists.parquet'),mode = "overwrite"))


def process_log_data(spark, input_data, output_data):
    """
    Read logs JSON files from S3 and processes them with Spark
    
    Params:
        spark: Spark session connected using AWS
        input_data: Directory where to find the JSON input files
        output_data: Directory where to save parquet files
    """

    log_data = input_data + "log_data/*/*/*.json"

    df = spark.read.json(log_data)
    df = df.filter(df.page == "NextSong")
    
    # Create the user table and save as parquet
    users_table = df.select("userId", "firstName", "lastName","gender","level").dropDuplicates(["userId"])
    users_table.createOrReplaceTempView('users')
    users_table.write.partitionBy("userId").parquet(os.path.join(output_data, 'users.parquet'), mode="overwrite")
    
    # use user-defined-function to organize the timestamp element
    get_timestamp = udf(lambda x: str(int(int(x)/1000.0)))
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    
    df = df.withColumn('timestamp', get_timestamp(df.ts)).withColumn("datetime", get_datetime(dfts))
    
    # Create the time table and save as parquet
    time_table = df.select(
        col("datetime").alias("start_time")
        hour("datetime").alias('hour')
        dayofmonth("datetime").alias("day"),
        weekofyear("datetime").alias("week"),
        month("datetime").alias("month"),
        year("datetime").alias("year")
    ).dropDuplicates(['start_time'])
    time_table.createOrReplaceTempView("time")
    time_table.write.partitionBy("year","month").parquet(os.path.join(output_data, "time.parquet"), mode="overwrite")
    
    # Load the song data to join with the log data
    song_data = os.path.join(input_data + "song_data/*/*/*/*.json")
    song_df = spark.read.json(song_data)
    
    # Inner join on the song and log data by the song's title
    df = df.join(song_df, song_df("title") == df("song"), "inner")
    
    # Collect and save the necessary data on songplays
    songplays_table = df.select(
        col('ts').alias('ts'),
        col('userId').alias('userId'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    ).withColumn('songplay_id', monotonically_increasing_id())
    songplays_table.createOrReplaceTempView('songplays')
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'songplays.parquet'), mode="overwrite")
    

def main():
    """
        Main function to run our ETL, assign input_data as the directory log_file and output_data as the directory to save our parquet files
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-project-out/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
