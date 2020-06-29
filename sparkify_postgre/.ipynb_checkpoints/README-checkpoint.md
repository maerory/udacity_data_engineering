# Sparkify Song Analysis

### Objective
- Create ETL Pipeline for ingesting 'song_data' and 'log_data' in json format using POSTGRE SQL

### Installation
Required Software:
- Postgre SQL
- Python Libraries: Pandas, Numpy, Psycopg2, ipython-sql, JupyterLab (or Notebook)

### Quick Guide
- First, unzip the `data.tar.gz`
- Initialize the POSTGRE DB
    - After installing the postgre sql, you can access the postgre terminal using `psql postgre`
    - Create the student user with `CREATE ROLE student with LOGIN PASSWORD 'student'`
    - Create the database with `CREATE DATABASE sparkifydb`
    - Grant access by `GRANT ALL PRIVILEGES ON DATABASE sparkifydb TO student`
    - Check whether the IP address on your local postgre cocinces with the connection in th py files
    - Run `create_tables.py` to create tables
    - Run `elt.py` to process using the ETL pipeline

### Schema Design
- To adhere to the STAR schema, I created the songplays table as the fact table where it contains the aggregation of details of user, song and the artist
    - songplays: songplay_id, imtestamp, user_id, level, song_id, artist_id, session_id, location, user_agent
- Then, the I created dimension table for each specifics
    - users: contains user detail (user_id, first_name, last_name, gender, level)
    - songs: contains song detail (song_id, title, artist_id, year, duration)
    - artists: contain artist detail (artist_id, name, location, latitude, longitude)
    - time: breaks down the timestamp record of songplays (start_time, hour, day, week, month, year, weekday)
- The dimension table is divided into the important factors that data analyst would focus on. The time table is additionally created to calling separate function on the timestamp object on processing.
![schema](sparkify_schema.png)

### ETL Pipeline
- The data is processed from json file: song_data, log_data
- song_data json is information regarding the song details, this is used to record the infomation about each songs
- log_data json is broad detail about the playing, information about the time, user, song detail is recorded. These information is divided to fill in the users, artists, and time tables.