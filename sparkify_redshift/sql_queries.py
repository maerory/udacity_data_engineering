import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist    varchar(100) not null,
    firstName varchar(20) not null,
    lastName  varchar(20) not null,
    gender    char(1) not null,
    length    float,
    level     varchar(4) not null,
    location  varchar(150),
    page      varchar(50),
    sessionId integer not null,
    song      varchar(200),
    ts        timestamp nut null,
    userid    varchar(18) not null,
	userAgent varchar(400)
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
	num_songs        integer not null,
	artist_id       varchar(20) not null,
	artist_latitude  float,
	artist_longitude float,
	artist_location  varchar(200),
	artist_name      varchar(100),
	song_id          varchar(20) not null,
	title            varchar(300) not null,
	duration         float not null,
	year             smallint not null
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
	songplay_id     integer IDENTITY(0,1) not null primary key,
	start_time      timestamp not null sortkey references times (start_time),
	user_id         varchar(18) not null references users (user_id),
	level           varchar,
	song_id         varchar(18) not null distkey references songs (song_id),
	artist_id       varchar(18) not null references artists (artist_id),
	session_id      integer not null,
	location        varchar(200),
	user_agent      varchar(400)
);
""")

user_table_create = ("""
CREATE TABLE users (
	user_id         varchar(18) primary key sort key,
	first_name      varchar(15),
	last_name       varchar(15),
	gender          char(1),
	level           varchar(4) not null
);
""")

song_table_create = ("""
CREATE TABLE songs (
	song_id         varchar(180) not null primary key distkey,
	title           varchar(400) not null,
	artist_id       varchar(18) not null,
	year            smallint not null,
	duration        float not null
);
""")

artist_table_create = ("""
CREATE TABLE artists (
	artist_id       varchar(18) primary key distkey,
	name            varchar(200) not null,
	location        varchar(300),
	latitude        float,
	longitude       float
);
""")

time_table_create = ("""
CREATE TABLE time (
	start_time      timestamp primary key sortkey distkey,
	hour            integer not null,
	day             smallint not null,
	week            integer not null,
	month           integer not null,
	year            integer not null,
	weekday         boolean not null
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_event FROM {}
CREDENTIALS 'aws_iam_role={}'
GZIP REGION 'us-west-2'
FORMAT AS JSON {};
""").format(config["S3"]["log_data"], config["IAM_ROLE"]["ARN"], config["S3"]["log_jsonpath"])

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
GZIP REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config["S3"]["song_data"], config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, songs_id, aritst_id, session_id, location, user_agent)
SELECT
	TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time,
	e.userId as user_id,
	s.song_id as song_id,
	s.artist_id as artist_id,
	e.sessionId as session_id,
	e.location as location,
	e.userAgent as user_agent
FROM staging_events e
JOIN staging_songs s
ON e.song = s.title;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
	DISTINCT userId as user_id,
	firstName as first_name,
	lastName as last_name,
	gender,
	level
FROM staging_events;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
	DISTINCT song_id,
	title,
	artist_id,
	year,
	duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
	DISTINCT artist_id
	artist_name as name,
	artists_location as location,
	artist_latitude as latitude,
	artist_longitude as longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO times (start_time, hour, day, week, month, year, weekday)
SELECT
	TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
	EXTRACT(hour FROM start_time) as hour,
	EXTRACT(day FROM start_time) as day,
	EXTRACT(week FROM start_time) as week,
	EXTRACT(month FROM start_time) as month,
	EXTRACT(year FROM start_time) as year,
	CASE WHEN EXTRACT(dayofweek FROM start_time) IN (1,2,3,4,5) THEN true ELSE false END as weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
