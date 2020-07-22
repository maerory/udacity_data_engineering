import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_cdc_table_drop = "DROP TABLE IF EXISTS staging_cdc_covid;"
nyt_table_drop = "DROP TABLE IF EXISTS nyt_covid;"
mobility_table_drop = "DROP TABLE IF EXISTS mobility;"

cdc_hospital_table_drop = "DROP TABLE IF EXISTS cdc_hospital;"
cdc_testing_table_drop = "DROP TABLE IF EXISTS cdc_testing;"
state_abb_table_drop = "DROP TABLE IF EXISTS state_abb;"

# CREATE TABLES
staging_cdc_table_create= ("""
CREATE TABLE staging_cdc_covid (
    recorded        date not null,
    state       char(2) not null,
    positive    bigint not null,
    negative    bigint not null,
    hospitalizedCurrently  int,
    hospitalizedCumulative int,
    inIcuCurrently         int,
    inIcuCumulative        int,
    onVentilatorCurrently  int,
    onVentilatorCumulative int,
    recovered              int,
    deathConfirmed         int,
    deathProbable          int,
    dataQualityGrade       char(2)
);
""")

nyt_table_create = ("""
CREATE TABLE staging_songs (
	recorded       date not null,
    state          char(2) not null,
    cases          int not null,
    deaths         int not null,
    primary key(recorded, state)
);
""")

mobility_table_create = ("""
CREATE TABLE mobility (
	recorded       date not null,
    state          char(2) not null,
    county         varchar(20),
    retail_recreation   float,
    grocery_pharmacy    float,
    parks               float,
    transit             float,
    workplaces          float,
    residential         float,
    primary key(recorded, state, county)
);
""")

cdc_hospital_table_create = ("""
CREATE TABLE cdc_hospital (
	recorded        date not null,
    state       char(2) not null,
    positive    bigint not null,
    negative    bigint not null,
    dataQualityGrade       char(2) not null,
    primary key (recorded, state)
);
""")

cdc_testing_table_create = ("""
CREATE TABLE cdc_testing (
	recorded        date not null,
    state       char(2) not null,
    hospitalizedCurrently  int,
    hospitalizedCumulative int,
    inIcuCurrently         int,
    inIcuCumulative        int,
    onVentilatorCurrently  int,
    onVentilatorCumulative int,
    recovered              int,
    deathConfirmed         int,
    deathProbable          int,
    primary key (recorded, state)
);
""")

state_abb_table_create = ("""
CREATE TABLE state_abb (
	state   varchar(10),
    code    char(2),
    primary key(state)
);
""")

# STAGING TABLES

staging_cdc_covid_copy = ("""
COPY staging_cdc_covid FROM {}
CREDENTIALS 'aws_iam_role={}'
GZIP REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config["S3"]["cdc_data"], config["IAM_ROLE"]["ARN"])

mobility_copy = ("""
COPY mobility FROM {}
CREDENTIALS 'aws_iam_role={}'
GZIP REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config["S3"]["mobility_data"], config["IAM_ROLE"]["ARN"])

nyt_covid_copy = ("""
COPY nyt_covid FROM {}
CREDENTIALS 'aws_iam_role={}'
GZIP REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config["S3"]["nyt_data"], config["IAM_ROLE"]["ARN"])

state_abb_covid_copy = ("""
COPY state_abb FROM {}
CREDENTIALS 'aws_iam_role={}'
GZIP REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config["S3"]["state_abbreviations"], config["IAM_ROLE"]["ARN"])


cdc_testing_table_insert = ("""
INSERT INTO cdc_testing (date, state, positive, negative, dataQuality)
SELECT
	s.date as date,
    s.state as state,
    s.positive as positive,
    s.negative as negative,
    s.dataQuality as dataQuality
FROM staging_cdc_covid s
""")

cdc_hospital_table_insert = ("""
INSERT INTO cdc_hospital (date, state, hospitalizedCurrently,
 hospitalizedCumulative, inIcuCurrently, inIcuCumulative,
 onVentilatorCurrently, onVentilatorCumulative, recovered,
 deathConfirmed, deathProbable)
SELECT
	s.date as date,
    s.state as state,
    s.hospitalizedCurrently as hospitalizedCurrently,
    s.hospitalizedCumulative as hospitalizedCumulative,
    s.inIcuCurrently as inIcuCurrently,
    s.inIcuCumulative as inIcuCumulative,
    s.onVentilatorCurrently  as onVentilatorCurrently
    s.onVentilatorCumulative as onVentilatorCumulative
    s.recovered as recovered
    s.deathConfirmed as deathConfirmed,
    s.deathProbable as deathProbable,
FROM staging_cdc_covid s
""")

# QUERY LISTS

create_table_queries = [staging_cdc_table_create, nyt_table_create,
mobility_table_create, cdc_hospital_table_create, cdc_testing_table_create,
state_abb_table_create]

drop_table_queries = [staging_cdc_table_drop, nyt_table_drop,
 mobility_table_drop, cdc_hospital_table_drop, cdc_testing_table_drop,
 state_abb_table_drop]

copy_table_queries = [staging_cdc_covid_copy, mobility_copy, nyt_covid_copy, state_abb_covid_copy]
insert_table_queries = [cdc_hospital_table_insert, cdc_testing_table_insert]
