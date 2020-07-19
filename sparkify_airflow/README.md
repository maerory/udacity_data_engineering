# Data Pipelines with Airflow

## Objective
Using AWS Apache Airflow, design a data pile line that reads in JSON raw data of Sparkify and perform ETL based on our schema

## Structure

![dag](dag.png)

### Default Parameters
- The DAG does not depend on past runs
- The task is retried 3 times with 5 minutes interval on failure
- Catch up and emailing is turned off

### Operators
1. Create Table Operator: Execute all the create table queries
2. Stage to Redshift Operator: Loads any JSON formatted files from S3 to Amazon Redshift. Can distinguish between JSON file or CSV file and allows backfilling based on execution time.
3. Facts and Dimension Operators: Utilize SQL helper class to run appropriate data transformations. Dimension loads are truncate-insert pattern while fact tables are append-type.
4. Data Quality Operator: Check on the number of data that has been queried.

### execution
`/opt/airflow/start.sh` and make sure key and secret key is saved and uncommented
