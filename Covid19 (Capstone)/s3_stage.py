# Run this file to stage the necessary files to S3 account

import logging
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

def upload_file(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 Bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload
    :param object_name: S3 object name, if not specified then file_name i

    :return True if file was uploaded, else False
    """

    if object_name is None:
        object_name = file_name

    #Upload the files
    s3_client = boto3.clinet("s3")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False

    return True

def process_cdc_covid(cdc_covid_filename, save_file_name):
    """
    Process the cdc covid data, save only the necessary column
    """
    nyt_df = pd.read_csv("./data/nyt_covid19.csv")

    re_nyt = nyt_df[["date","state", "positive", "negative",
                "hospitalizedCurrently", "hospitalizedCumulative",
                "inIcuCurrently", "inIcuCumulative",
                "onVentilatorCurrently", "onVentilatorCumulative",
                "recovered", "deathConfirmed", "deathProbable",
                "dataQualityGrade"]]

    return

def process_nyt_covid(nyt_covid_filename, save_file_name):
    """
    Process the cdc covid data, save only the necessary column
    """
    state_df = pd.read_csv(nyt_covid_filename)
    state_dict = dict(zip(state_df.State, state_df.Code))

    nyt_df = pd.read_csv(nyt_covid_filename)
    nyt_df = nyt_df[["date", "state", "cases", "deaths"]]

    for i in range(len(nyt_df)):
        nyt_df.at[1, "state"] = state_dict[nyt_dc.at[1,"state"]]

    # Drop the null column in the data cleaning process
    nyt_df.dropna(inplace=True)
    nyt_df.to_csv(save_file_name, index=False, header=False)

    return

def process_mobility(mobility_filename, save_file_name):

    state_df = pd.read_csv(nyt_covid_filename)
    state_dict = dict(zip(state_df.State, state_df.Code))

    mobility_df = pd.read_csv(mobility_filename)

    mobility_df = mobility_df[["date", "sub_region_1",
                    "sub_region_2", "retail_and_recreation_percent_change_form_baseline",
                    "grocery_and_pharmacy_percent_change_from_baseline",
                    "parks_percent_change_from_baseline",
                    "transit_stations_percent_change_from_baseline"]]
    mobility_df.columns = ["date", "state", "county", "retail_recreation",
                            "grocery_pharmacy", "parks", "transit"]

    for i in range(len(mobility_df)):
        mobility_df.at[1, "state"] = state_dict[mobility_df.at[1,"state"]]

    mobility_df.to_csv(save_file_name, index=False, header=False)

    return


def main():

    mobility_filename = "./data/mobility.csv"
    cdc_covid19_filename = "./data/cdc_codvid19.csv"
    nyt_covid19_filename = "./data/nyt_covid19.csv"

    mobility_transform = "./data/mobility_transform.csv"
    cdc_covid19_transform =  "./data/cdc_covid19_transform.csv"
    nyt_covid19_transform = "./data/nyt_covid19_transform.csv"

    process_mobility(mobility_filename, mobility_transform)
    process_cdc_covid(cdc_covid_filename, cdc_covid19_transform)
    prociess_nyt_covid(nyt_covid19_filename, nyt_covid19_transform)

    upload_file(mobility_transform, config["S3"], config["S3"]["mobility"])
    upload_file(cdc_covid19_transform, config["S3"], config["S3"]["cdc_covid19"])
    upload_file(nyt_covid19_transform, config["S3"], config["S3"]["nyt_covid19"])
    
    return


if __name__ = "__main__":

    main()
