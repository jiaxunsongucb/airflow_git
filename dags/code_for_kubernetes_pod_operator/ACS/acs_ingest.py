from airflow.macros.roofstock_plugin import run_with_logging, connect_to_s3, connect_to_snowflake, pod_xcom_push, pod_xcom_pull
from airflow.models import Variable
import pandas as pd
import requests
import zipfile
import io
from datetime import datetime
import numpy as np
import imaplib
import email
import email.utils
import time


from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.macros.roofstock_plugin import send_message, pd_read_s3, check_key_exists_S3
from airflow.operators.roofstock_plugin import BashMessageOperator
import boto3
from datetime import datetime, timedelta
from ftplib import FTP
import io
import os
import pandas as pd
import re
import subprocess
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, INTEGER, SMALLINT, NUMERIC
import time
import zipfile


###########################################################
# Global helper functions
###########################################################
def _ftp_traverse(ftp, callable, logger):
    entry_list = ftp.nlst()

    for entry in entry_list:
        try:  # the entry is a directory
            ftp.cwd(entry)
            logger.info("Traversing FTP directory - {}...".format(ftp.pwd()))

            # recursively call the function itself
            _ftp_traverse(ftp, callable, logger)

            # backtrack
            ftp.cwd("..")

        except:  # the entry is a file, call callable function
            callable(ftp, entry)


def _connect_ftp(host, attempt, logger=None):
    i = 0
    for _ in range(attempt):
        try:
            ftp = FTP(host, timeout=15)
            ftp.login()
            return ftp
        except Exception as e:
            if logger:
                logger.warn("FTP connection failed {} time(s)!".format(i + 1))
                logger.error(e)
            i += 1

    if i == attempt:
        raise Exception("FTP connection failed!")


###########################################################
# FTP to S3
###########################################################
@run_with_logging()
def sequence_FTP_to_S3(logger, **kwargs):
    """
    Given the year, tansfer all the ACS summary files (sequence files) from FTP to S3
    e.g.
    year = 2016
    transfer all the data from ftp2.census.gov/programs-surveys/acs/summary_file/2016/data/5_year_seq_by_state/
    to S3 roofstock-data-lake/RawData/ACS/summary_file/2016/data/5_year_seq_by_state/
    """
    # Initialize variables
    year = kwargs["year"]
    state = kwargs["state"]

    def _unzip(s3_path, myfile):
        logger.info(f"Exctrating zip file...")
        try:
            # Read the file as a zipfile and process the members
            with zipfile.ZipFile(myfile, mode='r') as zipf:
                for file in zipf.infolist():
                    fileName = file.filename
                    key_path = "/".join(s3_path.split("/")[:-1] + [fileName])
                    logger.info(f"Uploading {key_path}...")
                    mybucket.put_object(Key=key_path, Body=zipf.read(file))
            logger.info("Successfully extracted file!")
        except Exception as e:
            logger.error(e)
            logger.warn(
                "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                    key, bucket))

    def _single_file_FTP_to_S3(ftp, entry):
        """
        callable funcation for _ftp_traverse
        copy single file from FTP to S3 without any judgment
        """
        file_path = ftp.pwd() + "/" + entry
        s3_path = file_path.replace("/programs-surveys/acs", "RawData/ACS")

        # check if the file/key already existed on S3
        if check_key_exists_S3(s3_resource, bucket, s3_path):
            logger.warn("Key {} already existed on S3, skipped!".format(s3_path))

        # if the file/key does not exist, upload file onto S3
        else:
            logger.info("Downloading data from FTP - {}...".format(file_path))
            try:
                # try to download file from FTP
                myfile = io.BytesIO()
                filename = "RETR {}".format(entry)
                resp = ftp.retrbinary(filename, myfile.write)
                myfile.seek(0)

                # upload file onto S3
                logger.info("Uploading data to S3 - {}...".format(s3_path))
                mybucket.put_object(Key=s3_path, Body=myfile)

                # unzip the file and upload it onto S3
                if s3_path.endswith(".zip"):
                    _unzip(s3_path, myfile)

                # check if the file has been uploaded
                if check_key_exists_S3(s3_resource, bucket, s3_path):
                    logger.info(f"Successfully uploaded {s3_path} to S3!")
                else:
                    with open("/home/jsong/failed_files.txt", "a") as f:
                        f.write(f"Failed to upload {s3_path} to S3!\n")
                    logger.warn(f"Failed to upload {s3_path} to S3!")
            except:
                logger.warn("Failed to download {} from FTP!".format(file_path))

    # set up S3
    s3_resource = boto3.resource("s3")
    bucket = "roofstock-data-lake"
    mybucket = s3_resource.Bucket(bucket)

    # connect FTP
    host = "ftp2.census.gov"
    ftp = _connect_ftp(host, 3, logger)

    # change the work directory
    ftp.cwd(f"/programs-surveys/acs/summary_file/{year}/data/5_year_seq_by_state/{state}/")
    # traverse the directory
    _ftp_traverse(ftp, _single_file_FTP_to_S3, logger)

    ftp.quit()


@run_with_logging()
def template_FTP_to_S3(logger, **kwargs):
    """
    Given the year, transfer all the template files from FTP to S3
    e.g.
    year = 2016
    Go to ftp2.census.gov/programs-surveys/acs/summary_file/2016/data/ to search for the template files,
    which is 2016_5yr_Summary_FileTemplates.zip,
    and then transfer them from FTP to S3.
    Template files are zip files, they will be extracted on Airflow VM and then uploaded to S3.
    """

    # Initialize variables
    year = kwargs["year"]

    # set up S3
    s3_resource = boto3.resource("s3")
    s3_client = boto3.client("s3")
    bucket = "roofstock-data-lake"
    mybucket = s3_resource.Bucket(bucket)
    s3_path = f"RawData/ACS/summary_file/{year}/data/5yr_templates/"

    # connect FTP
    host = "ftp2.census.gov"
    ftp = _connect_ftp(host, 3, logger)

    # change the work directory
    ftp.cwd(f"/programs-surveys/acs/summary_file/{year}/data/")

    entry_list = ftp.nlst()
    for entry in entry_list:
        if re.search(fr"_5yr_Summary_?FileTemplates.zip", entry):

            file_path = ftp.pwd() + "/" + entry
            logger.info("Downloading data from FTP - {}...".format(file_path))
            try:
                # try to download file from FTP
                myfile = io.BytesIO()
                filename = "RETR {}".format(entry)
                resp = ftp.retrbinary(filename, myfile.write)
                myfile.seek(0)
            except:
                logger.warn("Failed to download {} from FTP!".format(file_path))

            # unzip file
            logger.info("Exctrating file - {}".format(entry))
            try:
                # Read the file as a zipfile and process the members
                with zipfile.ZipFile(myfile, mode='r') as zipf:
                    for file in zipf.infolist():
                        file_name = file.filename
                        if file_name[-1] != "/":
                            file_name = file_name.split("/")[-1]

                            # check if the file/key already existed on S3
                            if check_key_exists_S3(s3_resource, bucket, s3_path + file_name):
                                logger.warn("Key {} already existed on S3, skipped!".format(s3_path + file_name))

                            else:
                                # upload file onto S3
                                logger.info("Uploading data to S3 - {}...".format(file_name))
                                mybucket.put_object(Key=s3_path + file_name, Body=zipf.read(file))
                logger.info("Successfully extracted file!")

            except Exception as e:
                logger.error(e)
                logger.warn(
                    "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                        key, bucket))

    ftp.quit()


@run_with_logging()
def docs_FTP_to_S3(logger, **kwargs):
    """
    Given the year, tansfer all the docs files from FTP to S3
    e.g.
    year = 2016
    Go to ftp2.census.gov/programs-surveys/programs-surveys/acs/summary_file/2016/documentation/ to search for the docs files,
    which are:
    -- sequence table number lookup file: ../user_tools/ACS_5yr_Seq_Table_Number_Lookup.xls
    -- table shells: ../user_tools/ACS2016_Table_Shells.xlsx
    -- appendices: ../tech_docs/ACS_2016_SF_5YR_Appendices.xls
    -- geo info lookup file: ../geography/5_year_Mini_Geo.xlsx
    and then transfer them from FTP to S3.
    ** Note: these files may have different naming patterns and living in dfferent folders over years,
    so here a recursively research and re matching is applied.
    """
    # Initialize variables
    year = kwargs["year"]

    def _single_file_FTP_to_S3(ftp, entry):
        """
        Check whether the file is a doc file (seq table number lookup file, table shells, mini geo files, appendices).
        If it is, then transfer it from FTP to S3.
        """
        file_path = ftp.pwd() + "/" + entry
        suffix = entry.split(".")[-1]
        s3_path = ""

        if (("5_year" in file_path) and entry == "Sequence_Number_and_Table_Number_Lookup.xls") or (
                entry == "ACS_5yr_Seq_Table_Number_Lookup.xls"):
            s3_path = f"RawData/ACS/summary_file/{year}/ACS_{year}_5yr_Seq_Table_Number_Lookup.{suffix}"
        elif (("5_year" in file_path) and ("5-Year_TableShells.xls" in entry)) or (
                ("year" not in file_path) and ("Shells.xls" in entry)):
            s3_path = f"RawData/ACS/summary_file/{year}/ACS_{year}_5yr_Table_Shells.{suffix}"
        elif "SF_5YR_Appendices.xls" in entry:
            s3_path = f"RawData/ACS/summary_file/{year}/ACS_{year}_5yr_Appendices.{suffix}"
        elif entry == "5_year_Mini_Geo.xlsx":
            s3_path = f"RawData/ACS/summary_file/{year}/ACS_{year}_5yr_Mini_Geo.xlsx"

        if s3_path:
            # check if the file/key already existed on S3
            if check_key_exists_S3(s3_resource, bucket, s3_path):
                logger.warn("Key {} already existed on S3, skipped!".format(s3_path))

            # if the file/key does not exist, upload file onto S3
            else:
                logger.info("Downloading data from FTP - {}...".format(file_path))
                try:
                    # try to download file from FTP
                    myfile = io.BytesIO()
                    filename = "RETR {}".format(entry)
                    resp = ftp.retrbinary(filename, myfile.write)
                    myfile.seek(0)
                except:
                    logger.warn("Failed to download {} from FTP!".format(file_path))

                # upload file onto S3
                logger.info("Uploading data to S3 - {}...".format(s3_path))
                mybucket.put_object(Key=s3_path, Body=myfile)

                # check if the file has been uploaded
                if check_key_exists_S3(s3_resource, bucket, s3_path):
                    logger.info(f"Successfully uploaded {s3_path} to S3!")
                else:
                    raise Exception(f"Failed to upload {s3_path} to S3!")

    # set up S3
    s3_resource = boto3.resource("s3")
    bucket = "roofstock-data-lake"
    mybucket = s3_resource.Bucket(bucket)

    # connect FTP
    host = "ftp2.census.gov"
    ftp = _connect_ftp(host, 3, logger)
    # change the current work directory
    ftp.cwd(f"/programs-surveys/acs/summary_file/{year}/documentation/")
    # traverse the directory
    _ftp_traverse(ftp, _single_file_FTP_to_S3, logger)

    ftp.quit()


if __name__ == "__main__":
    docs_FTP_to_S3()