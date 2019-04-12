## ACS Data Airflow DAG file
# Author: Jiaxun Song
# How to write an Airflow DAG: http://michal.karzynski.pl/blog/2017/03/19/developing-workflows-with-apache-airflow/
# This file performs the general flow of the annual American Community Survey update.
#   This flow should overwrite things by default.
# It's assumed that there is enough disk space on the local machine to dump temp files to. 
#   We'll have a separate Airflow DAG monitor the amount of disk space available on the Azure Airflow VM.
# Any errors should be output to Airflow logs
# https://gtoonstra.github.io/etl-with-airflow/index.html
# DO NOT FORGET TO ADD A SOFT LINK TO THIS FILE IN THE /airflow/dags/... DIRECTORY ON THE AIRFLOW VM!!!

# TO DO:
# - CLEAN file of Ns, -s
# - Explanation of Symbols:
#    An '**' entry in the margin of error column indicates that either no sample observations or too few sample observations were available to compute a standard error and thus the margin of error. A statistical test is not appropriate.
#    An '-' entry in the estimate column indicates that either no sample observations or too few sample observations were available to compute an estimate, or a ratio of medians cannot be calculated because one or both of the median estimates falls in the lowest interval or upper interval of an open-ended distribution.
#    An '-' following a median estimate means the median falls in the lowest interval of an open-ended distribution.
#    An '+' following a median estimate means the median falls in the upper interval of an open-ended distribution.
#    An '***' entry in the margin of error column indicates that the median falls in the lowest interval or upper interval of an open-ended distribution. A statistical test is not appropriate.
#    An '*****' entry in the margin of error column indicates that the estimate is controlled. A statistical test for sampling variability is not appropriate.
#    An 'N' entry in the estimate and margin of error columns indicates that data for this geographic area cannot be displayed because the number of sample cases is too small.
#    An '(X)' means that the estimate is not applicable or not available.
# - Schedule this to run each year

# TEST CASES:

###########################################################
## Packages
###########################################################
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
## Global helper functions
###########################################################
def _ftp_traverse(ftp, callable, logger):
    entry_list = ftp.nlst()
    
    for entry in entry_list:
        try: # the entry is a directory
            ftp.cwd(entry)
            logger.info("Traversing FTP directory - {}...".format(ftp.pwd()))
            
            # recursively call the function itself
            _ftp_traverse(ftp, callable, logger)
            
            # backtrack
            ftp.cwd("..")
        
        except: # the entry is a file, call callable function
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
## FTP to S3
###########################################################

@send_message()
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
            logger.warn("Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(key, bucket))
            
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

@send_message()
def template_FTP_to_S3(logger, **kwargs):
    """
    Given the year, tansfer all the template files from FTP to S3
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
        if re.search(r"_5yr_Summary_?FileTemplates.zip", entry):
            
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
                            if check_key_exists_S3(s3_resource, bucket, s3_path+file_name): 
                                logger.warn("Key {} already existed on S3, skipped!".format(s3_path+file_name))
                            
                            else:
                                # upload file onto S3
                                logger.info("Uploading data to S3 - {}...".format(file_name))
                                mybucket.put_object(Key=s3_path+file_name, Body=zipf.read(file))
                logger.info("Successfully extracted file!")

            except Exception as e:
                logger.error(e)
                logger.warn("Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(key, bucket)) 
    
    ftp.quit()      

@send_message()
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
        
        if (("5_year" in file_path) and entry == "Sequence_Number_and_Table_Number_Lookup.xls") or (entry == "ACS_5yr_Seq_Table_Number_Lookup.xls"):
            s3_path = f"RawData/ACS/summary_file/{year}/ACS_{year}_5yr_Seq_Table_Number_Lookup.{suffix}"
        elif (("5_year" in file_path) and ("5-Year_TableShells.xls" in entry)) or (("year" not in file_path) and ("Shells.xls" in entry)):
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

###########################################################
## Aggregate and copy data from S3 to Snowflake
###########################################################

@send_message()
def copy_geo_S3_to_Snowflake(logger, **kwargs):
    """
    Given the year, aggregate the mini geo data and cpoy the data into Snowflake.
    Specifically, the steps are:
    -- walk through all the sheets (each sheet for a single state) in the excel file, concat them into one df,
    -- extract some other handful geo info,
    -- stage on S3 roofstock-data-lake/RawData/ACS/geo_info/,
    -- copy the data into Snowflake.
    """
    # Initialize variables    
    year = kwargs["year"]

    # S3 setup
    s3_client = boto3.client("s3")
    bucket = "roofstock-data-lake"
    s3_resource = boto3.resource("s3")
    mybucket = s3_resource.Bucket(bucket)
    
    # get geo info file, concat each sheet into one df
    s3_path = f"RawData/ACS/summary_file/{year}/ACS_{year}_5yr_Mini_Geo.xlsx"
    obj = s3_client.get_object(Bucket=bucket, Key=s3_path)
    xl = pd.ExcelFile(io.BytesIO(obj["Body"].read()))
    
    geo_all = pd.DataFrame()
    for sheet_name in xl.sheet_names:  # loop all sheet names
        logger.info(f"Loading data from state - {sheet_name}")
        geo_all = pd.concat([geo_all, xl.parse(sheet_name)], axis=0, sort=False)  # read a specific sheet to DataFrame
    
    geo_all = geo_all.loc[:, geo_all.columns[0]:geo_all.columns[3]] # make sure we only keep first 4 columns
    geo_all.columns = ["STATE", "LOGRECNO", "GEOID", "Name"] # State abbreviation including US, Logical Record Number, Geography ID, Geography Name
    geo_all["LOGRECNO"] = geo_all["LOGRECNO"].astype(str).str.pad(7, 'left', '0')
    geo_all = geo_all.rename({"Name":"PLACE_NAME", "STATE":"STUSAB"}, axis=1)
    
    geo_all = geo_all[["STUSAB", "LOGRECNO", "GEOID", "PLACE_NAME"]]
    geo_all.reset_index(inplace=True)
    geo_all.drop(["index"], axis=1, inplace=True)
    
    # extract geo info
    geo_all["level"] = geo_all["GEOID"].str[:3]
    geo_all["component"] = geo_all["GEOID"].str[3:5]
    # geo_all = geo_all[geo_all["level"].isin(["040", "050", "140", "150", "310", "860"]) & geo_all["component"].isin(["00"])]
    geo_all["place_list"] = geo_all["PLACE_NAME"].str.split(",")
    geo_all["State"] = ""
    geo_all["County"] = ""
    geo_all["Census_Tract"] = ""
    geo_all["Block_Group"] = ""
    geo_all["CBSA"] = ""
    geo_all["ZCTA"] = ""
    geo_all.loc[geo_all[geo_all["level"].isin(["040", "050", "140", "150"])].index,"State"] = geo_all[geo_all["level"].isin(["040", "050", "140", "150"])]["place_list"].apply(lambda x:x[-1] if len(x)>=1 else "NA")
    geo_all.loc[geo_all[geo_all["level"].isin(["050", "140", "150"])].index,"County"] = geo_all[geo_all["level"].isin(["050", "140", "150"])]["place_list"].apply(lambda x:x[-2] if len(x)>=2 else "NA")
    geo_all.loc[geo_all[geo_all["level"].isin(["140", "150"])].index,"Census_Tract"] = geo_all[geo_all["level"].isin(["140", "150"])]["place_list"].apply(lambda x:x[-3] if len(x)>=3 else "NA")
    geo_all.loc[geo_all[geo_all["level"].isin(["150"])].index,"Block_Group"] = geo_all[geo_all["level"].isin(["150"])]["place_list"].apply(lambda x:x[-4] if len(x)>=4 else "NA")
    geo_all.loc[geo_all[geo_all["level"].isin(["310"])].index,"CBSA"] = geo_all[geo_all["level"].isin(["310"])]["PLACE_NAME"]
    geo_all.loc[geo_all[geo_all["level"].isin(["860"])].index,"ZCTA"] = geo_all[geo_all["level"].isin(["860"])]["PLACE_NAME"].str[-5:]
    geo_all.drop(["level", "component", "place_list"], axis=1, inplace=True)

    # stage data upto S3
    logger.info("Uploading staging csv onto S3...")
    csv_buffer = io.StringIO()
    geo_all.to_csv(csv_buffer, index=False)
    s3_path = f"RawData/ACS/geo_info/Mini_Geo_5yr_{year}.csv"
    mybucket.put_object(Key=s3_path, Body=csv_buffer.getvalue())
            
    # check if the file has been uploaded
    if check_key_exists_S3(s3_resource, bucket, s3_path):
        logger.info(f"Successfully uploaded {s3_path} to S3!")
    else:
        raise Exception(f"Failed to upload {s3_path} to S3!")
        
    # load to Snowflake
    logger.info("Copying data from S3 to Snowflake...")
    engine = create_engine(URL(
        account = Variable.get("SNOWFLAKE_ACCOUNT"),
        user = Variable.get("SNOWFLAKE_USER"),
        password = Variable.get("SNOWFLAKE_PASSWORD"),
        database = "CENSUS_DB",
        schema = "RAW_ACS",
        warehouse = Variable.get("SNOWFLAKE_WH")
    ))
    
    # create table
    engine.execute(
        (f"CREATE OR REPLACE TABLE CENSUS_DB.RAW_ACS.GEOMETA{year}5 "
          "(STUSAB VARCHAR(2) COMMENT 'State/US Abbreviation', "
          "LOGRECNO VARCHAR(7) COMMENT 'Logical Record Number', "
          "GEOID VARCHAR(32) COMMENT 'Unified geographic identifiers', "
          "PLACE_NAME VARCHAR(256) COMMENT 'Human-readable place names', "
          "State VARCHAR(256) COMMENT 'State/US', "
          "County VARCHAR(256) COMMENT 'County', "
          "Census_Tract VARCHAR(256) COMMENT 'Census Tract', "
          "Block_Group VARCHAR(256) COMMENT 'Block Group', "
          "CBSA VARCHAR(64) COMMENT 'Core based statistical areas', "
          "ZCTA VARCHAR(5) COMMENT 'ZIP Code Tabulation Areas');")
    )         
    
    # copy data
    engine.execute(
        f"COPY INTO CENSUS_DB.RAW_ACS.GEOMETA{year}5 FROM @ACS_DATA_GEO/Mini_Geo_5yr_{year}.csv FILE_FORMAT = ACS_DATA_GEO ON_ERROR = 'ABORT_STATEMENT';"
    )

    # grant permissions
    engine.execute(
        f"GRANT SELECT ON TABLE CENSUS_DB.RAW_ACS.GEOMETA{year}5 TO ROLE DATAENGINEERING_ROLE;"
    )
    engine.dispose()

@send_message()
def copy_lookup_S3_to_Snowflake(logger, **kwargs):
    """
    Given the year, aggregate the lookup data and cpoy the data into Snowflake.
    Specifically, the steps are:
    -- walk through the templates folder, concat all the template files into one df,
    -- stage on S3 roofstock-data-lake/RawData/ACS/lookup/, (yes, template files are called "lookup" files now)
    -- copy the data into Snowflake.
    """

    # Initialize variables
    year = kwargs["year"]
    
    # Snowflake setup.
    engine = create_engine(URL(
        account = Variable.get("SNOWFLAKE_ACCOUNT"),
        user = Variable.get("SNOWFLAKE_USER"),
        password = Variable.get("SNOWFLAKE_PASSWORD"),
        database = "CENSUS_DB",
        schema = "RAW_ACS",
        warehouse = Variable.get("SNOWFLAKE_WH")
    ))        
    
    # S3 setup
    s3_client = boto3.client("s3")
    bucket = "roofstock-data-lake"
    s3_resource = boto3.resource("s3")
    mybucket = s3_resource.Bucket(bucket)
    
    logger.info(f"Loading templates data - year {year}...")
    
    prefix = f"RawData/ACS/summary_file/{year}/data/5yr_templates/"
    path_list = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    
    lookup_df = pd.DataFrame()
    for o in path_list["Contents"]:
        s3_path = o["Key"]
        seq = re.findall(r"templates/(?:S|s)eq(\d*)\.xls", s3_path)
        if seq:
            sequence = int(seq[0])
            logger.info(f"Loading sequence - {sequence}...")
            
            template = pd_read_s3(bucket, s3_path, s3_client, excel=True)
            template = template.iloc[:, 6:]

            # concat data
            template = template.stack().to_frame().reset_index(level=1,col_level=0).reset_index(level=0).drop("index", axis=1)
            template["SEQ"] = sequence
            template.columns = ["VARID", "LABEL", "SEQ"]
            template = template[["VARID", "SEQ", "LABEL"]]
            lookup_df = pd.concat([lookup_df, template], ignore_index=True, sort=False)
    
    # stage data upto S3
    logger.info("Uploading staging csv onto S3...")
    csv_buffer = io.StringIO()
    lookup_df.to_csv(csv_buffer, index=False)
    s3_path = f"RawData/ACS/lookup/Lookup_5yr_{year}.csv"
    mybucket.put_object(Key=s3_path, Body=csv_buffer.getvalue())
            
    # check if the file has been uploaded
    if check_key_exists_S3(s3_resource, bucket, s3_path):
        logger.info(f"Successfully uploaded {s3_path} to S3!")
    else:
        raise Exception(f"Failed to upload {s3_path} to S3!")
    
    # create table
    engine.execute(
        (f"CREATE OR REPLACE TABLE CENSUS_DB.RAW_ACS.LOOKUP{year}5 "
         "(VARID VARCHAR(16) COMMENT 'Variable identifiers defined by US Census Bureau, indicate the original table and the column number of the variable', "
         "SEQ VARCHAR(3) COMMENT 'Sequence number for lookup', "
         "LABEL VARCHAR(512) COMMENT 'The label from the original table');")
    )
    
    # copy data
    engine.execute(
        f"COPY INTO CENSUS_DB.RAW_ACS.LOOKUP{year}5 FROM @ACS_DATA_LOOKUP/Lookup_5yr_{year}.csv FILE_FORMAT = ACS_DATA_LOOKUP ON_ERROR = 'ABORT_STATEMENT';"
    )
    
    # grant permissions
    engine.execute(
        f"GRANT SELECT ON TABLE CENSUS_DB.RAW_ACS.LOOKUP{year}5 TO ROLE DATAENGINEERING_ROLE;"
    )
    engine.dispose()

@send_message()
def copy_sequence_S3_to_Snowflake(logger, **kwargs):
    """
    Given the year and sequence number, cpoy the sequence data into Snowflake.
    """
    # Initialize variables
    sequence = kwargs["sequence"]
    year = kwargs["year"]

    # set up S3
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    bucket = "roofstock-data-lake"
    mybucket = s3_resource.Bucket(bucket)
    
    # set up Snowflake
    engine = create_engine(URL(
        account = Variable.get("SNOWFLAKE_ACCOUNT"),
        user = Variable.get("SNOWFLAKE_USER"),
        password = Variable.get("SNOWFLAKE_PASSWORD"),
        database = "CENSUS_DB",
        schema = "RAW_ACS",
        warehouse = Variable.get("SNOWFLAKE_WH")
    ))
    
    # get the headers from lookup table
    logger.info(f"Loading sequence {sequence}...")
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(f"SELECT VARID FROM CENSUS_DB.RAW_ACS.LOOKUP{year}5 WHERE SEQ={sequence};")
    result = cursor.fetchall()
    headers = [t[0] for t in result]
    
    # check if the sequence file exists
    if not headers:
        logger.warn(f"Sequence {sequence} in Lookup table CENSUS_DB.RAW_ACS.LOOKUP{year}5 on Snowflake does not exist!")
    else:
        # create table
        header_fixed = ["FILEID", "FILETYPE", "STUSAB", "CHARITER", "SEQUENCE", "LOGRECNO"]
        logger.info(f"Creating tables - CENSUS_DB.RAW_ACS.E{year}5{sequence:04}, CENSUS_DB.RAW_ACS.M{year}5{sequence:04}...")
        engine.execute(
                "CREATE OR REPLACE TABLE CENSUS_DB.RAW_ACS.E{}5{:04} ({});".format(year, sequence, " VARCHAR(16), ".join(header_fixed) + " VARCHAR(16), " + " NUMBER(16,2), ".join(headers) + " NUMBER(16,2)")
            )        

        engine.execute(
                "CREATE OR REPLACE TABLE CENSUS_DB.RAW_ACS.M{}5{:04} ({});".format(year, sequence, " VARCHAR(16), ".join(header_fixed) + " VARCHAR(16), " + " NUMBER(16,2), ".join(headers) + " NUMBER(16,2)")
            ) 
        
        # copy data into tables
        prefix = f"RawData/ACS/summary_file/{year}/data/5_year_seq_by_state/"
        path_list = s3_client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")

        # loop all states
        for o in path_list.get("CommonPrefixes"):
            path_root = o.get("Prefix")
            logger.info(f"Parsing sub folder - {path_root}")

            # loop all levels
            for level in ["All_Geographies_Not_Tracts_Block_Groups", "Tracts_Block_Groups_Only"]:
                logger.info(f"Parsing level - {level}")

                # get state abbreviation from a csv file starts with g{year} in each sub-folder
                s3_path = f"{path_root}{level}/g{year}"
                state_abbr = s3_client.list_objects(Bucket=bucket, Prefix=s3_path)["Contents"][0]["Key"][-6:-4]

                if level == "Tracts_Block_Groups_Only" and state_abbr == "us":
                    logger.info(f"No Tracts_Block_Groups_Only level for us.")
                
                else:
                    aws_path = (path_root + level).replace("RawData/ACS/summary_file/", "")
                    # copy data
                    logger.info(f"Copying data from S3 to Snowflake - sequence: {sequence}, state: {state_abbr}...")
                    
                    engine.execute(
                            f"COPY INTO CENSUS_DB.RAW_ACS.E{year}5{sequence:04} FROM @ACS_DATA_SEQ/{aws_path}/e{year}5{state_abbr}{sequence:04}000.txt FILE_FORMAT = ACS_DATA_SEQ ON_ERROR = 'ABORT_STATEMENT';"
                        )

                    engine.execute(
                            f"COPY INTO CENSUS_DB.RAW_ACS.M{year}5{sequence:04} FROM @ACS_DATA_SEQ/{aws_path}/m{year}5{state_abbr}{sequence:04}000.txt FILE_FORMAT = ACS_DATA_SEQ ON_ERROR = 'ABORT_STATEMENT';"
                        )
                    
                    # grant permissions
                    engine.execute(
                        f"GRANT SELECT ON TABLE CENSUS_DB.RAW_ACS.E{year}5{sequence:04} TO ROLE DATAENGINEERING_ROLE;"
                    )
                    
                    engine.execute(
                        f"GRANT SELECT ON TABLE CENSUS_DB.RAW_ACS.M{year}5{sequence:04} TO ROLE DATAENGINEERING_ROLE;"
                    )
        
        engine.dispose()

###########################################################
## Upadte VARIABLE_LIST and FACT tables on Snowflake
###########################################################
        
@send_message()
def upload_variable_list_to_S3(logger, **kwargs):
    """
    Copy the ACS_variable_list.csv file from local file system to S3.
    """
    # S3 setup
    s3_client = boto3.client("s3")
    bucket = "roofstock-data-lake"
    s3_resource = boto3.resource("s3")
    mybucket = s3_resource.Bucket(bucket)
    
    # upload ACS_variable_list.csv from local file system to S3.
    logger.info("Uploading ACS_variable_list.csv local file system to S3...")
    file_path = "/home/jsong/airflow/dags/ACS/ACS_variable_list.csv"
    s3_path = "RawData/ACS/ACS_variable_list.csv"
    mybucket.put_object(Key=s3_path, Body=open(file_path, "rb"))

    logger.info("Uploading ACS_variable_list.xlsx local file system to S3...")
    file_path = "/home/jsong/airflow/dags/ACS/ACS_variable_list.xlsx"
    s3_path = "RawData/ACS/ACS_variable_list.xlsx"
    mybucket.put_object(Key=s3_path, Body=open(file_path, "rb"))

@send_message()
def upload_variable_list_to_Snowflake(logger, **kwargs):
    """
    Copy VARIABLE_LIST data from S3 to Snowflake.
    """
    # Initialize variables    
    year = kwargs["year"]
        
    # load to Snowflake
    logger.info("Copying ACS_variable_list.csv from S3 to Snowflake...")
    engine = create_engine(URL(
        account = Variable.get("SNOWFLAKE_ACCOUNT"),
        user = Variable.get("SNOWFLAKE_USER"),
        password = Variable.get("SNOWFLAKE_PASSWORD"),
        database = "CENSUS_DB",
        schema = "PROD_ACS",
        warehouse = Variable.get("SNOWFLAKE_WH")
    ))
    
    # create table
    engine.execute(
        (f"CREATE OR REPLACE TABLE CENSUS_DB.PROD_ACS.VARIABLE_LIST "
          "(GROUP_NAME VARCHAR(256) COMMENT 'Variable groups in terms of purposes', "
          "ROOFSTOCK_NAME VARCHAR(256) COMMENT 'Human-readable variable names', "
          "FORMULA VARCHAR(512) COMMENT 'Formula for the calculation of the variable', "
          "STATISTIC VARCHAR(16) COMMENT 'Statistic types, e.g., count, mean, median, rate, etc.', "
          "START_YEAR NUMBER(4,0) COMMENT 'The earliest available year', "
          "END_YEAR NUMBER(4,0) COMMENT 'The latest available year, 9999 means variable will available for all the following years after the earliest available year', "
          "NOTES VARCHAR(256) COMMENT 'Notes');")
    )         

    # copy data
    engine.execute(
        f"COPY INTO CENSUS_DB.PROD_ACS.VARIABLE_LIST FROM @ACS_VARIABLE_LIST/ACS_variable_list.csv FILE_FORMAT = ACS_VARIABLE_LIST ON_ERROR = 'ABORT_STATEMENT';"
    )
    
    # replace end_year 9999 with the current year
    engine.execute(
        f"UPDATE CENSUS_DB.PROD_ACS.VARIABLE_LIST SET END_YEAR = {year} WHERE END_YEAR = 9999;"
    )

    # grant permissions
    for role in ["DATAENGINEERING_ROLE", "ANALYST_ROLE", "DATASCIENCE_ROLE", "BUSINESS_ROLE", "ETL_ROLE"]:
        engine.execute(
            f"GRANT SELECT ON TABLE CENSUS_DB.PROD_ACS.VARIABLE_LIST TO ROLE {role};"
        )
    engine.dispose()

@send_message()
def create_fact_table(logger, **kwargs):
    """
    Create table CENSUS_DB.PROD_ACS.FACT on Snowflake
    """
    # Setup Snowflake
    engine = create_engine(URL(
        account = Variable.get("SNOWFLAKE_ACCOUNT"),
        user = Variable.get("SNOWFLAKE_USER"),
        password = Variable.get("SNOWFLAKE_PASSWORD"),
        database = "CENSUS_DB",
        schema = "PROD_ACS",
        warehouse = Variable.get("SNOWFLAKE_WH")
    ))
    
    # Read table CENSUS_DB.PROD_ACS.VARIABLE_LIST from Snowflake
    logger.info("Reading table CENSUS_DB.PROD_ACS.VARIABLE_LIST from Snowflake...")
    sql = "SELECT * FROM CENSUS_DB.PROD_ACS.VARIABLE_LIST;"
    df = pd.read_sql(sql=sql, con=engine)    
    
    # Create table CENSUS_DB.PROD_ACS.FACT 
    logger.info("Creating table CENSUS_DB.PROD_ACS.FACT...")
    df["col_type"] = df["statistic"].apply(lambda x : "NUMBER(9, 6)" if x in ["rate", "percentage"] else "NUMBER(16, 2)").values.tolist()
    df["col_type"] = df["roofstock_name"] + " " + df["col_type"]
    col_type = df["col_type"].unique().tolist()
    
    engine.execute(
        "CREATE OR REPLACE TABLE CENSUS_DB.PROD_ACS.FACT (YEAR NUMBER(4,0), GEOID VARCHAR(32), {});".format(", ".join(col_type))
    )
    
    logger.info("Copying GEO INFO from CENSUS_DB.PROD_ACS.GEOMETA to CENSUS_DB.PROD_ACS.FACT...")
    engine.execute(
        ("INSERT INTO CENSUS_DB.PROD_ACS.FACT (YEAR, GEOID) "
         "SELECT YEAR, GEOID FROM CENSUS_DB.PROD_ACS.GEOMETA;")
    )

    # grant permissions
    for role in ["DATAENGINEERING_ROLE", "ANALYST_ROLE", "DATASCIENCE_ROLE", "BUSINESS_ROLE", "ETL_ROLE"]:
        engine.execute(
            f"GRANT SELECT ON TABLE CENSUS_DB.PROD_ACS.FACT TO ROLE {role};"
        )

    engine.dispose()

@send_message()
def pull_variables_from_raw_tables(logger, **kwargs):
    """
    Pull variables from raw tables.
    """
    # Initialize variables    
    year = kwargs["year"]

    def _run_command(command):
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while True:
            output = process.stdout.readline()
            if (not process.poll() and not output) or process.poll():
                break
            if output:
                logger.info(output.decode("utf-8").strip())
                # print(output.decode("utf-8").strip())

    def _fix_divided_by_0(formula):
        p = formula.find("/")
        if p != -1:
            return formula[:p+1] + " NULLIF(" + formula[p+2:] + ", 0)"
        else:
            return formula
    
    # Setup Snowflake
    engine = create_engine(URL(
        account = Variable.get("SNOWFLAKE_ACCOUNT"),
        user = Variable.get("SNOWFLAKE_USER"),
        password = Variable.get("SNOWFLAKE_PASSWORD"),
        database = "CENSUS_DB",
        schema = "PROD_ACS",
        warehouse = Variable.get("SNOWFLAKE_WH")
    ))
    
    # Read table CENSUS_DB.PROD_ACS.VARIABLE_LIST from Snowflake
    sql = "SELECT * FROM CENSUS_DB.PROD_ACS.VARIABLE_LIST;"
    df = pd.read_sql(sql=sql, con=engine)    
    grouped = df.groupby("group_name")

    for group_name, group_df in grouped:
        logger.info(f"Building temporary table CENSUS_DB.PROD_ACS.ACS_BY_GROUP_TEMP for the group {group_name}...")

        variables = list(set(group_df["formula"].str.findall("(?:B|C)[\w\d]*_[\w\d]*").sum()))
        roofstock_name_list = group_df["roofstock_name"].tolist()
        roofstock_names = ", ".join(roofstock_name_list)
        formula_list = group_df["formula"].tolist()
        formula_list = [_fix_divided_by_0(f) for f in formula_list]
        select_col = ", ".join([pair[0] + " as " + pair[1] for pair in zip(formula_list, roofstock_name_list)])
        start_year = group_df["start_year"].max()
        end_year = min(group_df["end_year"].max(), year)

        var_dic = {"variables" : variables, 
                   "select_col" : select_col, 
                   "roofstock_names" : roofstock_names, 
                   "start_year" : start_year, 
                   "end_year" : end_year}

        command = f"""
                cd /home/jsong/airflow/dags/ACS/dbt
                dbt run --models ACS_BY_GROUP_TEMP --profiles-dir /home/jsong/.dbt --vars "{var_dic}"
              """

        _run_command(command)

        for col in roofstock_name_list:
            logger.info(f"Copying column {col} from CENSUS_DB.PROD_ACS.ACS_BY_GROUP_TEMP to CENSUS_DB.PROD_ACS.FACT...")

            # copy data
            engine.execute(
                (f"UPDATE CENSUS_DB.PROD_ACS.FACT AS target "
                 f"SET {col} = source.{col} "
                 f"FROM CENSUS_DB.PROD_ACS.ACS_BY_GROUP_TEMP AS source "
                 f"WHERE target.GEOID = source.GEOID AND target.YEAR = source.YEAR AND source.{col} IS NOT NULL;")
            )

        engine.execute(
            f"DROP TABLE CENSUS_DB.PROD_ACS.ACS_BY_GROUP_TEMP;"
        )
    engine.dispose()

###########################################################
## Run entire ETL process
###########################################################

# Main DAG for this process 
# Scheduler, use cron format: https://airflow.incubator.apache.org/scheduler.html
default_args = {
    'owner': 'jsong',
    'start_date': datetime.utcnow(),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('acs_ingest', default_args=default_args)

# Run all steps
year = 2017
# --------------------------------------------------------
# Transfer data from FTP to S3
transfer_template = PythonOperator(
    provide_context=True,
    task_id="template_FTP_to_S3", 
    dag=dag, 
    python_callable=template_FTP_to_S3,
    op_kwargs={"year":year}
    )

transfer_docs = PythonOperator(
    provide_context=True,
    task_id="docs_FTP_to_S3", 
    dag=dag, 
    python_callable=docs_FTP_to_S3, 
    op_kwargs={"year":year}
    )

# subDAG for transfer sequence files
# In this way, the files can be transfered in parallel
def subdag_transfer_sequence(parent_dag_name, child_dag_name, default_args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args
        )
    
    state_list = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'DistrictOfColumbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'NewHampshire', 'NewJersey', 'NewMexico', 'NewYork', 'NorthCarolina', 'NorthDakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'PuertoRico', 'RhodeIsland', 'SouthCarolina', 'SouthDakota', 'Tennessee', 'Texas', 'UnitedStates', 'Utah', 'Vermont', 'Virginia', 'Washington', 'WestVirginia', 'Wisconsin', 'Wyoming']
    
    for state in state_list:
        PythonOperator(
            task_id=f"{child_dag_name}-State-{state}", 
            dag=dag_subdag,
            wait_for_downstream=True,
            python_callable=sequence_FTP_to_S3,
            provide_context=True,
            op_kwargs={"year":year, "state":state}
            )

    return dag_subdag

transfer_sequence = SubDagOperator(
    provide_context=True,
    task_id='sequence_FTP_to_S3',
    subdag=subdag_transfer_sequence('acs_ingest', 'sequence_FTP_to_S3', default_args),
    dag=dag
    )

# --------------------------------------------------------
# Delete zip files

# TODO
delete_zips = DummyOperator(
    task_id="delete_zips",
    dag=dag)

# --------------------------------------------------------
# Populate database
copy_geo = PythonOperator(
    task_id="copy_geo_S3_to_Snowflake", 
    dag=dag, 
    python_callable=copy_geo_S3_to_Snowflake, 
    provide_context=True, 
    op_kwargs={"year":year}
    )

copy_lookup = PythonOperator(
    task_id="copy_lookup_S3_to_Snowflake", 
    dag=dag, 
    python_callable=copy_lookup_S3_to_Snowflake, 
    provide_context=True, 
    op_kwargs={"year":year}
    )

# subDAG for sequence files copy
# In this way, the files can be copied in parallel
def subdag_copy_sequence(parent_dag_name, child_dag_name, default_args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args
        )

    for sequence in range(1, 150):
        PythonOperator(
            task_id=f"{child_dag_name}-Seq{sequence}", 
            dag=dag_subdag,
            wait_for_downstream=True,
            python_callable=copy_sequence_S3_to_Snowflake,
            provide_context=True,
            op_kwargs={"year":year, "sequence":sequence}
            )

    return dag_subdag

copy_sequence = SubDagOperator(
    provide_context=True,
    task_id='copy_sequence_S3_to_Snowflake',
    subdag=subdag_copy_sequence('acs_ingest', 'copy_sequence_S3_to_Snowflake', default_args),
    dag=dag
    )

# --------------------------------------------------------
# Update VARIABLE_LISTS and FACT tables

bash_command_geometa = f"""
                        cd /home/jsong/airflow/dags/ACS/dbt
                        dbt run --models GEOMETA --profiles-dir /home/jsong/.dbt
                        """
update_geometa = BashMessageOperator(
    task_id="update_geometa", 
    dag=dag,
    bash_command=bash_command_geometa
    )

def subdag_update_fact_on_Snowflake(parent_dag_name, child_dag_name, default_args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args
        )
    
    variable_list_to_S3 = PythonOperator(
        task_id="upload_variable_list_to_S3", 
        dag=dag_subdag, 
        wait_for_downstream=True,
        python_callable=upload_variable_list_to_S3, 
        provide_context=True
        )
    
    variable_list_to_Snowflake = PythonOperator(
        task_id="upload_variable_list_to_Snowflake", 
        dag=dag_subdag, 
        wait_for_downstream=True,
        python_callable=upload_variable_list_to_Snowflake, 
        provide_context=True, 
        op_kwargs={"year":year}
        )

    create_fact_table_on_Snowflake = PythonOperator(
        task_id="create_fact_table", 
        dag=dag_subdag, 
        wait_for_downstream=True,
        python_callable=create_fact_table, 
        provide_context=True
        )
    
    variables_from_raw_tables = PythonOperator(
        task_id="pull_variables_from_raw_tables", 
        dag=dag_subdag, 
        wait_for_downstream=True,
        python_callable=pull_variables_from_raw_tables, 
        provide_context=True, 
        op_kwargs={"year":year}
        )
    
    variable_list_to_S3 >> variable_list_to_Snowflake >> create_fact_table_on_Snowflake >> variables_from_raw_tables
    return dag_subdag

update_fact = SubDagOperator(
    provide_context=True,
    task_id='update_fact',
    subdag=subdag_update_fact_on_Snowflake('acs_ingest', 'update_fact', default_args),
    dag=dag
    )

# --------------------------------------------------------
# Build graph
transfer_docs >> copy_geo >> update_geometa
transfer_template >> copy_lookup >> copy_sequence
transfer_sequence >> copy_sequence >> delete_zips
update_fact.set_upstream([update_geometa, copy_lookup, copy_sequence])