from airflow.macros.roofstock_plugin import run_with_logging, connect_to_s3, get_s3_bucket_name, connect_to_snowflake, pod_xcom_push, pod_xcom_pull, run_dbt
from airflow.models import Variable
from datetime import datetime
import time
from ftplib import FTP
import pandas as pd
import re
import zipfile
import io


###########################################################
# Global helper functions
###########################################################
def _connect_ftp(host, attempt=5):
    i = 0
    for _ in range(attempt):
        try:
            ftp = FTP(host, timeout=15)
            ftp.login()
            return ftp
        except Exception as e:
            print("FTP connection failed {} time(s)!".format(i + 1))
            print(e)
            time.sleep(10)
            i += 1

    if i == attempt:
        raise Exception("FTP connection failed!")


def _ftp_traverse(ftp, callback):
    entry_list = ftp.nlst()

    for entry in entry_list:
        try:  # the entry is a directory
            ftp.cwd(entry)
            print("Traversing FTP directory - {}...".format(ftp.pwd()))

            # recursively call the function itself
            _ftp_traverse(ftp, callback)

            # backtrack
            ftp.cwd("..")

        except:  # the entry is a file, call callable function
            callback(ftp, entry)


def _ftp_download_file(ftp, entry):
    myfile = io.BytesIO()
    filename = "RETR {}".format(entry)
    ftp.retrbinary(filename, myfile.write)
    myfile.seek(0)
    return myfile

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

    # setup S3
    s3hook = connect_to_s3()
    AWS_S3_BUCKET_NAME = get_s3_bucket_name()
    mybucket = s3hook.get_bucket(AWS_S3_BUCKET_NAME)

    def _unzip(s3_path, myfile):
        logger.info(f"Exctrating zip file...")
        # Read the file as a zipfile and process the members
        with zipfile.ZipFile(myfile, mode='r') as zipf:
            for file in zipf.infolist():
                fileName = file.filename
                key_path = "/".join(s3_path.split("/")[:-1] + [fileName])
                logger.info(f"Uploading {key_path}...")
                mybucket.put_object(Key=key_path, Body=zipf.read(file))
        logger.info("Successfully extracted file!")

    def _single_file_FTP_to_S3(ftp, entry):
        """
        callable funcation for _ftp_traverse
        copy single file from FTP to S3 without any judgment
        """
        file_path = ftp.pwd() + "/" + entry
        s3_path = file_path.replace("/programs-surveys/acs", "RawData/ACS")

        # check if the file/key already existed on S3
        if s3hook.check_for_key(key=s3_path, bucket_name=AWS_S3_BUCKET_NAME):
            logger.warning("Key {} already existed on S3, skipped!".format(s3_path))

        # if the file/key does not exist, upload file onto S3
        else:
            logger.info("Downloading data from FTP - {}...".format(file_path))
            # try to download file from FTP
            myfile = _ftp_download_file(ftp, entry)

            # upload file onto S3
            logger.info("Uploading data to S3 - {}...".format(s3_path))
            mybucket.put_object(Key=s3_path, Body=myfile)

            # unzip the file and upload it onto S3
            if s3_path.endswith(".zip"):
                _unzip(s3_path, myfile)


    # connect FTP
    host = "ftp2.census.gov"
    ftp = _connect_ftp(host)

    # change the work directory
    ftp.cwd(f"/programs-surveys/acs/summary_file/{year}/data/5_year_seq_by_state/{state}/")
    # traverse the directory
    _ftp_traverse(ftp, _single_file_FTP_to_S3)

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

    # setup S3
    s3hook = connect_to_s3()
    AWS_S3_BUCKET_NAME = get_s3_bucket_name()
    mybucket = s3hook.get_bucket(AWS_S3_BUCKET_NAME)

    s3_path = f"RawData/ACS/summary_file/{year}/data/5yr_templates/"

    # connect FTP
    host = "ftp2.census.gov"
    ftp = _connect_ftp(host, 3)

    # change the work directory
    ftp.cwd(f"/programs-surveys/acs/summary_file/{year}/data/")

    entry_list = ftp.nlst()
    for entry in entry_list:
        if re.search(fr"_5yr_Summary_?FileTemplates.zip", entry):

            file_path = ftp.pwd() + "/" + entry
            logger.info("Downloading data from FTP - {}...".format(file_path))
            # download file from FTP
            myfile = _ftp_download_file(ftp, entry)

            # unzip file
            logger.info("Exctrating file - {}".format(entry))
            # Read the file as a zipfile and process the members
            with zipfile.ZipFile(myfile, mode='r') as zipf:
                for file in zipf.infolist():
                    file_name = file.filename
                    if file_name[-1] != "/":
                        file_name = file_name.split("/")[-1]

                        # check if the file/key already existed on S3
                        if s3hook.check_for_key(key=s3_path + file_name, bucket_name=AWS_S3_BUCKET_NAME):
                            logger.warning("Key {} already existed on S3, skipped!".format(s3_path + file_name))

                        else:
                            # upload file onto S3
                            logger.info("Uploading data to S3 - {}...".format(file_name))
                            mybucket.put_object(Key=s3_path + file_name, Body=zipf.read(file))
            logger.info("Successfully extracted file!")

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

    # setup S3
    s3hook = connect_to_s3()
    AWS_S3_BUCKET_NAME = get_s3_bucket_name()
    mybucket = s3hook.get_bucket(AWS_S3_BUCKET_NAME)

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
            if s3hook.check_for_key(key=s3_path, bucket_name=AWS_S3_BUCKET_NAME):
                logger.warning("Key {} already existed on S3, skipped!".format(s3_path))

            # if the file/key does not exist, upload file onto S3
            else:
                logger.info("Downloading data from FTP - {}...".format(file_path))
                # download file from FTP
                myfile = _ftp_download_file(ftp, entry)

                # upload file onto S3
                logger.info("Uploading data to S3 - {}...".format(s3_path))
                mybucket.put_object(Key=s3_path, Body=myfile)

    # connect FTP
    host = "ftp2.census.gov"
    ftp = _connect_ftp(host)
    # change the current work directory
    ftp.cwd(f"/programs-surveys/acs/summary_file/{year}/documentation/")
    # traverse the directory
    _ftp_traverse(ftp, _single_file_FTP_to_S3)

    ftp.quit()


###########################################################
# Aggregate and copy data from S3 to Snowflake
###########################################################
@run_with_logging()
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

    # setup S3
    s3hook = connect_to_s3()
    AWS_S3_BUCKET_NAME = get_s3_bucket_name()
    mybucket = s3hook.get_bucket(AWS_S3_BUCKET_NAME)

    # setup Snowflake
    con = connect_to_snowflake(database="CENSUS_DB", schema="RAW_ACS")
    cur = con.cursor()

    # get geo info file, concat each sheet into one df
    s3_path = f"RawData/ACS/summary_file/{year}/ACS_{year}_5yr_Mini_Geo.xlsx"
    myfile = io.BytesIO()
    mybucket.download_fileobj(Key=s3_path, Fileobj=myfile)
    xl = pd.ExcelFile(myfile)

    geo_all = pd.DataFrame()
    for sheet_name in xl.sheet_names:  # loop all sheet names
        logger.info(f"Loading data from state - {sheet_name}")
        geo_all = pd.concat([geo_all, xl.parse(sheet_name)], axis=0, sort=False)  # read a specific sheet to DataFrame

    geo_all = geo_all.loc[:, geo_all.columns[0]:geo_all.columns[3]]  # make sure we only keep first 4 columns
    geo_all.columns = ["STATE", "LOGRECNO", "GEOID",
                       "Name"]  # State abbreviation including US, Logical Record Number, Geography ID, Geography Name
    geo_all["LOGRECNO"] = geo_all["LOGRECNO"].astype(str).str.pad(7, 'left', '0')
    geo_all = geo_all.rename({"Name": "PLACE_NAME", "STATE": "STUSAB"}, axis=1)

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
    geo_all.loc[geo_all[geo_all["level"].isin(["040", "050", "140", "150"])].index, "State"] = \
    geo_all[geo_all["level"].isin(["040", "050", "140", "150"])]["place_list"].apply(
        lambda x: x[-1] if len(x) >= 1 else "NA")
    geo_all.loc[geo_all[geo_all["level"].isin(["050", "140", "150"])].index, "County"] = \
    geo_all[geo_all["level"].isin(["050", "140", "150"])]["place_list"].apply(lambda x: x[-2] if len(x) >= 2 else "NA")
    geo_all.loc[geo_all[geo_all["level"].isin(["140", "150"])].index, "Census_Tract"] = \
    geo_all[geo_all["level"].isin(["140", "150"])]["place_list"].apply(lambda x: x[-3] if len(x) >= 3 else "NA")
    geo_all.loc[geo_all[geo_all["level"].isin(["150"])].index, "Block_Group"] = geo_all[geo_all["level"].isin(["150"])][
        "place_list"].apply(lambda x: x[-4] if len(x) >= 4 else "NA")
    geo_all.loc[geo_all[geo_all["level"].isin(["310"])].index, "CBSA"] = geo_all[geo_all["level"].isin(["310"])][
        "PLACE_NAME"]
    geo_all.loc[geo_all[geo_all["level"].isin(["860"])].index, "ZCTA"] = geo_all[geo_all["level"].isin(["860"])][
                                                                             "PLACE_NAME"].str[-5:]
    geo_all.drop(["level", "component", "place_list"], axis=1, inplace=True)

    # stage data upto S3
    logger.info("Uploading staging csv onto S3...")
    csv_buffer = io.StringIO()
    geo_all.to_csv(csv_buffer, index=False)
    s3_path = f"RawData/ACS/geo_info/Mini_Geo_5yr_{year}.csv"
    mybucket.put_object(Key=s3_path, Body=csv_buffer.getvalue())

    # load to Snowflake
    logger.info("Copying data from S3 to Snowflake...")

    # create table
    cur.execute(
        (f"CREATE OR REPLACE TABLE GEOMETA{year}5 "
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
    cur.execute(
        f"COPY INTO GEOMETA{year}5 FROM @ACS_DATA_GEO/Mini_Geo_5yr_{year}.csv FILE_FORMAT = ACS_DATA_GEO ON_ERROR = 'ABORT_STATEMENT';"
    )

    # grant permissions
    cur.execute(
        f"GRANT SELECT ON TABLE GEOMETA{year}5 TO ROLE DATAENGINEERING_ROLE;"
    )

    con.close()


@run_with_logging()
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

    # setup S3
    s3hook = connect_to_s3()
    AWS_S3_BUCKET_NAME = get_s3_bucket_name()
    mybucket = s3hook.get_bucket(AWS_S3_BUCKET_NAME)

    # setup Snowflake
    con = connect_to_snowflake(database="CENSUS_DB", schema="RAW_ACS")
    cur = con.cursor()

    logger.info(f"Loading templates data - year {year}...")

    prefix = f"RawData/ACS/summary_file/{year}/data/5yr_templates/"
    path_list = s3hook.list_keys(bucket_name=AWS_S3_BUCKET_NAME, prefix=prefix)

    lookup_df = pd.DataFrame()
    for s3_path in path_list:
        seq = re.findall(r"templates/(?:S|s)eq(\d*)\.xls", s3_path)
        if seq:
            sequence = int(seq[0])
            logger.info(f"Loading sequence - {sequence}...")

            myfile = io.BytesIO()
            mybucket.download_fileobj(Key=s3_path, Fileobj=myfile)
            template = pd.read_excel(myfile)
            template = template.iloc[:, 6:]

            # concat data
            template = template.stack().to_frame().reset_index(level=1, col_level=0).reset_index(level=0).drop("index",
                                                                                                               axis=1)
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

    # load to Snowflake
    logger.info("Copying data from S3 to Snowflake...")

    # create table
    cur.execute(
        (f"CREATE OR REPLACE TABLE LOOKUP{year}5 "
         "(VARID VARCHAR(16) COMMENT 'Variable identifiers defined by US Census Bureau, indicate the original table and the column number of the variable', "
         "SEQ VARCHAR(3) COMMENT 'Sequence number for lookup', "
         "LABEL VARCHAR(512) COMMENT 'The label from the original table');")
    )

    # copy data
    cur.execute(
        f"COPY INTO LOOKUP{year}5 FROM @ACS_DATA_LOOKUP/Lookup_5yr_{year}.csv FILE_FORMAT = ACS_DATA_LOOKUP ON_ERROR = 'ABORT_STATEMENT';"
    )

    # grant permissions
    cur.execute(
        f"GRANT SELECT ON TABLE LOOKUP{year}5 TO ROLE DATAENGINEERING_ROLE;"
    )

    con.close()


@run_with_logging()
def copy_sequence_S3_to_Snowflake(logger, **kwargs):
    """
    Given the year and sequence number, cpoy the sequence data into Snowflake.
    """
    # Initialize variables
    sequence = kwargs["sequence"]
    year = kwargs["year"]

    # setup S3
    s3hook = connect_to_s3()
    AWS_S3_BUCKET_NAME = get_s3_bucket_name()

    # setup Snowflake
    con = connect_to_snowflake(database="CENSUS_DB", schema="RAW_ACS")
    cur = con.cursor()

    # get the headers from lookup table
    logger.info(f"Loading sequence {sequence}...")
    cur.execute(f"SELECT VARID FROM LOOKUP{year}5 WHERE SEQ={sequence};")
    result = cur.fetchall()
    headers = [t[0] for t in result]

    # check if the sequence file exists
    if not headers:
        logger.warning(f"Sequence {sequence} in Lookup table LOOKUP{year}5 on Snowflake does not exist!")
    else:
        # create table
        header_fixed = ["FILEID", "FILETYPE", "STUSAB", "CHARITER", "SEQUENCE", "LOGRECNO"]
        logger.info(
            f"Creating tables - E{year}5{sequence:04}, M{year}5{sequence:04}...")
        cur.execute(
            "CREATE OR REPLACE TABLE E{}5{:04} ({});".format(year, sequence, " VARCHAR(16), ".join(
                header_fixed) + " VARCHAR(16), " + " NUMBER(16,2), ".join(headers) + " NUMBER(16,2)")
        )

        cur.execute(
            "CREATE OR REPLACE TABLE M{}5{:04} ({});".format(year, sequence, " VARCHAR(16), ".join(
                header_fixed) + " VARCHAR(16), " + " NUMBER(16,2), ".join(headers) + " NUMBER(16,2)")
        )

        # copy data into tables
        prefix = f"RawData/ACS/summary_file/{year}/data/5_year_seq_by_state/"
        path_list = s3hook.list_prefixes(bucket_name=AWS_S3_BUCKET_NAME, prefix=prefix, delimiter="/")

        # loop all states
        for path_root in path_list:
            logger.info(f"Parsing sub folder - {path_root}")

            # loop all levels
            for level in ["All_Geographies_Not_Tracts_Block_Groups", "Tracts_Block_Groups_Only"]:
                logger.info(f"Parsing level - {level}")

                # get state abbreviation from a csv file starts with g{year} in each sub-folder
                s3_path = f"{path_root}{level}/g{year}"
                state_abbr = s3hook.list_keys(bucket_name=AWS_S3_BUCKET_NAME, prefix=s3_path)[0][-6:-4]

                if level == "Tracts_Block_Groups_Only" and state_abbr == "us":
                    logger.info(f"No Tracts_Block_Groups_Only level for US.")

                else:
                    aws_path = (path_root + level).replace("RawData/ACS/summary_file/", "")
                    # copy data
                    logger.info(f"Copying data from S3 to Snowflake - sequence: {sequence}, state: {state_abbr}...")

                    cur.execute(
                        f"COPY INTO E{year}5{sequence:04} FROM @ACS_DATA_SEQ/{aws_path}/e{year}5{state_abbr}{sequence:04}000.txt FILE_FORMAT = ACS_DATA_SEQ ON_ERROR = 'ABORT_STATEMENT';"
                    )

                    cur.execute(
                        f"COPY INTO M{year}5{sequence:04} FROM @ACS_DATA_SEQ/{aws_path}/m{year}5{state_abbr}{sequence:04}000.txt FILE_FORMAT = ACS_DATA_SEQ ON_ERROR = 'ABORT_STATEMENT';"
                    )

                    # grant permissions
                    cur.execute(
                        f"GRANT SELECT ON TABLE E{year}5{sequence:04} TO ROLE DATAENGINEERING_ROLE;"
                    )

                    cur.execute(
                        f"GRANT SELECT ON TABLE M{year}5{sequence:04} TO ROLE DATAENGINEERING_ROLE;"
                    )

        con.close()


###########################################################
# Update VARIABLE_LIST and FACT tables on Snowflake
###########################################################
@run_with_logging()
def update_geometa(logger, **kwargs):
    """
    Copy the ACS_variable_list.csv file from local file system to S3.
    """
    dbt_dir = kwargs.get("dbt_dir", "/root/airflow/code/dags/code_for_kubernetes_pod_operator/ACS/dbt")
    dbt_profiles_dir = kwargs.get("dbt_profiles_dir", "/root/.dbt")
    var_dic = {"current_year": datetime.today().year,
               "raw_data_schema": "RAW_ACS" if Variable.get("AIRFLOW_ENV") == "PROD" else "TEST_RAW_ACS"}
    run_dbt(dbt_dir=dbt_dir, dbt_profiles_dir=dbt_profiles_dir, model_name="GEOMETA", var_dic=var_dic)

@run_with_logging()
def upload_variable_list_to_S3(logger, **kwargs):
    """
    Copy the ACS_variable_list.csv file from local file system to S3.
    """
    # setup S3
    s3hook = connect_to_s3()
    AWS_S3_BUCKET_NAME = get_s3_bucket_name()
    mybucket = s3hook.get_bucket(AWS_S3_BUCKET_NAME)

    # upload ACS_variable_list.csv from local file system to S3.
    logger.info("Uploading ACS_variable_list.csv local file system to S3...")
    file_path = "ACS_variable_list.csv"
    s3_path = "RawData/ACS/ACS_variable_list.csv"
    mybucket.put_object(Key=s3_path, Body=open(file_path, "rb"))

    logger.info("Uploading ACS_variable_list.xlsx local file system to S3...")
    file_path = "ACS_variable_list.xlsx"
    s3_path = "RawData/ACS/ACS_variable_list.xlsx"
    mybucket.put_object(Key=s3_path, Body=open(file_path, "rb"))


@run_with_logging()
def upload_variable_list_to_Snowflake(logger, **kwargs):
    """
    Copy VARIABLE_LIST data from S3 to Snowflake.
    """
    # Initialize variables
    year = kwargs["year"]

    # setup Snowflake
    con = connect_to_snowflake(database="CENSUS_DB", schema="PROD_ACS")
    cur = con.cursor()

    # load to Snowflake
    logger.info("Copying ACS_variable_list.csv from S3 to Snowflake...")

    # create table
    cur.execute(
        (f"CREATE OR REPLACE TABLE VARIABLE_LIST "
         "(GROUP_NAME VARCHAR(256) COMMENT 'Variable groups in terms of purposes', "
         "ROOFSTOCK_NAME VARCHAR(256) COMMENT 'Human-readable variable names', "
         "FORMULA VARCHAR(512) COMMENT 'Formula for the calculation of the variable', "
         "STATISTIC VARCHAR(16) COMMENT 'Statistic types, e.g., count, mean, median, rate, etc.', "
         "START_YEAR NUMBER(4,0) COMMENT 'The earliest available year', "
         "END_YEAR NUMBER(4,0) COMMENT 'The latest available year, 9999 means variable will available for all the following years after the earliest available year', "
         "NOTES VARCHAR(256) COMMENT 'Notes');")
    )

    # copy data
    cur.execute(
        f"COPY INTO VARIABLE_LIST FROM @ACS_VARIABLE_LIST/ACS_variable_list.csv FILE_FORMAT = ACS_VARIABLE_LIST ON_ERROR = 'ABORT_STATEMENT';"
    )

    # replace end_year 9999 with the current year
    cur.execute(
        f"UPDATE VARIABLE_LIST SET END_YEAR = {year} WHERE END_YEAR = 9999;"
    )

    # grant permissions
    for role in ["DATAENGINEERING_ROLE", "ANALYST_ROLE", "DATASCIENCE_ROLE", "BUSINESS_ROLE", "ETL_ROLE"]:
        cur.execute(
            f"GRANT SELECT ON TABLE VARIABLE_LIST TO ROLE {role};"
        )

    con.close()


@run_with_logging()
def create_fact_table(logger, **kwargs):
    """
    Create table CENSUS_DB.PROD_ACS.FACT on Snowflake
    """
    # setup Snowflake
    con = connect_to_snowflake(database="CENSUS_DB", schema="PROD_ACS")
    cur = con.cursor()

    # Read table CENSUS_DB.PROD_ACS.VARIABLE_LIST from Snowflake
    logger.info("Reading table CENSUS_DB.PROD_ACS.VARIABLE_LIST from Snowflake...")
    sql = "SELECT * FROM VARIABLE_LIST;"
    df = pd.read_sql(sql=sql, con=con)

    # Create table CENSUS_DB.PROD_ACS.FACT
    logger.info("Creating table CENSUS_DB.PROD_ACS.FACT...")
    df["col_type"] = df["STATISTIC"].apply(
        lambda x: "NUMBER(9, 6)" if x in ["rate", "percentage"] else "NUMBER(16, 2)").values.tolist()
    df["col_type"] = df["ROOFSTOCK_NAME"] + " " + df["col_type"]
    col_type = df["col_type"].unique().tolist()

    cur.execute(
        "CREATE OR REPLACE TABLE FACT (YEAR NUMBER(4,0), GEOID VARCHAR(32), {});".format(
            ", ".join(col_type))
    )

    logger.info("Copying GEO INFO from GEOMETA to CENSUS_DB.PROD_ACS.FACT...")
    cur.execute(
        ("INSERT INTO FACT (YEAR, GEOID) "
         "SELECT YEAR, GEOID FROM GEOMETA;")
    )

    # grant permissions
    for role in ["DATAENGINEERING_ROLE", "ANALYST_ROLE", "DATASCIENCE_ROLE", "BUSINESS_ROLE", "ETL_ROLE"]:
        cur.execute(
            f"GRANT SELECT ON TABLE FACT TO ROLE {role};"
        )

    con.close()


@run_with_logging()
def pull_variables_from_raw_tables(logger, **kwargs):
    """
    Pull variables from raw tables.
    """
    # Initialize variables
    year = kwargs["year"]
    dbt_dir = kwargs.get("dbt_dir", "/root/airflow/code/dags/code_for_kubernetes_pod_operator/ACS/dbt")
    dbt_profiles_dir = kwargs.get("dbt_profiles_dir", "/root/.dbt")
    AIRFLOW_ENV = Variable.get("AIRFLOW_ENV")

    def _fix_divided_by_0(formula):
        p = formula.find("/")
        if p != -1:
            return formula[:p + 1] + " NULLIF(" + formula[p + 2:] + ", 0)"
        else:
            return formula

    # setup Snowflake
    con = connect_to_snowflake(database="CENSUS_DB", schema="PROD_ACS")
    cur = con.cursor()

    # Read table CENSUS_DB.PROD_ACS.VARIABLE_LIST from Snowflake
    sql = "SELECT * FROM VARIABLE_LIST;"
    df = pd.read_sql(sql=sql, con=con)
    print(df.columns)
    grouped = df.groupby("GROUP_NAME")

    for group_name, group_df in grouped:
        logger.info(f"Building temporary table ACS_BY_GROUP_TEMP for the group {group_name}...")

        variables = list(set(group_df["FORMULA"].str.findall("(?:B|C)[\w\d]*_[\w\d]*").sum()))
        roofstock_name_list = group_df["ROOFSTOCK_NAME"].tolist()
        roofstock_names = ", ".join(roofstock_name_list)
        formula_list = group_df["FORMULA"].tolist()
        formula_list = [_fix_divided_by_0(f) for f in formula_list]
        select_col = ", ".join([pair[0] + " as " + pair[1] for pair in zip(formula_list, roofstock_name_list)])
        start_year = group_df["START_YEAR"].max()
        end_year = min(group_df["END_YEAR"].max(), year)

        var_dic = {"variables": variables,
                   "select_col": select_col,
                   "roofstock_names": roofstock_names,
                   "start_year": 2017,
                   "end_year": 2017,
                   "raw_data_schema": "RAW_ACS" if AIRFLOW_ENV == "PROD" else "TEST_RAW_ACS"}

        run_dbt(dbt_dir=dbt_dir, dbt_profiles_dir=dbt_profiles_dir, model_name="ACS_BY_GROUP_TEMP", var_dic=var_dic)

        for col in roofstock_name_list:
            logger.info(f"Copying column {col} from ACS_BY_GROUP_TEMP to FACT...")

            # copy data
            cur.execute(
                (f"UPDATE FACT AS target "
                 f"SET {col} = source.{col} "
                 f"FROM ACS_BY_GROUP_TEMP AS source "
                 f"WHERE target.GEOID = source.GEOID AND target.YEAR = source.YEAR AND source.{col} IS NOT NULL;")
            )

        cur.execute(
            f"DROP TABLE ACS_BY_GROUP_TEMP;"
        )

    con.close()

if __name__ == "__main__":
    year = 2017
    # docs_FTP_to_S3(**{"year": year})
    # template_FTP_to_S3(**{"year": year})
    # sequence_FTP_to_S3(**{"year": year, "state": "California"})
    #
    # copy_geo_S3_to_Snowflake(**{"year": year})
    # copy_lookup_S3_to_Snowflake(**{"year": year})
    # copy_sequence_S3_to_Snowflake(**{"year": year, "sequence": 1})
    #
    # update_geometa(**{"dbt_dir": "/Users/jiaxunsong/Desktop/Roofstock/datatools/airflow_git/dags/code_for_kubernetes_pod_operator/ACS/dbt",
    #                   "dbt_profiles_dir": "/Users/jiaxunsong/Desktop/Roofstock/datatools/airflow_git/.dbt"})
    # upload_variable_list_to_S3()
    # upload_variable_list_to_Snowflake(**{"year": year})
    # create_fact_table()
    pull_variables_from_raw_tables(**{"year": year,
                                      "dbt_dir": "/Users/jiaxunsong/Desktop/Roofstock/datatools/airflow_git/dags/code_for_kubernetes_pod_operator/ACS/dbt",
                                      "dbt_profiles_dir": "/Users/jiaxunsong/Desktop/Roofstock/datatools/airflow_git/.dbt"})
