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

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
# pd.set_option('display.width', 5)


@run_with_logging()
def attachment_to_s3(logger, **kwargs):
    """
    Function
    ----------------------
    Monitor Office 365 Outlook Inbox,
    whenever the notification email is received,
    get email attachment into S3

    Parameter
    ----------------------
    None

    Return
    ----------------------
    None
    """
    logger.info(kwargs["test"])
    # get airflow env
    AIRFLOW_ENV = Variable.get("AIRFLOW_ENV")

    # setup S3
    s3hook = connect_to_s3()
    AWS_S3_BUCKET_NAME = Variable.get("AWS_S3_NAME")

    # setup Snowflake
    DATABASE = "SCHOOLSCORES_DB"
    SCHEMA = "RAW_GREATSCHOOLS" if AIRFLOW_ENV == "PROD" else AIRFLOW_ENV + "_" + "RAW_GREATSCHOOLS"
    con = connect_to_snowflake(database=DATABASE, schema=SCHEMA)

    # load tracker file (a csv file tracks the download history)
    # create the schema and table on snowflake first!
    greatschools_tracker = pd.read_sql_query("SELECT * FROM DOWNLOAD_HISTORY", con)
    logger.info("Information in DOWNLOAD_HISTORY:")
    print(greatschools_tracker)

    # If there is update, has_update will be True
    has_update = False

    # connect with the email inbox
    mail = imaplib.IMAP4_SSL('outlook.office365.com', port=993)
    mail.login(Variable.get("EMAIL_FROMADDR"), Variable.get("EMAIL_FROMADDR_PASSWORD"))
    mail.select("INBOX")

    # search for the emails from the organization
    typ, data = mail.search(None, "(FROM feedsupport@greatschools.org)")
    id_list = str(data[0], encoding="utf8").split()

    # iterate all the emails
    for mail_id in id_list:
        typ, data = mail.fetch(mail_id, "(RFC822)")
        text = data[0][1]
        msg = email.message_from_bytes(text)
        # prepare the information for the tracker file
        retrieve_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        email_datetime = datetime.strptime(time.strftime("%Y-%m-%d %H:%M:%S", email.utils.parsedate(msg["Date"])),
                                           "%Y-%m-%d %H:%M:%S")

        if AIRFLOW_ENV == "PROD":
            stage_location = f"RawData/GreatSchools/{email_datetime.strftime('%Y%m%d')}"
        else:
            stage_location = f"RawData/GreatSchools/{AIRFLOW_ENV}/{email_datetime.strftime('%Y%m%d')}"

        email_subject = msg["Subject"]
        is_feed_update = "GreatSchools Feed Update" in email_subject

        # if the email has already been retrieved, skip it
        if np.datetime64(email_datetime) in greatschools_tracker["EMAIL_DATETIME"].values and greatschools_tracker.loc[
            greatschools_tracker["EMAIL_DATETIME"] == np.datetime64(email_datetime), "UPDATED"].values[0]:
            continue

        # save the information in the tracker file
        if np.datetime64(email_datetime) not in greatschools_tracker["EMAIL_DATETIME"].values:
            con.cursor().execute(
                "INSERT INTO DOWNLOAD_HISTORY(RETRIEVE_DATETIME, EMAIL_DATETIME, EMAIL_SUBJECT, IS_FEED_UPDATE, UPDATED, STAGE_LOCATION) "
                "VALUES('{}','{}','{}',{},{},'{}')".format(retrieve_datetime, email_datetime, email_subject,
                                                           is_feed_update, False, stage_location))

        # if the email is not for feed update, skip it
        if not is_feed_update:
            continue
        has_update = True

        logger.info(f"Found feed update email: received at {email_datetime}, subject: {email_subject}!")

        for part in msg.walk():
            # walk through the email, find the attachment part and download it
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            filename = part.get_filename()
            logger.info(f"Found attachment: {filename}!")
            data = part.get_payload(decode=True)
            if not data:
                continue

            # download attachment
            s3_path = f"{stage_location}/{filename}"
            s3hook.load_file_obj(file_obj=io.BytesIO(data), key=s3_path, bucket_name=AWS_S3_BUCKET_NAME, replace=True)

    # Push XComs
    xcom = {"stage_location": stage_location if has_update else ""}
    pod_xcom_push(xcom)

    mail.close()
    mail.logout()
    con.close()


@run_with_logging()
def staging_to_s3(logger, **kwargs):
    """
    Function
    ----------------------
    Download zip files from greatschools to Airflow VM,
    upload the zip files to S3,
    unzip these files.
    Aggregate the data into four csv files,
    upload these csv files to S3,
    delete the raw data from VM disk.

    Parameter
    ----------------------
    None

    Return
    ----------------------
    None
    """
    # Initialize variables
    s3_folder = pod_xcom_pull("greatschools_ingest", "attachment_to_s3", "stage_location")
    logger.info(f"Got stage loaction: {s3_folder}")

    # set up S3
    s3hook = connect_to_s3()
    AWS_S3_BUCKET_NAME = Variable.get("AWS_S3_NAME")
    mybucket = s3hook.get_bucket(AWS_S3_BUCKET_NAME)

    # Download files from greatschools to memory
    file_list = ["local-greatschools-feed-flat.zip",
                 "local-gs-official-overall-rating-result-feed-flat.zip",
                 "local-gs-official-overall-rating-feed-flat.zip",
                 "local-greatschools-city-feed-flat.zip"]

    for filename in file_list:
        logger.info(f"Downloading file - {filename}...")
        url = f"http://www.greatschools.org/feeds/flat_files/{filename}"
        r = requests.get(url, timeout=5,
                         auth=(Variable.get("GREATSCHOOLS_USERNAME"), Variable.get("GREATSCHOOLS_PASSWORD")))
        data = io.BytesIO(r.content)

        logger.info(f"Uploading file - {filename}...")
        s3_path = f"{s3_folder}/{filename}"
        mybucket.put_object(Key=s3_path, Body=data)

        logger.info(f"Extracting file - {filename}...")
        # create one empty pandas DataFrame
        df_all = pd.DataFrame()
        with zipfile.ZipFile(data, "r") as zipf:
            # Walk through the files, aggregate data inside each zip
            for file in zipf.infolist():
                file_name = file.filename
                logger.info(file_name)
                try:
                    # for some unknown reasons, there are extra tabs in the data
                    data_by_state = pd.read_csv(io.BytesIO(zipf.read(file)), sep="\t", dtype=str)
                except:
                    logger.warning(f"Dirty data - {file_name}! Trying to clean it...")
                    file_data = zipf.read(file).decode("utf-8")
                    file_data = file_data.replace("\t \t", "\t")
                    data_by_state = pd.read_csv(io.StringIO(file_data), sep='\t', dtype=str)
                df_all = pd.concat([df_all, data_by_state], axis=0, sort=False)
        logger.info(f"Extracted file - {filename} successfully!")

        # Clean the columns
        df_all.columns = [col.replace("-", "") for col in df_all.columns.tolist()]

        # Upload file onto S3
        logger.info(f"Uploading {filename[:-4]}.csv onto S3...")
        csv_buffer = io.StringIO()
        df_all.to_csv(csv_buffer, index=False, sep=",")
        s3_path = f"{s3_folder}/{filename[:-4].replace('-', '_')}.csv"
        mybucket.put_object(Key=s3_path, Body=csv_buffer.getvalue())


@run_with_logging()
def copy_to_snowflake(logger, **kwargs):
    """
    Function
    ----------------------
    Create or replace tables,
    copy data from S3 to Snowflake.

    Parameter
    ----------------------
    None

    Return
    ----------------------
    None
    """
    # get airflow env
    AIRFLOW_ENV = Variable.get("AIRFLOW_ENV")

    # Initialize variables
    s3_folder = pod_xcom_pull("greatschools_ingest", "attachment_to_s3", "stage_location")
    logger.info(f"Got stage loaction: {s3_folder}")
    s3_subfolder = s3_folder.replace("RawData/GreatSchools/", "")

    # Snowflake setup.
    DATABASE = "SCHOOLSCORES_DB"
    SCHEMA = "RAW_GREATSCHOOLS" if AIRFLOW_ENV == "PROD" else AIRFLOW_ENV + "_" + "RAW_GREATSCHOOLS"
    con = connect_to_snowflake(database=DATABASE, schema=SCHEMA)
    cur = con.cursor()

    logger.info("Creating table GsDirectory and copying data...")
    cur.execute(
        (f"CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.GsDirectory "
         "(entity VARCHAR(16), "
         "gsid VARCHAR(16), "
         "universalid VARCHAR(16), "
         "ncescode VARCHAR(16), "
         "stateid VARCHAR(32), "
         "name VARCHAR(128), "
         "description VARCHAR(512), "
         "schoolsummary VARCHAR(2048), "
         "levelcode VARCHAR(16), "
         "level VARCHAR(32), "
         "street VARCHAR(128), "
         "city VARCHAR(64), "
         "state VARCHAR(2), "
         "zip VARCHAR(16), "
         "phone VARCHAR(32), "
         "fax VARCHAR(32), "
         "county VARCHAR(64), "
         "fipscounty VARCHAR(5), "
         "lat NUMBER(10, 6), "
         "lon NUMBER(10, 6), "
         "website VARCHAR(1024), "
         "subtype VARCHAR(1024), "
         "overviewurl VARCHAR(1024), "
         "parentreviewsurl VARCHAR(1024), "
         "testscoresurl VARCHAR(512), "
         "type VARCHAR(16), "
         "districtid VARCHAR(16), "
         "universaldistrictid VARCHAR(16), "
         "districtname VARCHAR(512), "
         "districtspending VARCHAR(16));")
    )

    cur.execute(
        (f"COPY INTO {DATABASE}.{SCHEMA}.GsDirectory "
         f"FROM @GREATSCHOOLS/{s3_subfolder}/local_greatschools_feed_flat.csv "
         "FILE_FORMAT = GREATSCHOOLS ON_ERROR = 'ABORT_STATEMENT';")
    )

    logger.info("Creating table GsRatings and copying data...")
    cur.execute(
        (f"CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.GsRatings "
         "(testratingid VARCHAR(10), "
         "universalid VARCHAR(10), "
         "rating NUMBER(2), "
         "url VARCHAR(512));")
    )

    cur.execute(
        (f"COPY INTO {DATABASE}.{SCHEMA}.GsRatings "
         f"FROM @GREATSCHOOLS/{s3_subfolder}/local_gs_official_overall_rating_result_feed_flat.csv "
         "FILE_FORMAT = GREATSCHOOLS ON_ERROR = 'ABORT_STATEMENT';")
    )

    logger.info("Creating table GsCityRatings and copying data...")
    cur.execute(
        (f"CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.GsCityRatings "
         "(id VARCHAR(10), "
         "name VARCHAR(128), "
         "state VARCHAR(2), "
         "rating NUMBER(2), "
         "url VARCHAR(512), "
         "lat NUMBER(10, 6), "
         "lon NUMBER(10, 6), "
         "active NUMBER(1));")
    )

    cur.execute(
        (f"COPY INTO {DATABASE}.{SCHEMA}.GsCityRatings "
         f"FROM @GREATSCHOOLS/{s3_subfolder}/local_greatschools_city_feed_flat.csv "
         "FILE_FORMAT = GREATSCHOOLS ON_ERROR = 'ABORT_STATEMENT';")
    )
    logger.info("Creating table GsDictionary and copying data...")
    cur.execute(
        (f"CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.GsDictionary "
         "(id VARCHAR(10), "
         "year NUMBER(4), "
         "description VARCHAR(1024));")
    )

    cur.execute(
        (f"COPY INTO {DATABASE}.{SCHEMA}.GsDictionary "
         f"FROM @GREATSCHOOLS/{s3_subfolder}/local_gs_official_overall_rating_feed_flat.csv "
         "FILE_FORMAT = GREATSCHOOLS ON_ERROR = 'ABORT_STATEMENT';")
    )

    logger.info("Granting permission on new tables...")
    for TABLE in ["GsDirectory", "GsRatings", "GsCityRatings", "GsDictionary"]:
        cur.execute(
            f"GRANT OWNERSHIP ON {DATABASE}.{SCHEMA}.{TABLE} TO ETL_ROLE;"
        )
        for ROLE in ["DATAENGINEERING_ROLE", "DATASCIENCE_ROLE", "ANALYST_ROLE"]:
            cur.execute(
                f"GRANT SELECT ON {DATABASE}.{SCHEMA}.{TABLE} TO {ROLE};"
            )

    # update DOWNLOAD_HISTORY
    logger.info("Updating DOWNLOAD_HISTORY table...")
    max_date = datetime.strptime(s3_subfolder[-8:], "%Y%m%d").strftime("%Y-%m-%d")
    cur.execute(
        f"UPDATE {DATABASE}.{SCHEMA}.DOWNLOAD_HISTORY "
        "SET UPDATED = TRUE "
        f"WHERE TO_DATE(EMAIL_DATETIME) <= '{max_date}';"
    )

    con.close()


if __name__ == "__main__":
    # attachment_to_s3()
    # staging_to_s3()
    copy_to_snowflake()
