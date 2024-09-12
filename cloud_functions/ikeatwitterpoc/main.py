#Import Required Libraries

import tweepy 
import pandas as pd
import csv
import re 
import string
import preprocessor as p
import csvwriter
import os
import google.cloud.logging
import logging
from google.cloud import storage
from google.cloud import bigquery
import EnvConfig as config

# Setup logging.
log_client = google.cloud.logging.Client()
log_client.setup_logging()

#Assingn secrets and Tokens to Fetch the data from Twitter API
consumer_key = config.consumer_key
consumer_secret = config.consumer_secret
access_key= config.access_key
access_secret = config.access_secret

#Assign other important variables
project_id = config.project_id
print(project_id)

def fetch(message):
    #Authorize with Tweepy’s OAuthhandler
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)

    #pass these authorization details to tweepy
    api = tweepy.API(auth,wait_on_rate_limit=True)

    """
    Extracting Specific Tweets from Twitter for each tweet in the Tweepy Cursor, 
    we search for the key words and pass it to write the contents into a csv file as shown after utf-8 encoding 

    """

    csvFile = open('ikea_twitter.csv', 'a')
    csvWriter = csv.writer(csvFile)

    search_words = "#IKEA"   
    new_search = search_words + " -filter:retweets"

    """
    In the code snippet below, I wish to retrieve the following attributes from the Metadata
    1. id
    2. created_at
    3. text
    4. screen_name/handle
    5. verified
    6. followers_count
    7. favorites_count
    8. retweet_count
    9. location

    """
    for tweet in tweepy.Cursor(api.search_tweets,q=new_search,count=1000,
                                lang="en",
                                since_id=0).items():
        csvWriter.writerow([tweet.id, tweet.created_at, tweet.text.encode('utf-8'),tweet.user.screen_name.encode('utf-8'), tweet.user.verified,tweet.user.followers_count,tweet.favorite_count, tweet.retweet_count, tweet.user.location.encode('utf-8')])
        logging.info(f"Fetched required Metadata from Twitter API")
        
    #Move the .csv file from local to cloud storage bucket
    client = storage.Client(project=project_id)
    mybucket = storage.bucket.Bucket(client=client, name=config.bucket)
    mydatapath = config.localpath 
    blob = mybucket.blob(config.blobpath)
    blob.upload_from_filename(mydatapath + 'ikea_twitter.csv')
    logging.info(f"uploaded Files to the cloud storage bucket")

    # Load the data from cloud storage bucket to Bigquery database to view the data in canonical format.
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set table_id to the ID of the table to create.
    # table_id = "project.dataset.table_name"
    table_id =project_id+'.'+config.dataset_id+'.'+config.table

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = config.uri

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    logging.info("Loaded {} rows.".format(destination_table.num_rows))

fetch()
