# Project: Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Data Sources

The data for this project is available on Amazon S3:

    s3://udacity-dend/

### Song Data

Each file is in JSON format and contains metadata about a song and the artist of that song.

Below is an example of what a single song file, TRAABJL12903CDCF1A.json

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Log Data

The log files in the dataset you'll be working with are partitioned by year and month.

![log-data](./img/log-data.png)

## Usage

### Configuration

Provide the information of your IAM-Role in the config file `dl.cfg`. Put in the information for your IAM-Role that can read and write S3 buckets.

    [S3]
    AWS_ACCESS_KEY_ID=
    AWS_SECRET_ACCESS_KEY=

### ETL pipeline

run the ETL script.

    python etl.py

### Notebook

> In case the EC2 cluster not completes the `etl.py` execution you are able to run te process through the file `sparkify.ipynb`
