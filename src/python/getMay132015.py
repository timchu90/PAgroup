# Extract data for may 13th
# Should be run directly on kaggle, using kaggle scripts

import sqlite3
import pandas as pd

conn = sqlite3.connect('../input/database.sqlite')

top10query = """CREATE TEMP TABLE RedditTop10Ct AS
      SELECT subreddit_id, COUNT(*) as ct
       FROM May2015 GROUP BY subreddit_id 
       ORDER BY ct DESC LIMIT 10"""
       
conn.execute(top10query)

EPOCH_START = 1430438400 #00:00:00 May 1st 2015
DAY_LENGTH = 60 * 60 * 24 #60 Seconds * 60 Minutes * 24 Hours
DAY_NUMBER = 13
START = EPOCH_START + (DAY_NUMBER - 1) * DAY_LENGTH
END = START + DAY_LENGTH

top1dayquery = """CREATE TEMP TABLE OneDay AS
    SELECT * from May2015 WHERE subreddit_id in (SELECT subreddit_id FROM RedditTop10Ct)
    """ + "AND created_utc >= " + str(START) + " AND created_utc < " + str(END) 


conn.execute(top1dayquery)
df = pd.read_sql("SELECT * FROM OneDay", conn)
df.to_pickle("reddit_may13_pandas_pickle")
df.to_csv("reddit_may13.csv", index = False)

