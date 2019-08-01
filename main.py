from __init__ import client, insert_data
from scraping import news_daily

import pandas as pd
from datetime import datetime
import json
import os


if __name__ == "__main__":

    # Initiate Constant
    PATH_DIRECTORY = "/Users/redthegx/project/nlp_scraping/raw_news/"
    PREFIX_FILE = "raw_scrapped_"
    POSTFIX_FILE = ".json"
    TODAY = str(datetime.today())[:10].replace("-","")

    # Create Directory Folder to Save Scraping Json File
    # Name depend on Daily date
    if not os.path.exists(PATH_DIRECTORY):
        os.makedirs(PATH_DIRECTORY)

    # Run Scraping Raw News
    news_json = news_daily()

    # Save Scraping Raw News to Directory Folder
    with open(PATH_DIRECTORY + PREFIX_FILE + TODAY + POSTFIX_FILE, mode="w", encoding="utf-8") as json_file:
        json_file.write(json.dumps(news_json))

    # Transform data in order to insert into ES
    df = pd.read_json(news_json, keep_default_dates=True).T
    json_form = df.to_dict(orient='records')

    # Insert records to ES
    for i, data in enumerate(json_form):
        insert_data(client, data)
        if i % 100 == 0:
            print("Successfully insert data to ES {}".format(i))