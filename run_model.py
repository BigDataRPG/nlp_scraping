from model.__init__ import news_predict_NER
from datetime import datetime
import json
import pandas as pd
from __init__ import client, insert_raw_data

if __name__ == "__main__":

    # Initiate Constant
    PATH_DIRECTORY = "/Users/redthegx/project/nlp_scraping/raw_news/"
    PREFIX_FILE = "raw_scrapped_"
    POSTFIX_FILE = ".json"
    TODAY = str(datetime.today())[:10].replace("-", "")

    # Save Scraping Raw News to Directory Folder
    with open(PATH_DIRECTORY + PREFIX_FILE + TODAY + POSTFIX_FILE, mode="r", encoding="utf-8") as json_file:
        json_data = json.load(json_file)

    # Transform data in order to insert into ES
    df = pd.read_json(json_data, keep_default_dates=True).T

    # Run Model Get Attributes
    df = news_predict_NER(df)

    # Transform data in order to insert into ES
    json_form = df.to_dict(orient='records')

    # Insert records to ES
    for i, data in enumerate(json_form):
        insert_raw_data(client, data)
        if i % 100 == 0:
            print("Successfully insert data to ES {}".format(i))