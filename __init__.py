from elasticsearch import Elasticsearch

def connect_es(severname, port):
    """HOW TO USE
    Default
    severname = 'localhost'
    port = 9200
    """

    client = Elasticsearch([{'host': severname, 'port': port}])
    return client

def insert_raw_data(client, data):
  
    client.index(index="raws",
                 doc_type="raw",
                 body=data)


def insert_news_data(client, data):
    client.index(index="raws",
                 doc_type="raw",
                 body=data)

client = connect_es("localhost", 9200)