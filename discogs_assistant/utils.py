from pymongo import MongoClient
import os, sys, re, time, json
from os import listdir
from os.path import isfile, join
import psycopg2
from google.cloud import pubsub_v1
from psycopg2.extras import execute_values
import requests
import lxml.html
from itertools import chain
from google.cloud import storage

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_NAME')
BUCKET_NAME = os.environ.get('RECOMMENDATION_MODEL_BUCKET')
flow_control = pubsub_v1.types.FlowControl(max_messages=10)

class MongoCollection:
    '''
    Connect to mongodb and return collection within context manager.
    '''

    def __init__(self, collection):
        host = os.environ.get('MONGO_HOST')
        user = os.environ.get('MONGO_DISCOGS_USER')
        pwd = os.environ.get('MONGO_DISCOGS_AUTH')
        db = os.environ.get('MONGO_DISCOGS_DB')
        uri = f'mongodb://{user}:{pwd}@{host}/{db}'
        self.client = MongoClient(uri)
        self.db = self.client.get_default_database()
        self.collection = self.db[collection]

    def __enter__(self):
        return self.collection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.close()

def connect_psql(ssl=False):
    '''
    Connects to Postgres with Discogs environment variables.

    Returns:
        pyscopg2.connection:
            Connection object to Postgres database
    '''
    # get environment variables
    host = os.environ.get('PSQL_DISCOGS_HOST')
    user = os.environ.get('PSQL_DISCOGS_USER')
    pwd = os.environ.get('PSQL_DISCOGS_PWD')
    dbname = os.environ.get('PSQL_DISCOGS_DBNAME')
    sslkey = os.environ.get('PSQL_SSL_KEY')
    sslcert = os.environ.get('PSQL_SSL_CERT')
    rootcert = os.environ.get('PSQL_ROOT_CERT')

    # connect
    conn = conn = psycopg2.connect(host=host, user=user, password=pwd, dbname=dbname)
    if ssl:
        conn = psycopg2.connect(host=host, user=user, password=pwd, dbname=dbname,
                                sslcert=sslcert, sslrootcert=rootcert,
                                sslkey=sslkey, sslmode='verify-ca')
    return conn

def get_pickle_directory():
    '''
    Returns path to directory where input-matrix and model pkl files live.

    Returns:
        str:
            Path to pickle directory.
    '''
    regex = '.+DiscogsAssistantModeling$'
    base_path = [path for path in sys.path if re.search(regex, path)][0]
    pickle_dir = base_path + '/modeling/pickle/'

    return pickle_dir

def get_recent_pickle(prefix):
    '''
    Retrieves path for pickle-object in pickle-directory given a particular prefix.

    Args:
        prefix (str):
            Category of pickle object.  e.g. "data" or "model"

    Returns:
        str:
            Full path of most recent pickle object given prefix category.
    '''
    pickle_dir = get_pickle_directory()
    # get list of files in pickle directory
    # TODO: add step to make sure you are in correct working directory
    onlyfiles = [f for f in listdir(pickle_dir) if isfile(join(pickle_dir, f))]
    # get only pickles with correct prefix
    result = [f for f in onlyfiles if re.search(f'^{prefix}_\d.+\.pkl$', f)]
    
    # filenames have a timestamp, so reverse order yields most recent first
    result = sorted(result, reverse=True)
    
    return pickle_dir + result[0]

def timeit(method):
    '''
    Decorator for timing function.  

    Returns:
        function:
            Wrapped function, execution time gets printed.
    '''

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print('%r %2.2f sec' % \
              (method.__name__, te-ts))
        return result

    return timed

def publisher_setup(publisher_name): # move project_id to env_var
    '''
    Sets up GCP Pub/Sub publisher client to topic.

    Args:
        publisher_name (str):
            Topic name

    Returns:
        tuple:
            PublisherClient object pointing to topic, topic_path string.
    '''
    project_id = GCP_PROJECT_ID
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, publisher_name)
    return publisher, topic_path

def subscriber_setup(subscription_name): # move project_id to env_var
    '''
    Sets up GCP Pub/Sub subscriber client to topic.

    Args:
        subscription_name (str):
            Subscription name

    Returns:
        tuple:
            SubscriberClient object pointing to topic sub, subscription_path string.
    '''
    project_id = GCP_PROJECT_ID
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, 
                                                     subscription_name)
    return subscriber, subscription_path

def flatten_list(array):
    '''
    Flattens an iterable to a 1D-list.

    Args:
        array (iterable):
           Any iterable 

    Returns:
        list:
            Flattened list
    '''
    return list(chain.from_iterable(array))

def url_to_lxml(url):
    '''
    Makes a GET request to url and parses streamed response into lxml object.

    Args:
        url (str):
            Url to scrape.

    Returns:
        tree (lxml.etree._ElementTree):
            XPath-parseable element tree of User's wantlist or collection.
            None if url results in anything besides a 200 status code.
    '''
    response = requests.get(url, stream=True)
    response.raw.decode_content = True
    if response.status_code == 200:
        tree = lxml.html.parse(response.raw)
    else:
        tree = None
    return tree

def make_chunks(list_, size):
    '''
    Yield successive n-sized chunks from list.

    Args:
        list_ (list):
            List to be split into chunks
        size (int):
            Chunk-size

    Yields:
        list:
            Sucessive sub-lists of original list split into chunks of size n.
    '''
    for i in range(0, len(list_), size):
        yield list_[i:i + size]

def upload_blob(bucket_name, destination_blob_name, source_file_name):
    '''
    Uploads a blob to the GCP Storage bucket.

    Args:
        bucket_name (str):
            GCP Storage bucket to upload to.
        destination_blob_name (str):
            Filename to upload blob as.
        source_file_name (str):
            Path of source file being uploaded.
    '''
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    # 'model/filename' or 'input-matrix/filename'
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(source_file_name, rewind=True)

def download_blob(bucket_name, source_blob_name, destination_file_name):
    '''
    Downloads a blob from the GCP Storage bucket.

    Args:
        bucket_name (str):
            GCP Storage bucket to download from.
        source_blob_name (str):
            Filename of blob being downloaded.
        destination_file_name (str):
            Path on local to download file to.
    '''
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)