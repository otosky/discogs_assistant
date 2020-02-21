import os, pickle, base64
import redis
from datetime import datetime
from psycopg2.extras import execute_values
import discogs_client
import pandas as pd
import scipy.sparse as sparse
from .utils import connect_psql, get_pickle_directory
from .utils import upload_blob, BUCKET_NAME
from io import BytesIO

DISCOGS_HEADER = os.environ.get('DISCOGS_API_HEADER')
REDIS_PORT = os.environ.get('REDIS_PORT')

class Cache:
    '''
    Wrapper class for functions related to updating transaction status for
    recommendation or cart requests in cache on private Redis server.
    '''

    @staticmethod
    def connect_app_cache():
        '''
        Connects to Redis instance serving as the status-update cache.

        Returns:
            redis.Redis connection object 
        '''
        redis_host = os.environ['REDIS_HOST']
        redis_pwd = os.environ['REDIS_PWD']
        return redis.Redis(host=redis_host, password=redis_pwd)

    @classmethod
    def update_transaction_status(cls, trans_id, status):
        '''
        Updates transaction status for a given transaction_id in Redis cache.

        Args:
            trans_id (str):
                22-character Transaction ID for cart request.
            status (str):
                Status code integer in string format.

        '''
        r = cls.connect_app_cache()
        r.set(trans_id, status)
        r.connection_pool.disconnect()
        return None

class Interactions:
    '''
    Wrapper class for functions related to getting User interactions, 
    release metadata, and creating/saving sparse interaction input-matrices. 

    Attributes:
        timestamp (datetime.datetime):
            Timestamp of object instantiation.
            Used as part of filename when saving input_matrix to Storage bucket.
        interactions_df (pandas.Dataframe):
            DataFrame of all interactions for all users.  
            Has columns: release_id - Discogs release_id
                         release_idx - Category code for "release_id"
                         username - Discogs username
                         user_id - Category code for "username"
            Used as input to matrix factorization model after conversion to sparse matrix.
        release_id_map (pandas.DataFrame):
            Reduced dataframe from interactions_df; maps "release_id" to "release_idx".
        sparse_item_user (scipy.sparse.csr_matrix):
            A sparse matrix version of interactions_df DataFrame where rows are
            "user_id", columns are "release_idx", and values are confidence value
            weighted scores for release.
    '''
    def __init__(self):
        # set UTC timestamp for filenaming later
        self.timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M")
    
    @staticmethod
    def get_user_interactions(username):
        '''
        Fetches a single user's Wantlist and Collection release_ids from database.

        Args:
            username (str):
                User to fetch interactions for.

        Returns:
            tuple:
                Both Wantlist DataFrame and Collection DataFrame as a tuple.
        '''
        # fetch wantlist and assign score 1 to each release
        q1 = '''
            SELECT w.release_id, username, artist_id, label
            FROM wantlist w
            JOIN release_artist ra
                ON ra.release_id = w.release_id
            JOIN release_label rl
                ON rl.release_id = w.release_id
            WHERE username=%s;
            '''

        # fetch collection and assign score 2 to each release
        q2 = '''
            SELECT c.release_id, username, artist_id, label
            FROM collection c
            JOIN release_artist ra
                ON ra.release_id = c.release_id
            JOIN release_label rl
                ON rl.release_id = c.release_id
            WHERE username=%s;
            '''
        with connect_psql() as conn:
            wantlist = pd.read_sql_query(q1, conn, params=[username])
            wantlist['score'] = 1
            collection = pd.read_sql_query(q2, conn, params=[username])
            collection['score'] = 2
        conn.close()

        return wantlist, collection

    @staticmethod
    def get_interactions_metadata(release_ids):
        '''
        Builds dataframe of all releases used to train model with full 
        release metadata.
        
        Args:
            release_ids (list):
                All release_ids (integers) to be input to recommendation model.

        Returns:
            pandas.DataFrame:
                All release_ids input to recommendation model with associated metatdata.
                Columns: "release_id"   - int, 
                         "image_file"   - str, 
                         "artist_id"    - int, 
                         "artist (name)"- str, 
                         "title"        - str,
                         "record label" - str, 
                         "genres"       - list of str, 
                         "styles"       - list of str
        '''
        
        q = '''
            SELECT r.id release_id, im.filename image_file, artist_id, 
                a.name artist, title, label, genres, styles
            FROM release r
            LEFT JOIN image_map im
                ON im.release_id = r.id
            JOIN release_artist ra
                ON ra.release_id = r.id
            JOIN release_label rl
                ON rl.release_id = r.id
            LEFT JOIN artist a
                ON a.id = ra.artist_id
            WHERE r.id IN %s;
            '''
        params = (tuple(release_ids),)
        with connect_psql() as conn:
            metadata = pd.read_sql_query(q, conn, params=params)
        conn.close()
        
        metadata = metadata.sort_values('release_id').reset_index(drop=True)
        return metadata

    @staticmethod
    def create_all_interactions_df(metadata_df, release_id_map):
        '''
        Merges DataFrame of metadata with dataframe that maps release_ids to release_idx.
        
        This creates a DataFrame of all the metadata for all releases input to
        recommendation model, to be used when storing all metadata for user
        recommendations.

        Args:
            metadata_df (pandas.DataFrame):
                DataFrame of metadata for all release_ids input to matrix
                factorization model.
            release_id_map (pandas.DataFrame):
                DataFrame that maps release_id to release_idx category codes.

        Returns:
            tuple:
                DataFrame of all interactions with metadata & a reduced DataFrame
                of the same where the release_idx column has been deduplicated.

                The former DataFrame will have duplicates for release_id and release_idx 
                since the joins from the metadata will sometimes show a release
                having two (or more) separate artists.  The dedupe DataFrame allows
                for faster hash indexing when filtering for each individual user's 
                recommendation metadata.
        '''
        all_interactions = metadata_df.merge(release_id_map)
        all_interactions_dedupe = all_interactions.drop_duplicates('release_idx').set_index('release_idx')
        
        return all_interactions, all_interactions_dedupe

    def create_df_interactions(self, psql_conn=None):
        # TODO remove psql_conn param in affected code
        '''
        Fetches a all users' Wantlist and Collection release_ids from database. 
        
        Formats DataFrame so that Wantlist & Collection receive different
        weight scores and username/release_id are converted to category codes - 
        a necessary step before converting to csr_matrix for ALS model.
        '''
        
        with connect_psql() as conn:
            # gather all users' wantlists, assign each a score of 1
            q = 'SELECT release_id, username FROM wantlist;'
            wantlists = pd.read_sql_query(q, conn)
            wantlists['score'] = 1
            # gather all users' collection, assign each a score of 2
            q = 'SELECT release_id, username FROM collection;'
            collection = pd.read_sql_query(q, conn)
            collection['score'] = 2
        conn.close()
        
        # combine wantlist + collection dataframes
        data = pd.concat([wantlists, collection], ignore_index=True)
        
        # map user_id and release_idx category codes 
        data['username'] = data['username'].astype("category")
        data['release_id'] = data['release_id'].astype("category")
        data['user_id'] = data['username'].cat.codes
        data['release_idx'] = data['release_id'].cat.codes
        self.interactions_df = data
        
        # creates reduced dataframe that maps release_id to release_idx codes
        release_id_map = data.drop_duplicates(subset='release_id')\
                             .drop(columns=['username', 'user_id', 'score'])
        self.release_id_map = release_id_map
    
    def create_sparse(self):
        '''
        Converts DataFrame of all user interactions to a sparse csr_matrix.

        Depends upon running create_df_interactions method first.
        '''
        # builds sparse matrix
        sparse_item_user = sparse.csr_matrix((self.interactions_df.score, 
                                             (self.interactions_df.release_idx, 
                                              self.interactions_df.user_id)))
        self.sparse_item_user = sparse_item_user

    def save_data(self):
        '''
        Saves Interactions object to local pickle file and to input-matrix bucket
        in GCP Storage.

        Necessary to have the object stored externally so that other micro-services
        can access it alongside the most recent ALS model.
        '''
        
        pickle_directory = get_pickle_directory()
        
        # dumps object to pickle
        filename = ''.join(['data_', self.timestamp, '.pkl'])
        full_path = ''.join([pickle_directory, filename])
        pickle.dump(self, open(full_path, 'wb'))
        
        # dumps object to GCP Storage bucket
        tmp = BytesIO()
        pickle.dump(self, tmp)
        upload_blob(BUCKET_NAME, f'input-matrix/{filename}', tmp)
    
    def load_data(self, filename):
        '''
        Placeholder

        Args:
            param (type):
                Explanation

        Returns:
            datatype:
                Explanation
        '''
        pass

class DiscogsScraper:
    '''
    Wrapper class for functions related to scraping Discogs pages.
    '''

    @staticmethod
    def parse_releases_interactions(tree):
        '''
        Parse release_ids from a given lxml tree.

        Args:
            tree (lxml.etree._ElementTree):
                XPath-parseable element tree of User's wantlist or collection
                from url_to_lxml function.

            Returns:
                tuple:
                    Total size of user's wantlist or collection (int), 
                    release_ids extracted from page (list of int).
        '''
        try:
            # gets total pagination element of form '1 – 250 of X'
            len_interactions = tree.xpath('//div[@class="pagination top "]//strong[@class="pagination_total"]/text()')[0]
        except:
            # if User doesn't have any items in wantlist/collection
            return 0, None
        len_interactions = len_interactions.split('of')[1]
        # beware the whitespace in this page element
        len_interactions = len_interactions.strip().replace(',','')
        releases = tree.xpath('//span/@data-post-url')
        release_ids = [r.split('release_id=')[-1] for r in releases]
        return int(len_interactions), release_ids

class UserProfileBase:
    '''
    General functions related to a User on Discogs Assistant.

    Args:
        username (str):
            Discogs username

    Attributes:
        username (str):
            Discogs username
        p_lim (int):
            Number of releases to include per page in Discogs wantlist/collection 
            GET requests.
    '''
    def __init__(self, username):
        
        self.username = username
        self.p_lim = 250
    
    @staticmethod
    def get_user_creation_time(username, redis_port=REDIS_PORT):
        '''
        Fetches User's creation time in Redis cache.

        Args:
            username (str):
                Discogs username
            redis_port (int):
                Redis port number

        Returns:
            datetime.datetime:
                Datetime reflecting creation time of User's hash set.
        '''
        r = redis.Redis(port=redis_port)
        creation = r.hget(username, 'created_at')
        creation = base64.b64decode(creation).decode('utf-8')
        
        format_ = "%Y-%m-%d %H:%M:%S.%f"
        creation = datetime.strptime(creation, format_)
        
        return creation

    def check_if_user_exists(self): 
        '''
        Checks whether User already exists in our system.  

        Used for web-app validation.

        Returns:
            boolean:
                True if User already exists in our database, False if not.
        '''
        
        with connect_psql() as conn:
            with conn.cursor() as cur:
                cur.callproc('check_user_exists', (self.username,))
                exists = bool(cur.fetchone()[0])
        
        return exists

    def update_collected_status(self):
        '''
        Flips boolean status in our database after User's profile has been collected.
        '''
    
        with connect_psql() as conn:
            with conn.cursor() as cur:
                cur.callproc('update_profile_collected_status', (True, self.username,))
        conn.close()

    def _setup_discogs_client(self, request_token=None, request_secret=None):
        '''
        Helper function to instantiate Discogs API client.

        Args:
            request_token (str):
                Request token associated with User's access_token; optional
            request_secret (str):
                Request secret associated with User's access_secret; optional

        Returns:
            discogs_client.Client:
                Discogs API Client object
        '''
        consumer_key = os.environ.get('DISCOGS_API_KEY')
        consumer_secret = os.environ.get('DISCOGS_API_SECRET')
        client = discogs_client.Client(DISCOGS_HEADER)
        client.set_consumer_key(consumer_key, consumer_secret)
        if request_token and request_secret:
            client.set_token(request_token, request_secret)
        
        return client

    # TODO - I think this can be deleted
    def redirect_user_to_auth(self):
        '''
        Generates authorization URL for user to authenticate their Discogs
        account with our application.

        Returns:
            discogs_client.Client:
                Discogs API Client object
        '''
        callback_url = 'https://www.discogsassistant.io'
        client = self._setup_discogs_client()
        token, secret, url = client.get_authorize_url(callback_url=callback_url)
        print('Please browse to the following URL {0}'.format(url))
        
        return client
        # oauth would redirect User to Auth page
        # call back would send JSON with token and verifier

    def _get_access_credentials(self):
        '''
        Fetches Discogs access credentials for user from database.
        '''
        
        with connect_psql() as conn:
           with conn.cursor() as cur:
               cur.callproc('get_access_creds', (self.username,))
               res = cur.fetchone()
               
        # unpack result
        self.access_token, self.access_secret = res
    
    def authenticate_as_user(self, verifier=None, validation=False, request_token=None, request_secret=None):
        '''
        Authenticates Discogs Client for user via Discogs API.

        Args:
            verifier (str):
                Returned verification code after User has exchanged request token
                for a access token; optional if validation is set False
            validation (boolean):
                Whether authentication is being used in the context of our app's
                registration.
            request_token (str):
                Request token associated with User's access_token; 
                optional if validation is set False.
            request_secret (str):
                Request secret associated with User's access_secret; 
                optional if validation is set False.

        Returns:
            discogs_client.Client:
                Discogs API Client object, authorized for User
        '''
        if validation:
            if request_token and request_secret and verifier:
                discogs_client = self._setup_discogs_client(request_token=request_token, 
                                                            request_secret=request_secret)
                self.access_token, self.access_secret = discogs_client.get_access_token(verifier)
            else:
                raise ValueError('''
                                 "request_token" and "request_secret" and 
                                 "verifier" arguments must not be None
                                 ''')
        else:
            discogs_client = self._setup_discogs_client()
            self._get_access_credentials()
        discogs_client.set_token(self.access_token, self.access_secret)

        return discogs_client

    def add_user_to_database(self, client, password, verifier=None):
        '''
        Upserts user to database after registration & authentication or a password
        change.

        Args:
            client (discogs_client.Client):
                Authenticated Discogs Client for user.
            password (str):
                Password that User has input.
            verifier (str):
                Returned verification code after User has exchanged request token
                for a access token.
        '''
        
        client.set_token(self.access_token, self.access_secret)
        
        # get identity details of User from Discogs API
        user_dict = client.identity()
        discogs_uid = user_dict.fetch('id')
        registered = user_dict.fetch('registered')[:10]
        today = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        q = 'CALL add_user(%s, %s, %s, %s, %s, %s, %s, %s)'
        insert_values = (self.username, registered, self.access_token, 
                         self.access_secret, verifier, discogs_uid, today, password)
        
        with connect_psql() as conn:
            with conn.cursor() as cur:
                cur.execute(q, insert_values)
        conn.close() 

    def dump_interactions_to_SQL(self, release_ids, interaction_type='wantlist'):
        '''
        Upserts release_ids of wantlist/collection for User to database.

        Args:
            release_ids (list):
                List of release ids (integers) for given user's interactions.
            interaction_type (str):
                Either "wantlist" or "collection".

        Returns:
            datatype:
                Explanation
        '''
        if interaction_type not in ['wantlist', 'collection']:
            raise TypeError("interaction_type parameter must be 'wantlist' or 'collection'")
        
        today = datetime.utcnow().date()
        
        insert_values = [(r, self.username, today) for r in release_ids]
        q = 'INSERT INTO %s (release_id, username, scraped) VALUES %%s ON CONFLICT DO NOTHING;' % interaction_type

        with connect_psql() as conn:
            with conn.cursor() as cur:
                execute_values(cur, q, insert_values)
        conn.close()