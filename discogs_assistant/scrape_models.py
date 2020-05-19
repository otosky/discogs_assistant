import redis
import os, base64
from math import ceil
from datetime import datetime
import discogs_client
from psycopg2.extras import execute_values
from .exceptions import APILimitExceeded
from concurrent.futures import ThreadPoolExecutor

from .utils import flatten_list, url_to_lxml, make_chunks, MongoCollection, \
                   connect_psql, publisher_setup, Cache

MAX_WORKERS = 4
DISCOGS_HEADER = os.environ.get('DISCOGS_API_HEADER')
REDIS_PORT = os.environ.get('REDIS_PORT')

# pub-sub
USER_SCRAPE_TOPIC = os.environ.get('USER_SCRAPE_TOPIC')
USER_API_TOPIC = os.environ.get('USER_API_TOPIC')
READY_TO_REC_TOPIC = os.environ.get('READY_TO_REC_TOPIC')

class ProfileGatheringDispatcher:
    '''
    Wrapper for methods related to gathering a user's wantlist or collection, 
    either using the Discogs API or web scraping.
    '''
    @staticmethod
    def check_interactions_count_sql(username, interaction_type='wantlist'):
        '''
        Fetches count of release ids for a given user and in either their
        wantlist or collection.

        Args:
            username (str):
                Discogs username
            interaction_type (str):
                Either "wantlist" or "collection"

        Returns:
            int:
                Number of releases in user's wantlist or collection.
        '''

        q = "SELECT COUNT(*) FROM %s WHERE username=%%s;" % interaction_type
        
        with connect_psql() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (username,))
                count = cur.fetchall()[0][0]
        return int(count)

    @staticmethod
    def trigger_scrapers(chunks, username, n_chunks, interaction, trans_id, online=True):
        '''
        Triggers Cloud Function scrapers to extract release ids from User's
        collection or wantlist urls.  Urls are batched in chunks of size n, if there
        are multiple pages worth of releases.

        Transaction status cache gets updated upon completion of message publishing.

        Args:
            chunks (List of Lists of Integers):
                Cart candidate release_ids grouped into lists of size N.
            username (str):
                Discogs username
            n_chunks (int):
                Total number of chunks; this is published in the message and 
                will be used as a decrement counter later to determine when
                all chunks have been scraped.
            interaction (str):
                Either "wantlist" or "collection"; interaction_type.
            trans_id (str):
                22-character Transaction ID for recommendations request.
        '''

        publisher, topic_path = publisher_setup(USER_SCRAPE_TOPIC)
        # if the list is empty no triggers sent
        for i in chunks:
            data = str(list(i))
            # Data must be a bytestring
            data = data.encode('utf-8')
            # When you publish a message, the client returns a future.
            # kwargs after 'data' get formatted as JSON under 'attributes
            future = publisher.publish(topic_path, data=data, username=str(username),  
                                        packet_size=str(n_chunks),
                                        interaction=str(interaction),
                                        trans_id=trans_id, status=str(3))
            print(future.result())
            print('Published messages.')
        if online:
            Cache.update_transaction_status(trans_id, status=str(3))
    
    @staticmethod
    def trigger_api_profile_collection(username, interaction, trans_id):
        '''
        Triggers Cloud function to gather User's profile from the Discogs API.
        This is a necessary operation if the User has set their profile to private.

        Args:
            username (str):
                Discogs username
            interaction (str):
                Either "wantlist" or "collection"; interaction_type.
            trans_id (str):
                22-character Transaction ID for recommendations request.
        '''

        publisher, topic_path = publisher_setup(USER_API_TOPIC)
        
        # Data must be a bytestring
        username = username.encode('utf-8')
        # When you publish a message, the client returns a future.
        # kwargs after 'data' get formatted as JSON under 'attributes
        future = publisher.publish(topic_path, data=username,
                                    interaction=str(interaction),
                                    trans_id=trans_id, status=str(3))
        print(future.result())
        print('Published messages.')
        Cache.update_transaction_status(trans_id, status=str(3))

    @staticmethod
    def set_user_packet_size(username, packet_size, expiry=3600):
        '''
        Sets total payload size as value for user's recommendation transaction
        key in Redis.  This will act as a counter for determining when all 
        packets have been collected.

        Args:
            username (str):
                Discogs username
            packet_size (int):
                Number of URLs being scraped.
            expiry (int):
                Time-to-live for Redis key, in seconds.  Default is 3600 (60 min).
        '''
        key = f'{username}:packet_size'
        redis_conn = redis.Redis()
        redis_conn.set(key, packet_size, ex=expiry)
        return None

    @staticmethod
    def increment_user_key(key, amount, expiry=None):
        '''
        Increments/Decrements User's recommendation transaction Key in Redis.  
        This acts as a counter for determining when all packets have been collected, 
        using the number of chunks sent out as Pub-Sub messages.

        Args:
            key (type):
                Key correlating to User's recommendation transaction request.
                Of form: 'username:trans_id:packet_size'.
            amount (int):
                Increment amount (can be negative).
            expiry (int or datetime.datetime.timedelta):
                Time-to-live for Redis key; optional
        '''
        redis_conn = redis.Redis()
        redis_conn.incrby(key, amount=amount)
        if expiry:
            redis_conn.expire(key, expiry)
        return None

    @staticmethod
    def check_user_scrape_complete(key):
        '''
        Checks to see whether User's profile gathering is complete.

        Sees whether user's transaction key has been decremented to 0.

        Args:
            key (type):
                Key correlating to User's recommendation transaction request.
                Of form: 'username:trans_id:packet_size'.

        Returns:
            boolean:
                True if gathering is complete; False if there are still more
                packets that are scraping.
        '''
        redis_conn = redis.Redis()
        packets_left = redis_conn.get(key)
        if int(packets_left) <= 0:
            return True
        else:
            return False

    @staticmethod
    def handoff_to_recalculate_user(username, trans_id):
        '''
        Publishes message that indicates User's profile has been fully gathered
        and that recommendation recalculation is ready to go.

        Args:
            username (str):
                Discogs username
            trans_id (str):
                22-character Transaction ID for recommendations request.
        '''
        username = username.encode('utf-8')
        
        publisher, topic_path = publisher_setup(READY_TO_REC_TOPIC)
        future = publisher.publish(topic_path, data=username, trans_id=trans_id, status=str(4))
        print(future.result())
        Cache.update_transaction_status(trans_id, status=str(4))
        return None

    @classmethod
    def scrape_extra_urls(cls, profile, trans_id, len_interactions, interaction_type, online=True):
        '''
        Generates extra urls for interaction_type to scrape (beyond page 1)
        and triggers cloud functions to execute scraping.

        Args:
            profile (UserProfileBase):
                UserProfile object for user.
            trans_id (str):
                22-character Transaction ID for recommendations request.
            len_interactions (int):
                Number of releases in user's wantlist or collection.
            interaction_type (str):
                Either "wantlist" or "collection".

        Returns:
            int:
                Number of triggers (chunks) sent out to scraper functions.
        '''
        # create extra wantlist urls for Cloud Function to scrape
        extra_urls = profile.get_remainder_urls(len_interactions, interaction_type=interaction_type)
        # chunk list of urls
        chunks = list(make_chunks(extra_urls, 10))
        n_chunks = len(chunks)
        packet_size = n_chunks
        # publish msg to scrape extra wantlist URLs
        cls.trigger_scrapers(chunks, profile.username, n_chunks, interaction_type, 
                             trans_id, online)
        print('triggering scrapers')
        return packet_size

    @classmethod
    def dispatch(cls, username, trans_id, existing=False, interaction_type='wantlist', online=True):
        '''
        Dispatch pipeline to gather User's Discogs profile.

        - Checks whether User Profile is public or private.
        - Determines whether our records are up-to-date; 
        - Hands-off to calculation if up-to-date; triggers scrapers if not.


        Args:
            username (str):
                Discogs username
            trans_id (str):
                22-character Transaction ID for recommendations request.
            interaction (str):
                Either "wantlist" or "collection"; interaction_type.

        Returns:
            int:
                Number of triggers sent out if scraping interactions;
                Returns 0 if our database is up-to-date, or 1 if the User's profile
                is set to private and gathering happens through API.
        '''

        profile = UserProfilePublic(username)

        xml_tree = profile.check_if_interactions_private(interaction_type=interaction_type)
        # if the req came back as 200, then User's profile is public
        if xml_tree:
            len_interactions, first250_releases = profile.get_interactions_len(xml_tree)
            if len_interactions:
                if online:
                    # dump to Postgres
                    profile.dump_interactions_to_SQL(first250_releases, interaction_type=interaction_type)
                else:
                    # dump to Postgres
                    profile.dump_interactions_to_SQL(first250_releases, 
                                                     interaction_type=interaction_type, 
                                                     non_user=True)
                    # TODO if len is > 5000: send msg alerting that this might take a while
                
                # checks to see if count from our database matches Discogs web
                if existing:
                    our_count = cls.check_interactions_count_sql(profile.username, 
                                                        interaction_type=interaction_type)
                    # modify length of list to be the diff between our DB and Discogs
                    if len_interactions > our_count:
                        len_interactions = len_interactions - our_count
                        # chunk and trigger scrapers
                        packet_size = cls.scrape_extra_urls(profile, trans_id, 
                                                            len_interactions, 
                                                            interaction_type, online)
                        return packet_size
                    else:
                        # no packets needed
                        return 0
                else:
                    packet_size = cls.scrape_extra_urls(profile, trans_id, 
                                                    len_interactions, interaction_type, online)
                    return packet_size
        # private profile
        else:
            # collect via API
            print('need API')
            private = UserProfilePrivate(username)
            client = private.authenticate_as_user()
            len_interactions = private.get_interaction_count(client, interaction_type)
            count = cls.check_interactions_count_sql(private.username, 
                                                        interaction_type=interaction_type)
            if len_interactions <= count:
                # no packets needed
                return 0
            else:
                cls.trigger_api_profile_collection(private.username, interaction_type, trans_id)
                return 1
        # fail-safe catchall
        return 0

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

    def dump_interactions_to_SQL(self, release_ids, interaction_type='wantlist', non_user=False):
        '''
        Upserts release_ids of wantlist/collection for User to database.

        Args:
            release_ids (list):
                List of release ids (integers) for given user's interactions.
            interaction_type (str):
                Either "wantlist" or "collection".
            non-user (boolean):
                Whether user is not yet registered on Discogs Assistant.

        Returns:
            datatype:
                Explanation
        '''
        if interaction_type not in ['wantlist', 'collection']:
            raise TypeError("interaction_type parameter must be 'wantlist' or 'collection'")
        
        if non_user:
            interaction_type = f'non_user_{interaction_type}'

        today = datetime.utcnow().date()
        
        insert_values = [(r, self.username, today) for r in release_ids]
        q = 'INSERT INTO %s (release_id, username, scraped) VALUES %%s ON CONFLICT DO NOTHING;' % interaction_type

        with connect_psql() as conn:
            with conn.cursor() as cur:
                execute_values(cur, q, insert_values)
        conn.close()

class UserProfilePublic(UserProfileBase):
    '''
    User with a publicly visible Interaction Type (Wantlist or Collection).

    Args:
        username (str):
            Discogs username

    Attributes:
        username (str):
            Discogs username
        p_lim (int):
            Number of releases to include per page in Discogs wantlist/collection 
            GET requests.
        wantlist_start_url (str):
            Base URL (page 1) for fetching User's wantlist release_ids.
        collection_start_url (str):
            Base URL (page 2) for fetching User's collection release_ids.
        private (list):
            List of interaction types that User has set to private.
    '''
    def __init__(self, username):
        super().__init__(username)
        self.wantlist_start_url = f'https://www.discogs.com/wantlist?page=1&limit={self.p_lim}&user={self.username}'
        self.collection_start_url = f'https://www.discogs.com/user/{self.username}/collection?page=1&limit={self.p_lim}'
        self.private = []
    
    def check_if_interactions_private(self, interaction_type='wantlist'):
        '''
        Tries to scrape first page of User's wantlist or collection to determine
        whether that particular interaction type is public or private.

        Args:
            interaction_type (str):
                Either "wantlist" or "collection".

        Returns:
            lxml.etree._ElementTree:
                XPath-parseable element tree of User's wantlist or collection
                from url_to_lxml function if interactions set public.
                Returns None if interactions have been set private.
        '''
        if interaction_type == 'wantlist':
            start_url = self.wantlist_start_url
        elif interaction_type == 'collection':
            start_url = self.collection_start_url
        else:
            raise TypeError

        tree = url_to_lxml(start_url)
        
        if not tree:
            self.private += [interaction_type]

        return tree

    def get_interactions_len(self, tree):
        '''
        Parses lxml tree to get first 250 release ids from page and total
        number of releases for given interaction type.

        Args:
            tree (lxml.etree._ElementTree):
                XPath-parseable element tree of User's wantlist or collection
                from url_to_lxml function.

        Returns:
            tuple:
                Total number of releases in User's wantlist/collection,
                first 250 release ids for User's wantlist/collection.
        '''
        len_interactions, first250_releases = DiscogsScraper.parse_releases_interactions(tree)
        return len_interactions, first250_releases
    
    def get_remainder_urls(self, len_interactions, interaction_type='wantlist'):
        '''
        Creates a list of the remaining page URLs to scrape in order to gather
        all of User's wantlist or collection releases.

        - Takes the total number of interaction items.
        - Divides that count by 250 (items per page).
        - Creates a list of urls by replacing the "page=" param of the 
          URL with range of page numbers needed to cover full span of items.

        Args:
            len_interaction (int):
                Total number of releases in User's wantlist/collection
            interaction_type (str):
                Either "wantlist" or "collection".

        Returns:
            list:
                List of URLs that will fetch all pages in a User's wantlist/collection
                on Discogs.com
        '''
        if interaction_type == 'wantlist':
            base_url = self.wantlist_start_url
        elif interaction_type == 'collection':
            base_url = self.collection_start_url
        else:
            raise TypeError
        
        num_pages = ceil(len_interactions / 250)
        pages = range(2, num_pages + 1)
        urls = [base_url.replace('page=1', f'page={p}') for p in pages]
        return urls
    
    # def dump_interactions_to_SQL(self, release_ids, interaction_type='wantlist'):
    #     if interaction_type not in ['wantlist', 'collection']:
    #         raise TypeError
    #     today = datetime.utcnow().date()
    #     insert_values = [(r, self.username, today) for r in release_ids]
    #     q = 'INSERT INTO %s (release_id, username, scraped) VALUES %%s ON CONFLICT DO NOTHING;' % interaction_type

    #     with connect_psql() as conn:
    #         with conn.cursor() as cur:
    #             execute_values(cur, q, insert_values)
    #     conn.close()

class UserProfilePrivate(UserProfileBase):
    '''
    User with a private Interaction Type (Wantlist or Collection).
    These interactions are only retrievable via authenticated Discogs API calls.

    Args:
        username (str):
            Discogs username

    Attributes:
        username (str):
            Discogs username
        p_lim (int):
            Number of releases to include per page in Discogs wantlist/collection 
            GET requests.
        wantlist (list):
            List of release ids (integers) in User's wantlist.
        collection (list):
            List of release ids (integers) in User's collection.
    '''
    def __init__(self, username):
        super().__init__(username)
    
    def _make_api_request_for_page(self, client, page_num, interaction_type='wantlist'):
        '''
        Formats url and makes GET request to Discogs API to fetch page of paginated 
        wantlist/collection for user.

        Args:
            client (discogs_client.Client):
                Authenticated Discogs API Client for user.
            page_num (int):
                Page number to make paginated API GET request for.
            interaction_type (str):
                Either "wantlist" or "collection".

        Returns:
            dict:
                JSON Discogs API response as dictionary.
        '''
        base_url = f'https://api.discogs.com/users/{self.username}'
        if interaction_type == 'wantlist':
            base_url = f'{base_url}/wants'
        elif interaction_type == 'collection':
            base_url = f'{base_url}/collection/folders/0/releases'
        else:
            raise ValueError("'interaction_type' must be either 'wantlist' or 'collection'")
        
        url_suffix = f'?page={page_num}&per_page=250'
        res = client._request('GET', f'{base_url}{url_suffix}')
        
        return res
        
    def _extract_release_ids(self, json, interaction_type='wantlist'):
        '''
        Parses release ids from json API response for a User's wantlist or collection.

        Args:
            param (type):
                Explanation
            interaction_type (str):
                Either "wantlist" or "collection".

        Returns:
            list:
                List of release ids (integers) in User's wantlist or collection.
        '''
        if interaction_type == 'wantlist':
            interaction_key = 'wants'
        elif interaction_type == 'collection':
            interaction_key = 'releases'
        else:
            raise ValueError("'interaction_type' must be either 'wantlist' or 'collection'")
        
        release_ids = [item['id'] for item in json[interaction_key]]
        
        return [release_ids]
    
    def _get_interaction_items(self, client, page=1, interaction_type='wantlist'):
        '''
        Helper function that extracts release ids for a paginated Discogs API 
        GET request retrieving either wantlist or collection items, and also returns
        the total number of pages.

        Args:
            client (discogs_client.Client):
                Authenticated Discogs API Client for user.
            page (int):
                Page number to make paginated API GET request for.
                Default = 1
            interaction_type (str):
                Either "wantlist" or "collection".

        Returns:
            tuple:
                Release ids in page for User's wantlist or collection, 
                and number of pages total in User's wantlist or collection.
        '''
        json = self._make_api_request_for_page(client, page, interaction_type)
        release_ids = self._extract_release_ids(json, interaction_type)
        num_pages = json['pagination']['pages']
        
        return release_ids, num_pages
    
    def _multithread_interaction_collection(self, client, interaction_type, max_workers=MAX_WORKERS):
        '''
        Multithreaded pipeline to gather all release ids from a User's particular
        interaction type (wantlist or collection).

        - Makes a primary GET request to retrieve first 250 release ids and 
          total number of pages.
        - If there are more pages to collect, will make GET requests with 
          n-multithreaded workers to subsequent pages.
        - Flattens all lists of release ids - each page generates a list -
          to a final list of all release ids for that interaction_type.

        Args:
            client (discogs_client.Client):
                Authenticated Discogs API Client for user.
            interaction_type (str):
                Either "wantlist" or "collection".
            max_workers (int):
                Number of threads to feed concurrent_futures.ThreadPoolExecutor
        
        Returns:
            list:
                All release ids in a User's wantlist or collection, in a flattened list.
        '''
        release_ids, num_pages = self._get_interaction_items(client, interaction_type=interaction_type)
        if num_pages > 120:
            excessive = APILimitExceeded()
            return excessive
            
            # need to return some object that will trigger an different workflow
            # cuz I don't want to bother with using these functions for more 
            # than 30,000 items; if someone has a wantlist/collection size that 
            # large then they gotta just make it public temporarily
        gen = ((client, page, interaction_type) for page in range(2, num_pages + 1))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(self._get_interaction_items, *zip(*gen))
            results = [i[0] for i in results]
            results = flatten_list(results)
        
        release_ids += results
        release_ids = flatten_list(release_ids)
        
        return release_ids
    
    def get_interaction_count(self, client, interaction_type='wantlist'):
        '''
        Retrieves number of items in wantlist or collection for User via Discogs
        API.

        Args:
            client (discogs_client.Client:
                Authenticated Discogs API Client for user.
            interaction_type (str):
                Either "wantlist" or "collection".

        Returns:
            int:
                Number of items in wantlist or collection for User.
        '''
        user = client.identity()
        if interaction_type == 'wantlist':
            return user.num_wantlist
        elif interaction_type == 'collection':
            return user.num_collection
        else:
            raise TypeError("Param 'interaction_type' must be 'wantlist' or 'collection'")

    def get_wantlist_items(self, client):
        '''
        Helper method to execute interaction_collection via multithreading
        for a User's wantlist.

        Args:
            client (discogs_client.Client:
                Authenticated Discogs API Client for user.
        '''
        self.wantlist = self._multithread_interaction_collection(client, 'wantlist')
        return None

    def get_collection_items(self, client):
        '''
        Helper method to execute interaction_collection via multithreading
        for a User's collection.

        Args:
            client (discogs_client.Client:
                Authenticated Discogs API Client for user.
        '''
        self.collection = self._multithread_interaction_collection(client, 'collection')
        return None

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