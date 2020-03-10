import time, os
from bson import json_util
from math import ceil
import redis
from datetime import datetime
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import scipy.sparse as sparse
from .exceptions import APILimitExceeded
from .utils import flatten_list, url_to_lxml, make_chunks, MongoCollection
from .utils import connect_psql, publisher_setup
from .gen_models import Cache, UserProfileBase, DiscogsScraper, Interactions

MAX_WORKERS = 4

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
                # dump to Postgres
                profile.dump_interactions_to_SQL(first250_releases, interaction_type=interaction_type)
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
                                                    len_interactions, interaction_type)
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

class TrainingUser:
    '''
    Discogs User whose interactions were used in recommendation model training
    set.

    Args:
        username (str):
            Discogs username

    Attributes:
        name (str):
            Discogs username
        userid (int):
            User's index id (category code) in the sparse matrix that forms
            the input to the recommendation matrix factorization.
        wantlist (list):
            List of release ids (integers) in User's wantlist.
        collection (list):
            List of release ids (integers) in User's collection.
        already_known_artists (list):
            List of artist ids (integers) that a User has already interacted with
            in their wantlist or collection.
        already_known_labels (list):
            List of record labels (strings) that a User has already interacted with
            in their wantlist or collection.
    '''
    # takes username and full set of raw_interactions release_ids & idxs without metadata
    def __init__(self, username):
        self.name = username
     
    def get_interactions(self, raw_interactions):
        '''
        Filters DataFrame of all training set user interactions to get wantlist, 
        collection, and user id for a particular user.

        Args:
            raw_interactions (pandas.DataFrame):
                DataFrame of all User interactions used to generate recommendation
                model.  Must have columns: "username", "user_id", "score", "release_id".
        '''
        self.userid = raw_interactions[raw_interactions.username==self.name].user_id.iloc[0]
        user_interactions = raw_interactions[raw_interactions['username']==self.name]
        wantlist_mask = user_interactions['score'] == 1
        self.wantlist = user_interactions['release_id'][wantlist_mask].astype('int')
        
        collection_mask = user_interactions['score'] == 2
        self.collection = user_interactions['release_id'][collection_mask].astype('int')
    
    def create_user_item_vector(self, sparse_item_user):
        '''
        Creates single user-to-item vector from a sparse matrix representing 
        items-to-users.

        Transposes item-to-user matrix and filters on user_id.

        Args:
            sparse_item_user (csr_matrix):
                Full sparse matrix of User interactions where rows represent 
                releases (item) and columns represent users.
        '''
        self.user_vector = sparse_item_user.T.tocsr()[self.userid]
    
    def get_already_known(self, all_interactions):
        '''
        Filters DataFrame of all releases used to generate recommendations for 
        just the artists and labels that a User has in their wantlist & collection.

        Args:
            all_interactions (pandas.DataFrame):
                DataFrame of all releases used as input to recommendation model,
                with full metadata.
        '''
        # TODO: parallelize this
        # create a list combining user wantlist and collection
        already_known_stack = pd.concat([self.wantlist, self.collection]).unique()
        # filter all_interactions dataframe for only releases in users' wantlist/collection
        mask = all_interactions['release_id'].isin(already_known_stack)
        already_known_df = all_interactions[mask]

        #create lists for artists and labels that user has in wantlist/collection
        self.already_known_artists = already_known_df['artist_id'].unique().tolist()
        self.already_known_labels = already_known_df['label'].unique().tolist()
    
    def get_recommendations(self, all_model_recs, release_id_map, 
                                      all_interactions_dedupe, oos_user=False):
        '''
        Filters all recommendations DataFrame (with full metadata) for User.

        Args:
            all_model_recs (numpy.ndarray):
                Full array of recommendations for all users.
            release_id_map (pandas.DataFrame):
                DataFrame that maps release_id to release_idx (category codes).
            all_interactions_dedupe (pandas.DataFrame):
                DataFrame of releases that model was trained on with full metadata,
                where the index is release_idx and is unique.
            oos_user (boolean):
                "Out-of-Sample User" - only set to True for NewUser subclass.
                TODO: Should be deprecated.
        '''
        # get recommendation (release_idxs) array for user
        if oos_user == False:
            recs_indices_array = all_model_recs[self.userid]
        else:
            recs_indices_array = [i[0] for i in all_model_recs]
            scores = [i[1] for i in all_model_recs]
        # add release metadata from all_interactions dataframe to user_recommendations
        full_recommendations = all_interactions_dedupe.reindex(index=recs_indices_array)
        full_recommendations = full_recommendations.reset_index(drop=True)
        # add a column denoting the rank for recommendation score
        full_recommendations['rank'] = full_recommendations.index + 1
        if oos_user == True:
            full_recommendations['score'] = scores

        self.recommendations = full_recommendations
    
    def filter_already_known(self):
        '''
        Returns recommendation dataframe with already known artists and labels
        filtered out.
        TODO: Deprecate.

        Returns:
            pandas.DataFrame:
                User's recommendation DataFrame with already known artists and 
                record labels filtered out.
        '''
        artist_mask = ~self.recommendations.artist_id.isin(self.already_known_artists)
        label_mask = ~self.recommendations.label.isin(self.already_known_labels)
        return self.recommendations[artist_mask & label_mask]

class RecalcUser:
    '''
    A User that is out-of-sample to the model training set, 
    i.e. a New User or a User requesting recalculation.

    Args:
        username (str):
            Discogs username

    Attributes:
        name (str):
            Discogs username
        wantlist (list):
            List of release ids (integers) in User's wantlist.
        collection (list):
            List of release ids (integers) in User's collection.
        already_known_artists (list):
            List of artist ids (integers) that a User has already interacted with
            in their wantlist or collection.
        already_known_labels (list):
            List of record labels (strings) that a User has already interacted with
            in their wantlist or collection.
    '''
    def __init__(self, username):
        self.name = username

    def get_interactions(self, already_known=True):
        '''
        Fetches a User's wantlist and collection items from Postgres and 
        sets them as attributes, as well as the artists & labels that the User 
        has previously interacted with.

        Args:
            already_known (boolean):
                This should be deprecated as it is unused. TODO
        '''
        # queries database and returns DataFrames for Wantlist & Collection
        self.wantlist, self.collection = Interactions.get_user_interactions(self.name)
        
        interactions = pd.concat([self.wantlist, self.collection])
        self.already_known_artists = interactions['artist_id'].unique().tolist()
        self.already_known_labels = interactions['label'].unique().tolist()

    def create_user_item_vector(self, sparse_item_user, release_id_map):
        '''
        Creates single user-to-item vector from the shape of sparse matrix that
        was originally fed to matrix factorization model.

        Args:
            sparse_item_user (csr_matrix):
                Full sparse matrix of all User interactions where rows represent 
                releases (item) and columns represent users.
            release_id_map (pandas.DataFrame):
                DataFrame that maps release_id to release_idx (category codes).
        '''
        self.userid = 0
        
        interactions = pd.concat([self.wantlist, self.collection])
        interactions = interactions.merge(release_id_map, how='inner', on='release_id')
        interactions['sparse_row_id'] = self.userid
        
        all_release_len = sparse_item_user.shape[0]
        user_vector = sparse.coo_matrix((interactions.score, 
                            (interactions['sparse_row_id'], interactions['release_idx'])),
                            shape = (1, all_release_len))
        self.user_vector = user_vector.tocsr()
        ## Why are these necessary??
        self.wantlist = self.wantlist['release_id']
        self.collection = self.collection['release_id']
    
    def get_recs_metadata(self, model_recs, release_id_map): 
        '''
        Retrieves all release metadata for User's recommendations, formatted
        as a DataFrame.

        Columns: "release_id"   - int, 
                 "image_file"   - str, 
                 "artist_id"    - int, 
                 "artist (name)"- str, 
                 "title"        - str,
                 "record label" - str, 
                 "genres"       - list of str, 
                 "styles"       - list of str

        Args:
            model_recs (numpy.ndarray):
                Array of recommendations/scores output by implicit ALS "recommend" method.
            release_id_map (pandas.DataFrame):
                DataFrame that maps release_id to release_idx (category codes).
        '''
        recs_indices_array = [i[0] for i in model_recs]
        scores = [i[1] for i in model_recs]

        # map back to release_id
        map_ = release_id_map.set_index('release_idx')
        map_ = map_.reindex(index=recs_indices_array).reset_index(drop=True)
        map_['score'] = scores

        # query SQL for all metadata
        meta_df = Interactions.get_interactions_metadata(map_['release_id'].astype('int').tolist())
        meta_df = meta_df.drop_duplicates(subset='release_id')
        full_recommendations = map_.merge(meta_df)

        full_recommendations['rank'] = full_recommendations.index + 1
        #full_recommendations['score'] = scores
        self.recommendations = full_recommendations

class RecommenderStorage:
    '''
    Wrapper class for methods that relate to storing recommendations in MongoDB.
    '''

    @staticmethod
    def dump_recs_to_mongo(username, person):
        '''
        Upserts single User's recommendations to MongoDB.

        Args:
            username (str):
                Discogs username
            person (TrainingUser or RecalcUser):
                Instantiated TrainingUser or RecalcUser object, where recs and 
                metadata methods have already been called.
        '''
        # store to Mongo
        with MongoCollection('users') as mongo:    
            post = {'username':username,
                    'recommendations': person.recommendations.to_dict('records'),
                    'previous_interactions': {
                                        'artists': person.already_known_artists,
                                        'labels': person.already_known_labels
                    }
                }

            post = json_util.dumps(post)
            post = json_util.loads(post)
            mongo.replace_one({'username': username}, post, upsert=True)
        return None

    @classmethod
    def store_user_recommendations(cls, username, idx, recommendations_array, 
                                raw_interactions, release_id_map,
                                all_interactions, all_interactions_dedupe,
                                person=None, batch_mode=True):
        '''
        Pipeline to store TrainingUser recommendations with metadata in MongoDB.

        Args:
            username (str):
                Discogs username
            idx (int):
                Index number for user - only used for logging purposes to get 
                a status update on batch.
            recommendations_array (numpy.ndarray):
                Full array of recommendations for all users.
            raw_interactions (pandas.DataFrame):
                DataFrame of all releases used as input to recommendation model,
                without full metadata.
            release_id_map (pandas.DataFrame):
                DataFrame that maps release_id to release_idx (category codes).
            all_interactions (pandas.DataFrame):
                DataFrame of all releases used as input to recommendation model,
                with full metadata.
            all_interactions_dedupe (pandas.DataFrame):
                DataFrame of all releases used as input to recommendation model,
                with full metadata, where release ids have been deduplicated.
            person ():
                TODO deprecate
            batch_mode ():
                TODO deprecate

        Returns:
            float:
                Pipeline execution time.
        '''
        # get User Recommendations with Metadata and Previous Interactions
        print(f'Starting user - {username} - #{idx}')
        start = time.time()
        if batch_mode:
            person = TrainingUser(username)
            person.get_interactions(raw_interactions)
            person.get_already_known(all_interactions)
            person.get_recommendations(recommendations_array, release_id_map, all_interactions_dedupe)
        else: # this section now unnecessary TODO: deprecate
            if not isinstance(person, RecalcUser):
                raise TypeError("Batch_Mode is off - you must specify a RecalcUser object as 'person'")
            person = person
            person.get_recs_metadata(recommendations_array, release_id_map)
        
        cls.dump_recs_to_mongo(username, person)

        return time.time() - start

    @classmethod
    def store_recalc_user_recommendations(cls, username, idx, recommendations_array, 
                                          release_id_map, person):
        '''
        Pipeline to store RecalcUser recommendations with metadata in MongoDB.

        Args:
            username (str):
                Discogs username
            idx (int):
                Index number for user.
            recommendations_array (numpy.ndarray):
                Array of recommendations/scores output by implicit ALS "recommend" method.
            release_id_map (pandas.DataFrame):
                DataFrame that maps release_id to release_idx (category codes).
            person (discogs_assistant.rec_models.RecalcUser):
                RecalcUser object for out-of-sample User.

        Returns:
            float:
                Pipeline execution time.
        '''
        # get User Recommendations with Metadata and Previous Interactions
        print(f'Starting user - {username} - #{idx}')
        start = time.time()
        # get all attributes for RecalcUser "person"
        person = person
        person.get_recs_metadata(recommendations_array, release_id_map)
        
        # store recommendations
        cls.dump_recs_to_mongo(username, person)
        
        return time.time() - start