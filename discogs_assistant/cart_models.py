import re, base64, os
import redis
from datetime import datetime, timedelta
from tenacity import retry, stop_after_delay, stop_after_attempt
import pandas as pd
import numpy as np
from .utils import connect_psql, publisher_setup
from .scrape_models import Cache

# Discogs Media Condition Grades
# Numeric codes used to filter SQL queries
MEDIA_COND_DICT = {'Any':0,
                   'Poor (P)':1,
                   'Fair (F)':2,
		           'Good (G)':3,
		           'Good Plus (G+)':4,	
		           'Very Good (VG)':5,
 		           'Very Good Plus (VG+)':6,
 		           'Near Mint (NM or M-)':7,
 		           'Mint (M)':8
                  }

# Function used to rank shopping carts
WEIGHT_FUNC = 'weight = score * quantity**2'
EXPIRY = timedelta(hours=24)
REDIS_PORT = os.environ.get('REDIS_PORT')

DISTRIBUTE_SCRAPERS_TOPIC = os.environ.get('DISTRIBUTE_SCRAPERS_TOPIC')
TRIGGER_SCRAPERS_TOPIC = os.environ.get('TRIGGER_SCRAPERS_TOPIC')
BUILD_CART_TOPIC = os.environ.get('BUILD_CART_TOPIC')

class CartCandidates:
    '''
    Methods related to gathering candidate releases for curated shopping carts.

    Args:
        username (str):
            Username of user requesting optimized carts.
        budget (int):
            Maximum price threshold for optimized carts.
        wantlist_proportion (float):
            Proportion of wantlist releases to recommendation releases present
            in shopping cart candidates.
        criteria (str):
            JSON string of cart criteria requested by User.
            e.g. 
        trans_id (str):
            22-character Transaction ID for cart request.
        port (int):
            Redis port number.
    
    Attributes:
        user (str):
            Username of user requesting optimized carts.
        criteria (str):
            JSON string of cart criteria requested by User.
            e.g. 
        budget (int):
            Maximum price threshold for optimized carts.
        wantlist_prop (float):
            Proportion of wantlist releases to recommendation releases present
            in shopping cart candidates.
        port (int):
            Redis port number
        trans_id (str):
            22-character Transaction ID for cart request.
        criteria_key_prefix (str):
            Composite key to identify primary criteria for cart request.
            Takes for "username|budget|wantlist_proportion".
        c_id_suffix (int):
            Suffix number to differentiate between cart requests made under 
            "username|budget|wantlist_proportion" composite key.
        is_first (boolean):
            Whether this is User's first cart request (in last 24 hours).
        duplicate_request (boolean):
            Whether User has already requested a cart under this criteria (in 
            last 24 hours).
    '''
    def __init__(self, username, budget, wantlist_proportion, 
                 criteria, trans_id, port=REDIS_PORT):

        self.user = username
        self.criteria = criteria
        self.budget = budget
        self.wantlist_prop = wantlist_proportion
        self.port = int(port)
        self.trans_id = trans_id
        # sets identifier for this grouping of criterion
        self.criteria_key_prefix = f'{self.user}|{self.budget}|{self.wantlist_prop}'
        # defaults
        self.c_id_suffix = 1
        self.is_first = False
        self.duplicate_request = False

    def check_if_unique_query(self):
        '''
        Checks to see if cart request is a duplicate of a request made by user 
        in the last 24 hours.
        '''
        r = redis.Redis(port=self.port)
        
        # check if user exists in Redis 
        # NOTE: records purged 24h after initial request in 24h window
        is_first = not r.hexists(self.user, 'requests')
        if is_first:
            self.is_first = True
        else:
            # get the values for this User|budget|wantlist_prop hashset
            # NOTE: keys for this hset are just ordinal numbers (1,2,3...)
            same_pp_criteria = r.hvals(self.criteria_key_prefix)
            same_pp_criteria = [val.decode('utf-8') for val in same_pp_criteria]
            # if this is a new set of criteria for this pricepoint,
            # then it's not a duplicate request; 
            ## suffix for this key will increment the total size of requests 
            ## at this budget|wantlist_prop by 1
            if self.criteria not in same_pp_criteria:
                self.duplicate_request = False
                self.c_id_suffix = len(same_pp_criteria) + 1
            # if it is a duplicate request, find the key matching the 
            # criteria value by index + 1; assign as criteria id suffix
            # NOTE: index is from Python list, so begins at 0
            else:
                self.duplicate_request = True
                self.c_id_suffix = same_pp_criteria.index(self.criteria) + 1
    
    # TODO - determine whether having a Mongo check here would be useful
    # this would save a call to build_carts, but it would mean that I have to disable 
    # the % scraped threshold in the logging to redis script
    # i.e. if I always check Mongo first to see if the criteria_id request has
    # already been completed, it will always just use the first iteration, where
    # potentially only a partial set of the candidates have been used.
    
    def _fetch_wantlist_items(self):
        '''
        Retrieves a proportional number of wantlist items based on User criteria.

        Queries database for User's full wantlist and then filters out any items
        that exceed a median price of the User's budget.

        Returns:
            pandas.DataFrame:
                A DataFrame of release_ids from user's wantlist where median
                price is below budget threshold. 

        '''
        # get full wantlist as long as release_id in current Discogs XML dump
        q = '''
            SELECT release_id 
            FROM wantlist AS w
            JOIN release AS r
            ON r.id = w.release_id
            WHERE username=%s
            '''
        
        with connect_psql() as conn:
            df = pd.read_sql_query(q, conn, params=(self.user,))

            # get prices for releases in User's wantlist
            q = '''
            SELECT release_id, median
            FROM price_stats
            WHERE release_id IN %s
            '''
            params = (tuple(df['release_id'].tolist()),)
            prices = pd.read_sql_query(q, conn, params=params)
        conn.close()
        
        df = df.merge(prices, how='left', on='release_id').fillna(0)
        # remove items exceeding budget threshold
        df = df[df['median'] < self.budget]
        df = df.drop(columns=['median']) # median prices no longer necessary
        
        return df.reset_index(drop=True)
    
    def get_additional_wantlist_items(self, wantlist_proportion, num_rec_rows):
        '''
        Fetches wantlist items below budget and pseudo-randomly subselects a quantity
        in proportion to wantlist_proportion.  wantlist_items + 
        recommendation_items will equal total number of eligible recommendations
        under budget threshold (max 2000 items).

        NOTES:
        Random seed is generated by a using the current date's ISO-day.  
        
        If total wantlist items is less than the target number of items 
        (wantlist_proportion * number of items from recommendations),
        then returns the full wantlist.
        
        If total wantlist items is greater, then it returns a random selection of 
        rows equal to target number of items.

        Args:
            wantlist_proportion (float)
                Proportion of wantlist releases to recommendation releases present
                in shopping cart candidates.
            num_rec_rows (int):
                Total number of recommended releases where median price is below
                budget.

        Returns:
            pandas.DataFrame:
                DataFrame of release_ids from User's wantlist where number of rows
                matches proportion set by wantlist_proportion.
        '''
        # get full list of release_ids from User wantlist where median < budget
        df = self._fetch_wantlist_items()
        
        target_num_items = int(wantlist_proportion * num_rec_rows)
        wantlist_size = df.shape[0]
        
        if wantlist_size < target_num_items:
            return df
        
        # if there are more wantlist items than target_num_items, subselect rows
        seed = datetime.now().timetuple().tm_yday # gets day of year 
        np.random.seed(seed)
        random_indices = np.random.choice(wantlist_size, target_num_items, replace=False)
        
        return df.reindex(random_indices).reset_index(drop=True)

    def subselect_recommendation_candidates(self, wantlist_df, rec_df, 
                                            wantlist_proportion, min_total_candidates=200):
        '''
        Subselects recommendation candidates for carts by taking the top-N 
        recommended release_ids per User's desired wantlist-to-recommendation proportion.

        Args:
            wantlist_df (DataFrame):
                Subselected DataFrame of release_ids from User's wantlist where 
                number of rows has been altered to match wantlist_proportion
            rec_df (DataFrame):
                DataFrame of all recommended releases where median price is
                less than User's defined budget.
            wantlist_proportion (float):
                Proportion of wantlist releases to recommendation releases present
                in shopping cart candidates.
            min_total_candidates (int):
                Total number of release candidates in final candidate set.
                (wantlist_releases + recommendation_releases)
        
        Returns:
            pandas.DataFrame:
                Top-N recommended releases under budget, where N equals the inverse
                proportion of "wantlist_proportion" times the full number of 
                eligible recommended releases.
        '''
        # return full DF if only recommended items requested
        if wantlist_proportion == 0:
            return rec_df
        # return empty DF if only wantlist items requested
        elif wantlist_proportion == 1:
            return rec_df.iloc[0:0]
        else:
            # subselect top-N recommended releases to match inverse proportion 
            # of wantlist_proportion
            wantlist_rows = wantlist_df.shape[0]
            rec_proportion = 1 - wantlist_proportion
            target_num_rows = int(rec_proportion * rec_df.shape[0])
            if wantlist_rows + target_num_rows < min_total_candidates:
                new_target_num_rows = min_total_candidates - wantlist_rows
                return rec_df.iloc[:new_target_num_rows]
            else:
                return rec_df.iloc[:target_num_rows]

    def consolidate_candidates_df(self, wantlist_df, rec_df):
        '''
        Concatenates wantlist release candidates and recommendation release
        candidates, assigning a recommendation weight to all wantlist items that is
        either the minimum value from the subselected recommendation releases or a 
        static 1, if the User only desired wantlist items in their carts.

        Args:
            wantlist_df (DataFrame):
                Subselected DataFrame of release_ids from User's wantlist where 
                number of rows has been altered to match wantlist_proportion
            rec_df (DataFrame):
                Subselected DataFrame of all recommended releases where median 
                price is less than User's defined budget.

        Returns:
            pandas.DataFrame:
                Concatenated DataFrame of release_ids from subselected wantlist
                and recommendation candidate dataframes.  Score column reflects 
                recommendation "weight".
        '''
        # if condition only applies when rec_df is empty
        if rec_df['score'].min() is np.NaN:
            wantlist_df['score'] = 1
        else:
            wantlist_df['score'] = rec_df['score'].min()    

        return pd.concat([wantlist_df, rec_df]).reset_index(drop=True)

    @retry(stop=(stop_after_delay(10) | stop_after_attempt(5)))
    def publish_handoff_message(self, candidates_df):
        '''
        Passes cart candidates to next stage in pipeline. 
        
        Either triggers market-scraper broker if cart request is unique, or will 
        move straight to cart optimization if request has previously been made.

        Args:
            candidates_df (pandas.DataFrame):
                DataFrame of cart candidates, having columns "release_id" and
                "score".
        
        Returns:
            None
        '''
        # set full criteria_id - used for Mongo dump identification
        self.c_id = f'{self.criteria_key_prefix};:{self.c_id_suffix}'
        
        # if this request is not a duplicate:
        if not self.duplicate_request:
            # trigger the market scraper script

            publisher, topic_path = publisher_setup(DISTRIBUTE_SCRAPERS_TOPIC)
            data = str(candidates_df['release_id'].tolist()).encode('utf-8') ##### TODO make this send the whole df as data
            future = publisher.publish(topic_path, data=data, username=str(self.user),
                                       criteria_id=str(self.c_id), trans_id=self.trans_id,
                                       status=str(2))
            Cache.update_transaction_status(self.trans_id, str(2))
        
        # if this is a duplicate request:
        else:
            publisher, topic_path = publisher_setup(BUILD_CART_TOPIC)
            # check Redis to ensure that prerequisite steps actually complete
            r = redis.Redis(port=self.port)
            is_ready = r.get(f'ready|{self.c_id}')
            # if the prereq steps are actually complete, publish msg
            # to divert straight to cart builder
            if is_ready:
                data = candidates_df.to_json(orient='split').encode('utf-8')
                future = publisher.publish(topic_path, data=data, 
                                           username=str(self.user),
                                           criteria_id=str(self.c_id), 
                                           criteria = self.criteria,
                                           trans_id=self.trans_id, status=str(3))
                Cache.update_transaction_status(self.trans_id, str(3))
                print('''Request has been executed previously - 
                         sending straight to cart_builder''')
                print('Message ID: ', future.result())
            else:
                # two scenarios could happen here:
                # A) the log_success script failed before creating a Redis entry
                #    denoting that request is ready; but request is actually
                #    safe to build cart from
                # B) the scrapers are still gathering listings and the user has
                #    made a duplicate request too quickly; request is NOT
                #    safe to build cart from
                self.duplicate_request = False
                r.hincrby(self.user, 'requests', -1)
                r.hincrby(self.user, f'requests@{self.budget}', -1)
                r.hincrby(self.user, 'dupe_requests')
                r.hincrby(self.user, f'dupe_requests@{self.budget}')
                # the error below will retrigger the function via tenacity,
                # but with the dupe request attribute flipped
                # should send message to trigger scraper script and complete from there
                print('Error finding duped request in Redis - RETRYING FROM TOP')
                raise TypeError

    ## SEE IF THIS IS ACTUALLY USED ANYMORE - LOOKS LIKE ONLY THE USER EXPIRY IS IMPORTANT HERE
    def log_to_redis(self, candidates, expiry=timedelta(hours=23)):
        '''
        Logs User request data to Redis.  ONLY USE METHOD AFTER PUBLISHING
        trigger or build_cart MESSAGE!!

        Note that it's in the inverse order of check_inverse_query.  So it goes
        from the case of a 'duplicate_request' funneled down to a 'first_request'.
        
        Args:
            param_name (type):
                summary of parameter
        
        Returns:
            type
                Summary of returned value.
        '''
        r = redis.Redis(port=self.port)
        # increment requests and request@budget keys for User
        r.hincrby(self.user, 'requests')
        if self.duplicate_request:
            # if it's a duplicate request decrement requests & increment 
            # corresponding dupe_request keys
            r.hincrby(self.user, 'requests', -1)
            r.hincrby(self.user, 'dupe_requests')
        # if not a duplicate request:
        else:
            # set a hash for criteria key prefix of form '{username}|{budget}|{wantlist_prop}'
            # key will be ordinal number suffix (1,2,3..)
            # value will be the criteria itself
            # e.g. {budget:50, country:'USA', min_media_cond:'Mint (M)', ...}
            r.hset(self.criteria_key_prefix, self.c_id_suffix, self.criteria)
            # log candidates
            r.hset(self.user, f'candidates@{self.c_id}', candidates)
            # if this budget has already been requested:
            if not self.is_first:
                pass
            # if this request IS the User's first (in past 24 hours)
            else:
                # set user creation time and expiry
                t = datetime.utcnow()
                format_ = "%Y-%m-%d %H:%M:%S.%f"
                # need to encode the datetime so that upon Mongo dump 
                # at end of pipeline, it can be decoded properly
                t = t.strftime(format_).encode('utf-8')
                t = base64.b64encode(t)
                # set User creation time in Redis
                r.hset(self.user, 'created_at', t) 
                # this expires the User's hashset
                r.expire(self.user, expiry)
            # add expiry to hash sets for user criteria key prefix
            # will be of form '{username}|{budget}|{wantlist_prop}' e.g. 'tosker|50|.50'
            user_ttl = r.ttl(self.user)
            r.expire(self.criteria_key_prefix, user_ttl)
    
    @staticmethod
    def update_scrapelist_from_redis(release_ids):
        '''
        Removes release_ids from scrape list if the market listings have already
        been scraped for the day.

        Args:
            release_ids (List of integers):
                A list of release_id candidates that might need to be scraped.
        
        Returns:
            List of integers
                Modified list of release_ids that haven't yet had market listings
                scraped.
        '''
        
        r = redis.Redis()
        to_scrape = [id_ for id_ in release_ids if not r.exists(f'release_{id_}')]
        
        return to_scrape

    @staticmethod
    def trigger_scrapers(chunks, user, criteria_id, total_msgs, trans_id):
        '''
        Triggers Cloud function via Pub/Sub message; scrapes all Discogs market listings for 
        release_ids grouped into chunks of size N.

        Args:
            chunks (List of Lists of Integers):
                Cart candidate release_ids grouped into lists of size N.
            user (str):
                User (username) requesting optimized carts.
            criteria_id (str):
                Identifier for criteria of cart request in Redis.
                Will be of form "username|budget|wantlist_proportion|integer_suffix".
            total_msgs (int):
                Number of chunks.  Will be used as decremented counter to determine
                when all messages have been consumed.
            trans_id (str):
                22-character Transaction ID for cart request.
        
        Returns:
            None
        '''
        
        publisher, topic_path = publisher_setup(TRIGGER_SCRAPERS_TOPIC)
        
        for i in chunks:
            data = str(list(i))
            # Data must be a bytestring
            data = data.encode('utf-8')
            # When you publish a message, the client returns a future.
            # kwargs after 'data' get formatted as JSON under 'attributes
            future = publisher.publish(topic_path, data=data, username=str(user), 
                                        criteria_id=str(criteria_id), 
                                        total_payload=str(total_msgs),
                                        trans_id=trans_id)
            print(future.result(), flush=True)
            print('Published messages.', flush=True)
    
    @staticmethod
    def trigger_build_carts(release_ids, user, criteria_id, trans_id, redis_port=REDIS_PORT):
        '''
        Triggers build_cart callback via Pub/Sub to compute optimized shopping carts.

        Args:
            release_ids (List of integers):
                Release_ids of cart candidates.
            user (str):
                Username of user requesting carts.
            criteria_id (str):
                Identifier for criteria of cart request in Redis.
                Will be of form "username|budget|wantlist_proportion|integer_suffix".
            trans_id (str):
                22-character Transaction ID for cart request.
            redis_port (int):
                Redis port number.
        
        Returns:
            None
        '''
        r = redis.Redis(port=redis_port)

        publisher, topic_path = publisher_setup(BUILD_CART_TOPIC)
        id_prefix, id_suffix = criteria_id.split(';:')
        #budget = id_prefix.split('|')[-1]
        candidates = r.hget(user, f'candidates@{criteria_id}') 
        criteria = r.hget(id_prefix, id_suffix)
        future = publisher.publish(topic_path, data=candidates, username=str(user),
                                    criteria_id=criteria_id, criteria=criteria, 
                                    trans_id=trans_id, status=str(3))
        Cache.update_transaction_status(trans_id, str(3))
        print(future.result(), flush=True)
        # set ready in Redis here
        r.set(f'ready|{criteria_id}', 1)
        expiry = r.ttl(user)
        r.expire(criteria_id, expiry)
        print(f'Proceeding to building carts for {user} criteria_id {criteria_id}', flush=True)

class MarketplaceLogger:

    '''
    Class wrapper for static functions related to logging the success of market 
    listing scrapers.  

    Adds scraped release_ids to Redis with expiry; checks if all user's candidate
    releases have been scraped; when complete, handsoff to final build_carts script.
    '''

    @staticmethod
    def convert_list_to_inclusion_dict(list_, key_prefix=None):
        '''
        Converts a list to a dictionary where the list values are keys
        and the values are all counts of 1.

        Args:
            list_ (List):
                List of integers or strings.

        Returns:
            dictionary:
                Dictionary where keys are the elements from the input list 
                (prepended by an optional prefix) and the values are 1.
        '''
        if not key_prefix:
            included = {str(elem):1 for elem in list_}
        else:
            included = {f'{key_prefix}{elem}':1 for elem in list_}
        return included
    
    @classmethod
    def add_kv_to_redis(cls, keys, values=None, expiry=EXPIRY, inclusion_dict=True):
        '''
        Adds key:value pair to Redis database from a list of values or dictionary,
        expiry time optional.

        Args:
            keys (List):
                List of primitive-type elements.
            values (List):
                List of primitive-type elements.
                Default: None
            expiry (timedelta or int):
                TTL for keys added to Redis.  Can be either a datetime.timedelta
                or an integer type representing time in seconds.
            inclusion_dict (boolean):
                Whether to transform list of keys into an "inclusion_dict", 
                which uses elements of "keys" as keys and count of 1 as values.

        Returns:
            None
        '''
        # instantiate Redis connection
        redis_conn = redis.Redis()
        
        if inclusion_dict:    
            # convert values to inclusion_dict
            pairs = cls.convert_list_to_inclusion_dict(keys, key_prefix='release_')
        elif not values:
            raise ValueError('You need to supply a list of values for the values parameter.')
        elif len(keys) != len(values):
            keys_len = len(keys)
            values_len = len(values)
            raise ValueError(f'''
                              Size mismatch in shape of "keys" list [{keys_len}] 
                              and "values" list [{values_len}].
                              ''')
        else:
            pairs = {key:value for key, value in zip(keys,values)}
        
        # add keys & values to Redis with expiry time
        redis_conn.mset(pairs)
        if expiry:
            for key in pairs.keys():
                redis_conn.expire(key, expiry)
        return None
    
    @staticmethod
    def increment_user_cart_key(key, expiry=EXPIRY):
        '''
        Increments User's Cart Criteria-Key in Redis.  
        
        This acts as a counter for determining when sufficient candidate releases 
        have been scraped, using the number of chunks sent out as Pub-Sub 
        messages as a target to reach.

        Args:
            key (str):
                User's Cart-Criteria-Key; 
                Format like "username|budget|wantlist_proportion|integer_suffix".

        Returns:
            None
        '''
        redis_conn = redis.Redis()
        redis_conn.incr(key)
        if expiry:
            redis_conn.expire(key, expiry)
        return None

    @staticmethod
    def check_user_scrape_complete(key, total_packet_size, threshold=1.0):
        '''
        Checks to see whether User-Criteria key has reached a sufficient threshold
        of chunks scraped.

        Args:
            key (str):
                User's Cart-Criteria-Key; 
                Format like "username|budget|wantlist_proportion|integer_suffix".
            total_packet_size (int):
                Total number of chunks that should have been scraped.
            threshold (float):
                Optional percentage threshold of scraped chunks to be 
                considered "completed" and move on to next stage in pipeline.

        Returns:
            boolean
                True if scraping is complete; False if there are still more
                chunks scraping or yet-to-be scraped.
        '''
        redis_conn = redis.Redis()
        currently_complete = redis_conn.get(key)
        pct_completed = int(currently_complete) / int(total_packet_size)
        if pct_completed >= threshold:
            return True
        else:
            return False

    @staticmethod
    def handoff_to_build_carts(username, criteria_id, trans_id):
        '''
        Sets "ready" keys for cart request and publishes message to handoff
        to "build_carts" script.

        Args:
            username (str):
                Explanation
            criteria_id (str):
                Identifier for criteria of cart request in Redis.
                Will be of form "username|budget|wantlist_proportion|integer_suffix".
            trans_id (str):
                22-character Transaction ID for cart request.

        Returns:
            None
        '''
        
        # log to Redis that cart_request ready to be built
        r = redis.Redis()
        r.set(f'ready|{criteria_id}', 1)
        expiry = r.ttl(username)
        r.expire(criteria_id, expiry)
        # grab cart candidates from Redis
        id_prefix, id_suffix = criteria_id.split(';:')
        budget = id_prefix.split('|')[-1] # this is never used??
        candidates = r.hget(username, f'candidates@{criteria_id}') ## EDIT
        # grab criteria from Redis
        criteria = r.hget(id_prefix, id_suffix)
        # publish msg to trigger build_cart script
        publisher, topic_path = publisher_setup(BUILD_CART_TOPIC)
        # publish message to trigger cart builder
        future = publisher.publish(topic_path, data=candidates, username=str(username),
                                criteria_id=str(criteria_id), criteria = criteria,
                                trans_id=trans_id, status=str(3))
        Cache.update_transaction_status(trans_id, str(3))
        print(future.result())
        return None

class MultiItemSeller:

    '''
    A seller with multiple items in their inventory in which the total exceeds
    user's given budget and therefore needs a knapsacking operation to determine
    the most optimized carts.

    Args:
        seller_id (int):
            Seller_id number from Discogs.  Used to filter all candidate listings
            DataFrame.
        all_listings (pandas.DataFrame):
            DataFrame of market listings for all candidate releases in User's 
            cart request.  Columns are: release_id, listing_id, media_cond_num, 
            sleeve_cond, seller_id, seller_rating, price, country, cart_url

    Attributes:
        seller_id (int):
            Seller_id number from Discogs.
        inventory (pandas.DataFrame):
            DataFrame of seller's market listings.  Columns are: release_id, 
            listing_id, media_cond_num, sleeve_cond, seller_id, seller_rating, 
            price, country, cart_url
        dyn_table (numpy.ndarray):
            Memoized table of knapsack options.  See method "dynamic_knapsack".
        topN_knapsack_indices (numpy.ndarray):

        topN_knapsack_weights (numpy.ndarray):

    '''

    def __init__(self, seller_id, all_listings):
        self.seller_id = seller_id
        inventory = all_listings[all_listings['seller_id'] == seller_id]
        self.inventory = inventory.set_index('listing_id')
        self.dyn_table = None

    @staticmethod
    def _eval_score(price, qty, rec_scores):
        '''
        Evaluates a weighted score for a shopping cart given it's quantity
        and constituent recommendation scores.

        Args:
            price (float):
                Total price of shopping cart.
            qty (int):
                Number of items in shopping cart.
            rec_scores (List):
                List of float values where each element is a recommendation score
                for a particular release.

        Returns:
            float:
                Weighted score of shopping cart.
        '''
        
        #weight = 1/price * qty**2 * np.mean(rec_scores)
        weight = qty**2 * np.mean(rec_scores)
        return weight

    @staticmethod
    def groupby_seller(df):
        '''
        Groups candidate release market listings by seller.

        Args:
            df (pandas.DataFrame):
                A DataFrame of all market listings for the candidate releases 
                of a User's cart request.

        Returns:
            pandas.DataFrame:
                Modified DataFrame where each seller's inventory is aggregated,
                yielding columns for total price, a list of cart items, a list 
                of cart_urls, mean recommendation score, and total quantity.
        '''

        grouped = df.groupby('seller_id').agg({'price': 'sum',
                                                'listing_id': lambda x: list(x),
                                                'cart_url': lambda x: list(x),
                                                'score': 'mean',
                                                'quantity': 'count'})\
                                        .sort_values(by=['price', 'score'], 
                                                        ascending=[True, False])\
                                        .reset_index()\
                                        .rename(columns={'listing_id':'cart_items'})
        return grouped

    def dynamic_knapsack(self, price_threshold):
        '''
        Memoizes a matrix through dynamic programming a la the "knapsack problem" 
        in the context of yielding the most optimal Discogs shopping cart given a 
        seller's inventory.  

        Args:
            price_threshold (int):
                Maximum budget for user.

        Returns:
            None
                Assigns full memoized table to attribute dyn_table.
        '''

        poss_items = self.inventory.shape[0]
        # initialize empty array
        table_shape = (poss_items+1, price_threshold+1)
        # matrix to hold evaluated scores 
        eval_matrix = np.zeros(table_shape) 
        # matrix to hold consolidated parameters for groups of items being compared
        param_matrix = np.empty(table_shape, dtype=object)
        empty_fill = (0,0,[])
        param_matrix.fill(empty_fill)
        
        # loop through items
        for i in range(1, poss_items+1):
            price, rec_score = self.inventory.iloc[i-1][['price', 'score']]
            
            for j in range(1, price_threshold+1):
                prior_row_score = eval_matrix[i-1][j]
                prior_row_params = param_matrix[i-1][j]
                # TODO REMOVE:
                #p_price, p_qty, p_rec_score = prior_row_params 
                
                if price <= j:
                    leftover_cash = int(j - price) # converting to int to take "floor" difference
                    # previous row's score & params
                    #memoed_score = eval_matrix[i-1, leftover_cash] TODO REMOVE
                    memoed_params = param_matrix[i-1, leftover_cash]
                    m_price, m_qty, m_rec_score = memoed_params
                    # new row's score & params
                    contending_params = m_price + price, m_qty + 1, m_rec_score + [rec_score]
                    contender = self._eval_score(*contending_params)
                    
                    # update matrices with contender values only if contender 
                    # score is greater than the prior row's score
                    if contender > prior_row_score:
                        eval_matrix[i,j] = contender
                        param_matrix[i,j] = contending_params
                    else:
                        eval_matrix[i,j] = prior_row_score
                        param_matrix[i,j] = prior_row_params
                
                else:
                    eval_matrix[i,j] = prior_row_score
                    param_matrix[i,j] = prior_row_params
        
        self.dyn_table = eval_matrix

    def _get_topN_knapsacks(self, n=1):
        '''
        Reverse-sorts memoized table of possible carts to find the indices
        and weights corresponding to the top-N knapsacks.

        Args:
            n (integer):
                Number of knapsacks to take, ranked from highest score to lowest.

        '''
        table_shape = self.dyn_table.shape
        # get all unique values and indices from memoized table
        weights, indices = np.unique(self.dyn_table, return_index=True)
        # reverse sort values and indices
        weights, indices = weights[::-1], indices[::-1]
        # convert flattened indices to 2d indices
        indices = [np.unravel_index(i, table_shape) for i in indices]
            
        self.topN_knapsack_indices = indices[:n]
        self.topN_knapsack_weights = weights[:n]

    def _get_knapsack_items(self, index):
        '''
        Traverses memoized knapsack table from index of topN options and fetches
        the items that correspond to that knapsack.

        Args:
            index (tuple):
                (Row, Column) index of last item in knapsack.

        Returns:
            List
                Row indices of items in knapsack corresponding to listings in 
                a DataFrame of seller's inventory.
        '''
        #num_options = self.dyn_table.shape[0] TODO REMOVE
        start_row, budget = index
        in_cart = []
        # loop over rows of knapsack_table in reverse
        for i in range(start_row, 0, -1):
            # start from index; compare value to row above; if not same, then it's in cart
            if self.dyn_table[i, int(budget)] != self.dyn_table[i-1, int(budget)]:
                in_cart.append(self.inventory.index[i-1])
                # update budget
                budget = budget - self.inventory['price'].iloc[i-1]
            else:
                continue
            # move up row and repeat until arriving at value of 0
        return in_cart

    def convert_knapsack_to_groupcart(self, knapsack_item_ids, weight_func=WEIGHT_FUNC):
        '''
        Consolidates top knapsack items and aggregates cart with a score against
        which it can be ranked.

        Args:
            knapsack_item_ids (List):
                Indices of items knapsack from seller's inventory DataFrame.
            weight_func (str):
                String representation of the function used to generate
                weight score for cart.
        Returns:
            pandas.DataFrame:
                DataFrame where optimized cart is aggregated,
                yielding columns for total price, a list of cart items, a list 
                of cart_urls, total quantity, and weighted score.
        '''
        # filter market_listings for knapsack items
        cart = self.inventory.reindex(knapsack_item_ids)
        # reset index so that listing_id is a regular column again
        cart = cart.reset_index()
        # aggregate cart
        grouped = self.groupby_seller(cart)
        # score cart on eval weight formula
        grouped = grouped.eval(weight_func)
        
        return grouped

class KnapsackTools:

    '''
    Wrapper class for knapsack pipeline functions.
    '''

    def __init__(self):
        # I can probably get rid of this...
        pass

    @staticmethod
    def get_all_optimized_knapsacks(all_listings, flagged_sellers, budget, top_n=1):
        '''
        Gets all optimized knapsacks for sellers that have been flagged.

        Flagged sellers will end up being ones whom have more than 2 items
        in their inventory where the total exceeds the User's budget.

        Args:
            all_listings (pandas.DataFrame):
                A DataFrame of all market listings for the candidate releases 
                of a User's cart request.
            flagged_sellers (List):
                List of seller_ids for whom knapsacking functions should be 
                used against.
            budget (int):
                Maximum price threshold for optimized carts.
            top_n (int):
                Number of top-ranked carts to take from each seller.

        Returns:
            List:
                List of DataFrames, where each DataFrame corresponds to an 
                optimized cart from a flagged seller.  A seller can have more
                than one cart in this list, if top_n > 1.
        '''
        
        all_carts = []
        # iterate through each flagged seller
        for seller_id in flagged_sellers:
            # instantiate MultiItemSeller object
            seller = MultiItemSeller(seller_id, all_listings)
            # perform dynamic programming operation on sellers inventory
            # where total sums below budget
            seller.dynamic_knapsack(budget)
            # get the top N knapsacks for this seller
            seller._get_topN_knapsacks(top_n)
            # iterate through top knapsacks
            for k in seller.topN_knapsack_indices:
                # get knapsack item ids
                cart_items = seller._get_knapsack_items(k)
                # create a grouped cart from knapsack items with evaluated weight score
                grouped = seller.convert_knapsack_to_groupcart(cart_items)
                # append this cart to master list
                all_carts.append(grouped)
        
        return all_carts
    
    @staticmethod
    def reassemble_cart_options(orig_options, all_knapsacks, budget, 
                                min_qty, top_n_carts=20):
        '''
        Consolidates all cart options, filters out those that don't meet cart
        criteria, and takes the top_N ranked carts.

        Args:
            orig_options (pandas.DataFrame):
                All market listings grouped and aggregated by seller.
            all_knapsacks (List):
                List of DataFrames, where each DataFrame represents an optimized
                cart.
            budget (int):
                Maximum price threshold for optimized carts.
            min_qty (int):
                Minimum quantity threshold for optimized carts.
            top_n_carts (int):
                Number of top-ranked carts to return

        Returns:
            pandas.DataFrame:
                All top-ranked optimized carts per seller, filtered by budget and 
                minimum quantity, sliced to take the top-N carts ranked by 
                weight score.
        '''

        # consolidate carts that originally met budget with all_knapsacks
        all_knapsacks.extend([orig_options])
        final_options = pd.concat(all_knapsacks, ignore_index=True)
        # sort final cart options by 'weight' score
        final_options = final_options.sort_values('weight', ascending=False)
        # ensure that total cart price less than budget
        final_options = final_options[final_options['price'] < budget]
        final_options = final_options[final_options['quantity'] >= min_qty]
        # slice for top N carts by 'weight' score; reset the index from 'seller_id'
        return final_options.iloc[:top_n_carts].reset_index(drop=True)

    @staticmethod
    def setup_candidates_df(candidates, budget, criteria, day):
        '''
        Sets up DataFrame of candidate market listings given a list of release ids
        and cart criteria.

        Args:
            candidates (pandas.DataFrame):
                DataFrame of candidate releases, having at least a column labeled
                "release_id".
            budget (int):
                Maximum price threshold for optimized carts.
            criteria (dictionary):
                Cart criteria for user's cart request.  Must have fields: "country",
                "min_media_cond", "seller_rating".
            day (datetime.datetime.date):
                Date to use for currency conversion.

        Returns:
            pandas.DataFrame:
                All market listings of candidate releases that meet cart_criteria,
                with columns for "release_id", "listing_id", "media_cond_num",
                "sleeve_cond", "seller_id", "seller_rating", "price", "country",
                "cart_url".
        '''
        
        # query db and get all listings; dump into pandas
        conn = connect_psql()
        q = '''
        SELECT
            release_id, listing_id, media_cond_num, sleeve_cond,
            seller_id, seller_rating, 
            price AS orig_price, 
            ROUND((price::NUMERIC / b.exch_rate_eur * t.exch_rate_eur), 2) AS price,
            currency AS orig_currency, t.currency_symbol AS target_currency,
            country, cart_url
        FROM
            market_listings ml
        LEFT JOIN media_cond_grades mcg
        ON ml.media_cond = mcg.media_cond_str
        INNER JOIN currency_conversion b 
        ON ml.currency = b.currency_symbol AND
            b.date = %s
        INNER JOIN currency_conversion t
        ON t.currency_abbr = %s AND
            t.date = %s
        WHERE release_id in %s AND 
            country in %s AND 
            media_cond_num >= %s;
        '''
        # TODO 
        country = tuple(criteria['country'],)
        media_cond = criteria['min_media_cond']
        media_cond = MEDIA_COND_DICT[media_cond]
        min_seller_rating = int(criteria['seller_rating'])
        currency = criteria['currency']

        params = (day, currency, day, tuple(candidates['release_id'].tolist()), 
                  country, media_cond)
        data = pd.read_sql_query(q, conn, params=params)
        data = data.merge(candidates, on='release_id')
        data['quantity'] = 1
        # preprocess price column
        # currency_remover = re.compile(r'[^\d.]+')
        # data.loc[:,'price'] = data.price.apply(lambda x: currency_remover.sub('', x)).astype(float)

        ## FILTER ITEMS FOR USER-CRITERIA:
        # filter out items above max_total_cart threshold
        data = data[data.price < budget]
        # filter out items where seller has rating below threshold
        data = data[data.seller_rating >= min_seller_rating]
        # get rid of duplicate releases per each seller (don't want double copies in cart)
        data = data.drop_duplicates(subset=['seller_id', 'release_id'])

        return data

    @staticmethod
    def get_listing_release_info(listing_ids, currency, day):
        '''
        Fetches release metadata for Discogs market listings.

        Args:
            listing_ids (List):
                List of integers, where each element is a Discogs listing_id.

        Returns:
            dictionary:
                DataFrame transformed into dictionary where each key is a listing_id
                and each value is a dictionary of form {column: row_value}.  The 
                columns refer to the release_metadata fields.
        '''
        # TODO add label id if you can figure out how normalized that table is (label)
        q = '''
            SELECT 
                ml.listing_id, r.id release_id, im.filename image_file, artist_id, 
                a.name artist, title, label, media_cond,
                price, currency,
                ROUND((price::NUMERIC / b.exch_rate_eur * t.exch_rate_eur), 2) AS adj_price,
                t.currency_symbol AS target_currency
            FROM market_listings ml
            JOIN release r
                ON ml.release_id = r.id
            LEFT JOIN image_map im
                ON im.release_id = r.id
            JOIN release_artist ra
                ON ra.release_id = r.id
            JOIN release_label rl
                ON rl.release_id = r.id
            LEFT JOIN artist a
                ON a.id = ra.artist_id
            INNER JOIN currency_conversion b 
            ON ml.currency = b.currency_symbol AND
                b.date = %s
            INNER JOIN currency_conversion t
            ON t.currency_abbr = %s AND
                t.date = %s
            WHERE ml.listing_id IN %s;
            '''
        params = (day, currency, day, tuple(listing_ids))
        with connect_psql() as conn:
            release_info = pd.read_sql_query(q, conn, params=params)
            # drop duplicates because some releases will have multiple artists/labels
            release_info = release_info.drop_duplicates(subset='listing_id')
            release_info = release_info.set_index('listing_id', drop=True)
            release_info['artist'] = release_info['artist'].fillna('Various')
            release_info['listing_id'] = release_info.index
        return release_info.to_dict('index')

    @staticmethod
    def get_seller_metadata(seller_ids):
        '''
        Fetches seller metadata for given list of seller_ids.  
        
        Metadata includes "seller_name", "seller_rating", and (shipping) "country".

        Args:
            seller_ids (List):
                List of integers, where each element corresponds to a Discogs
                seller_id.

        Returns:
            dictionary:
                DataFrame transformed into dictionary where each key is a seller_id
                and each value is a dictionary of form {column: row_value}.  The 
                columns refer to the seller_metadata fields.
        '''
        
        q = '''
            SELECT seller_id, seller_name, seller_rating, country
            FROM market_sellers
            WHERE seller_id IN %s
            '''
        params = (tuple(seller_ids),)
        with connect_psql() as conn:
            seller_info = pd.read_sql_query(q, conn, params=params)
            seller_info = seller_info.set_index('seller_id', drop=True)
            seller_info['seller_id'] = seller_info.index
        return seller_info.to_dict('index')

    @classmethod
    def knapsack_pipeline(cls, candidates_df, budget, min_qty, currency, weight_func=WEIGHT_FUNC, top_n=20):
        '''
        Pipeline to get top-N optimized shopping carts.
        
        Steps:
        1. group by sellers
        2. separate sellers that need knapsacking
        3. knapsack em
        4. reassemble all options
        5. take top N

        Args:
            candidates_df (pandas.DataFrame):
                A DataFrame of all market listings for the candidate releases 
                of a User's cart request.
            budget (int):
                Maximum price threshold for optimized carts.
            min_qty (int):
                Minimum quantity threshold for optimized carts.
            weight_func (str):
                String representation of the function used to generate
                weight score for cart.
            top_n (int):
                Number of top-ranked carts to return.  Default is 20.

        Returns:
            pandas.DataFrame:
                DataFrame of optimized carts, aggregated to yield columns for 
                total price, a list of cart items, a list of cart_urls, 
                total quantity, weighted score, and the cart's rank.
        '''
        # group items by seller
        grouped_carts_sellers = MultiItemSeller.groupby_seller(candidates_df)
        # compute cart weight
        grouped_carts_sellers = grouped_carts_sellers.eval(weight_func)
        
        # flag sellers with more than 1 item aggregated, 
        # where total price exceeds threshold price
        flagged_sellers = grouped_carts_sellers.query('price > @budget and quantity > 1')
        flagged_sellers = flagged_sellers['seller_id'].values
        grouped_carts_sellers = grouped_carts_sellers.query('price <= @budget')

        # get all combinations of possible carts from these sellers
        all_knapsacks = cls.get_all_optimized_knapsacks(candidates_df, flagged_sellers, budget)

        # add knapsacks back to carts that originally met budget; limit to top N carts
        top_carts = cls.reassemble_cart_options(grouped_carts_sellers, 
                                            all_knapsacks, budget, min_qty, top_n_carts=top_n)
        top_carts['rank'] = top_carts.index + 1
        top_carts['target_currency'] = currency
        
        return top_carts