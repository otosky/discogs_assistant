import time, os, pickle
from bson import json_util
from math import ceil
import redis
from datetime import datetime
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import scipy.sparse as sparse
from .exceptions import APILimitExceeded
from .utils import flatten_list, url_to_lxml, make_chunks, MongoCollection, \
                   connect_psql, publisher_setup, BUCKET_NAME, upload_blob, \
                   get_pickle_directory
from io import BytesIO

MAX_WORKERS = 4

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
        # TODO optimize this - queries are WAY slow; maybe move to Parquet
        '''
        Fetches a all users' Wantlist and Collection release_ids from database. 
        
        Formats DataFrame so that Wantlist & Collection receive different
        weight scores and username/release_id are converted to category codes - 
        a necessary step before converting to csr_matrix for ALS model.
        '''
        
        with connect_psql() as conn:
            # gather all users' wantlists, assign each a score of 1
            q = '''
            SELECT release_id, username 
            FROM wantlist
            UNION
            SELECT release_id, username
            FROM non_user_wantlist;
            '''
            wantlists = pd.read_sql_query(q, conn)
            wantlists['score'] = 1
            # gather all users' collection, assign each a score of 2
            q = '''
            SELECT release_id, username 
            FROM collection
            UNION
            SELECT release_id, username
            FROM non_user_collection;'''
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

    def create_user_item_vector(self, release_id_map):
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
        
        all_release_len = release_id_map['release_idx'].max()
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

        full_recommendations['artist'] = full_recommendations['artist'].fillna('Various')
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