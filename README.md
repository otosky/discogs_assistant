# discogs_assistant
Code base of Python tools supporting [discogsassistant.io](https://discogsassistant.io) - a web-application for Discogs Users that generates album recommendations and curates optimized Discogs shopping carts.

***

To install:

`pip install git+https://github.com/otosky/discogs_assistant.git`

## Modules

**gen_models.py:**

Classes related to fetching User interactions from database prior to fitting ALS recommendation model, connecting to Redis cache, and Discogs API user-authentication.

**rec_models.py**

Classes related to collecting User profiles from Discogs, fetching and storing recommended release metadata for Batch recommendations, and recalculating single-user recommendations on-the-fly.

_These are used in the recommendation microservices._

**cart_models.py**

Classes related to scraping Discogs market listings and computing optimized shopping carts via a 0/1 knapsack-style algorithm.  

_These are used in the cart-building microservices._

***
## General Overview of the Application:
![Infrastructure Overview](/images/discogs_assistant_architecture_overview.png)

## Recommendation-Related Services:
![Recommendation In-Depth](/images/discogs_assistant_architecture_recs.png)

### When a User clicks to request their recommendations:

1. A message is published from Flask to kickstart the pipeline.
2. ***collect_user_profile* consumes message**
    * Checks to see if their profile in our database is up-to-date with Discogs.
    * If not, publishes message to move on to step 3.
    * If so, publishes message moves on to step 5.
3. **Serverless scrapers (Cloud Functions) are triggered to fetch wantlist/collection items for User in chunks so that our database is current and up-to-date.**
    * New interactions are stored in Postgres.
    * Message is published to move on to step 4.
4. ***log_user_up_to_date* consumes messages from step 3 denoting success of function.**
    * These messages decrement a counter for the total number of chunks in transaction, to determine when all chunks have been successfully executed.
    * This payload counter is a key/value in the Redis "Temp Cache".
    * When all chunks have been executed, a message is published to move on to step 5.
5. ***upsert_user* consumes message that User's profile is up-to-date and ready to calculate recommendations.**
    * Fetches User's wantlist/collection items from Postgres.
    * Formats User's interactions into a sparse user-item vector.
    * Calculates User recommendations by feeding vector to most recent ALS matrix factorization model, see [implicit ALS](https://github.com/benfred/implicit/blob/master/implicit/als.py) `recalculate_user` methods for implementation details.
    * Stores User recommendations with recommendation score and release metadata to MongoDB as JSON.

*Each step updates a Redis cache that stores the status of the request transaction.  The front-end polls this status_update cache updating the User as different stages of the pipeline progress, and also lets the browser know when the Mongo database is finally ready to output album recommendations.*

## Cart-Related Services:
![Cart In-Depth](/images/discogs_assistant_architecture_carts.png)

### When a User clicks to request optimized shopping carts:

1. **They input and select criteria to customize their shopping carts from a web-form, specifying:**
    * Budget
    * Minimum Media Condition (lowest accepted vinyl grade)
    * Minimum Seller Rating
    * Minimum Cart Quantity (number of records)
    * Seller Location
    * Wantlist-to-Recommendation Ratio (whether to compute carts with only recommended-albums, only wantlist-albums, or some mix of the two)
2. When the criteria form is submitted, a message is published to kickstart the pipeline.
3. ***get_candidates* determines which Discogs releases to scrape for marketplace listings**
    * If only recommendation albums desired:
        * Release_ids and recommendation scores pulled from MongoDB recommendation storage.
    * If only wantlist items desired:
        * Wantlist items pulled from Postgres and each given a uniform "recommendation score".
    * If a mixture of wantlist & recommendation items desired, the top-N recommended releases will be mixed with a subselection of wantlist items to achieve desired ratio.  *Subselection of wantlist items is based on a random seed if the User's wishlist is larger than the desired proportion.*
    * Candidates are disqualified if their median selling price is greater than the User's budget.
    * List of candidate release_ids and scores are published as message to Step 4.
4. ***trigger_market_scrapers* handles determining which releases need market listings scrape and triggers serverless scrapers in batches.**
    * When a scraper extracts the market listings for a given release_id, it sets a key/value in Redis with a TTL of 24 hours.  This means that every listing used to compute an optimized cart are out-of-sync with Discogs by *at most* 24 hours. I think this is reasonable enough, given my experience with Discogs.
    * This step checks Redis before sending out triggers to Cloud Function scrapers to reduce the list of releases to scrape, if they've already been scraped within the last 24 hours.
    * Remaining releases-to-be-scraped are chunked in batches and sent as Pub/Sub message triggers to Step 5. *Note: total number of chunks sent as part of message to determine when total job is completed in Step 6.*
5. **Serverless scrapers (Cloud Functions) are triggered to scrape market listings for batched release_ids.**
    * Market listings and seller information are persisted in Postgres.
    * Message is published to move on to step 6.
6. ***log_marketplace_success* consumes messages from Step 5 denoting success of scraper function.**
    * Each message decrements a counter for the total number of chunks in transaction, to determine when all chunks have been successfully executed.
    * This payload counter is a key/value in the Redis "Temp Cache".
    * When all chunks have been executed, a message is published to move on to Step 7.
7. ***build_carts* performs an 0/1 knapsack algorithm to compute the best scoring shopping carts given the User's budget and other cart criteria.**
    * All market listings for candidate release_ids are filtered for User's cart criteria (see Step 1 criteria).
    * Function that determines "score" is dictated by `quantity**2 * recommendation_score"`
    * Candidate market listings are grouped by seller.
        * If the subtotal for a seller's inventory < budget, then score as-is.
        * If the subtotal > budget, then flag this seller as needing "knapsack" algorithm.
    * Knapsack algorithm performs some dynamic programming to determine the top-N best carts for these flagged sellers. More on that here: [insert link to blog post] *Note: after the top-1 knapsack, you won't **always** get the subsequent-N top possible knapsacks, but it functions well enough as a heuristic (and is way more efficient than trying to iterate through all possibilities as a power-set).*
    * All candidate carts are then sorted by "score" descending.
    * Top-ranked carts are stored to MongoDB with their constituting release and seller metadata.  N is set to 20 carts here.

*Each step updates a Redis cache that stores the status of the request transaction.  The front-end polls this status_update cache updating the User as different stages of the pipeline progress, and also lets the browser know when the Mongo database is finally ready to output optimized shopping carts.*
