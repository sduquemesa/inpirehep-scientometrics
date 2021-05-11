#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Download and store on MongoDB documents from INSPIREHEP API

    author = Sebastian Duque Mesa
    maintainer = Sebastian Duque Mesa
    email = sebastian.duquem [at] udea.edu.co

    INSPIREHEP API doc at https://github.com/inspirehep/rest-api-doc
"""

# Import library for HTTP requests
import requests
from requests.exceptions import HTTPError
import json
import sys

# library for parsing URL params from response
import urllib.parse as urlparse
from urllib.parse import parse_qs

# library for rate limit timeout
import time

# logging library
import logging
handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s-%(funcName)s:\t%(message)s', datefmt="%Y-%m-%d %H:%M:%S", handlers=[handler])


# MongoDB database library
import pymongo
from pymongo import MongoClient


### PARAMETERS ###

BASE_URL = r'https://inspirehep.net/api/literature'
# QUERY = r'unal'
QUERY = r'cn cms or cn atlas or cn lhcb or cn alice'

SIZE = 10                    # results per api call
SORT = 'mostrecent'         # Most recent records appear first
# SORT = 'mostcited'         # Most cited records appear first
PAGE = 1                      # Initial page
FIELDS = 'titles,'\
            'authors.full_name,'\
            'authors.affiliations,'\
            'authors.bai,'\
            'referenced_authors_bais,'\
            'author_count,'\
            'publication_info,'\
            'document_type,'\
            'inspire_categories,'\
            'references,'\
            'citation_count,'\
            'citation_count_without_self_citations,'\
            'collaborations,'\
            'arxiv_eprints,'\
            'preprint_date,'\
            'citeable,'\
            'abstracts'
FORMAT = 'json'
EARLIEST_DATE = '1990--2021'

NUM_HITS_API_LIMIT = 10000
SIZE_API_LIMIT = 1000


def call_api(params:dict) -> dict:

    logging.info('making API call with params {}'.format(params))
    try:
        response = requests.get(
            BASE_URL,
            params = params
            )
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        logging.info('response time {}'.format(response.elapsed.total_seconds()))
    except HTTPError as http_err:
        logging.exception("HTTP exception occurred")
        if response.status_code == 400:
            logging.debug('Exceeded maximum number of results per page ({})'.format(SIZE_API_LIMIT))
        elif response.status_code == 429:           # when rate limited
            timeout = response.headers['x-retry-in']
            logging.debug('rate limited, sleeping for {:f.2}s'.format(timeout))
            time.sleep(timeout)
            return call_api(params)                 # try again recursevely
        elif response.status_code == 504:           # when server error
            time.sleep(1)
            return call_api(params)                 # try again recursevely
        else:                                       # another HTTP error
            logging.debug('Unknown {} HTTP error: {}'.format(response.status_code, response))
            print('Unknown {} HTTP error: {}'.format(response.status_code, response))
            sys.exit()
    except Exception as err:
        logging.exception("non-HTTP exception occurred")
        logging.exception(err)
        print("non-HTTP exception occurred:\n {}".format(err))
        sys.exit()
    else:   # Success on API call
        response.encoding = 'UTF-8'
        json_text = response.text
        # remove $ from keys to avoid conflicts with database
        json_text = json_text.replace(r'"$ref"','"ref"')
        json_text = json_text.replace(r'"$schema"','"schema"')
        logging.debug('data succesfully obtained and parsed')
        return json.loads(json_text)


def parse_url_params(url:str) -> dict:
    '''parse params from an URL string and return as dictionary'''
    parsed_url = urlparse.urlparse(url)
    params = parse_qs(parsed_url.query)
    return params


def interval_split(interval:list) -> list:
    w = (interval[1] - interval[0]) // 2
    interval_list = [[interval[0], interval[0]+w],[interval[0]+w+1,interval[1]]]
    logging.info('splitting interval {} into {}'.format(interval, interval_list))
    return interval_list


def insert_one_to_db(doc:dict):
    ''' insert a single document to DB and skip if it is already in collection.'''

    logging.debug('inserting article with id:{}'.format(doc['id']))
    try:
        # insert document to database using doc id as database id
        db_result = db_collection.insert_one(doc)
        return True
    except pymongo.errors.DuplicateKeyError:
        # skip document because it already exists in collection
        logging.debug("duplicated key, document already in DB")
        return False
    except Exception as err:
        logging.exception("unknown error while inserting to db")
        sys.exit()


def insert_many_to_db(docs:list, db_collection:pymongo.database.Database):
    ''' inserts many documents to DB and skip those already in collection.'''

    logging.debug('trying to insert {} documents to db'.format(len(docs)))
    try:
        db_collection.insert_many(docs, ordered = False)
    except pymongo.errors.BulkWriteError as bwe:
        for error in bwe.details['writeErrors']:
            if error['code'] == 11000:
                logging.debug('Document {} already in collection'.format(error['op']['id']))
            else:
                logging.exception("unknown error while inserting to db, error code {}".format(error['code']))
                sys.exit()
        logging.debug('inserted {} documents to db'.format(bwe.details['nInserted']))


def paginate_and_save(query_params:dict, db_collection:pymongo.database.Database):

    inserted_ids = []

    logging.info('downloading page {}'.format(query_params['page']))
    response_json = call_api(query_params)
    documents = response_json['hits']['hits']
    if len(documents) > 0:
        insert_many_to_db(documents, db_collection)    # insert articles from response to DB
        inserted_ids.extend([document['id'] for document in documents])

    # if next page: parse next_url params and call recursively
    if 'next' in response_json['links'].keys():
        query_params = parse_url_params(response_json['links']['next'])
        downloaded_ids = paginate_and_save(query_params, db_collection)
        inserted_ids.extend(downloaded_ids)
        return inserted_ids
    else:
        return inserted_ids

def get_num_hits(query_params:dict) -> int:

    response_json = call_api({
        'q': query_params['q'],
        'sort': query_params['sort'],
        'size': 1,
        'page': 1,
        'fields': 'id',
        'earliest_date': query_params['earliest_date']
    })
    hits = response_json['hits']['total']
    logging.info('number of hits {}'.format(hits))

    return hits

def download_docs(query_params:dict, db_collection:pymongo.database.Database) -> list:
    '''Get documents from API and store on DB.
    Split by date is also handled by this function.
    '''

    ids_downloaded = []

    # This first api call will check the number of hits
    # if it is greater then 10k documents then the query needs
    # to be splitted till we get under the API limit.
    # The strategy here is to split the query by year buckets.
    hits = get_num_hits(query_params)

    if hits == 0: return ids_downloaded

    if hits < NUM_HITS_API_LIMIT:
        downloaded_docs_id_list = paginate_and_save(query_params,db_collection)
        ids_downloaded.extend(downloaded_docs_id_list)
    else:
        logging.info('number of hits ABOVE the API limit, splitting into time buckets')
        # split by date and download
        dates = list(map(int, query_params['earliest_date'].split('--')))
        splitted_date_range = interval_split(dates)
        for date_range in splitted_date_range:
            query_params['earliest_date'] = "{}--{}".format(date_range[0],date_range[1])
            logging.info('downloading docs in date-range {}'.format(date_range))
            downloaded_docs_id_list = download_docs(query_params, db_collection)
            ids_downloaded.extend(downloaded_docs_id_list)

    return ids_downloaded


if __name__ == '__main__':

    logging.info('connecting to database')
    db_client = MongoClient('localhost', 27017)
    # db_client = MongoClient('172.19.31.5', 27017)
    db = db_client['inspirehep']    # select "inspirehep" database
    db_collection = db['lhc']          # use the "lhc" collection of the database

    # create index on id to ensure no duplicated entries
    db_collection.create_index([('id', pymongo.ASCENDING)], unique=True)

    logging.info('starting collection')

    query_params = {
        'q': QUERY,
        'sort': SORT,
        'size': SIZE,
        'page': PAGE,
        'fields': FIELDS,
        'earliest_date': EARLIEST_DATE
    }

    ids_downloaded = download_docs(query_params, db_collection)
    print(len(ids_downloaded))

    # add field to indicate this documents belongs to the main query
    db_collection.update_many({},{"$set":{"is_parent_document":True}}, upsert=False)

    # remove documents from unwanted collaboration "Herschel Atlas"
    documents = db_collection.delete_many({'metadata.collaborations.value': "Herschel ATLAS"})