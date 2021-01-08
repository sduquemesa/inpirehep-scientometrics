#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Download and store on MongoDB documents from INSPIREHEP API

    author = Sebastian Duque Mesa
    credits = Sebastian Duque Mesa, Diego A. Restrepo, Jose David Ruiz
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
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(funcName)s-\t\t\t\t%(message)s')


# MongoDB database library
import pymongo
from pymongo import MongoClient
logging.info('connecting to database')
# db_client = MongoClient('localhost', 27017)
db_client = MongoClient('172.19.31.5', 27017)
db = db_client['inspirehep']
db_collection = db['cern']        # use the "articles" collection of the database


### PARAMETERS ###

BASE_URL = r'https://inspirehep.net/api/literature'
QUERY = r'cn cms or cn atlas or cn lhcb or cn alice'

SIZE = 100                    # results per api call
# SORT = 'mostrecent'         # Most recent records appear first
SORT = 'mostcited'         # Most cited records appear first
PAGE = 1                      # Initial page
FIELDS = 'titles,authors.full_name,authors.affiliations,authors.bai,publication_info,document_type,inspire_categories,references,citation_count,citation_count_without_self_citations'
FORMAT = 'json'

def api_call(params:dict):

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
        if response.status_code == 429:             # when hit the rate limit
            timeout = response.headers['x-retry-in']
            logging.debug('rate limited, sleeping for {:f.2}s'.format(timeout))
            time.sleep(timeout)
            return api_call(params)                 # try again recursevely
        if response.status_code == 504:             # when server error
            return api_call(params)                 # try again recursevely
        else:                                       # another HTTP error
            sys.exit()

    except Exception as err:
        logging.exception("non-HTTP exception occurred")
        logging.exception(err)
        sys.exit()
    
    else:   # Success on API call
        # response.encoding = 'UTF-8'
        json_text = response.text
        # remove $ from keys to avoid conflicts with database
        json_text = json_text.replace(r'$ref','ref')      
        json_text = json_text.replace(r'$schema','schema')
        # replace "id" label to "_id" to use as database id
        json_text = json_text.replace(r'"id"','"_id"')
        logging.debug('data succesfully obtained and parsed')

        return json.loads(json_text)


def parse_url_params(url:str):
    parsed_url = urlparse.urlparse(url)
    params = parse_qs(parsed_url.query)

    # modify params to only include required fields and set response size
    params['size'] = SIZE
    params['fields'] = FIELDS

    return params

def paginate(response_json:dict) -> list:

    citation_ids = []

    while 'next' in response_json['links'].keys():

        # parse and update params for next query using the "next" URL provided by the API
        next_url = response_json['links']['next']
        logging.debug('next URL {}'.format(next_url))
        params = parse_url_params(next_url)
        response_json = api_call(params)    # API call

        # save result and get list of document ids
        insert_many_to_db(response_json['hits']['hits'])    # insert articles to db
        citation_ids.extend([int(citation['_id']) for citation in response_json['hits']['hits']])    # append article ids to the citation list

        num_total_citations = response_json['hits']['total']
        logging.debug('\t↓citations: {}/{}'.format(len(citation_ids)+SIZE, num_total_citations))

    else:
        logging.debug('END, no next URL')
        return citation_ids

def get_citations(citations_url:str) -> list:

    logging.debug('getting citations from URL {}'.format(citations_url))

    citation_ids = []   # list for ids of citations
    
    # parse initial params for citations query using the URL provided by the API
    params = parse_url_params(citations_url)
    response_json = api_call(params)

    num_total_citations = response_json['hits']['total']

    # Currently the API limits the number of results to 10K.
    # If results are less than 10k, proceed with regular pagination 
    if (num_total_citations != 0 and num_total_citations <= 10e3):

        # save result and get list of document ids
        insert_many_to_db(response_json['hits']['hits'])    # insert articles to db
        citation_ids.extend([int(citation['_id']) for citation in response_json['hits']['hits']])  # append article ids to the citation list

        logging.debug('\tcitations: {}/{}'.format(len(citation_ids), num_total_citations))
        
        # If there's a next page, get it, parse it and append to db.
        if 'next' in response_json['links'].keys():
            citation_ids.extend(paginate(response_json))
            
        logging.debug('Citations {}/{}, ids: {}'.format(len(citation_ids),num_total_citations,citation_ids))

        # return the list of the ids of the citations
        return citation_ids 
        
    # If results are more than 10k, split request into several queries with less than 10k results
    elif num_total_citations > 10e3:

        for date_range in ['2000--2015', '2016--2021']:

            params['earliest_date'] = date_range
            response_json = api_call(params)

            # save result and get list of document ids
            insert_many_to_db(response_json['hits']['hits'])    # insert articles to db
            citation_ids.extend([int(citation['_id']) for citation in response_json['hits']['hits']])  # append article ids to the citation list

            logging.debug('\tcitations: {}/{}'.format(len(citation_ids), num_total_citations))
            
            # If there's a next page, get it, parse it and append to db.
            if 'next' in response_json['links'].keys():
                citation_ids.extend(paginate(response_json))

        logging.debug('Citations {}/{}, ids: {}'.format(len(citation_ids),num_total_citations,citation_ids))
        
        # return the list of the ids of the citations
        return citation_ids 

    else:
        logging.debug('\tno citations')
        return []   



def insert_one_to_db(doc:dict):

    # insert a single document to DB if is not already in collection.
    logging.debug('inserting document id:{}'.format(doc['_id']))
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


def insert_many_to_db(docs:list):

    logging.debug('inserting {} documents to db'.format(len(docs)))
    try:
        db_collection.insert_many(docs, ordered = False)
    except pymongo.errors.BulkWriteError as bwe:
        for error in bwe.details['writeErrors']:
            if error['code'] == 11000:
                logging.debug('Document {} already in collection'.format(error['op']['_id']))
            else:
                logging.exception("unknown error while inserting to db, error code {}".format(error['code']))
                sys.exit()
        logging.debug('Inserted {} documents to db'.format(bwe.details['nInserted']))



if __name__ == '__main__':

    logging.info('starting collection')

    # initial params
    num_docs = 0
    params = {
        'q': QUERY,         # URL-encoded query
        'sort': SORT,    
        'size': SIZE, 
        'page': PAGE,           # start from first page
        'fields': FIELDS
    }

    while True:

        response_json = api_call(params)
        num_total_docs = response_json['hits']['total'] # number of results or "hits" of the query

        # Parse documents
        for doc in response_json['hits']['hits']:       # go over every single-document result
            logging.debug( 'doc id {}\t\t {}/{}'.format(doc['_id'], num_docs, num_total_docs) )

            # get all document citations and insert to db
            citations_url = doc['links']['citations']
            citations_ids = get_citations(citations_url)

            doc['citations_ids'] = citations_ids        # append citations ids to document
            insert_one_to_db(doc)                       # insert document to db

            num_docs += 1

        if 'next' in response_json['links'].keys():
            # parse and update params for next query using the "next" URL provided by the API
            next_url = response_json['links']['next']
            logging.debug('next URL {}'.format(next_url))
            next_url_parsed = urlparse.urlparse(next_url)
            params = parse_qs(next_url_parsed.query)
        else:
            # end of documents
            logging.info('END')
            break
