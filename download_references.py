#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Download and store on MongoDB documents from INSPIREHEP API

    author = Sebastian Duque Mesa
    maintainer = Sebastian Duque Mesa
    email = sebastian.duquem [at] udea.edu.co

    INSPIREHEP API doc at https://github.com/inspirehep/rest-api-doc
"""
import api_request

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

SIZE = 200                    # results per api call
SORT = 'mostrecent'         # Most recent records appear first
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

if __name__ == '__main__':

    logging.info('connecting to database')
    db_client = MongoClient('localhost', 27017)
    # db_client = MongoClient('172.19.31.5', 27017)
    db = db_client['inspirehep']    # select "inspirehep" database
    db_collection = db['lhc']          # use the "lhc" collection of the database

    # create index on id to ensure no duplicated entries
    db_collection.create_index([('id', pymongo.ASCENDING)], unique=True)

    logging.info('starting collection')

    # get documents that belong to the LHC collaborations (parent documents)
    # and have no cited_by field (because it has not been downloaded yet)
    documents = list(db_collection.find({"is_parent_document":True, "cited_by": {"$exists": False}},{'id':1}))
    num_docs = len(documents)
    logging.info('DOWNLOADING CITATIONS for {} documents'.format(num_docs))

    for i, document in enumerate(documents):
        doc_id = document['id']
        logging.info('CITATIONS for document with id {} [{:.2%}]'.format(doc_id,i/num_docs))
        query = 'refersto recid {}'.format(doc_id)
        query_params = {
            'q': query,
            'sort': SORT,
            'size': SIZE,
            'page': PAGE,
            'fields': FIELDS,
            'earliest_date': EARLIEST_DATE
        }
        ids_downloaded = api_request.download_docs(query_params, db_collection)
        logging.info('number of downloaded citations {}'.format(len(ids_downloaded)))
        logging.info('trying to store citated_by info on document {}'.format(doc_id))
        db_collection.update_one({"id": doc_id}, {"$set":{"cited_by":ids_downloaded}})
        logging.info('stored citated_by info on document {}'.format(doc_id))