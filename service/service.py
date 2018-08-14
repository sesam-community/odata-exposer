"""
Simple service that implements part of Odata 4 protocol for getting data from
Sesam.io powered applications
"""
import json
import logging
import os
import re

from flask import Flask, Response, request

from service.infrastructure.datasource import DataSource

BASE_URL = os.environ.get('BASE_URL')
JWT_TOKEN = os.environ.get('JWT_TOKEN')
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

APP = Flask(__name__)

logging.basicConfig(level=LOG_LEVEL)

if BASE_URL is None or JWT_TOKEN is None:
    logging.error("BASE_URL or JWT_TOKEN environmental variables were not provided. ")

DATA_SOURCE = DataSource(BASE_URL, JWT_TOKEN)
DATA_SOURCE.init()


@APP.route('/')
def get_datasets():
    """
    Odata service base url
    Output all available EntitySets
    :return:
    """
    return Response(json.dumps(DATA_SOURCE.get_datasets()), mimetype='application/json')


@APP.route('/<string:endpoint>')
def process(endpoint):
    """
    Main service function that outputs data
    :param endpoint: service endpoint (in out case simply name of dataset we want to fetch)
    :return: list of entities if endpoint is dataset name or one entity if endpoint contains
    entity id
    """
    id_regex_matcher = re.search(r'\((.*?)\)', endpoint)

    if id_regex_matcher is None:  # fetch data set
        return process_dataset(endpoint)

    return process_item(endpoint, id_regex_matcher.group(1).replace("'", ""))


def process_item(endpoint, entity_id):
    """
    process request to fetch simple entity
    :param endpoint: dataset from which entity will be fetched
    :param entity_id: entity id
    :return: Flask response with json entity
    """
    item = DATA_SOURCE.get_entity(endpoint.split("(")[0], entity_id)
    return Response(json.dumps(item), mimetype='application/json')


def process_dataset(endpoint):
    """
    process request for fetching list of entities
    :param endpoint:
    :return: flask response with list of json entities for given endpoint
    """
    skiptoken = request.args.get('skiptoken')
    if skiptoken is None:
        skiptoken = 0
    return Response(json.dumps(DATA_SOURCE.get_data(endpoint, skiptoken)),
                    mimetype='application/json')


if __name__ == '__main__':
    logging.info("Starting service")
    APP.run(debug=True, host='0.0.0.0', threaded=True, port=os.environ.get('PORT', 5000))
