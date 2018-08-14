import requests
import logging
from flask import Flask, request, abort


class DataSource:
    url = ""
    token = ""
    __datasets = {}

    def __init__(self, url, jwt_token) -> None:
        super().__init__()
        if url is None or jwt_token is None:
            raise ValueError("url and token must be provided")
        self.url = url
        self.token = jwt_token

    def init(self):
        response_json = self.__get_response(self.url)
        for item in response_json:
            if 'user' == item['runtime']['origin']:
                self.__datasets[item['_id']] = item
                logging.info("Added dataset id: %s with origin %s", item['_id'],
                             item['runtime']['origin'])

    def __get_response(self, url):
        logging.info("requesting url: %s", url)
        headers = {'Authorization': 'Bearer {}'.format(self.token)}
        response = requests.get(url, headers=headers)
        return response.json()

    def get_datasets(self):
        result = {'@odata.context': request.base_url + "$metadata", 'value': []}
        for item in self.__datasets:
            result['value'].append({
                'name': item,
                'kind': 'EntitySet',
                'url': item
            })
        return result

    def get_data(self, endpoint, skip=0):
        if endpoint not in self.__datasets:
            abort(404)
        endpoint_url = "{}/{}/entities?limit=25&deleted=false&since={}".format(self.url, endpoint,
                                                                               skip)

        data = self.__get_response(endpoint_url)
        skiptoken = str(int(skip) + 25)
        result = {'@odata.context': request.base_url + "$metadata#" + endpoint
            , '@odata.nextLink': request.base_url + "?skiptoken=" + skiptoken
            , 'value': []}
        for item in data:
            item['@odata.id'] = request.base_url + "('" + item['_id'] + "')"
            result['value'].append(item)
        return result

    def get_entity(self, endpoint, entity_id):
        if endpoint not in self.__datasets:
            abort(404)
        endpoint_url = "{}/{}/entities/{}".format(self.url, endpoint, entity_id)
        item = self.__get_response(endpoint_url)

        item['@odata.id'] = request.base_url
        return item
