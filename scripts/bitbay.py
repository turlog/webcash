import uuid
import hmac
import json
from datetime import datetime
import requests


class BitBay:

    def __init__(self, key, secret):
        self.__key = key
        self.__secret = secret
        self.__session = requests.Session()

    def auth_headers(self, query=None):
        timestamp = str(int(datetime.now().timestamp()))
        message = self.__key + timestamp + (json.dumps(query) if query is not None else '')
        signature = hmac.digest(self.__secret.encode('ascii'), message.encode('ascii'), 'sha512').hex()
        return {
            'API-Key': self.__key,
            'API-Hash': signature,
            'Request-Timestamp': timestamp,
            'operation-id': str(uuid.uuid4())
        }

    def request(self, method, endpoint, query=None):
        response = self.__session.request(
            method=method,
            json=query or {},
            headers=self.auth_headers(query),
            url=f'https://api.bitbay.net/rest/{endpoint}'
        ).json()
        if response.get('status') == 'Ok':
            return response
        else:
            raise ValueError(response.pop('errors', ['UNKNOWN_ERROR']))

    def collection(self, method, endpoint, query=None):
        query = query or {}
        query['nextPageCursor'] = 'start'

        while True:
            response = self.request(method, endpoint, query)
            yield from response['items']
            if query.get('nextPageCursor') == response.get('nextPageCursor'):
                break
            query['nextPageCursor'] = response['nextPageCursor']
