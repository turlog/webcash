import os
import uuid
import hmac
import json
from datetime import datetime
import requests
import logging


class BitBay:

    def __init__(self, key, secret):
        self.__key = key
        self.__secret = secret
        self.__session = requests.Session()

    def auth_headers(self, query=None):
        timestamp = str(int(datetime.now().timestamp()))
        message = self.__key + timestamp + (json.dumps(query) if query else '')
        signature = hmac.digest(self.__secret.encode('ascii'), message.encode('ascii'), 'sha512').hex()
        return {
            'API-Key': self.__key,
            'API-Hash': signature,
            'Request-Timestamp': timestamp,
            'operation-id': str(uuid.uuid4())
        }

    def request(self, method, endpoint, query=None):
        items = []
        query = query or {}
        query['nextPageCursor'] = 'start'

        while True:
            response = self.__session.request(
                method=method,
                json=query or {},
                headers=self.auth_headers(query),
                url=f'https://api.bitbay.net/rest/{endpoint}'
            ).json()
            if response.get('status') == 'Ok':
                items.extend(response['items'])
                if query.get('nextPageCursor') == response.get('nextPageCursor'):
                    break
                query['nextPageCursor'] = response['nextPageCursor']
            else:
                raise ValueError(response.get('errors', ['UNKNOWN_ERROR']))

        return items


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    bitbay = BitBay(os.environ.get('BITBAY_PUBLIC'), os.environ.get('BITBAY_PRIVATE'))

    data = bitbay.request('POST', 'trading/history/transactions', {
        'markets': ['BTC-PLN'],
        'rateFrom': '1',
        'rateTo': '99999',
        'userAction': 'buy',
        'fromTime': None,
        'toTime': None,
    })

    print(json.dumps(data, indent=4))

