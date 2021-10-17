import uuid
import hmac
import json
from datetime import datetime

import requests


## API VERSION 1.0.2

# PUBLIC GET trading/ticker/{trading_pair}
# PUBLIC SUBSCRIBE trading/ticker/{trading_pair}
# PUBLIC GET trading/stats/{trading_pair}
# PUBLIC SUBSCRIBE trading/stats/{trading_pair}
# PUBLIC GET trading/orderbook/{trading_pair}
# PUBLIC SUBSCRIBE trading/orderbook/{trading_pair}
# PUBLIC GET trading/orderbook-limited/{trading_pair}/{limit}
# PUBLIC SUBSCRIBE trading/orderbook-limited/{trading_pair}/{limit}
# PUBLIC GET trading/transactions/{trading_pair} (limit, fromTime)
# PUBLIC SUBSCRIBE trading/transactions/{trading_pair} (limit, fromTime)
# PUBLIC GET trading/candle/history/{trading_pair}/{resolution} (from, to)

# PRIVATE GET trading/offer/{trading_pair}
# PRIVATE SUBSCRIBE trading/offers/{market_code}
# PRIVATE POST trading/offer/{trading_pair} (amount, rate, price, offerType, mode, postOnly, fillOrKill, immediateOrCancel, firstBalanceId, secondBalanceId)
# PRIVATE DELETE trading/offer/{trading_pair}/{offer_id}/{offer_type}/{price}

# PRIVATE GET trading/config/{trading_pair}
# PRIVATE POST trading/config/{trading_pair} (first, second)

# PRIVATE GET trading/stop/offer/{market_code}
# PRIVATE SUBSCRIBE trading/stop/offers/{market_code}
# PRIVATE POST trading/stop/offer/{market_code} (offerType, amount, stopRate, mode, rate, balances, ignoreInvalidStopRate)
# PRIVATE DELETE trading/stop/offer/{market_code}/{offer_id}

# PRIVATE GET trading/history/transactions (markets, rateFrom, rateTo fromTime, toTime, userAction, nextPageCursor)
# PRIVATE SUBSCRIBE trading/history/transactions/{market_code} (markets, rateFrom, rateTo, fromTime, toTime, userAction, nexPageCursor)

# PRIVATE GET balances/BITBAY/history (balancesId, balanceCurrencies, fromTime, toTime, fromValue, toValue, balanceTypes, types, sort)
# PRIVATE SUBSCRIBE balances/bitbay/history (balancesId, balanceCurrencies, fromTime, toTime, fromValue, toValue, balanceTypes, types, sort)

# PRIVATE GET balances/BITBAY/balance
# PRIVATE SUBSCRIBE balances/balance/bitbay/updatefunds
# PRIVATE POST balances/BITBAY/balance (currency, type, name)
# PRIVATE PUT balances/BITBAY/balance/{wallet_id} (name)
# PRIVATE POST balances/BITBAY/balance/transfer/{source_id}/{destination_id} (currency, funds)

# PRIVATE GET cantor_service/rates/{currency_source}/{target_currency}
# PUBLIC SUBSCRIBE cantor_service/rates/{currency_source}/{target_currency}
# PRIVATE GET cantor_service/rates/{currency_source}/{target_currency}/{quantity}
# PRIVATE POST cantor_service/exchanges (sourceBalanceId, targetBalanceId, sourceCurrency, targetCurrency, amount, rate)
# PRIVATE GET cantor_service/history (page, size, sort, fromTime, toTime, markets, statuses)
# PRIVATE GET cantor_service/markets
# PRIVATE SUBSCRIBE cantor_service/exchanges/status


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
            json=None if method == 'GET' else (query or {}),
            params=query if method == 'GET' else None,
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
