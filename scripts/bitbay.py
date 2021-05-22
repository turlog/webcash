import os
import uuid
import hmac
import json
from datetime import datetime
import requests

if __name__ == '__main__':

    request = requests.Request(
        method='GET',
        url='https://api.bitbay.net/rest/trading/history/transactions'
    ).prepare()

    public = os.environ.get('BITBAY_PUBLIC')
    private = os.environ.get('BITBAY_PRIVATE')

    timestamp = str(int(datetime.now().timestamp()))

    signature = hmac.digest(private.encode('ascii'), (public + timestamp + (request.body or '')).encode('ascii'), 'sha512').hex()

    request.headers.update({
        'API-Key': public,
        'API-Hash': signature,
        'Request-Timestamp': timestamp,
        'operation-id': str(uuid.uuid4()),
    })

    with requests.Session() as session:
        response = session.send(request)
        print(json.dumps(response.json(), indent=4))
