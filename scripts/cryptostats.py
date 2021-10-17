import os
import asyncio

from types import SimpleNamespace
from decimal import Decimal
from datetime import datetime

from bitbay import BitBay


async def get_stats():

    async with BitBay(os.environ.get('BITBAY_API_KEY'), os.environ.get('BITBAY_SECRET')) as api:

        wallet = {}

        ticker = {
            market.split('-')[0]: {
                'rate': Decimal(ticker['rate']),
                'format': f"16.{ticker['market']['first']['scale']}f"
            } for market, ticker
            in (await api.request('GET', f'trading/ticker'))['items'].items()
            if market.endswith('-PLN')
        }

        for transaction in reversed(await api.collection('POST', 'trading/history/transactions')):
            target_currency, source_currency = transaction['market'].split('-', maxsplit=1)
            commission = Decimal(transaction['commissionValue'])
            amount = Decimal(transaction['amount'])
            time = datetime.fromtimestamp(int(transaction['time'])/1000).strftime('%Y.%m.%d %H:%M:%S')
            rate = Decimal(transaction['rate'])

            wallet.setdefault(target_currency, {
                'amount': 0, 'cost': 0, 'profit': 0, 'commission': 0, 'history': []
            })

            if transaction['userAction'] == 'Buy':
                wallet[target_currency]['amount'] += amount - commission
                wallet[target_currency]['cost'] += amount * rate
                wallet[target_currency]['commission'] += commission * rate

                wallet[target_currency]['history'].append([
                    time, 'BUY',
                    amount, target_currency,
                    amount * rate, source_currency,
                    rate, wallet[target_currency]['cost'] / wallet[target_currency]['amount']
                ])

            if transaction['userAction'] == 'Sell':
                average_cost = wallet[target_currency]['cost'] / wallet[target_currency]['amount']
                wallet[target_currency]['profit'] += amount * (rate - average_cost) - commission
                wallet[target_currency]['amount'] -= amount
                wallet[target_currency]['cost'] = wallet[target_currency]['amount'] * average_cost
                wallet[target_currency]['commission'] += commission

                wallet[target_currency]['history'].append([
                    time, 'SELL',
                    amount, target_currency,
                    amount*rate, source_currency,
                    rate, average_cost
                ])

        for currency in sorted(wallet):

            w = SimpleNamespace(**wallet[currency])

            print(
                f'{w.amount:{ticker[currency]["format"]}} {currency:4} = '
                f'{w.cost:8.2f}{(ticker[currency]["rate"]*w.amount-w.cost):+9.2f} PLN '
                f'({(ticker[currency]["rate"]-w.cost/w.amount)*w.amount/w.cost*100 if w.amount else 0:+7.2f} %) '
                f'\N{GREEK CAPITAL LETTER SIGMA} = {w.profit:8.2f} PLN ({w.commission:8.2f} PLN)'
            )


if __name__ == '__main__':

    asyncio.run(get_stats())
