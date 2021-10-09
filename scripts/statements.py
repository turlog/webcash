import re
import sys
import csv
import datetime
import tabulate

from ruamel.yaml import YAML
from colorama import Fore, Style

from glob import glob
from decimal import Decimal
from collections import defaultdict
from functools import cached_property, lru_cache

from piecash import open_book
from piecash import Transaction, Split

epsilon = 7
YMD_pattern = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
DMY_pattern = re.compile(r'[0-9]{2}-[0-9]{2}-[0-9]{4}')
iban_pattern = re.compile(r'[0-9]{2} ?[0-9]{4} ?[0-9]{4} ?[0-9]{4} ?[0-9]{4} ?[0-9]{4} ?[0-9]{4}')


class GnuCash:

    def __init__(self, uri):
        self.uri = uri

    @cached_property
    def book(self):
        return open_book(
            uri_conn=self.uri, readonly=True, open_if_lock=True, do_backup=False
        )

    def transactions(self, account, from_date=None, to_date=None):

        query = self.book.session.query(Split.quantity).join(Transaction).filter(
            Split.account == self.book.accounts(name=account)
        ).add_columns(Transaction.description, Transaction.post_date)

        if from_date is not None:
            query = query.filter(Transaction.post_date >= from_date)

        if to_date is not None:
            query = query.filter(Transaction.post_date <= to_date)

        for row in query.order_by(Transaction.post_date.desc()).all():
            yield row[-1], *row[:-1]


def get_iban_from_file(fn):
    with open(fn, 'rb') as buffer:
        return int(iban_pattern.search(
            buffer.read(1024).decode('ascii', errors='ignore')
        ).group(0).replace(' ', ''))


def parse_mbank_csv(fn):
    with open(fn, 'r', encoding='windows-1250') as infile:
        reader = csv.reader(infile, delimiter=';')
        for line in reader:
            if len(line) == 7 and YMD_pattern.match(line[0]):
                yield (
                    datetime.date(*map(int, line[0].split('-'))),
                    Decimal(re.sub(r'(-?[0-9]+),([0-9]+)[A-Z]+', '\\1.\\2', line[4].replace(' ', ''))),
                    re.sub(r'\s+', ' ', line[1].strip())
                )


def parse_santander_csv(fn):
    with open(fn, 'rt', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=',')
        for line in reader:
            if len(line) == 9 and DMY_pattern.match(line[0]):
                yield (
                    datetime.date(*map(int, reversed(line[1].split('-')))),
                    Decimal(line[5].replace(',', '.')),
                    line[2].strip()
                )


def parse_ing_csv(fn):
    with open(fn, 'rt', encoding='windows-1250') as infile:
        reader = csv.reader(infile, delimiter=';')
        for line in reader:
            if len(line) == 21 and YMD_pattern.match(line[0]):
                yield (
                    datetime.date(*map(int, line[0].split('-'))),
                    Decimal(line[8 if line[1] else 10].replace(',', '.')),
                    line[2].strip() + ' ' + line[3].strip()
                )


if __name__ == '__main__':

    with open(__file__.replace('.py', '.yml')) as cfg_file:
        configuration = YAML().load(cfg_file)

    connections = {
        name: GnuCash(uri) for name, uri
        in configuration.get('connections', {}).items()
    }

    parser = {
        'mBank': parse_mbank_csv,
        'Santander': parse_santander_csv,
        'ING': parse_ing_csv
    }

    for pattern in sys.argv[1:]:
        for in_file in glob(pattern):
            cfg = configuration['importers'][get_iban_from_file(in_file)]

            messages = []
            transactions = defaultdict(list)

            for date, amount, description in parser[cfg['format']](in_file):
                if '(mDM)' not in description:
                    transactions[(date, f'{amount:.2f}')].append(description)

            for (date, amount), descriptions in transactions.items():
                if len(descriptions) > 1:
                    for description in descriptions:
                        messages.append((date, amount, description, 'AMBIGUOUS', Style.DIM))

            delta = datetime.timedelta(days=epsilon)
            from_date = min(transactions)[0]
            to_date = max(transactions)[0]

            for date, amount, description in connections[cfg['connection']].transactions(
                    cfg['account'], from_date=from_date-delta, to_date=to_date+delta
            ):
                amount = f'{amount:.2f}'

                for shift in (int((x // 2 - x) * (x % 2 * 2-1)) for x in range(epsilon*2+1)):
                    shifted_date = date + datetime.timedelta(days=shift)
                    if transactions.get((shifted_date, amount)):
                        transactions[(shifted_date, amount)].pop()
                        if shift:
                            messages.append((date, amount, description, f'\N{RIGHTWARDS ARROW} {shifted_date}', Style.DIM))
                        break
                else:
                    if from_date <= date <= to_date:
                        messages.append((date, amount, description, 'GNUCASH', Fore.WHITE))

            for (date, amount), descriptions in transactions.items():
                for description in descriptions:
                    messages.append((date, amount, description, 'CSV FILE', Style.BRIGHT))

            print(tabulate.tabulate([
                (color+str(date), amount, description[:140], status+Style.RESET_ALL)
                for date, amount, description, status, color in sorted(messages)
            ], headers=('Date', 'Amount', 'Description', 'Status')))
