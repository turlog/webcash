import re
import csv
import datetime
import tabulate

import chardet
from io import StringIO

import click

from unidecode import unidecode
from ruamel.yaml import YAML
from lxml import etree

from colorama import Fore, Style
from furl import furl

from getpass import getpass

from glob import glob
from decimal import Decimal
from collections import defaultdict
from functools import cached_property
from fnmatch import fnmatch

from piecash import open_book
from piecash import Commodity, Transaction, Split


YMD_pattern = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
DMY_pattern = re.compile(r'[0-9]{2}-[0-9]{2}-[0-9]{4}')
iban_pattern = re.compile(r'[0-9]{2} ?[0-9]{4} ?[0-9]{4} ?[0-9]{4} ?[0-9]{4} ?[0-9]{4} ?[0-9]{4}')


class GnuCash:

    def __init__(self, uri, read_only=True):
        self.uri = uri
        self.read_only = read_only

    @cached_property
    def book(self):
        return open_book(
            uri_conn=self.uri, readonly=self.read_only, open_if_lock=True, do_backup=False
        )

    def accounts(self, pattern):
        for account in self.book.accounts:
            if fnmatch(account.fullname, pattern):
                yield account.fullname

    def transactions(self, account, from_date=None, to_date=None):

        query = self.book.session.query(Split.quantity).join(Transaction).join(Commodity).filter(
            Split.account == self.book.accounts(fullname=account)
        ).add_columns(Commodity.mnemonic, Transaction.description, Transaction.post_date)

        if from_date is not None:
            query = query.filter(Transaction.post_date >= from_date)

        if to_date is not None:
            query = query.filter(Transaction.post_date <= to_date)

        for row in query.order_by(Transaction.post_date.desc()).all():
            yield row[-1], *row[:-1]


def detect_importer_from_file(fn):
    with open(fn, 'rb') as buffer:
        return int(iban_pattern.search(
            buffer.read(1024).decode('ascii', errors='ignore')
        ).group(0).replace(' ', ''))


def text(fn):

    with open(fn, 'rb') as stream:
        buffer = stream.read()
        return StringIO(buffer.decode(chardet.detect(buffer)['encoding']))


def parse_mbank_csv(fn):
    with text(fn) as infile:
        reader = csv.reader(infile, delimiter=';')
        for line in reader:
            if len(line) == 7 and YMD_pattern.match(line[0]):
                integral, fraction, currency = re.match(r'(-?[0-9]+),([0-9]+)([A-Z]+)', line[4].replace(' ', '')).groups()
                yield (
                    datetime.date(*map(int, line[0].split('-'))),
                    Decimal(f'{integral}.{fraction}'),
                    currency,
                    re.sub(r'\s+', ' ', line[1].strip())
                )


def parse_santander_csv(fn):
    with text(fn) as infile:
        reader = csv.reader(infile, delimiter=',')
        for line in reader:
            if len(line) == 9 and DMY_pattern.match(line[0]):
                yield (
                    datetime.date(*map(int, reversed(line[1].split('-')))),
                    Decimal(line[5].replace(',', '.')),
                    'PLN',
                    line[2].strip()
                )


def parse_ing_csv(fn):
    with text(fn) as infile:
        reader = csv.reader(infile, delimiter=';')
        for line in reader:
            if len(line) == 21 and YMD_pattern.match(line[0]):
                yield (
                    datetime.date(*map(int, line[0].split('-'))),
                    Decimal(line[8 if line[1] else 10].replace(',', '.')),
                    'PLN',
                    line[2].strip() + ' ' + line[3].strip()
                )


def parse_nest_csv(fn):
    with text(fn) as infile:
        reader = csv.reader(infile, delimiter=',')
        for line in reader:
            if len(line) > 9 and DMY_pattern.match(line[0]):
                yield(
                    datetime.date(*map(int, reversed(line[0].split('-')))),
                    Decimal(line[3]),
                    'PLN',
                    line[7]
                )


def parse_revolut_csv(fn):
    with text(fn) as infile:
        reader = csv.reader(infile, delimiter=',')
        for line in reader:
            if YMD_pattern.match(line[2]):
                yield (
                    datetime.date(*map(int, line[2].split(' ')[0].split('-'))),
                    Decimal(line[5]),
                    line[7],
                    line[4]
                )


def parse_toyota_xml(fn):
    with open(fn, 'rb') as infile:
        for operacja in etree.parse(infile).getroot().findall('.//operacja'):
            operacja = {child.tag: child.text for child in operacja.getchildren()}
            yield (
                datetime.date(*map(int, operacja['data_ksiegowa'].split('-'))),
                (+1 if operacja['strona'] == 'MA' else -1) * Decimal(operacja['kwota']),
                'PLN',
                operacja['tresc1']
            )

@click.command()
@click.argument('statements', nargs=-1)
@click.option('--configuration', '-c', type=click.File(encoding='utf-8'), required=True)
@click.option('--elevate', '-e', is_flag=True, default=False)
@click.option('--update', '-u', is_flag=True, default=False)
@click.option('--target', '-i')
def cli(statements, configuration, elevate, update, target):
    configuration = YAML().load(configuration)
    epsilon = configuration.get('options', {}).get('epsilon', 7)

    username, password = (input('Username: '), getpass('Password: ')) if elevate else (None, None)

    connections = {}

    for name, uri in configuration.get('connections', {}).items():
        uri = furl(uri)
        if elevate:
            uri.set(username=username, password=password)
        connections[name] = GnuCash(uri.tostr(), read_only=not elevate)

    parser = {
        'mBank': parse_mbank_csv,
        'Santander': parse_santander_csv,
        'ING': parse_ing_csv,
        'Toyota': parse_toyota_xml,
        'Nest': parse_nest_csv,
        'Revolut': parse_revolut_csv,
    }

    for pattern in statements:
        for in_file in glob(pattern):
            target = target or detect_importer_from_file(in_file)
            cfg = configuration['importers'][target]

            messages = []
            transactions = defaultdict(list)

            for date, amount, currency, description in parser[cfg['format']](in_file):
                transactions[(date, f'{amount:.2f}', currency)].append(unidecode(description))

            for (date, amount, currency), descriptions in transactions.items():
                if len(descriptions) > 1:
                    for description in descriptions:
                        messages.append((date, amount, currency, description, 'DUPLICATE', Style.DIM))

            delta = datetime.timedelta(days=epsilon)
            from_date = min(transactions)[0]
            to_date = max(transactions)[0]

            connection = connections[cfg['connection']]

            for account in connection.accounts(cfg['account']):
                for date, amount, currency, description in connection.transactions(
                        account, from_date=from_date-delta, to_date=to_date+delta
                ):
                    amount = f'{amount:.2f}'

                    for shift in (int((x // 2 - x) * (x % 2 * 2-1)) for x in range(epsilon*2+1)):
                        shifted_date = date + datetime.timedelta(days=shift)
                        if transactions.get((shifted_date, amount, currency)):
                            transactions[(shifted_date, amount, currency)].pop()
                            if shift:
                                messages.append((date, amount, currency, description, f'\N{RIGHTWARDS ARROW} {shifted_date}', Style.DIM))
                            break
                    else:
                        if from_date <= date <= to_date:
                            messages.append((date, amount, currency, description, 'GNUCASH', Fore.WHITE))

            for (date, amount, currency), descriptions in transactions.items():
                for description in descriptions:
                    messages.append((date, amount, currency, description, 'EXPORT', Style.BRIGHT))

            print(tabulate.tabulate([
                (color+str(date), amount, currency, description[:140], status+Style.RESET_ALL)
                for date, amount, currency, description, status, color in sorted(messages)
            ], headers=('Date', 'Amount', 'Currency', 'Description', 'Status'), floatfmt=".2f"))

            if update:
                book = connections[cfg['connection']].book

                for date, amount, currency, description, status, color in sorted(messages):
                    if status == 'EXPORT':
                        try:
                            fn = cfg['account'].replace('*', currency)
                            source = book.accounts(fullname=fn)
                        except KeyError:
                            print(f'Source account {fn} could not be found.')
                            continue

                        try:
                            fn = cfg['update'].replace('*', currency)
                            target = book.accounts(fullname=fn)
                        except KeyError:
                            print(f'Target account {fn} could not be found.')
                            continue

                        transaction = Transaction(
                            currency=book.commodities(namespace='CURRENCY', mnemonic=currency),
                            description=description,
                            post_date=date,
                            splits=[
                                Split(
                                    account=source,
                                    value=Decimal(amount)
                                ),
                                Split(
                                    account=target,
                                    value=-Decimal(amount)
                                ),
                            ]
                        )
                        book.session.add(transaction)
                book.flush()
                book.save()


if __name__ == '__main__':

    cli()
