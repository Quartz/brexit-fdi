#!/usr/bin/env python

import agate
import proof


text = agate.Text()
number = agate.Number(null_values=[':', '', None])

COLUMN_NAMES = ['partner', 'geo', 'indicator', 'variable', 'year', 'value']
COLUMN_TYPES = [text, text, text, text, text, number]


def load_data(data):
    tables = []

    for i in range(1, 41):
        print('Loading table %i' % i)
        tables.append(agate.Table.from_csv('data/bop_fdi_main_%i_Data.csv' % i, column_names=COLUMN_NAMES, column_types=COLUMN_TYPES, encoding='latin1'))

    data['table'] = agate.Table.merge(tables)


def uk_stocks(data):
    uk_stocks = data['table'].where(lambda r: r['indicator'] == 'Direct investment stocks - Million ECU/EUR' and r['geo'] == 'United Kingdom' and r['year'] == '2012')

    inbound_stocks = uk_stocks.where(lambda r: r['variable'] == 'Financial account, Direct investment, In the reporting economy').select(['partner', 'value']).rename({
        'value': 'inbound'
    })
    outbound_stocks = uk_stocks.where(lambda r: r['variable'] == 'Financial account, Direct investment, Abroad').select(['partner', 'value']).rename({
        'value': 'outbound'
    })

    data['uk_stocks'] = inbound_stocks.join(outbound_stocks, 'partner', columns=['outbound'])
    data['uk_stocks'].to_csv('uk_stocks.csv')


def spit_it_out(data):
    data['table'].print_table(10)


if __name__ == '__main__':
    data_loaded = proof.Analysis(load_data)
    data_loaded.then(uk_stocks)

    data_loaded.run()
