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
    stocks = data['table'].where(lambda r: r['indicator'] == 'Direct investment stocks - Million ECU/EUR' and r['geo'] == 'United Kingdom' and r['year'] == '2012')

    inbound_stocks = stocks.where(lambda r: r['variable'] == 'Financial account, Direct investment, In the reporting economy').select(['partner', 'value']).rename({
        'value': 'inbound'
    })
    outbound_stocks = stocks.where(lambda r: r['variable'] == 'Financial account, Direct investment, Abroad').select(['partner', 'value']).rename({
        'value': 'outbound'
    })

    data['uk_stocks'] = inbound_stocks.join(outbound_stocks, 'partner', columns=['outbound'])
    data['uk_stocks'].to_csv('uk_stocks.csv')


def uk_flows(data):
    flows = data['table'].where(lambda r: r['indicator'] == 'Direct investment flows - Million ECU/EUR' and r['geo'] == 'United Kingdom' and r['year'] == '2012')

    inbound_flows = flows.where(lambda r: r['variable'] == 'Financial account, Direct investment, In the reporting economy').select(['partner', 'value']).rename({
        'value': 'inbound'
    })
    outbound_flows = flows.where(lambda r: r['variable'] == 'Financial account, Direct investment, Abroad').select(['partner', 'value']).rename({
        'value': 'outbound'
    })

    data['uk_flows'] = inbound_flows.join(outbound_flows, 'partner', columns=['outbound'])
    data['uk_flows'].to_csv('uk_flows.csv')


def spit_it_out(data):
    data['table'].print_table(10)


if __name__ == '__main__':
    data_loaded = proof.Analysis(load_data)
    data_loaded.then(uk_stocks)
    data_loaded.then(uk_flows)

    data_loaded.run()
