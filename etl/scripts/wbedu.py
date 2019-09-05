# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os

from ddf_utils.str import to_concept_id
from ddf_utils.index import create_index_file

# configuration of file path
source_data = 'source/EdStats_Data.csv'
out_dir = '../../'


def extract_entities_country(data):
    country = data[['country_name', 'country_code']].copy()
    country = country.drop_duplicates()
    country.columns = ['name', 'country']
    country['country'] = country['country'].map(to_concept_id)
    return country


def extract_concepts_continuous(data):
    cont = data[['indicator_code', 'indicator_name']].copy()
    cont = cont.drop_duplicates()
    cont.columns = ['concept', 'name']
    cont['concept'] = cont['concept'].map(to_concept_id)
    cont['concept_type'] = 'measure'
    return cont


def extract_concepts_discrete(data):
    disc = [['name', 'Name', 'string'],
            ['year', 'Year', 'time'],
            ['country', 'Country', 'entity_domain']
            ]
    disc_df = pd.DataFrame(disc, columns=['concept', 'name', 'concept_type'])

    return disc_df


def extract_datapoints(data):
    dps = data.drop(['country_name', 'indicator_name'], axis=1)
    dps['country_code'] = dps['country_code'].map(to_concept_id)
    dps['indicator_code'] = dps['indicator_code'].map(to_concept_id)
    dps = dps.rename(columns={'country_code': 'country'})

    for c, ids in dps.groupby('indicator_code').groups.items():
        df = dps.ix[ids].copy()
        df = df.drop(['indicator_code'], axis=1)
        df = df.set_index('country')

        df = df.unstack().dropna().reset_index()

        df.columns = ['year', 'country', c]

        yield c, df


if __name__ == '__main__':
    print('reading source files...')
    data = pd.read_csv(source_data)
    data.columns = list(map(to_concept_id, data.columns))

    print('creating concepts files...')
    cont = extract_concepts_continuous(data)
    path = os.path.join(out_dir, 'ddf--concepts--continuous.csv')
    cont.to_csv(path, index=False)

    disc = extract_concepts_discrete(data)
    path = os.path.join(out_dir, 'ddf--concepts--discrete.csv')
    disc.to_csv(path, index=False)

    print('creating entities files...')
    country = extract_entities_country(data)
    path = os.path.join(out_dir, 'ddf--entities--country.csv')
    country.to_csv(path, index=False)

    print('creating datapoints files...')
    for c, df in extract_datapoints(data):
        path = os.path.join(out_dir, 'ddf--datapoints--{}--by--country--year'.format(c))
        df.to_csv(path, index=False)

    print('creating index file...')
    create_index_file(out_dir)

    print('Done.')
