import codecs
import copy
import datetime
import json
import os
import uuid as GenUUID

import numpy as np
import pandas as pd

from shapely.geometry import mapping, shape

from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.types import JSON, Float, Text, DateTime, Integer
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB


from arches_rascoll import general_configs


def make_geo_point_geojson(lat, lon, to_json=True):
    lat = float(lat)
    lon = float(lon)
    geo_dict =  {
        "type": "Point",
        "coordinates": [lon, lat]
    }
    if to_json:
        return json.dumps(geo_dict, ensure_ascii=False)
    return geo_dict


def prep_raw_geo_data(df):
    cols = ['specific_place', 'specific_place_uri', 'specific_geojson', 'latitude', 'longitude']
    cols_b = ['specific_place_2', 'specific_place_uri_2', 'specific_geojson_2', 'latitude_2', 'longitude_2']
    col_rename = {c: c.replace('_2', '') for c in cols_b}
    geo_confs = [
        ('specific_geojson', cols, None,),
        ('specific_geojson_2', cols_b, col_rename,)
    ]
    dfs = []
    for geo_col, cols, rn in geo_confs:
        index = df[geo_col].notnull()
        df_g = df[index][cols].groupby(cols).size().reset_index(name='count')
        df_g = df_g.sort_values(by='count', ascending=False)
        if rn:
            df_g.rename(columns=rn, inplace=True)
        dfs.append(df_g)
    df_all_geo = pd.concat(dfs)
    # Ensure we have unique rows for a given URI.
    df_all_geo.drop_duplicates(subset=['specific_place_uri'], inplace=True)
    df_all_geo['place_uuid'] = ''
    df_all_geo['geo_point'] = ''
    df_all_geo['statement'] = ''
    for _, row in df_all_geo.iterrows():
        lat = row['latitude']
        lon = row['longitude']
        if np.isnan(lat) or np.isnan(lon):
            continue
        index = df_all_geo['specific_place_uri'] == row['specific_place_uri']
        statement_text = f"{row['specific_place']} (URI: {row['specific_place_uri']})"
        df_all_geo.loc[index, 'place_uuid'] = str(GenUUID.uuid4())
        df_all_geo.loc[index, 'statement'] = statement_text
        df_all_geo.loc[index, 'geo_point'] = make_geo_point_geojson(lat, lon)
    return df_all_geo


def prepare_save_geo_data(
    df=None, 
    raw_path=general_configs.RAW_IMPORT_CSV, 
    save_path=general_configs.IMPORT_PLACES_CSV
):
    if df is None:
        df = pd.read_csv(raw_path)
    df_all_geo = prep_raw_geo_data(df)
    df_all_geo.to_csv(save_path, index=False)
    return df_all_geo


def prepare_rsci_place_data(
    df=None, 
    raw_path=general_configs.RAW_IMPORT_CSV,
    geo_path=general_configs.IMPORT_PLACES_CSV,
    rsci_geo_path=general_configs.IMPORT_RSCI_PLACES_CSV,
):
    if df is None:
        df = pd.read_csv(raw_path)
    if not os.path.exists(geo_path):
        df_all_geo = prepare_save_geo_data(
            df, 
            raw_path=raw_path,
            save_path=geo_path,
        )
    else:
        df_all_geo = pd.read_csv(geo_path)
    keep_cols = ['rsci_uuid', 'specific_place_uri', 'specific_place_uri_2']
    df_rsci_geo = df[keep_cols].copy()
    df_rsci_geo.rename(columns={'specific_place_uri': 'specific_place_uri_1'}, inplace=True)
    copy_geo_cols = ['place_uuid', 'specific_place_uri', 'geo_point']
    df_geo_trim = df_all_geo[copy_geo_cols].copy()
    for i in [1, 2]:
        renames = {col: f'{col}_{i}' for col in df_geo_trim.columns.tolist()}
        df_act_geo = df_geo_trim.copy()
        df_act_geo.rename(columns=renames, inplace=True)
        join_col = f'specific_place_uri_{i}'
        df_rsci_geo = df_rsci_geo.merge(df_act_geo, how='left', left_on=join_col, right_on=join_col)
    good_index = (df_rsci_geo['place_uuid_1'].notnull() | df_rsci_geo['place_uuid_2'].notnull())
    df_rsci_geo = df_rsci_geo[good_index].copy()
    df_rsci_geo.to_csv(rsci_geo_path, index=False)
    return df_rsci_geo