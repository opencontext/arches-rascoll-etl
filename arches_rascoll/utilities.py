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


def make_full_path_filename(path, filename):
    """ makes a full filepath and file name string """
    os.makedirs(path, exist_ok=True)
    return os.path.join(path, filename)


def load_serialized_json(path, filename):
    dir_file = os.path.join(path, filename)
    if not os.path.exists(dir_file):
        return None
    file_dict = json.load(open(dir_file))
    return file_dict


def save_serialized_json(path, filename, dict_obj):
    """ saves a data in the appropriate path + file """
    dir_file = make_full_path_filename(path, filename)
    json_output = json.dumps(
        dict_obj,
        indent=4,
        ensure_ascii=False,
    )
    file = codecs.open(dir_file, 'w', 'utf-8')
    file.write(json_output)
    file.close()


def save_sql(sqls, file_path=general_configs.ARCHES_INSERT_SQL_PATH):
    sql_str = '\n\n'.join(sqls)
    print(f'Save SQL to: {file_path}')
    with open(file_path, "w") as outfile:
        outfile.write(sql_str)
    return sql_str


def execute_sql(sql_text, db_url=general_configs.ARCHES_DB_URL):
    engine = create_engine(db_url)
    with engine.connect() as con:
        con.execute(text(sql_text))


def prepare_import_schema(staging_schema=general_configs.STAGING_SCHEMA_NAME):
    sql = f"CREATE SCHEMA IF NOT EXISTS {staging_schema};"
    execute_sql(sql)


def drop_import_table(tab_name, staging_schema=general_configs.STAGING_SCHEMA_NAME):
    sql = f"DROP TABLE IF EXISTS {staging_schema}.{tab_name};"
    execute_sql(sql)




