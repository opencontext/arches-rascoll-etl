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


def lookup_data_type_sql_str(data_type):
    """Maps a SQLAlchemy data type object to a SQL string """
    mapped_data_type = general_configs.DATA_TYPES_SQL.get(data_type)
    if mapped_data_type is None:
        # the key lookup for an ARRAY(UUID) field doesn't work, so
        # we'll just assume we mean an uuid array.
        mapped_data_type = 'uuid[]'
    return mapped_data_type


def make_related_object_dict_and_res_x_res_id(
    resource_id,
    rel_type_id,
    inverse_rel_type_id,
):
    """Make a dictionary for the related object"""
    res_x_res_id = str(GenUUID.uuid4())
    rel_obj = {
        # This is the resource instance id that we are linking TO (towards)
        "resourceId": resource_id,
        "ontologyProperty": rel_type_id,
        "resourceXresourceId": res_x_res_id,
        "inverseOntologyProperty": inverse_rel_type_id,
    }
    return rel_obj, res_x_res_id


def get_card_data_for_node_in_graph(node_alias, graph_name):
    """Get the card data for a node in a graph"""
    sql = f"""
    SELECT 
        c.cardid, 
        c.name->>'en' AS card_name, 
        c.nodegroupid, 
        n.name AS node_name,
        n.alias, 
        n.graphid, 
        ng.parentnodegroupid, 
        pc.name->>'en' AS par_card_name, 
        g.name->>'en' AS graph_name, 
        g.slug
    FROM cards AS c
    JOIN nodes AS N ON c.nodegroupid = n.nodegroupid
    JOIN node_groups AS ng ON n.nodegroupid = ng.nodegroupid
    LEFT JOIN cards AS pc ON ng.parentnodegroupid = pc.nodegroupid
    JOIN graphs AS g ON n.graphid = g.graphid 
    WHERE n.alias = '{node_alias}'
    AND g.name->>'en' = '{graph_name}'
    """
    engine = create_engine(general_configs.ARCHES_DB_URL)
    df = pd.read_sql(sql, engine)
    d_list = df.to_dict(orient='records')
    if len(d_list) == 0:
        return None
    return d_list[0]