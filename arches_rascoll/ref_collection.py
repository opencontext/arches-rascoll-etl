import copy
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
from arches_rascoll import places
from arches_rascoll import utilities

"""
# Use like this in a Python shell:

from arches_rascoll import ref_collection
dfs = ref_collection.prepare_all_transformed_data()
sqls = ref_collection.prepare_all_sql_inserts()

"""


def save_data_to_csv_with_objects_as_json(df_stage, col_data_types, path):
    """Saves a dataframe to a CSV file with JSON objects as strings"""
    df_temp = df_stage.copy()
    for col, data_type in col_data_types.items():
        mapped_data_type = utilities.lookup_data_type_sql_str(data_type)
        if mapped_data_type == 'jsonb':
            index = (
                df_temp[col].notnull() 
            )
            df_temp.loc[index, col] = df_temp[index][col].apply(lambda x: json.dumps(x))
        elif mapped_data_type == 'uuid':
            index = (
                df_temp[col].notnull() 
            )
            df_temp.loc[index, col] = df_temp[index][col].apply(lambda x: str(x))
        elif mapped_data_type == 'uuid[]':
            index = (
                df_temp[col].notnull() 
            )
            df_temp.loc[index, col] = df_temp[index][col].apply(lambda x: json.dumps(x))
    df_temp.to_csv(path, index=False)


def make_objs_from_json_strings(df_stage, col_data_types):
    """Makes JSON objects from JSON strings in a dataframe"""
    for col, data_type in col_data_types.items():
        mapped_data_type = utilities.lookup_data_type_sql_str(data_type)
        if mapped_data_type == 'jsonb':
            index = (
                df_stage[col].notnull() 
                & df_stage[col].apply(lambda x: isinstance(x, str))
                & (df_stage[col] != 'NaN')
            )
            df_stage.loc[~index, col] = ''
            df_stage.loc[index, col] = df_stage[index][col].apply(lambda x: json.loads(x))
        elif mapped_data_type == 'uuid[]':
            index = (
                df_stage[col].notnull() 
            )
            df_stage.loc[~index, col] = ''
            df_stage.loc[index, col] = df_stage[index][col].apply(lambda x: json.loads(x))
    return df_stage


def make_transformed_value(act_raw_value, data_type, value_transform):
    """Makes a transformed value based on the data type and value transform"""
    if data_type == JSONB \
        and value_transform == general_configs.copy_value \
        and isinstance(act_raw_value, str):
        # We need to convert the string to a JSON object.
        try:
            act_raw_value = json.loads(act_raw_value)
        except json.JSONDecodeError:
            # If we can't convert the string to JSON, we'll just skip it.
            return None
    return value_transform(act_raw_value)


def prep_transformed_data(df, configs):
    """Prepares a dataset from the dataframe df for transformation into a staging table"""
    dict_rows = {}
    col_data_types = {}
    for _, row in df.iterrows():
        # Given the small data volumes, I'm not bothering to optimize performance with
        # vectorized operations. We'll just iterate through the rows.
        raw_pk = row[configs.get('raw_pk_col')]
        if not dict_rows.get(raw_pk):
            dict_rows[raw_pk] = {}
        for mapping in configs.get('mappings'):
            raw_col = mapping.get('raw_col')
            stage_field_prefix = mapping.get('stage_field_prefix')
            value_transform = mapping.get('value_transform')
            targ_field = mapping.get('targ_field')
            data_type = mapping.get('data_type')
            stage_targ_field = f'{stage_field_prefix}{targ_field}'
            col_data_types[stage_targ_field] = data_type
            if pd.isnull(row[raw_col]):
                continue
            act_raw_value = row[raw_col]
            # The transformed value will be the value that we will insert into the staging table and
            # then moved into Arches
            all_transformed_values = []
            transformed_value = make_transformed_value(act_raw_value, data_type, value_transform)
            all_transformed_values.append(transformed_value)
            if mapping.get('tile_other_fields'):
                for tile_other_field_config in mapping.get('tile_other_fields', []):
                    other_raw_col = tile_other_field_config.get('raw_col')
                    if pd.isnull( row[other_raw_col]):
                        continue
                    other_raw_value = row[other_raw_col]
                    other_data_type = tile_other_field_config.get('data_type')
                    other_targ_field = tile_other_field_config.get('targ_field')
                    other_stage_targ_field = f'{stage_field_prefix}{other_targ_field}'
                    other_value_transform = tile_other_field_config.get('value_transform')
                    other_transformed_value = make_transformed_value(other_raw_value, other_data_type, other_value_transform)
                    if other_transformed_value is None:
                        continue
                    all_transformed_values.append(other_transformed_value)
                    dict_rows[raw_pk][other_stage_targ_field] = other_transformed_value
                    col_data_types[other_stage_targ_field] = other_data_type
            # Check to see if at least one field has a transformed value.
            transformed_value_ok = False
            for transformed_value in all_transformed_values:
                if transformed_value is not None:
                    transformed_value_ok = True
            if not transformed_value_ok:
                continue
            if mapping.get('make_tileid'):
                tileid = str(GenUUID.uuid4())
                staging_tileid = f'{stage_field_prefix}tileid'
                dict_rows[raw_pk][staging_tileid] = tileid
                col_data_types[staging_tileid] = UUID
            dict_rows[raw_pk][stage_targ_field] = transformed_value
            default_values = mapping.get('default_values', [])
            for d_col, d_type, d_val in default_values:
                default_col = f'{stage_field_prefix}{d_col}'
                col_data_types[default_col] = d_type
                dict_rows[raw_pk][default_col] = d_val
            if mapping.get('related_resources'):
                # We have related resources to populate for this field.
                rel_objs = {}
                for rel_dict in mapping.get('related_resources', []):
                    # We have related resources to populate in a dictionary
                    # Make a source_rel_objs_field for the related resources. Note that multiple source
                    # columns can go into the same related resources field, and that they will be
                    # grouped by the group_source_field.
                    multi_value = rel_dict.get('multi_value', False)
                    resource_id = row[rel_dict.get('source_field_to_uuid')]
                    if pd.isnull(resource_id) or not resource_id or str(resource_id) == 'NaN':
                        continue
                    group_source_field = rel_dict.get('group_source_field', '')
                    source_rel_objs_field = f'{stage_field_prefix}{group_source_field}related_objs'
                    if not rel_objs.get(source_rel_objs_field):
                        if multi_value:
                            rel_objs[source_rel_objs_field] = []
                        else:
                            rel_objs[source_rel_objs_field] = {}
                    res_x_res_id = str(GenUUID.uuid4())
                    rel_obj = {
                        # This is the resource instance id that we are linking TO (towards)
                        "resourceId": resource_id,
                        "ontologyProperty": rel_dict.get('rel_type_id'),
                        "resourceXresourceId": res_x_res_id,
                        "inverseOntologyProperty": rel_dict.get('inverse_rel_type_id'),
                    }
                    if multi_value:
                        rel_objs[source_rel_objs_field].append(rel_obj)
                    else:
                        rel_objs[source_rel_objs_field][res_x_res_id] = rel_obj.copy()
                for source_rel_objs_field, rel_obj_vals in rel_objs.items():
                    if not rel_obj_vals:
                        continue
                    col_data_types[source_rel_objs_field] = JSONB
                    # only add one resource.
                    dict_rows[raw_pk][source_rel_objs_field] = copy.deepcopy(rel_obj_vals)
            if not mapping.get('tile_data'):
                continue
            tile_data_col = f'{stage_field_prefix}tile_data'
            col_data_types[tile_data_col] = general_configs.JSONB
            tile_data = {}
            for key, val in mapping.get('tile_data', {}).items():
                if val == general_configs.TILE_DATA_COPY_FLAG:
                    tile_data[key] = copy.deepcopy(dict_rows[raw_pk][stage_targ_field])
                else:
                    tile_data[key] = copy.deepcopy(val)
            dict_rows[raw_pk][tile_data_col] = tile_data
    rows = [dict(row) for _, row in dict_rows.items()]
    df_staging = pd.DataFrame(rows)
    return df_staging, col_data_types


def prepare_all_transformed_data(
    df=None,
    raw_path=general_configs.RAW_IMPORT_CSV, 
    all_configs=general_configs.ALL_MAPPING_CONFIGS, 
    regenerate=False,
    staging_schema=general_configs.STAGING_SCHEMA_NAME,
    db_url=general_configs.ARCHES_DB_URL,
):
    if df is None:
        df = pd.read_csv(raw_path)
    dfs = {}
    for configs in all_configs:
        staging_table = configs.get('staging_table')
        print(f'Preparing data for: {staging_table}')
        utilities.drop_import_table(staging_table)
        trans_path = utilities.make_full_path_filename(
            general_configs.DATA_DIR, 
            f'{staging_table}.csv'
        )
        if not configs.get('load_path'):
            # Use the main data frame of reference and sample collection items.
            df_stage, col_data_types = prep_transformed_data(df, configs)
        else:
            # Use a separate data frame for the data prior to transformation.
            df_load = pd.read_csv(configs.get('load_path'))
            df_stage, col_data_types = prep_transformed_data(df_load, configs)
        if not regenerate and os.path.exists(trans_path):
            # Yes this is inefficient. We're always regenerating a df_stage even if we
            # don't want to regenerate and will be throwing away the newly created df_stage
            # by reading an existing dataset. But we still want the col_data_types, so
            # we'll just suffer with the inefficiency.
            df_stage = pd.read_csv(trans_path)
            print(f'Loaded previously prepared {len(df_stage.index)} rows of data for: {staging_table}')
            df_stage = make_objs_from_json_strings(df_stage, col_data_types)
        save_data_to_csv_with_objects_as_json(df_stage, col_data_types, trans_path)
        # Always replace the data in the stating schema. We dropped the staging table above 
        # at the top of this loop.
        engine = utilities.create_engine(db_url)
        df_stage.to_sql(
            staging_table,
            con=engine,
            schema=staging_schema,
            if_exists='replace',
            index=False,
            dtype=col_data_types,
        )
        dfs[staging_table] = df_stage
    return dfs


def prepare_all_sql_inserts(
    all_configs=general_configs.ALL_MAPPING_CONFIGS,
    staging_schema=general_configs.STAGING_SCHEMA_NAME,
    relational_views_sqls=general_configs.ARCHES_REL_VIEW_PREP_SQLS,
    total_count=15000,
    increment=15000,
    add_tile_update_sqls=False,
):
    sqls = []
    if relational_views_sqls:
        # Add the SQL statements for the relational views.
        sqls += relational_views_sqls
    for configs in all_configs:
        staging_table = configs.get('staging_table')
        model_staging_schema = configs.get('model_staging_schema')
        start = 0
        source_tab = f'{staging_schema}.{staging_table}'
        while start < total_count:
            for mapping in configs.get('mappings'):
                insert_fields = []
                not_null_fields = []
                where_conditions = []


                stage_field_prefix = mapping.get('stage_field_prefix')
                targ_table = mapping.get('targ_table')
                targ_field = mapping.get('targ_field')

                # Add the resourceinstanceid to the insert fields, it should be always present
                insert_fields.append(
                    ('resourceinstanceid', 'resourceinstanceid::uuid')
                )

                limit_offset = f'LIMIT {increment} OFFSET {start}'
                if targ_field == 'resourceinstanceid':
                    # We're importing resource instances, so skip the offsets. We'll import them all at once.
                    limit_offset = ''
                    if start > 0:
                        # No need to make duplicate queries for resource instances. We'll do them all at once.
                        continue
                
                # We're turning off and not doing the limit_offset thing.
                limit_offset = ''


                # Now handle tileid fields. Tileids are made for attribute data added to an resource instance.
                # They will be used to know that we haven't already added certain tile data to a resource instance.
                if mapping.get('make_tileid'):
                    targ_tileid_field = 'tileid'
                    staging_tileid_field = f'{stage_field_prefix}tileid'
                    staging_tileid_select_field_type = f'{staging_tileid_field}::uuid'
                    insert_fields.append(
                        (targ_tileid_field, staging_tileid_select_field_type)
                    )
                    where_conditions.append(    
                        f'({source_tab}.{staging_tileid_select_field_type} NOT IN (SELECT tileid FROM tiles))'
                    )
                    where_conditions.append(
                        f'({source_tab}.{staging_tileid_select_field_type} IS NOT NULL)'
                    )
                else:
                    where_conditions.append(
                        f'({source_tab}.resourceinstanceid NOT IN (SELECT resourceinstanceid FROM {model_staging_schema}.{targ_table}))'
                    )

                # This is for the main data value that goes into the target table. Generally this will be tile data,
                # except for inserts into the resourceinstance table.
                stage_targ_field = f'{stage_field_prefix}{targ_field}'
                targ_data_type_sql = utilities.lookup_data_type_sql_str(mapping.get('data_type'))
                stage_targ_field_and_type = f'{stage_targ_field}::{targ_data_type_sql}'
                not_null_fields.append(stage_targ_field_and_type)
                if mapping.get('source_geojson'):
                    # We need to add a transformation function to change the geojson to a PostGIS geometry.
                    stage_targ_field_and_type = f"ST_AsText(ST_GeomFromGeoJSON({stage_targ_field}))"
                
                # Add the target field to the insert fields.
                act_insert_field = (targ_field, stage_targ_field_and_type)
                if act_insert_field not in insert_fields:
                    insert_fields.append(act_insert_field)

                
                if mapping.get('related_tileid'):
                    # The current insert is related to a previously inserted tileid. We need to add the tileid for
                    # the association
                    rel_tile_config = mapping.get('related_tileid')
                    targ_relatated_tileid_field = f'{rel_tile_config.get("targ_tile_field")}'
                    source_related_tileid_field_and_type = f'{rel_tile_config.get("source_tile_field")}::uuid'
                    insert_fields.append(
                        (targ_relatated_tileid_field, source_related_tileid_field_and_type)
                    )


                # Now we need to handle related_resources. These are JSONB fields that define relationships between
                # resource instances.
                done_source_rel_objs_fields = []
                rel_dict_i = 0
                for rel_dict in mapping.get('related_resources', []):
                    rel_dict_i += 1
                    multi_value = rel_dict.get('multi_value', False)
                    group_source_field = rel_dict.get('group_source_field', '')
                    source_rel_objs_field = f'{stage_field_prefix}{group_source_field}related_objs'
                    if source_rel_objs_field in done_source_rel_objs_fields:
                        continue
                    done_source_rel_objs_fields.append(source_rel_objs_field)
                    targ_rel_objs_field = rel_dict.get('targ_field')
                    if multi_value:
                        safe_source = f"""
                        coalesce(
                            case jsonb_typeof({source_rel_objs_field}) 
                                when 'array' then {source_rel_objs_field} 
                                else '[]'::jsonb end
                            ) as {source_rel_objs_field}_{rel_dict_i}
                        """
                        insert_fields.append(
                            (targ_rel_objs_field, f'{safe_source}')
                        )
                    else:
                        insert_fields.append(
                            (targ_rel_objs_field, f'{source_rel_objs_field}::jsonb')
                        )
                    not_null_fields.append(f'{source_rel_objs_field}::jsonb')

                # Process configurations for other data fields that belong to this same tileid
                for tile_other_dict in mapping.get('tile_other_fields', []):
                    other_targ_field = tile_other_dict.get('targ_field')
                    other_stage_targ_field = f'{stage_field_prefix}{other_targ_field}'
                    other_data_type_sql = utilities.lookup_data_type_sql_str(tile_other_dict.get('data_type'))
                    other_stage_targ_field_and_type = f'{other_stage_targ_field}::{other_data_type_sql}'
                    not_null_fields.append(other_stage_targ_field_and_type)
                    insert_fields.append(
                        (other_targ_field, other_stage_targ_field_and_type)
                    )

                # Make a not null condition for the insert statement.
                not_null_condition = ' OR '.join([f'({source_tab}.{not_null_field} IS NOT NULL)' for not_null_field in not_null_fields])
                not_null_condition = f'({not_null_condition})'
                where_conditions.append(not_null_condition)

                # Make the where condition for the insert statement.
                where_condition_sql = ' AND \n'.join(where_conditions)

                targ_fields_sql = ', \n'.join([tf for tf, _ in insert_fields])
                stage_fields_sql = ', \n'.join([s_field_and_type for _, s_field_and_type in insert_fields])

                # Now we can build the SQL query.
                sql = f"""
                INSERT INTO {model_staging_schema}.{targ_table} (
                    {targ_fields_sql}
                ) SELECT
                    {stage_fields_sql}
                
                FROM {source_tab}
                WHERE {where_condition_sql}
                ORDER BY {source_tab}.resourceinstanceid
                {limit_offset}
                ;
                """
                sqls.append(sql)
                if not mapping.get('tile_data'):
                    # No need to do a SQL UPDATE on the tile data.
                    continue
                if not add_tile_update_sqls:
                    # We're not adding the tile data to the SQL statements.
                    continue
                # Compose a SQL UPDATE statement for the tile data.
                tile_data_col = f'{stage_field_prefix}tile_data'
                nodegroupid_col = f'{stage_field_prefix}nodegroupid'
                tile_sql = f"""

                UPDATE tiles
                SET sortorder = 0,
                nodegroupid = {source_tab}.{nodegroupid_col}::uuid,
                tiledata = {source_tab}.{tile_data_col}::jsonb
                FROM {source_tab}
                WHERE {source_tab}.{staging_tileid_field}::uuid = tiles.tileid::uuid;

                """
                sqls.append(tile_sql)
            start += increment
    utilities.save_sql(sqls)
    return sqls 




