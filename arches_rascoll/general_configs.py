import codecs
import copy
import datetime
import json
import os
import uuid as GenUUID


from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.types import JSON, Float, Text, DateTime, Integer
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB


# Note, the database credentials in the DB URL are set to the default values for a local Arches install, 
# these should be changed to match your own database and set with the ARCHES_DB_URL environment variable.
ARCHES_DB_URL = os.getenv('ARCHES_DB_URL', 'postgresql://postgres:postgis@127.0.0.1:5434/rascoll')
IMPORT_DIR = os.getenv('RASCOLL_ETL_DIR', '/home/ekansa/gci-data')
RAW_IMPORT_CSV = os.path.join(IMPORT_DIR, 'gci-all-orig.csv')
ARCHES_INSERT_SQL_PATH =  os.path.join(IMPORT_DIR, 'etl_sql.txt')

STAGING_SCHEMA_NAME = 'staging'
IMPORT_TABLE_NAME = 'rsci'

# For this demo, we're using the AfRSC resource and sample collection resource model.
# Alter this as needed to fit your own
RSCI_UUID = 'bda239c6-d376-11ef-a239-0275dc2ded29'
RSCI_MODEL_NAME = 'reference_and_sample_collection_item'

# The UUID for the English language value. This is the prefLabel relates to the
# English concept (id: '38729dbe-6d1c-48ce-bf47-e2a18945600e')
ENG_VALUE_UUID = 'bc35776b-996f-4fc1-bd25-9f6432c1f349'

PREFERRED_TERM_TYPE_UUID = '8f40c740-3c02-4839-b1a4-f1460823a9fe'

TILE_DATA_COPY_FLAG = '----COPY:stage_targ_field----'

DATA_TYPES_SQL = {
    JSONB: 'jsonb',
    UUID: 'uuid',
    Integer: 'integer',
    Float: 'float',
    Text: 'text',
    DateTime: 'timestamp',
    ARRAY(UUID): 'uuid[]',
}

def copy_value(value):
    return value

def make_lang_dict_value(value, lang='en'):
    return {
        lang: {
            'value': str(value),
            'direction': 'ltr',
        }
    }

RSCI_MAPPING_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'rsci',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'mappings': [
        {
            'raw_col': 'rsci_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
            ], 
        },
        {
            'raw_col': 'Common Name',
            'targ_table': 'name',
            'stage_field_prefix': 'common_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type_', ARRAY(UUID), [PREFERRED_TERM_TYPE_UUID],),
                ('name_language_', ARRAY(UUID), [ENG_VALUE_UUID],),
                ('nodegroupid', UUID, 'bda409e0-d376-11ef-a239-0275dc2ded29',),
            ], 
        },
        {
            'raw_col': 'Additional Names',
            'targ_table': 'name',
            'stage_field_prefix': 'additional_names_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type_', ARRAY(UUID), ['0798bf2c-ab07-43d7-81f4-f1e2d20251a1'],),
                ('name_language_', ARRAY(UUID), [ENG_VALUE_UUID],),
                ('nodegroupid', UUID, 'bda409e0-d376-11ef-a239-0275dc2ded29',),
            ], 
        },
        {
            'raw_col': 'Barcode No.',
            'targ_table': 'identifier',
            'stage_field_prefix': 'barcode_no_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'identifier_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('identifier_type', ARRAY(UUID), ['ae7f2811-3fee-4624-bc74-9451bd05be2d'],),
                ('nodegroupid', UUID, 'bda3962c-d376-11ef-a239-0275dc2ded29',),
            ], 
        },
    ],
}


IMPORT_PLACES_CSV = os.path.join(IMPORT_DIR, 'gci-all-places.csv')

PLACE_MODEL_UUID = '3dda9f54-d771-11ef-825b-0275dc2ded29'
PLACE_MODEL_NAME = 'place_clone'

PLACE_STATEMENT_TYPE_UUIDS = ['72202a9f-1551-4cbc-9c7a-73c02321f3ea','df8e4cf6-9b0b-472f-8986-83d5b2ca28a0',]

PLACE_NAME_TILE_DATA = {
    "3ddadbfe-d771-11ef-825b-0275dc2ded29": [PREFERRED_TERM_TYPE_UUID,], # type
    "3ddaccea-d771-11ef-825b-0275dc2ded29": None, # source
    "3ddadafa-d771-11ef-825b-0275dc2ded29": None, # _label
    "3ddadcee-d771-11ef-825b-0275dc2ded29": [ENG_VALUE_UUID,], # language
    "3ddacdf8-d771-11ef-825b-0275dc2ded29": TILE_DATA_COPY_FLAG,
}

PLACE_STATEMENT_TILE_DATA = {
    "3ddae356-d771-11ef-825b-0275dc2ded29": PLACE_STATEMENT_TYPE_UUIDS, # type
    "3ddada14-d771-11ef-825b-0275dc2ded29": None, # _label
    "3ddaa0f8-d771-11ef-825b-0275dc2ded29": None, # statement creation
    "3ddad744-d771-11ef-825b-0275dc2ded29": [ENG_VALUE_UUID,], # language
    "3ddacee8-d771-11ef-825b-0275dc2ded29": TILE_DATA_COPY_FLAG,
}


PLACE_MAPPING_CONFIGS = {
    'model_id': PLACE_MODEL_UUID,
    'staging_table': PLACE_MODEL_NAME,
    'model_staging_schema': PLACE_MODEL_NAME,
    'raw_pk_col': 'place_uuid',
    'load_path': IMPORT_PLACES_CSV,
    'mappings': [
        {
            'raw_col': 'place_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'default_values': [
                ('graphid', UUID, PLACE_MODEL_UUID,),
                ('graphpublicationid', UUID, 'e2b081a8-d7f6-11ef-8ff3-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
            ], 
        },
        {
            'raw_col': 'specific_place',
            'targ_table': 'name',
            'stage_field_prefix': 'specific_place_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('type', ARRAY(UUID), [PREFERRED_TERM_TYPE_UUID],),
                ('language', ARRAY(UUID), [ENG_VALUE_UUID],),
                ('nodegroupid', UUID, '3ddab19c-d771-11ef-825b-0275dc2ded29',),
            ],
            'tile_data': PLACE_NAME_TILE_DATA, 
        },
        {
            'raw_col': 'statement',
            'targ_table': 'statement',
            'stage_field_prefix': 'statement_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('type', ARRAY(UUID),  PLACE_STATEMENT_TYPE_UUIDS,),
                ('language', ARRAY(UUID), [ENG_VALUE_UUID],),
                ('nodegroupid', UUID, '3ddac588-d771-11ef-825b-0275dc2ded29',),
            ],
            'tile_data': PLACE_STATEMENT_TILE_DATA,
        },
        {
            'raw_col': 'specific_place_uri',
            'targ_table': 'external_uri',
            'stage_field_prefix': 'specific_place_uri_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'external_uri',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('nodegroupid', UUID, '3ddaa8e6-d771-11ef-825b-0275dc2ded29',),
            ], 
        },
        {
            'raw_col': 'geo_point',
            'targ_table': 'defined_by',
            'stage_field_prefix': 'geo_point_',
            'value_transform': copy_value,
            'targ_field': 'defined_by',
            'data_type': JSONB,
            'make_tileid': True,
            'source_geojson': True,
            'default_values': [
                ('nodegroupid', UUID, '3ddabeda-d771-11ef-825b-0275dc2ded29',),
            ], 
        },
    ],
}



IMPORT_RSCI_PLACES_CSV = os.path.join(IMPORT_DIR, 'gci-all-rsci-places.csv')

RSCI_PLACE_PRODUCTION_TYPE_IDS = ['d1adc747-6773-47c2-8470-a2ef0ab23fb9',]
REL_RSCI_PLACE_REL_TYPE_ID = 'ac41d9be-79db-4256-b368-2f4559cfbe55'
REL_RSCI_PLACE_INVERSE_REL_TYPE_ID = 'ac41d9be-79db-4256-b368-2f4559cfbe55'
REL_RSCI_PLACE_NODEID = 'bda5889c-d376-11ef-a239-0275dc2ded29'

RSCI_PLACE_MAPPING_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'rsci_place',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'load_path': IMPORT_RSCI_PLACES_CSV,
    'mappings': [
        {
            'raw_col': 'rsci_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
            ], 
        },
        {
            'raw_col': 'geo_point_1',
            'targ_table': 'production_',
            'stage_field_prefix': 'geo_point_1_',
            'value_transform': copy_value,
            'targ_field': 'production_location_geo',
            'data_type': JSONB,
            'make_tileid': True,
            'source_geojson': True,
            'default_values': [
                ('production_type', ARRAY(UUID), RSCI_PLACE_PRODUCTION_TYPE_IDS,),
                ('nodegroupid', UUID, 'bda43726-d376-11ef-a239-0275dc2ded29',),
            ],
            'related_resources': [
                {
                    'targ_field': 'production_location',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'place_uuid_1',
                    'rel_type_id': REL_RSCI_PLACE_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_RSCI_PLACE_INVERSE_REL_TYPE_ID,
                    'rel_nodeid': REL_RSCI_PLACE_NODEID,
                },
            ]
        },
        {
            'raw_col': 'geo_point_2',
            'targ_table': 'production_',
            'stage_field_prefix': 'geo_point_2_',
            'value_transform': copy_value,
            'targ_field': 'production_location_geo',
            'data_type': JSONB,
            'make_tileid': True,
            'source_geojson': True,
            'default_values': [
                ('production_type', ARRAY(UUID), RSCI_PLACE_PRODUCTION_TYPE_IDS,),
                ('nodegroupid', UUID, 'bda43726-d376-11ef-a239-0275dc2ded29',),
            ],
            'related_resources': [
                {
                    'targ_field': 'production_location',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'place_uuid_2',
                    'rel_type_id': REL_RSCI_PLACE_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_RSCI_PLACE_INVERSE_REL_TYPE_ID,
                    'rel_nodeid': REL_RSCI_PLACE_NODEID,
                },
            ]
        },
    ],
}

RSCI_NOTES_STATEMENT_TYPE_IDS = ['9886efe9-c323-49d5-8d32-5c2a214e5630',] # sample description
RSCI_PHYS_FORM_STATEMENT_TYPE_IDS = ['72c01bf3-60a3-4a09-bc33-ddbd508c145f',] # condition

RSCI_STATEMENTS_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'rsci_statements',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'mappings': [
        {
            'raw_col': 'rsci_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
            ], 
        },
        {
            'raw_col': 'Notes',
            'targ_table': 'statement',
            'stage_field_prefix': 'notes_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('statement_type', ARRAY(UUID), RSCI_NOTES_STATEMENT_TYPE_IDS,),
                ('statement_language_', ARRAY(UUID), [ENG_VALUE_UUID],),
                ('nodegroupid', UUID, 'bda499a0-d376-11ef-a239-0275dc2ded29',),
            ],
        },
        {
            'raw_col': 'Physical Form',
            'targ_table': 'statement',
            'stage_field_prefix': 'physical_form_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('statement_type', ARRAY(UUID), RSCI_PHYS_FORM_STATEMENT_TYPE_IDS,),
                ('statement_language_', ARRAY(UUID), [ENG_VALUE_UUID],),
                ('nodegroupid', UUID, 'bda499a0-d376-11ef-a239-0275dc2ded29',),
            ],
        },
    ],
}




ALL_MAPPING_CONFIGS = [
    RSCI_MAPPING_CONFIGS,
    PLACE_MAPPING_CONFIGS,
    RSCI_PLACE_MAPPING_CONFIGS,
    RSCI_STATEMENTS_CONFIGS,
]




ARCHES_REL_VIEW_PREP_SQLS = [
    f"""
    SELECT __arches_create_resource_model_views('{RSCI_UUID}');
    """,
    
    f"""
    SELECT __arches_create_resource_model_views('{PLACE_MODEL_UUID}');
    """,
]


