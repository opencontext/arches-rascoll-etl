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

current_directory = os.getcwd()
DATA_DIR = os.getenv('RASCOLL_ETL_DIR', os.path.join(current_directory, 'data'))
RAW_IMPORT_CSV = os.path.join(DATA_DIR, 'gci-all-orig.csv')
ARCHES_INSERT_SQL_PATH =  os.path.join(DATA_DIR, 'etl_sql.txt')

STAGING_SCHEMA_NAME = 'staging'
IMPORT_TABLE_NAME = 'rsci'

# For this demo, we're using the AfRSC resource and sample collection resource model.
# Alter this as needed to fit your own
RSCI_UUID = 'bda239c6-d376-11ef-a239-0275dc2ded29'
RSCI_MODEL_NAME = 'reference_and_sample_collection_item'


# NOTE: Lots of the UUIDs to concept items are actually the UUIDs for
# preLabel "values" (in the Arches "values" table) that are related to the concept.
# At first this is super confusing. 

# The UUID for the English language value. This is the prefLabel relates to the
# English concept (id: '38729dbe-6d1c-48ce-bf47-e2a18945600e')
ENG_VALUE_UUID = 'bc35776b-996f-4fc1-bd25-9f6432c1f349'

# These UUIDs are actually for the prefLabel value that is related to the concepts
# for these types... Again, a reminder about this likely point of confusion.
PREFERRED_TERM_TYPE_UUID = '8f40c740-3c02-4839-b1a4-f1460823a9fe'
ALT_NAME_TYPE_UUID = '0798bf2c-ab07-43d7-81f4-f1e2d20251a1'

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


RSCI_BARCODE_TYPE_UUIDS = ['ae7f2811-3fee-4624-bc74-9451bd05be2d']

RSCI_NAME_TILE_DATA = {
    "bda5ce4c-d376-11ef-a239-0275dc2ded29": [PREFERRED_TERM_TYPE_UUID,], # type
    "bda511e6-d376-11ef-a239-0275dc2ded29": None, # source
    "bda5852c-d376-11ef-a239-0275dc2ded29": None, # _label
    "bda5e77e-d376-11ef-a239-0275dc2ded29": [ENG_VALUE_UUID,], # language
    "bda5cf14-d376-11ef-a239-0275dc2ded29": TILE_DATA_COPY_FLAG,
}

RSCI_ALT_NAME_TILE_DATA = {
    "bda5ce4c-d376-11ef-a239-0275dc2ded29": [ALT_NAME_TYPE_UUID,], # type
    "bda511e6-d376-11ef-a239-0275dc2ded29": None, # source
    "bda5852c-d376-11ef-a239-0275dc2ded29": None, # _label
    "bda5e77e-d376-11ef-a239-0275dc2ded29": [ENG_VALUE_UUID,], # language
    "bda5cf14-d376-11ef-a239-0275dc2ded29": TILE_DATA_COPY_FLAG,
}

RSCI_STATEMENT_TILE_DATA = {
    "bda54b02-d376-11ef-a239-0275dc2ded29": RSCI_BARCODE_TYPE_UUIDS, # type
    "bda559a8-d376-11ef-a239-0275dc2ded29": None, # _label
    "bda57dc0-d376-11ef-a239-0275dc2ded29": None, # source
    "9e729fcc-d714-11ef-8c40-0275dc2ded29": None, # data assignment
    "bda5c60e-d376-11ef-a239-0275dc2ded29": TILE_DATA_COPY_FLAG,
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
            'tile_data': RSCI_NAME_TILE_DATA,
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
                ('name_type_', ARRAY(UUID), [ALT_NAME_TYPE_UUID],),
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
                ('identifier_type', ARRAY(UUID), RSCI_BARCODE_TYPE_UUIDS,),
                ('nodegroupid', UUID, 'bda3962c-d376-11ef-a239-0275dc2ded29',),
            ],
            'tile_data': RSCI_STATEMENT_TILE_DATA,
        },
    ],
}





#---------------------------------#
#- PLACE CONFIGS -----------------#
#---------------------------------#

IMPORT_PLACES_CSV = os.path.join(DATA_DIR, 'gci-all-places.csv')

PLACE_MODEL_UUID = '3dda9f54-d771-11ef-825b-0275dc2ded29'
PLACE_MODEL_NAME = 'place'

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



#---------------------------------#
#- RSCI PLACE MAPPING CONFIGS-----#
#---------------------------------#

IMPORT_RSCI_PLACES_CSV = os.path.join(DATA_DIR, 'gci-all-rsci-places.csv')

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



#---------------------------------#
#- RSCI STATEMENT CONFIGS --------#
#---------------------------------#


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

#---------------------------------#
#- GROUP CONFIGS -----------------#
#---------------------------------#
GROUP_MODEL_UUID = '36956e50-d770-11ef-8f5d-0275dc2ded29'
GROUP_MODEL_NAME = 'group_'
IMPORT_RAW_GROUP_CSV = os.path.join(DATA_DIR, 'gci-all-groups.csv')

# UUID for the NFPA group, to reference for branches about types of safety standards
NFPA_GROUP_UUID = '19f9b6b2-02bf-4018-ae12-ed619d81571a'

# This is here to start out the gci-all-groups.csv file. Add to that file as needed.
GROUP_DATA = [
    {
        'group_uuid': NFPA_GROUP_UUID,
        'group_name': 'National Fire Protection Association (NFPA)',
    },
]

GROUP_NAME_TILE_DATA = {
    "3695fbe0-d770-11ef-8f5d-0275dc2ded29": [PREFERRED_TERM_TYPE_UUID,], # type
    "3695ec86-d770-11ef-8f5d-0275dc2ded29": None, # source
    "3695e092-d770-11ef-8f5d-0275dc2ded29": None, # _label
    "3696269c-d770-11ef-8f5d-0275dc2ded29": [ENG_VALUE_UUID,], # language
    "3696194a-d770-11ef-8f5d-0275dc2ded29": TILE_DATA_COPY_FLAG,
}

GROUP_MAPPING_CONFIGS = {
    'model_id': GROUP_MODEL_UUID,
    'staging_table': GROUP_MODEL_NAME,
    'model_staging_schema': GROUP_MODEL_NAME,
    'raw_pk_col': 'group_uuid',
    'load_path': IMPORT_RAW_GROUP_CSV,
    'mappings': [
        {
            'raw_col': 'group_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'default_values': [
                ('graphid', UUID, GROUP_MODEL_UUID,),
                ('graphpublicationid', UUID, '3fd6e10e-d8c6-11ef-9ef7-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
            ], 
        },
        {
            'raw_col': 'group_name',
            'targ_table': 'name',
            'stage_field_prefix': 'group_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type', ARRAY(UUID), [PREFERRED_TERM_TYPE_UUID],),
                ('name_language', ARRAY(UUID), [ENG_VALUE_UUID],),
                ('nodegroupid', UUID, '3695cff8-d770-11ef-8f5d-0275dc2ded29',),
            ],
            'tile_data': GROUP_NAME_TILE_DATA, 
        },
        
    ],
}


#---------------------------------#
#- RSCI GROUP SAFETY CONFIGS -----#
#---------------------------------#
IMPORT_RSCI_GROUPS_SAFTEY_CSV = os.path.join(DATA_DIR, 'gci-all-rsci-groups-safety.csv')
# These are the same "is related to" values as used to relate RSCI to the place model
REL_RSCI_GROUP_REL_SAFETY_TYPE_ID = REL_RSCI_PLACE_REL_TYPE_ID
REL_RSCI_GROUP_REL_INVERSE_SAFETY_TYPE_ID = REL_RSCI_PLACE_INVERSE_REL_TYPE_ID


RSCI_SAFETY_GROUP_MAPPINGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'rsci_group_safety',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'load_path': IMPORT_RSCI_GROUPS_SAFTEY_CSV,
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
            'raw_col': 'fire_safety_value_uuid',
            'targ_table': 'fire_safety_classification',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'fire_safety_classification_classification',
            'data_type': UUID,
            'make_tileid': True,
            'default_values': [
                ('nodegroupid', UUID, 'bda455bc-d376-11ef-a239-0275dc2ded29',),
            ],
            'related_resources': [
                {
                    'targ_field': 'fire_safety_classification_hold_for',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'group_uuid',
                    'rel_type_id': REL_RSCI_GROUP_REL_SAFETY_TYPE_ID,
                    'inverse_rel_type_id': REL_RSCI_GROUP_REL_INVERSE_SAFETY_TYPE_ID,
                    'rel_nodeid': 'bda5f8e0-d376-11ef-a239-0275dc2ded29', # fire saftey hold for nodeid
                },
            ]
        },
        {
            'raw_col': 'health_safety_value_uuid',
            'targ_table': 'health_safety_classification',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'health_safety_classification_classification',
            'data_type': UUID,
            'make_tileid': True,
            'default_values': [
                ('nodegroupid', UUID, 'bda25802-d376-11ef-a239-0275dc2ded29',),
            ],
            'related_resources': [
                {
                    'targ_field': 'health_safety_classification_hold_for',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'group_uuid',
                    'rel_type_id': REL_RSCI_GROUP_REL_SAFETY_TYPE_ID,
                    'inverse_rel_type_id': REL_RSCI_GROUP_REL_INVERSE_SAFETY_TYPE_ID,
                    'rel_nodeid': 'bda4e2d4-d376-11ef-a239-0275dc2ded29', # health saftey hold for nodeid
                },
            ]
        },
        {
            'raw_col': 'other_safety_value_uuid',
            'targ_table': 'general_safety_classification',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'general_safety_classification_classification',
            'data_type': UUID,
            'make_tileid': True,
            'default_values': [
                ('nodegroupid', UUID, 'bda4e2d4-d376-11ef-a239-0275dc2ded29',),
            ],
            'related_resources': [
                {
                    'targ_field': 'general_safety_classification_hold_for',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'group_uuid',
                    'rel_type_id': REL_RSCI_GROUP_REL_SAFETY_TYPE_ID,
                    'inverse_rel_type_id': REL_RSCI_GROUP_REL_INVERSE_SAFETY_TYPE_ID,
                    'rel_nodeid': 'bda5348c-d376-11ef-a239-0275dc2ded29', # general saftey hold for nodeid
                },
            ]
        },
        {
            'raw_col': 'reactivity_safety_value_uuid',
            'targ_table': 'general_safety_classification',
            'stage_field_prefix': 'reactivity_safety_',
            'value_transform': copy_value,
            'targ_field': 'general_safety_classification_classification',
            'data_type': UUID,
            'make_tileid': True,
            'default_values': [
                ('nodegroupid', UUID, 'bda4e2d4-d376-11ef-a239-0275dc2ded29',),
            ],
            'related_resources': [
                {
                    'targ_field': 'general_safety_classification_hold_for',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'group_uuid',
                    'rel_type_id': REL_RSCI_GROUP_REL_SAFETY_TYPE_ID,
                    'inverse_rel_type_id': REL_RSCI_GROUP_REL_INVERSE_SAFETY_TYPE_ID,
                    'rel_nodeid': 'bda5348c-d376-11ef-a239-0275dc2ded29', # general saftey hold for nodeid
                },
            ]
        },
    ],
}




#---------------------------------#
#- PERSON CONFIGS ----------------#
#---------------------------------#
PERSON_MODEL_UUID = 'e1d0ea1a-d770-11ef-8c40-0275dc2ded29'
PERSON_MODEL_NAME = 'person'
IMPORT_RAW_PERSON_CSV = os.path.join(DATA_DIR, 'gci-all-persons.csv')

FULLNAME_TYPE_VALUE_UUID = '828a2e14-8976-4d99-96d0-aeb1bd4223cc'

PERSON_NAME_TILE_DATA = {
    "e1d1d63c-d770-11ef-8c40-0275dc2ded29": [FULLNAME_TYPE_VALUE_UUID,], # type
    "e1d1d7ea-d770-11ef-8c40-0275dc2ded29": None, # source
    "e1d1cc64-d770-11ef-8c40-0275dc2ded29": None, # _label
    "e1d1a70c-d770-11ef-8c40-0275dc2ded29": None, # part
    "e1d1cb88-d770-11ef-8c40-0275dc2ded29": [ENG_VALUE_UUID,], # language
    "e1d1ddda-d770-11ef-8c40-0275dc2ded29": TILE_DATA_COPY_FLAG,
}

PERSON_MAPPING_CONFIGS = {
    'model_id': PERSON_MODEL_UUID,
    'staging_table': PERSON_MODEL_NAME,
    'model_staging_schema': PERSON_MODEL_NAME,
    'raw_pk_col': 'person_uuid',
    'load_path': IMPORT_RAW_PERSON_CSV,
    'mappings': [
        {
            'raw_col': 'person_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'default_values': [
                ('graphid', UUID, PERSON_MODEL_UUID,),
                ('graphpublicationid', UUID, '3fd6e10e-d8c6-11ef-9ef7-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
            ], 
        },
        {
            'raw_col': 'person_name',
            'targ_table': 'name',
            'stage_field_prefix': 'person_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type', ARRAY(UUID), [FULLNAME_TYPE_VALUE_UUID],),
                ('name_language', ARRAY(UUID), [ENG_VALUE_UUID],),
                ('nodegroupid', UUID, 'e1d0f244-d770-11ef-8c40-0275dc2ded29',),
            ],
            'tile_data': PERSON_NAME_TILE_DATA, 
        },
        
    ],
}



#---------------------------------#
#- SET CONFIGS -------------------#
#---------------------------------#
SET_MODEL_UUID = 'da0ed58e-d771-11ef-af99-0275dc2ded29'
SET_MODEL_NAME = 'collection_or_set'
IMPORT_RAW_SET_CSV = os.path.join(DATA_DIR, 'gci-all-sets.csv')

GCI_REF_COL_SET_UUID = 'e6d28c12-9efa-4d22-8ac9-acdb8a4f6087'

SET_DATA = [
    {
        'set_uuid': GCI_REF_COL_SET_UUID, 
        'set_name': 'Getty Conservation Institute (GCI) Reference Collection',
    },
]

SET_NAME_TILE_DATA = {
    "da0f4e7e-d771-11ef-af99-0275dc2ded29": [PREFERRED_TERM_TYPE_UUID,], # type
    "da0f3f24-d771-11ef-af99-0275dc2ded29": None, # source
    "da0f311e-d771-11ef-af99-0275dc2ded29": None, # _label
    "da0f3740-d771-11ef-af99-0275dc2ded29": [ENG_VALUE_UUID,], # language
    "da0f5676-d771-11ef-af99-0275dc2ded29": TILE_DATA_COPY_FLAG,
}

SET_MAPPING_CONFIGS = {
    'model_id': SET_MODEL_UUID,
    'staging_table': SET_MODEL_NAME,
    'model_staging_schema': SET_MODEL_NAME,
    'raw_pk_col': 'set_uuid',
    'load_path': IMPORT_RAW_SET_CSV,
    'mappings': [
        {
            'raw_col': 'set_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'default_values': [
                ('graphid', UUID, SET_MODEL_UUID,),
                ('graphpublicationid', UUID, '3fd6e10e-d8c6-11ef-9ef7-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
            ], 
        },
        {
            'raw_col': 'set_name',
            'targ_table': 'name',
            'stage_field_prefix': '',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type', ARRAY(UUID), [PREFERRED_TERM_TYPE_UUID],),
                ('name_language', ARRAY(UUID), [ENG_VALUE_UUID],),
                ('nodegroupid', UUID, 'da0ef9d8-d771-11ef-af99-0275dc2ded29',),
            ],
            'tile_data':SET_NAME_TILE_DATA, 
        },
        
    ],
}




#---------------------------------#
#- PROVENANCE ACTIVITY CONFIGS ---#
#---------------------------------#
PROV_ACT_MODEL_UUID = '26a55ac6-d772-11ef-825b-0275dc2ded29'
PROV_ACT_MODEL_NAME = 'provenance_activity'

IMPORT_RAW_PROV_ACT_CSV = os.path.join(DATA_DIR, 'gci-all-provenance-activity.csv')

PROV_ACT_EVENT_TYPE_TRANSFERED_VALUE_UUID = '9435c2af-2773-49d0-b85e-abb3539723da'
PROV_ACT_ACQUIRE_CARRIED_OUT_BY_NODE_ID = '26a62ed8-d772-11ef-825b-0275dc2ded29'
PROV_ACT_ACQUIRE_FROM_NODE_ID = '26a60520-d772-11ef-825b-0275dc2ded29'
PROV_ACT_ACQUIRE_TITLE_OF_NODE_ID = '26a65a98-d772-11ef-825b-0275dc2ded29'

# These are the same "is related to" values as used to relate RSCI to the place model
REL_PROV_ACT_CARRIED_OUT_BY_REL_TYPE_ID = REL_RSCI_PLACE_REL_TYPE_ID
REL_PROV_ACT_CARRIED_OUT_BY_REL_INVERSE_TYPE_ID = REL_RSCI_PLACE_INVERSE_REL_TYPE_ID
REL_PROV_ACT_TRANS_TITLE_FROM_REL_TYPE_ID = REL_RSCI_PLACE_REL_TYPE_ID
REL_PROV_ACT_TRANS_TITLE_FROM_REL_INVERSE_TYPE_ID = REL_RSCI_PLACE_INVERSE_REL_TYPE_ID

# The transfered title of relationship type is blank
REL_PROV_ACT_TRANS_TITLE_OF_REL_TYPE_ID = ''
REL_PROV_ACT_TRANS_TITLE_OF_REL_INVERSE_TYPE_ID = ''

PROV_ACT_NAME_TILE_DATA = {
    "26a63fd6-d772-11ef-825b-0275dc2ded29": [PREFERRED_TERM_TYPE_UUID,], # type
    "26a640bc-d772-11ef-825b-0275dc2ded29": None, # source
    "26a621fe-d772-11ef-825b-0275dc2ded29": None, # _label
    "26a61042-d772-11ef-825b-0275dc2ded29": [ENG_VALUE_UUID,], # language
    "26a6331a-d772-11ef-825b-0275dc2ded29": TILE_DATA_COPY_FLAG,
}

PROV_ACT_MAPPING_CONFIGS = {
    'model_id': PROV_ACT_MODEL_UUID,
    'staging_table': PROV_ACT_MODEL_NAME,
    'model_staging_schema': PROV_ACT_MODEL_NAME,
    'raw_pk_col': 'prov_act_uuid',
    'load_path': IMPORT_RAW_PROV_ACT_CSV,
    'mappings': [
        {
            'raw_col': 'prov_act_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'default_values': [
                ('graphid', UUID, PROV_ACT_MODEL_UUID,),
                ('graphpublicationid', UUID, '3fd6e10e-d8c6-11ef-9ef7-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
            ], 
        },
        {
            'raw_col': 'prov_act_name',
            'targ_table': 'prov_name',
            'stage_field_prefix': 'prov_act_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('type', ARRAY(UUID), [PREFERRED_TERM_TYPE_UUID],),
                ('language', ARRAY(UUID), [ENG_VALUE_UUID],),
                ('nodegroupid', UUID, '26a5c4f2-d772-11ef-825b-0275dc2ded29',),
            ],
            'tile_data': PROV_ACT_NAME_TILE_DATA, 
        },
        {
            'raw_col': 'prov_act_name',
            'targ_table': 'acquisition',
            'stage_field_prefix': 'acquisition_',
            'value_transform': make_lang_dict_value,
            'targ_field': '_label',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('nodegroupid', UUID, '26a58b04-d772-11ef-825b-0275dc2ded29',),
            ],
            'related_resources': [
                {
                    'group_source_field': 'carried_out_by_',
                    'multi_value': True,
                    'targ_field': 'carried_out_by',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_by_person_1_uuid',
                    'rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_INVERSE_TYPE_ID,
                    'rel_nodeid': PROV_ACT_ACQUIRE_CARRIED_OUT_BY_NODE_ID,
                },
                {
                    'group_source_field': 'carried_out_by_',
                    'multi_value': True,
                    'targ_field': 'carried_out_by',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_by_person_2_uuid',
                    'rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_INVERSE_TYPE_ID,
                    'rel_nodeid': PROV_ACT_ACQUIRE_CARRIED_OUT_BY_NODE_ID,
                },
                {
                    'group_source_field': 'carried_out_by_',
                    'multi_value': True,
                    'targ_field': 'carried_out_by',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_by_group_1_uuid',
                    'rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_INVERSE_TYPE_ID,
                    'rel_nodeid': PROV_ACT_ACQUIRE_CARRIED_OUT_BY_NODE_ID,
                },
                {
                    'group_source_field': 'carried_out_by_',
                    'multi_value': True,
                    'targ_field': 'carried_out_by',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_by_group_2_uuid',
                    'rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_INVERSE_TYPE_ID,
                    'rel_nodeid': PROV_ACT_ACQUIRE_CARRIED_OUT_BY_NODE_ID,
                },
                {
                    'group_source_field': 'transferred_title_from_',
                    'multi_value': True,
                    'targ_field': 'transferred_title_from',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_from_group_1_uuid',
                    'rel_type_id': REL_PROV_ACT_TRANS_TITLE_FROM_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_TRANS_TITLE_FROM_REL_INVERSE_TYPE_ID,
                    'rel_nodeid': PROV_ACT_ACQUIRE_FROM_NODE_ID,
                },
                {
                    'group_source_field': 'transferred_title_from_',
                    'multi_value': True,
                    'targ_field': 'transferred_title_from',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_from_group_2_uuid',
                    'rel_type_id': REL_PROV_ACT_TRANS_TITLE_FROM_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_TRANS_TITLE_FROM_REL_INVERSE_TYPE_ID,
                    'rel_nodeid': PROV_ACT_ACQUIRE_FROM_NODE_ID,
                },
                {
                    'group_source_field': 'transferred_title_of_',
                    'multi_value': True,
                    'targ_field': 'transferred_title_of',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'rsci_uuid',
                    'rel_type_id': REL_PROV_ACT_TRANS_TITLE_OF_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_TRANS_TITLE_OF_REL_INVERSE_TYPE_ID,
                    'rel_nodeid': PROV_ACT_ACQUIRE_FROM_NODE_ID,
                },
            ],
        },
        {
            'raw_col': 'Acquisition Date__begin_of_the_begin',
            'targ_table': 'acquisition_timespan',
            'stage_field_prefix': 'acquisition_timespan_',
            'value_transform': copy_value,
            'targ_field': 'begin_of_the_begin',
            'data_type': DateTime,
            'make_tileid': True,
            'default_values': [
                ('nodegroupid', UUID, '26a57510-d772-11ef-825b-0275dc2ded29',),
            ],
            'related_tileid': {
                'source_tile_field': 'acquisition_tileid',
                'targ_tile_field': 'acquisition',
            },
            'tile_other_fields': [
                # Mappings for other fields to includ in the same tile
                {
                    'raw_col': 'Acquisition Date__end_of_the_begin',
                    'targ_field': 'end_of_the_begin',
                    'data_type': DateTime,
                    'value_transform': copy_value,
                },
                {
                    'raw_col': 'Acquisition Date__begin_of_the_end',
                    'targ_field': 'begin_of_the_end',
                    'data_type': DateTime,
                    'value_transform': copy_value,
                },
                {
                    'raw_col': 'Acquisition Date__end_of_the_end',
                    'targ_field': 'end_of_the_end',
                    'data_type': DateTime,
                    'value_transform': copy_value,
                }
            ],
        },
    ],
}




ALL_MAPPING_CONFIGS = [
    RSCI_MAPPING_CONFIGS,
    PLACE_MAPPING_CONFIGS,
    RSCI_PLACE_MAPPING_CONFIGS,
    RSCI_STATEMENTS_CONFIGS,
    GROUP_MAPPING_CONFIGS,
    RSCI_SAFETY_GROUP_MAPPINGS,
    PERSON_MAPPING_CONFIGS,
    SET_MAPPING_CONFIGS,
    PROV_ACT_MAPPING_CONFIGS,
]




ARCHES_REL_VIEW_PREP_SQLS = [
    f"""
    SELECT __arches_create_resource_model_views('{RSCI_UUID}');
    """,
    
    f"""
    SELECT __arches_create_resource_model_views('{PLACE_MODEL_UUID}');
    """,

    f"""
    SELECT __arches_create_resource_model_views('{GROUP_MODEL_UUID}');
    """,
    f"""
    SELECT __arches_create_resource_model_views('{PERSON_MODEL_UUID}');
    """,
    f"""
    SELECT __arches_create_resource_model_views('{PROV_ACT_MODEL_UUID}');
    """,
    f"""
    SELECT __arches_create_resource_model_views('{SET_MODEL_UUID}');
    """,
]
