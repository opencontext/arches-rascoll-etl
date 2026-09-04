import codecs
import copy
import datetime
import json
import os
import uuid as GenUUID


from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.types import JSON, Float, Text, DateTime, Integer, Numeric
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB


# Note, the database credentials in the DB URL are set to the default values for a local Arches install, 
# these should be changed to match your own database and set with the ARCHES_DB_URL environment variable.
ARCHES_DB_URL = os.getenv('ARCHES_DB_URL', 'postgresql://postgres:postgis@127.0.0.1:5434/rascolls')

ARCHES_V8 = True
# UUID of the resource_instance_lifecycle_state_id for the Arches 8.0.0 release
ARCHES_V8_RESOURCE_INSTANCE_LIFECYCLE_STATE_ID = 'f75bb034-36e3-4ab4-8167-f520cf0b4c58'
# ARCHES_V8_RESOURCE_INSTANCE_LIFECYCLE_STATE_ID =  '9375c9a7-dad2-4f14-a5c1-d7e329fdde4f'

current_directory = os.getcwd()
DATA_DIR = os.getenv('RASCOLL_ETL_DIR', os.path.join(current_directory, 'data'))
RAW_IMPORT_CSV = os.path.join(DATA_DIR, 'gci-rsci-all-with-edits.csv')
ARCHES_INSERT_SQL_PATH =  os.path.join(DATA_DIR, 'etl_sql.txt')

STAGING_SCHEMA_NAME = 'staging'
IMPORT_TABLE_NAME = 'rsci'

# For this demo, we're using the AfRSC resource and sample collection resource model.
# Alter this as needed to fit your own
RSCI_UUID = 'd4e956f7-9fad-4fd2-94e3-563a3b2c3585'
RSCI_MODEL_NAME = 'reference_and_sample_collection_item'

FIELD_SAMPLES_TRANSACTION_ID = '723b9446-9488-4df9-a4e3-ed18041c2632'


CONTROLLED_LIST_CACHE_DICT = {}
# NOTE: Lots of the UUIDs to concept items are actually the UUIDs for
# preLabel "values" (in the Arches "values" table) that are related to the concept.
# At first this is super confusing. 
def get_controlled_list_objs_by_pref_labels(pref_labels, list_id=None,):
    from arches_rascoll import controlled_lists
    key = (str(pref_labels), str(list_id),)
    if key in CONTROLLED_LIST_CACHE_DICT:
        return CONTROLLED_LIST_CACHE_DICT.get(key)
    result = controlled_lists.make_json_for_controlled_list_items(
        pref_labels=pref_labels,
        list_id=list_id,
    )
    CONTROLLED_LIST_CACHE_DICT[key] = result
    return result


# TODO: Remove these legacy IDs
ENG_VALUE_UUID = None
PREFERRED_TERM_TYPE_UUID = None


# Language objects list_id='f7fc4f6d-fd46-4881-846f-4a08bc1a3fef'
# pref_labels=['English (language)',]
LANGUAGES_ENGLISH_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['English (language)',],
    list_id='f7fc4f6d-fd46-4881-846f-4a08bc1a3fef',
)


# Metatype list_id='c82d6f85-ae67-4c28-b07a-c114f6d6ba50'
# pref_labels=['brief text',]
METATYPE_BRIEF_TEXT_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['brief text',],
    list_id='c82d6f85-ae67-4c28-b07a-c114f6d6ba50',
)


# Name Types - Generic list_id='80028d07-9c98-48cf-84db-f77bc01c8bbc'
# pref_labels=['preferred terms',]
NAME_TYPES_GENERIC_PREFERRED_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['preferred terms',],
    list_id='80028d07-9c98-48cf-84db-f77bc01c8bbc',
)


TILE_DATA_COPY_FLAG = '----COPY:stage_targ_field----'

DATA_TYPES_SQL = {
    JSONB: 'jsonb',
    UUID: 'uuid',
    Integer: 'integer',
    Float: 'float',
    Numeric: 'numeric',
    Text: 'text',
    DateTime: 'timestamp',
    ARRAY(UUID): 'uuid[]',
}


REL_LINK_REL_TYPE_ID = 'ac41d9be-79db-4256-b368-2f4559cfbe55'
REL_LINK_INVERSE_REL_TYPE_ID = 'ac41d9be-79db-4256-b368-2f4559cfbe55'


def copy_numeric_value(value):
    if not value:
        return None
    try:
        f_value = float(value)
    except:
        f_value = None
    return f_value


def copy_value(value):
    if isinstance(value, dict):
        return copy.deepcopy(value)
    if isinstance(value, list):
        return copy.deepcopy(value)
    return value

def make_lang_dict_value(value, lang='en'):
    return {
        lang: {
            'value': str(value),
            'direction': 'ltr',
        }
    }





# Name Types - Physical Thing list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce'
# pref_labels=['primary name',]
NAME_TYPES_PHYSICAL_THING_PRIMARY_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['primary name',],
    list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce',
)

# Name Types - Physical Thing list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce'
# pref_labels=['alternate titles',]
NAME_TYPES_PHYSICAL_THING_ALTERNATE_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['alternate titles',],
    list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce',
)

# Identifier Types - Physical Thing list_id='f4a87a34-8288-4cec-8e20-b28bd567ce0a'
# pref_labels=['Barcode',]
ID_TYPE_PHYSICAL_THING_BARCODE_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['Barcode',],
    list_id='f4a87a34-8288-4cec-8e20-b28bd567ce0a',
)

# Metatype list_id='c82d6f85-ae67-4c28-b07a-c114f6d6ba50'
# pref_labels=['facet type',]
METATYPE_FACET_TYPE_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['facet type',],
    list_id='c82d6f85-ae67-4c28-b07a-c114f6d6ba50',
)

# Facet_type list_id='44984237-b8fc-49c1-a94d-06fc8e38c48e'
# pref_labels=['reference collection items',]
FACET_TYPE_RSCI_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['reference collection items',],
    list_id='44984237-b8fc-49c1-a94d-06fc8e38c48e',
)


def value_transform_facet_type_list_items_obj(value, default_value=FACET_TYPE_RSCI_LIST_ITEMS):
    """A value transform specifically to make facet type list objects for RSCI items"""
    if not value:
        return None
    return default_value


ETL_RSCI_TRANSACTION_ID = '7f44c3ec-116a-4cc6-ab16-fc48b09c2401'
RSCI_MAPPING_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci',
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
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, ETL_RSCI_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'READY_common_name',
            'targ_table': 'name',
            'stage_field_prefix': 'common_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type_', JSONB, NAME_TYPES_PHYSICAL_THING_PRIMARY_LIST_ITEMS,),
                ('name_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, ETL_RSCI_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'READY_additional_names',
            'targ_table': 'name',
            'stage_field_prefix': 'additional_names_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type_', JSONB, NAME_TYPES_PHYSICAL_THING_ALTERNATE_LIST_ITEMS,),
                ('name_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, ETL_RSCI_TRANSACTION_ID,),
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
                ('identifier_type', JSONB, ID_TYPE_PHYSICAL_THING_BARCODE_LIST_ITEMS,),
                ('transactionid', UUID, ETL_RSCI_TRANSACTION_ID,),
            ],
        },
        {
            'raw_col': 'facet_type_value_uuid',
            'targ_table': 'facet_type',
            'stage_field_prefix': '',
            'value_transform': value_transform_facet_type_list_items_obj,
            'targ_field': 'facet_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('facet_type_metatype', JSONB, METATYPE_FACET_TYPE_LIST_ITEMS,),
                ('transactionid', UUID, ETL_RSCI_TRANSACTION_ID,),
            ],
        }
    ],
}





#---------------------------------#
#- PLACE CONFIGS -----------------#
#---------------------------------#

IMPORT_PLACES_CSV = os.path.join(DATA_DIR, 'gci-all-places.csv')

PLACE_MODEL_UUID = '3dda9f54-d771-11ef-825b-0275dc2ded29'
PLACE_MODEL_NAME = 'place'

# List: Statement Types - Generic list_id='f44f7240-35c5-49e8-a0e3-2ffe308f0862'
# pref_labels=['brief text', 'sources (general concept)', ]
PLACE_STATEMENT_TYPE_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['brief text', 'sources (general concept)', ],
    list_id='f44f7240-35c5-49e8-a0e3-2ffe308f0862',
)

ETL_PLACE_TRANSACTION_ID = '2c18324a-f562-4a49-b55e-fe2d5b0bf535'
PLACE_MAPPING_CONFIGS = {
    'model_id': PLACE_MODEL_UUID,
    'staging_table': f'etl_{PLACE_MODEL_NAME}',
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
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, PLACE_MODEL_UUID,),
                ('graphpublicationid', UUID, 'a3e9145b-ba55-4793-a0fa-189e0f404ca7',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, ETL_PLACE_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'specific_place',
            'targ_table': 'name',
            'stage_field_prefix': 'specific_place_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type', JSONB, NAME_TYPES_GENERIC_PREFERRED_LIST_ITEMS,),
                ('name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, ETL_PLACE_TRANSACTION_ID,),
            ],
        },
        {
            'raw_col': 'statement',
            'targ_table': 'statement',
            'stage_field_prefix': 'statement_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('statement_type_metatype', JSONB, METATYPE_BRIEF_TEXT_LIST_ITEMS),
                ('statement_type', JSONB,  PLACE_STATEMENT_TYPE_LIST_ITEMS,),
                ('statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, ETL_PLACE_TRANSACTION_ID,),
            ],
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
                ('transactionid', UUID, ETL_PLACE_TRANSACTION_ID,),
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
                ('transactionid', UUID, ETL_PLACE_TRANSACTION_ID,),
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


ETL_GROUP_TRANSACTION_ID = 'ae797286-1d55-4d4e-a6ee-1af5b6f46181'

GROUP_MAPPING_CONFIGS = {
    'model_id': GROUP_MODEL_UUID,
    'staging_table': f'etl_{GROUP_MODEL_NAME}',
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
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, GROUP_MODEL_UUID,),
                ('graphpublicationid', UUID, '166c7b56-b36e-4cfa-9dc0-7d232f147d69',), # updated 2025-11-04
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, ETL_GROUP_TRANSACTION_ID,),
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
                ('name_type', JSONB, NAME_TYPES_GENERIC_PREFERRED_LIST_ITEMS,),
                ('name_language',  JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, ETL_GROUP_TRANSACTION_ID,),
            ],
        },
        
    ],
}



#---------------------------------#
#- PERSON CONFIGS ----------------#
#---------------------------------#
PERSON_MODEL_UUID = 'e1d0ea1a-d770-11ef-8c40-0275dc2ded29'
PERSON_MODEL_NAME = 'person'
IMPORT_RAW_PERSON_CSV = os.path.join(DATA_DIR, 'gci-all-persons.csv')

# Name Types - Personal list_id='36169008-3624-4589-98f8-47653f6db663'
# pref_labels=['full names (personal names)',]
PERSON_FULL_NAME_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['full names (personal names)',],
    list_id='36169008-3624-4589-98f8-47653f6db663',
)


ETL_PERSON_TRANSACTION_ID = '8efdc9d4-6109-438c-b45a-456608f7255c'

PERSON_MAPPING_CONFIGS = {
    'model_id': PERSON_MODEL_UUID,
    'staging_table': f'etl_{PERSON_MODEL_NAME}',
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
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, PERSON_MODEL_UUID,),
                ('graphpublicationid', UUID, '3fd6e10e-d8c6-11ef-9ef7-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, ETL_PERSON_TRANSACTION_ID,),
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
                ('name_type', JSONB, PERSON_FULL_NAME_LIST_ITEMS,),
                ('name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, ETL_PERSON_TRANSACTION_ID,),
            ],
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

ETL_SET_TRANSACTION_ID = '10d2be08-0849-4e6a-ab1d-3977cf6ccfdc'

SET_MAPPING_CONFIGS = {
    'model_id': SET_MODEL_UUID,
    'staging_table': f'etl_{SET_MODEL_NAME}',
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
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, SET_MODEL_UUID,),
                ('graphpublicationid', UUID, '3fd6e10e-d8c6-11ef-9ef7-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, ETL_SET_TRANSACTION_ID,),
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
                ('name_type', JSONB, NAME_TYPES_GENERIC_PREFERRED_LIST_ITEMS,),
                ('name_language',  JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, ETL_SET_TRANSACTION_ID,),
            ],
        },
        
    ],
}



#---------------------------------#
#- RSCI STATEMENT CONFIGS --------#
#---------------------------------#

# Use the Note value UUID, rather than sample description.
RSCI_NOTES_STATEMENT_TYPE_IDS = ['5e89d4e0-392c-4ad4-bd02-e5254e8a4740',] # Note
RSCI_PHYS_FORM_STATEMENT_TYPE_IDS = ['72c01bf3-60a3-4a09-bc33-ddbd508c145f',] # condition

# Statement Types - Physical Thing list_id='a16a4edc-c916-4293-af98-44d76ce6cba7'
# pref_labels=['note',]
STATEMENT_PHYS_THING_NOTE_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['note',],
    list_id='a16a4edc-c916-4293-af98-44d76ce6cba7',
)



# Statement Types - Physical Thing list_id='a16a4edc-c916-4293-af98-44d76ce6cba7'
# pref_labels=['sample description',]
STATEMENT_PHYS_THING_SAMPLE_DESCRIPTION_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['sample description',],
    list_id='a16a4edc-c916-4293-af98-44d76ce6cba7',
)

# Statement Types - Physical Thing list_id='a16a4edc-c916-4293-af98-44d76ce6cba7'
# pref_labels=['experiment description',]
STATEMENT_PHYS_THING_EXPERIMENT_DESCRIPTION_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['experiment description',],
    list_id='a16a4edc-c916-4293-af98-44d76ce6cba7',
)


ETL_RSCI_STATEMENTS_TRANSACTION_ID = '0dbb46cc-9809-4cc1-995f-19779cfb0a5f'

RSCI_STATEMENTS_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_statements',
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
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, ETL_RSCI_STATEMENTS_TRANSACTION_ID,),
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
                ('statement_type', JSONB, STATEMENT_PHYS_THING_NOTE_LIST_ITEMS,),
                ('statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, ETL_RSCI_STATEMENTS_TRANSACTION_ID,),
            ],
        },
        {
            'raw_col': 'READY_physical_form',
            'targ_table': 'statement',
            'stage_field_prefix': 'physical_form_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('statement_type', JSONB, STATEMENT_PHYS_THING_SAMPLE_DESCRIPTION_LIST_ITEMS,),
                ('statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, ETL_RSCI_STATEMENTS_TRANSACTION_ID,),
            ],
        },
        {
            'raw_col': 'Experiments',
            'targ_table': 'statement',
            'stage_field_prefix': 'exp_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('statement_type', JSONB, STATEMENT_PHYS_THING_EXPERIMENT_DESCRIPTION_LIST_ITEMS,),
                ('statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, ETL_RSCI_STATEMENTS_TRANSACTION_ID,),
            ],
        }
    ],
}




#---------------------------------#
#- RSCI GROUP SAFETY CONFIGS -----#
#---------------------------------#
IMPORT_RSCI_GROUPS_SAFTEY_CSV = os.path.join(DATA_DIR, 'gci-all-rsci-groups-safety.csv')
# These are the same "is related to" values as used to relate RSCI to the place model
REL_RSCI_GROUP_REL_SAFETY_TYPE_ID = REL_LINK_REL_TYPE_ID
REL_RSCI_GROUP_REL_INVERSE_SAFETY_TYPE_ID = REL_LINK_INVERSE_REL_TYPE_ID


# Safety Classification Classification list_id='de36422b-71cb-49dd-9dd6-4366753497bc'
# pref_labels=['sample description',]
SAFETY_CLASSIFICATION_NFPA_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['NFPA 704 Hazard Identification System',],
    list_id='de36422b-71cb-49dd-9dd6-4366753497bc',
)

ETL_RSCI_GROUP_SAFETY_TRANSACTION_ID = '3160cae5-d786-48b5-8357-00e2b2e80dc4'

RSCI_SAFETY_GROUP_MAPPINGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_group_safety',
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
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, ETL_RSCI_GROUP_SAFETY_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'all_safety',
            'targ_table': 'nfpa_safety_classification',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'nfpa_safety_classifcation_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('nfpa_safety_classification_classification', JSONB, SAFETY_CLASSIFICATION_NFPA_LIST_ITEMS,),
                ('transactionid', UUID, ETL_RSCI_GROUP_SAFETY_TRANSACTION_ID,),
            ],
            'related_resources': [
                {
                    'targ_field': 'nfpa_safety_classification_hold_for',
                    'multi_value': True,
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'group_uuid',
                    'rel_type_id': REL_RSCI_GROUP_REL_SAFETY_TYPE_ID,
                    'inverse_rel_type_id': REL_RSCI_GROUP_REL_INVERSE_SAFETY_TYPE_ID,
                    # 'rel_nodeid': 'bda5f8e0-d376-11ef-a239-0275dc2ded29', # hold for nodeid
                },
            ]
        },
    ],
}



#---------------------------------#
#- RSCI ACQUISITION CONFIGS    ---#
#---------------------------------#
IMPORT_RSCI_ACQUISITION_CSV = os.path.join(DATA_DIR, 'gci-all-rsci-acquistition.csv')

# These are the same "is related to" values as used to relate RSCI to the place model
REL_PROV_ACT_CARRIED_OUT_BY_REL_TYPE_ID = REL_LINK_REL_TYPE_ID
REL_PROV_ACT_CARRIED_OUT_BY_REL_INVERSE_TYPE_ID = REL_LINK_INVERSE_REL_TYPE_ID
REL_PROV_ACT_TRANS_TITLE_FROM_REL_TYPE_ID = REL_LINK_REL_TYPE_ID
REL_PROV_ACT_TRANS_TITLE_FROM_REL_INVERSE_TYPE_ID = REL_LINK_INVERSE_REL_TYPE_ID

# Event Types - Acquisition Event list_id='f3eca78c-7547-4ca5-a7eb-b130391b40ed'
# pref_labels=['transfer (method of acquisition)',]
EVENT_TPYPE_ACQUISITION_TRANSFER_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['transfer (method of acquisition)',],
    list_id='f3eca78c-7547-4ca5-a7eb-b130391b40ed',
)

# Statement Types list_id='1adae7ea-4884-432f-af44-c9aea3a48a3d'
# pref_labels=['brief text',]
STATEMENT_TYPE_BRIEF_TEXT_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['brief text',],
    list_id='1adae7ea-4884-432f-af44-c9aea3a48a3d',
)

# TimeSpan types list_id='c53d3557-e033-4aa1-b020-0dc2b690025f'
# pref_lavels=[,]
TIMESPAN_TYPES_APPROXIMATE_LIST_ITEMS = None

RSCI_ACQUISITION_TRANSACTION_ID = '360b2314-aba9-4b21-a693-4c832808105f'

RSCI_ACQUISITION_MAPPINGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_acquistition',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'load_path': IMPORT_RSCI_ACQUISITION_CSV,
    'mappings': [
        {
            'raw_col': 'rsci_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, RSCI_ACQUISITION_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'acquisition_type',
            'targ_table': 'acquisition',
            'stage_field_prefix': 'acq_',
            'value_transform': copy_value,
            'targ_field': 'acquisition_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_ACQUISITION_TRANSACTION_ID,),
            ],
            'related_resources': [
                {
                    'group_source_field': 'carried_out_by_',
                    'multi_value': True,
                    'targ_field': 'acquisition_carried_out_by',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_by_person_1_uuid',
                    'rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_INVERSE_TYPE_ID,
                    # 'rel_nodeid': PROV_ACT_ACQUIRE_CARRIED_OUT_BY_NODE_ID,
                },
                {
                    'group_source_field': 'carried_out_by_',
                    'multi_value': True,
                    'targ_field': 'acquisition_carried_out_by',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_by_person_2_uuid',
                    'rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_INVERSE_TYPE_ID,
                    # 'rel_nodeid': PROV_ACT_ACQUIRE_CARRIED_OUT_BY_NODE_ID,
                },
                {
                    'group_source_field': 'carried_out_by_',
                    'multi_value': True,
                    'targ_field': 'acquisition_carried_out_by',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_by_group_1_uuid',
                    'rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_INVERSE_TYPE_ID,
                    # 'rel_nodeid': PROV_ACT_ACQUIRE_CARRIED_OUT_BY_NODE_ID,
                },
                {
                    'group_source_field': 'carried_out_by_',
                    'multi_value': True,
                    'targ_field': 'acquisition_carried_out_by',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_by_group_2_uuid',
                    'rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_CARRIED_OUT_BY_REL_INVERSE_TYPE_ID,
                    # 'rel_nodeid': PROV_ACT_ACQUIRE_CARRIED_OUT_BY_NODE_ID,
                },
                {
                    'group_source_field': 'transferred_title_from_',
                    'multi_value': True,
                    'targ_field': 'acquisition_transferred_title_from',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_from_group_1_uuid',
                    'rel_type_id': REL_PROV_ACT_TRANS_TITLE_FROM_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_TRANS_TITLE_FROM_REL_INVERSE_TYPE_ID,
                    # 'rel_nodeid': PROV_ACT_ACQUIRE_FROM_NODE_ID,
                },
                {
                    'group_source_field': 'transferred_title_from_',
                    'multi_value': True,
                    'targ_field': 'acquisition_transferred_title_from',
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'acq_from_group_2_uuid',
                    'rel_type_id': REL_PROV_ACT_TRANS_TITLE_FROM_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_PROV_ACT_TRANS_TITLE_FROM_REL_INVERSE_TYPE_ID,
                    # 'rel_nodeid': PROV_ACT_ACQUIRE_FROM_NODE_ID,
                },
            ],
        },
        {
            'raw_col': 'acquisition_note',
            'targ_table': 'acquisition_statement',
            'stage_field_prefix': 'acq_state_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'acquisition_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('acquisition_statement_type', JSONB, STATEMENT_TYPE_BRIEF_TEXT_LIST_ITEMS,),
                ('acquisition_statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_ACQUISITION_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'acq_tileid',
                'targ_tile_field': 'acquisition',
            },
        },
        {
            'raw_col': 'Acquisition Date__edtf',
            'targ_table': 'acquisition_timespan',
            'stage_field_prefix': 'acq_time_',
            'value_transform': copy_value,
            'targ_field': 'acquisition_timespan_edtf',
            'data_type': Text,
            'make_tileid': True,
            'default_values': [
                ('acquisition_timespan_type', JSONB, TIMESPAN_TYPES_APPROXIMATE_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_ACQUISITION_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'acq_tileid',
                'targ_tile_field': 'acquisition',
            },
        },
    ],
}


#---------------------------------#
#- RSCI PRODUCTION CONFIGS -----#
#---------------------------------#

IMPORT_RAW_PROD_CSV = os.path.join(DATA_DIR, 'gci-all-production.csv')
# PRODUCTION_NODE_ID = 'bda43726-d376-11ef-a239-0275dc2ded29'

# Event Types - Production list_id='f0b85b97-f3ae-41dd-ac4e-6f1e249a9dbb'
# pref_labels=['note',]
# Let's leave this blank, since nothing seems applicable in this list
EVENT_TYPE_PRODUCTION_LIST_ITEMS = None

# Statement Types list_id='1adae7ea-4884-432f-af44-c9aea3a48a3d'
# pref_labels = ['producer description', ]
STATEMENT_PRODUCER_DESC_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels = ['producer description', ],
    list_id='1adae7ea-4884-432f-af44-c9aea3a48a3d',
)

# Statement Types list_id='1adae7ea-4884-432f-af44-c9aea3a48a3d'
# pref_labels = ['expiration date statement',]
STATEMENT_EXPIRE_DATE_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels = ['expiration date statement', ],
    list_id='1adae7ea-4884-432f-af44-c9aea3a48a3d',
)

REL_RSCI_PLACE_REL_TYPE_ID = REL_LINK_REL_TYPE_ID
REL_RSCI_PLACE_INVERSE_REL_TYPE_ID = REL_LINK_INVERSE_REL_TYPE_ID
# REL_RSCI_PLACE_NODEID = 'bda5889c-d376-11ef-a239-0275dc2ded29'
REL_RSCI_PLACE_NODEID = None

RSCI_PRODUCTION_TRANSACTION_ID = '8db59037-c1d1-49e8-927d-9d1fdfd33636'

RSCI_PRODUCTION_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_production',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'load_path': IMPORT_RAW_PROD_CSV,
    'mappings': [
        {
            'raw_col': 'rsci_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, RSCI_PRODUCTION_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'prod_label',
            'targ_table': 'production_',
            'stage_field_prefix': 'prod_1_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'production__label',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('production_type', JSONB, EVENT_TYPE_PRODUCTION_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_PRODUCTION_TRANSACTION_ID,),
            ],
            'related_resources': [
                {
                    'group_source_field': 'production_carried_out_by_',
                    'multi_value': True,
                    'targ_field': 'production_carried_out_by',
                    'source_field_from_uuid': 'rsci_uuid',
                    'source_field_to_uuid': 'manu_group_uuid',
                    'rel_type_id': REL_LINK_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_LINK_INVERSE_REL_TYPE_ID,
                    # 'rel_nodeid': PRODUCTION_NODE_ID,
                },
                {
                    'group_source_field': 'production_location_',
                    'targ_field': 'production_location',
                    'multi_value': True,
                    'source_field_from_uuid': 'rsci_uuid',
                    'source_field_to_uuid': 'place_uuid_1',
                    'rel_type_id': REL_RSCI_PLACE_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_RSCI_PLACE_INVERSE_REL_TYPE_ID,
                    # 'rel_nodeid': REL_RSCI_PLACE_NODEID,
                },
                {
                    'group_source_field': 'production_location_',
                    'targ_field': 'production_location',
                    'multi_value': True,
                    'source_field_from_uuid': 'rsci_uuid',
                    'source_field_to_uuid': 'place_uuid_2',
                    'rel_type_id': REL_RSCI_PLACE_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_RSCI_PLACE_INVERSE_REL_TYPE_ID,
                    # 'rel_nodeid': REL_RSCI_PLACE_NODEID,
                },
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'geo_point_1',
                    'targ_field': 'production_location_geo',
                    'data_type': JSONB,
                    'source_geojson': True,
                    'value_transform': copy_value,
                },
            ]
        },
        {
            'raw_col': 'READY_origination_date_edtf_1',
            'targ_table': 'production_time',
            'stage_field_prefix': 'pt_d1_',
            'value_transform': copy_value,
            'targ_field': 'production_time_edtf',
            'data_type': Text,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_PRODUCTION_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'prod_1_tileid',
                'targ_tile_field': 'production_',
            },
        },
        {
            'raw_col': 'READY_origination_date_edtf_2',
            'targ_table': 'production_time',
            'stage_field_prefix': 'pt_d2_',
            'value_transform': copy_value,
            'targ_field': 'production_time_edtf',
            'data_type': Text,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_PRODUCTION_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'prod_1_tileid',
                'targ_tile_field': 'production_',
            },
        },
        {
            'raw_col': 'READY_origination_date_edtf_3',
            'targ_table': 'production_time',
            'stage_field_prefix': 'pt_d3_',
            'value_transform': copy_value,
            'targ_field': 'production_time_edtf',
            'data_type': Text,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_PRODUCTION_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'prod_1_tileid',
                'targ_tile_field': 'production_',
            },
        },
        {
            'raw_col': 'origination_date_statement',
            'targ_table': 'production_statement',
            'stage_field_prefix': 'prod_statement_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'production_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'related_tileid': {
                'source_tile_field': 'prod_1_tileid',
                'targ_tile_field': 'production_',
            },
            'default_values': [
                ('production_statement_type', JSONB, STATEMENT_PRODUCER_DESC_LIST_ITEMS,),
                ('production_statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_PRODUCTION_TRANSACTION_ID,),
            ],
        },
        {
            'raw_col': 'READY_expiration_date_statement',
            'targ_table': 'production_statement',
            'stage_field_prefix': 'prod_exp_statement_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'production_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'related_tileid': {
                'source_tile_field': 'prod_1_tileid',
                'targ_tile_field': 'production_',
            },
            'default_values': [
                ('production_statement_type', JSONB, STATEMENT_EXPIRE_DATE_LIST_ITEMS,),
                ('production_statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_PRODUCTION_TRANSACTION_ID,),
            ],
        },
    ],
}


#---------------------------------#
#- RSCI CHEMICAL MATERIAL CONFIGS #
#---------------------------------#


# Name Types - Physical Thing list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce'
# pref_labels=['chemical name',]

def make_material_type_list_items(value):
    """Make list items list objects from material type pref_label values"""
    if not value:
        return None
    pref_labels = [str(value).strip()]
    # Material list_id='9a2ee63b-d696-4686-979b-994597790289'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=pref_labels,
        list_id='9a2ee63b-d696-4686-979b-994597790289',
    )

def make_attributes_nat_synth_list_items(value):
    """Make list items list objects from attributes (characteristics) pref_label values"""
    if not value:
        return None
    pref_labels = [str(value).strip()]
    # attributes (characteristics) list_id='9a61af90-9fd1-48fd-b5e1-e45bb8a6db5b'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=pref_labels,
        list_id='9a61af90-9fd1-48fd-b5e1-e45bb8a6db5b',
    )

NAME_PHYSICAL_THINGS_CHEMICAL_NAMES_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['chemical name',],
    list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce',
)

NAME_CHEMICAL_FORMULA_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['Chemical Formula',],
    list_id='c6b8b496-ff23-49e4-a001-464c1934eb47',
)

NAME_CAS_REGISTRY_NUMBER_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['CAS Registry Number (CAS RN®)',],
    list_id='70b5d993-8545-4a07-a105-4bbeb40f362a',
)


IMPORT_RAW_RSCI_CHEMICAL_MATERIAL_CSV = os.path.join(DATA_DIR, 'gci-all-chemical-material.csv')


RSCI_CHEMICAL_MATERIAL_TRANSACTION_ID = '5dc77295-e547-4a1a-897f-9f560454fc65'

RSCI_CHEMICAL_MATERIAL_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_chemical_material',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'load_path': IMPORT_RAW_RSCI_CHEMICAL_MATERIAL_CSV,
    'mappings': [
        {
            'raw_col': 'rsci_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, RSCI_CHEMICAL_MATERIAL_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'READY_sample_type',
            'targ_table': 'chemical_material',
            'stage_field_prefix': 'chem_',
            'value_transform': make_material_type_list_items,
            'targ_field': 'chemical_material',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_CHEMICAL_MATERIAL_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'READY_natural-synthetic',
            'targ_table': 'attribute_type',
            'stage_field_prefix': 'attrib_type_',
            'value_transform': make_attributes_nat_synth_list_items,
            'targ_field': 'attribute_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_CHEMICAL_MATERIAL_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'Chemical Name',
            'targ_table': 'chemical_material_name',
            'stage_field_prefix': 'chem_',
            'value_transform': copy_value,
            'targ_field': 'chemical_material_name_content',
            'data_type': Text,
            'make_tileid': True,
            'default_values': [
                ('chemical_material_name_type_', JSONB, NAME_PHYSICAL_THINGS_CHEMICAL_NAMES_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_CHEMICAL_MATERIAL_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'chem_tileid',
                'targ_tile_field': 'chemical_material',
            }, 
        },
        {
            'raw_col': 'Chemical (CAS) No.',
            'targ_table': 'chemical_material_identifier',
            'stage_field_prefix': 'chem_id_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'chemical_material_identifier_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('chemical_material_identifier_type', JSONB, NAME_CAS_REGISTRY_NUMBER_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_CHEMICAL_MATERIAL_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'chem_tileid',
                'targ_tile_field': 'chemical_material',
            }, 
        },
        {
            'raw_col': 'Chemical Formula',
            'targ_table': 'chemical_material_name',
            'stage_field_prefix': 'chem_form_',
            'value_transform': copy_value,
            'targ_field': 'chemical_material_name_content',
            'data_type': Text,
            'make_tileid': True,
            'default_values': [
                ('chemical_material_name_type_', JSONB, NAME_CHEMICAL_FORMULA_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_CHEMICAL_MATERIAL_TRANSACTION_ID,),
            ], 
            'related_tileid': {
                'source_tile_field': 'chem_tileid',
                'targ_tile_field': 'chemical_material',
            },
        },
    ],
}



#---------------------------------#
#- RSCI COLORS CONFIGS -----------#
#---------------------------------#

# NOTE: These are deprecated
COLOR_CONCEPT_MAPPINGS_CSV = os.path.join(DATA_DIR, 'color_concept_mappings.csv')
IMPORT_RAW_RSCI_COLORS_CSV = os.path.join(DATA_DIR, 'gci-all-rsci-colors.csv')


def make_color_type_list_items(value):
    """Make list items list objects from color type pref_label values"""
    if not value:
        return None
    if not isinstance(value, list):
        raw_pref_labels = json.loads(value)
    else:
        raw_pref_labels = value
    raw_pref_labels = list(set(raw_pref_labels))
    pref_labels = []
    no_suffix_pref_labels = ['colorless', 'multicolored',]
    for label in raw_pref_labels:
        if not label in no_suffix_pref_labels:
            # add a suffix to the label
            label += ' (color)'
        pref_labels.append(label)
    # Color list_id='1008354b-3ac2-4a8c-88b8-6aad2302a916'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=pref_labels,
        list_id='1008354b-3ac2-4a8c-88b8-6aad2302a916',
    )

# Color Names list_id='080ddad2-56e9-4492-b59d-50a10ecdf26c'
COLOR_NAMES_LEGACY_COLOR_NAMES_TYPE_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['Legacy Color Name',],
    list_id='080ddad2-56e9-4492-b59d-50a10ecdf26c',
)

# Color Names list_id='080ddad2-56e9-4492-b59d-50a10ecdf26c'
COLOR_NAMES_COLOUR_INDEX_TYPE_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['Colour Index™ Generic Name',],
    list_id='080ddad2-56e9-4492-b59d-50a10ecdf26c',
)


RSCI_COLORS_TRANSACTION_ID = '595fde96-b315-484a-ac95-7febf9e36daa'

RSCI_COLORS_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_colors',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'load_path': IMPORT_RAW_RSCI_COLORS_CSV ,
    'mappings': [
        {
            'raw_col': 'rsci_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, RSCI_COLORS_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'color_pref_labels',
            'targ_table': 'has_color',
            'stage_field_prefix': 'has_color_',
            'value_transform': make_color_type_list_items,
            'targ_field': 'color_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_COLORS_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'Color',
            'targ_table': 'color_name',
            'stage_field_prefix': 'color_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'color_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('color_name_type', JSONB, COLOR_NAMES_LEGACY_COLOR_NAMES_TYPE_LIST_ITEMS,),
                ('color_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_COLORS_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'has_color_tileid',
                'targ_tile_field': 'has_color',
            },
        },
        {
            'raw_col': 'Color Index (CI) No. 1',
            'targ_table': 'color_name',
            'stage_field_prefix': 'color_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'color_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('color_name_type', JSONB, COLOR_NAMES_COLOUR_INDEX_TYPE_LIST_ITEMS,),
                ('color_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_COLORS_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'has_color_tileid',
                'targ_tile_field': 'has_color',
            },
        },
        {
            'raw_col': 'Color Index (CI) No. 2',
            'targ_table': 'color_name',
            'stage_field_prefix': 'color_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'color_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('color_name_type', JSONB, COLOR_NAMES_COLOUR_INDEX_TYPE_LIST_ITEMS,),
                ('color_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_COLORS_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'has_color_tileid',
                'targ_tile_field': 'has_color',
            },
        },
        {
            'raw_col': 'Color Index (CI) No. 3',
            'targ_table': 'color_name',
            'stage_field_prefix': 'color_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'color_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('color_name_type', JSONB, COLOR_NAMES_COLOUR_INDEX_TYPE_LIST_ITEMS,),
                ('color_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_COLORS_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'has_color_tileid',
                'targ_tile_field': 'has_color',
            },
        },
        {
            'raw_col': 'Color Index (CI) No. 4',
            'targ_table': 'color_name',
            'stage_field_prefix': 'color_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'color_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('color_name_type', JSONB, COLOR_NAMES_COLOUR_INDEX_TYPE_LIST_ITEMS,),
                ('color_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_COLORS_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'has_color_tileid',
                'targ_tile_field': 'has_color',
            },
        },
    ],
}


#---------------------------------#
#- RSCI CURRENT LOCATION CONFIGS -#
#---------------------------------#

GETTY_CENTER_PLACE_UUID = '52c75bc1-7797-4ef8-b829-7ce771e4968a'

IMPORT_RAW_RSCI_CURRENT_LOCATION_CSV = os.path.join(DATA_DIR, 'gci-all-rsci-current-location.csv')


RSCI_CURRENT_LOCATION_TRANSACTION_ID = '28225b33-2e29-42be-8613-22ad979071a9'

RSCI_CURRENT_LOCATION_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_current_location',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'load_path': IMPORT_RAW_RSCI_CURRENT_LOCATION_CSV,
    'mappings': [
        {
            'raw_col': 'rsci_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, RSCI_CURRENT_LOCATION_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'current_location_place_dict',
            'targ_table': 'current_location',
            'stage_field_prefix': 'cur_loc_',
            'value_transform': copy_value,
            'targ_field': 'current_location',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('current_location_statement_type', JSONB, PLACE_STATEMENT_TYPE_LIST_ITEMS,),
                ('current_location_statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_CURRENT_LOCATION_TRANSACTION_ID,),
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'current_location_statement',
                    'targ_field': 'current_location_statement_content',
                    'data_type': JSONB,
                    'value_transform': make_lang_dict_value,
                },
            ],
        },
    ],
}




#---------------------------------#
#- RSCI DIMENSIONS CONFIGS       -#
#---------------------------------#
IMPORT_RAW_RSCI_DIMENSIONS_CSV = os.path.join(DATA_DIR, 'gci-all-clean-dimensions.csv')

def make_dimension_unit_list_items(value):
    """Make list items list objects from QUDT Units pref_label values"""
    if not value:
        return None
    # QUDT Units list_id='1737a1b8-263e-4876-aef6-95a471546d80'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=[value],
        list_id='1737a1b8-263e-4876-aef6-95a471546d80',
    )


def make_dimension_kind_list_items(value):
    """Make list items list objects from QUDT Quantity Kinds pref_label values"""
    if not value:
        return None
    # QUDT Units list_id='792e80fe-a092-43b5-9ccf-4dcd7bbcbe86'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=[value],
        list_id='792e80fe-a092-43b5-9ccf-4dcd7bbcbe86',
    )


RSCI_DIMENSIONS_TRANSACTION_ID = '4e1f24ce-b500-40f5-8367-0ec9c34d5dd4'

RSCI_DIMENSIONS_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_dimensions',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'load_path': IMPORT_RAW_RSCI_DIMENSIONS_CSV,
    'mappings': [
        {
            'raw_col': 'rsci_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, RSCI_DIMENSIONS_TRANSACTION_ID ,),
            ], 
        },
        {
            'raw_col': 'READY_dim1_type',
            'targ_table': 'dimension',
            'stage_field_prefix': 'dim1_',
            'value_transform': make_dimension_kind_list_items,
            'targ_field': 'dimension_type_',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
               ('transactionid', UUID, RSCI_DIMENSIONS_TRANSACTION_ID ,),
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_dim1_value',
                    'targ_field': 'dimension_value_',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'READY_dim1_lowestvalue',
                    'targ_field': 'dimension_lowest_possible_value',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'READY_dim1_highestvalue',
                    'targ_field': 'dimension_highest_possible_value',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'READY_dim1_unit',
                    'targ_field': 'dimension_unit',
                    'data_type': JSONB,
                    'value_transform': make_dimension_unit_list_items,
                },
                {
                    'raw_col': 'READY_dim1_note',
                    'targ_field': 'dimension_statement_content',
                    'data_type': JSONB,
                    'value_transform': make_lang_dict_value,
                },
            ], 
        },
        {
            'raw_col': 'READY_dim2_type',
            'targ_table': 'dimension',
            'stage_field_prefix': 'dim2_',
            'value_transform': make_dimension_kind_list_items,
            'targ_field': 'dimension_type_',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_DIMENSIONS_TRANSACTION_ID ,),
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_dim2_value',
                    'targ_field': 'dimension_value_',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                
                # these values are totally empty.
                #{
                #    'raw_col': 'READY_dim2_lowestvalue',
                #    'targ_field': 'dimension_lowest_possible_value',
                #    'data_type': Numeric,
                #    'value_transform': copy_numeric_value,
                #},
                #{
                #    'raw_col': 'READY_dim2_highestvalue',
                #    'targ_field': 'dimension_highest_possible_value',
                #    'data_type': Numeric,
                #    'value_transform': copy_numeric_value,
                #},
               
                {
                    'raw_col': 'READY_dim2_unit',
                    'targ_field': 'dimension_unit',
                    'data_type': JSONB,
                    'value_transform': make_dimension_unit_list_items,
                },
                # this is empty
                #{
                #    'raw_col': 'READY_dim2_note',
                #    'targ_field': 'dimension_statement_content',
                #    'data_type': JSONB,
                #    'value_transform': make_lang_dict_value,
                #},
            ], 
        },
    ],
}





#---------------------------------#
#- RSCI MATERIAL CONFIGS --------#
#---------------------------------#



# Name Types - Physical Thing list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce'
# pref_labels=['GBIF name',]
NAME_PHYSICAL_THINGS_GBIF_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['GBIF name',],
    list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce',
)

# Name Types - Physical Thing list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce'
# pref_labels=['component name',]
NAME_PHYSICAL_THINGS_COMPONENT_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['component name',],
    list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce',
)


# Statement Types - Physical Thing list_id='a16a4edc-c916-4293-af98-44d76ce6cba7'
# pref_labels=['biological taxonomy statement',]
STATEMENT_PHYS_THING_BIO_TAXON_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['biological taxonomy statement',],
    list_id='a16a4edc-c916-4293-af98-44d76ce6cba7',
)

STATEMENT_PHYS_THING_MATS_TECH_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['materials/technique description',],
    list_id='a16a4edc-c916-4293-af98-44d76ce6cba7',
)

STATEMENT_PHYS_THING_MIX_RATIO_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['mixture ratio',],
    list_id='a16a4edc-c916-4293-af98-44d76ce6cba7',
)

STATEMENT_PHYS_THING_MEASUREMENT_DESCRIPTION_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['measurement description',],
    list_id='a16a4edc-c916-4293-af98-44d76ce6cba7',
)



# Metatype list_id='c82d6f85-ae67-4c28-b07a-c114f6d6ba50'
# pref_labels=['material components',]
METATYPE_MATERIAL_COMPONENTS_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['material components',],
    list_id='c82d6f85-ae67-4c28-b07a-c114f6d6ba50',
)

# NOTE: These are deprecated.
CONCEPTS_MATERIALS_CSV = os.path.join(DATA_DIR, 'concepts_materials.csv')
CONCEPTS_MATERIALS_RDF = os.path.join(DATA_DIR, 'concepts_materials.rdf')
CONCEPTS_OBJECT_TYPE_CSV = os.path.join(DATA_DIR, 'concepts_object_type.csv')
CONCEPTS_OBJECT_TYPE_RDF = os.path.join(DATA_DIR, 'concepts_object_type.rdf')
NEW_CONCEPTS_OBJECT_TYPE_RDF = os.path.join(DATA_DIR, 'new_concepts_object_type.rdf')

IMPORT_RAW_RSCI_MATERIALS_OBJ_TYPE_CSV = os.path.join(DATA_DIR, 'gci-all-rsci-matterials.csv')


def make_mixture_type_list_items(value):
    """Make list items list objects from mixture type values"""
    if not value:
        return None
    # Mixture Type list_id='ce9c94e3-bb70-4e87-9fef-eb0c8a6ac1ac'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=[value,],
        list_id='ce9c94e3-bb70-4e87-9fef-eb0c8a6ac1ac',
    )

def make_attribute_type_list_items(value):
    """Make list items list objects from attribute_type values"""
    if not value:
        return None
    # attributes (characteristics) list_id='9a61af90-9fd1-48fd-b5e1-e45bb8a6db5b'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=[value,],
        list_id='9a61af90-9fd1-48fd-b5e1-e45bb8a6db5b',
    )

def make_sample_object_type_list_items(value):
    if not value:
        return None
    # Object Types - Physical Thing list_id='56991802-f539-4b22-b5a9-b1945fceb52b'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=['samples',],
        list_id='56991802-f539-4b22-b5a9-b1945fceb52b',
    )

def make_material_list_items(value):
    """Make list items list objects from material values"""
    if not value:
        return None
    # Materials list_id='9a2ee63b-d696-4686-979b-994597790289'
    if not isinstance(value, list):
        pref_labels = json.loads(value)
    else:
        pref_labels = value
    pref_labels = list(set(pref_labels))
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=pref_labels,
        list_id='9a2ee63b-d696-4686-979b-994597790289',
    )


def make_part_type_list_items(value):
    if not value:
        return None
    value = value.strip()
    # Object Part Function Types list_id='73a6d800-669d-48b7-a1fb-2ecdaa0a4827'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=[value,],
        list_id='73a6d800-669d-48b7-a1fb-2ecdaa0a4827',
    )


REMOVAL_REL_PROPERY_ID = 'ac41d9be-79db-4256-b368-2f4559cfbe55'
REMOVAL_INV_REL_PROPERY_ID = 'ac41d9be-79db-4256-b368-2f4559cfbe55'

RSCI_MATERIALS_TYPES_TRANSACTION_ID = '42feb182-0a6e-445e-85c5-8f5839fd687f'

RSCI_MATERIALS_TYPES_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_materials_types',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'load_path': IMPORT_RAW_RSCI_MATERIALS_OBJ_TYPE_CSV,
    'mappings': [
        {
            'raw_col': 'rsci_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'gbif_canonical_name',
            'targ_table': 'name',
            'stage_field_prefix': 'gbif_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type_', JSONB, NAME_PHYSICAL_THINGS_GBIF_LIST_ITEMS,),
                ('name_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'gbif_statement',
            'targ_table': 'statement',
            'stage_field_prefix': 'gbif_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('statement_type', JSONB, STATEMENT_PHYS_THING_BIO_TAXON_LIST_ITEMS,),
                ('statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ], 
        },
        {

            'raw_col': 'rsci_uuid',
            'targ_table': 'object_type',
            'stage_field_prefix': 'object_type_',
            'value_transform': make_sample_object_type_list_items,  # Note a specific function for this.
            'targ_field': 'object_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'materials_pref_labels',
            'targ_table': 'material',
            'stage_field_prefix': 'mat_',
            'value_transform': make_material_list_items,
            'targ_field': 'material',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'READY_mixture_type',
            'targ_table': 'mixture',
            'stage_field_prefix': 'mix_',
            'value_transform': make_mixture_type_list_items, # Note a specific function for this.
            'targ_field': 'mixture_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('mixture_type_metatype', JSONB, METATYPE_MATERIAL_COMPONENTS_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'READY_mixture_note_technique',
            'targ_table': 'mixture_statement',
            'stage_field_prefix': 'mix_state_tech_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'mixture_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('mixture_statement_type', JSONB, STATEMENT_PHYS_THING_MATS_TECH_LIST_ITEMS,),
                ('mixture_statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'mix_tileid',
                'targ_tile_field': 'mixture',
            },
        },
        {
            'raw_col': 'READY_mixture_note_ratio',
            'targ_table': 'mixture_statement',
            'stage_field_prefix': 'mix_state_ratio_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'mixture_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('mixture_statement_type', JSONB, STATEMENT_PHYS_THING_MIX_RATIO_LIST_ITEMS,),
                ('mixture_statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'mix_tileid',
                'targ_tile_field': 'mixture',
            },
        },

        # Pigment parts / component data
        {
            'raw_col': 'READY_comp_pigment_name',
            'targ_table': 'has_part',
            'stage_field_prefix': 'part_pig_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'part_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_name_type', JSONB, NAME_PHYSICAL_THINGS_COMPONENT_LIST_ITEMS,),
                ('part_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'mix_tileid',
                'targ_tile_field': 'mixture',
            },
            'related_resources': [
                {
                    'group_source_field': 'part_removed_from_',
                    'multi_value': True,
                    'targ_field': 'part_removed_from',
                    'source_field_from_uuid': 'rsci_uuid',
                    'source_field_to_uuid': 'READY_comp_pigment_INCOLLECTION',
                    'rel_type_id': REMOVAL_REL_PROPERY_ID,
                    'inverse_rel_type_id': REMOVAL_INV_REL_PROPERY_ID,
                },
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_comp_pigement_type',
                    'targ_field': 'part_type',
                    'data_type': JSONB,
                    'value_transform': make_part_type_list_items,
                },
            ], 
        },
        {
            'raw_col': 'READY_comp_pigment_dim_note',
            'targ_table': 'part_dimension',
            'stage_field_prefix': 'part_pig_dim_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'part_dimension_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_dimension_statement_type', JSONB, STATEMENT_PHYS_THING_MEASUREMENT_DESCRIPTION_LIST_ITEMS),
                ('part_dimension_statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'part_pig_tileid',
                'targ_tile_field': 'has_part',
            },
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_comp_pigment_dim_value',
                    'targ_field': 'part_dimension_value_',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'READY_comp_pigment_dim_unit',
                    'targ_field': 'part_dimension_unit',
                    'data_type': JSONB,
                    'value_transform': make_dimension_unit_list_items,
                },
                {
                    'raw_col': 'READY_comp_pigment_dim_type',
                    'targ_field': 'part_dimension_type_',
                    'data_type': JSONB,
                    'value_transform': make_dimension_kind_list_items,
                },
            ], 
        },

        # Base Mix parts / component data
        {
            'raw_col': 'READY_comp_basemix_name',
            'targ_table': 'has_part',
            'stage_field_prefix': 'part_base_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'part_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_name_type', JSONB, NAME_PHYSICAL_THINGS_COMPONENT_LIST_ITEMS,),
                ('part_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'mix_tileid',
                'targ_tile_field': 'mixture',
            },
            'related_resources': [
                {
                    'group_source_field': 'part_removed_from_',
                    'multi_value': True,
                    'targ_field': 'part_removed_from',
                    'source_field_from_uuid': 'rsci_uuid',
                    'source_field_to_uuid': 'READY_comp_basemix_INCOLLECTION',
                    'rel_type_id': REMOVAL_REL_PROPERY_ID,
                    'inverse_rel_type_id': REMOVAL_INV_REL_PROPERY_ID,
                },
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_comp_basemix_type',
                    'targ_field': 'part_type',
                    'data_type': JSONB,
                    'value_transform': make_part_type_list_items,
                },
            ], 
        },
        {
            'raw_col': 'READY_comp_basemix_dim_note',
            'targ_table': 'part_dimension',
            'stage_field_prefix': 'part_base_dim_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'part_dimension_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_dimension_statement_type', JSONB, STATEMENT_PHYS_THING_MEASUREMENT_DESCRIPTION_LIST_ITEMS),
                ('part_dimension_statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'part_base_tileid',
                'targ_tile_field': 'has_part',
            },
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_comp_basemix_dim_value',
                    'targ_field': 'part_dimension_value_',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'READY_comp_basemix_dim_unit',
                    'targ_field': 'part_dimension_unit',
                    'data_type': JSONB,
                    'value_transform': make_dimension_unit_list_items,
                },
                {
                    'raw_col': 'READY_comp_basemix_dim_type',
                    'targ_field': 'part_dimension_type_',
                    'data_type': JSONB,
                    'value_transform': make_dimension_kind_list_items,
                },
            ], 
        },

        # Comp1 parts / component data
        {
            'raw_col': 'READY_comp1_name',
            'targ_table': 'has_part',
            'stage_field_prefix': 'part_comp1_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'part_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_name_type', JSONB, NAME_PHYSICAL_THINGS_COMPONENT_LIST_ITEMS,),
                ('part_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'mix_tileid',
                'targ_tile_field': 'mixture',
            },
            'related_resources': [
                {
                    'group_source_field': 'part_removed_from_',
                    'multi_value': True,
                    'targ_field': 'part_removed_from',
                    'source_field_from_uuid': 'rsci_uuid',
                    'source_field_to_uuid': 'READY_comp1_INCOLLECTION',
                    'rel_type_id': REMOVAL_REL_PROPERY_ID,
                    'inverse_rel_type_id': REMOVAL_INV_REL_PROPERY_ID,
                },
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_comp1_type',
                    'targ_field': 'part_type',
                    'data_type': JSONB,
                    'value_transform': make_part_type_list_items,
                },
            ], 
        },
        {
            'raw_col': 'READY_comp1_dim_note',
            'targ_table': 'part_dimension',
            'stage_field_prefix': 'part_comp1_dim_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'part_dimension_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_dimension_statement_type', JSONB, STATEMENT_PHYS_THING_MEASUREMENT_DESCRIPTION_LIST_ITEMS),
                ('part_dimension_statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'part_comp1_tileid',
                'targ_tile_field': 'has_part',
            },
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_comp1_dim_value',
                    'targ_field': 'part_dimension_value_',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'READY_comp1_dim_unit',
                    'targ_field': 'part_dimension_unit',
                    'data_type': JSONB,
                    'value_transform': make_dimension_unit_list_items,
                },
                {
                    'raw_col': 'READY_comp1_dim_type',
                    'targ_field': 'part_dimension_type_',
                    'data_type': JSONB,
                    'value_transform': make_dimension_kind_list_items,
                },
            ], 
        },

        # Comp2 parts / component data
        {
            'raw_col': 'READY_comp2_name',
            'targ_table': 'has_part',
            'stage_field_prefix': 'part_comp2_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'part_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_name_type', JSONB, NAME_PHYSICAL_THINGS_COMPONENT_LIST_ITEMS,),
                ('part_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'mix_tileid',
                'targ_tile_field': 'mixture',
            },
            'related_resources': [
                {
                    'group_source_field': 'part_removed_from_',
                    'multi_value': True,
                    'targ_field': 'part_removed_from',
                    'source_field_from_uuid': 'rsci_uuid',
                    'source_field_to_uuid': 'READY_comp2_INCOLLECTION',
                    'rel_type_id': REMOVAL_REL_PROPERY_ID,
                    'inverse_rel_type_id': REMOVAL_INV_REL_PROPERY_ID,
                },
            ],
            'skip_tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_comp2_type',
                    'targ_field': 'part_type',
                    'data_type': JSONB,
                    'value_transform': make_part_type_list_items,
                },
            ], 
        },
        {
            'raw_col': 'READY_comp2_dim_note',
            'targ_table': 'part_dimension',
            'stage_field_prefix': 'part_comp2_dim_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'part_dimension_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_dimension_statement_type', JSONB, STATEMENT_PHYS_THING_MEASUREMENT_DESCRIPTION_LIST_ITEMS),
                ('part_dimension_statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'part_comp2_tileid',
                'targ_tile_field': 'has_part',
            },
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_comp2_dim_value',
                    'targ_field': 'part_dimension_value_',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'READY_comp2_dim_unit',
                    'targ_field': 'part_dimension_unit',
                    'data_type': JSONB,
                    'value_transform': make_dimension_unit_list_items,
                },
                {
                    'raw_col': 'READY_comp2_dim_type',
                    'targ_field': 'part_dimension_type_',
                    'data_type': JSONB,
                    'value_transform': make_dimension_kind_list_items,
                },
            ], 
        },

        # Comp3 parts / component data
        {
            'raw_col': 'READY_comp3_name',
            'targ_table': 'has_part',
            'stage_field_prefix': 'part_comp3_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'part_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_name_type', JSONB, NAME_PHYSICAL_THINGS_COMPONENT_LIST_ITEMS,),
                ('part_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'mix_tileid',
                'targ_tile_field': 'mixture',
            },
            'related_resources': [
                {
                    'group_source_field': 'part_removed_from_',
                    'multi_value': True,
                    'targ_field': 'part_removed_from',
                    'source_field_from_uuid': 'rsci_uuid',
                    'source_field_to_uuid': 'READY_comp3_INCOLLECTION',
                    'rel_type_id': REMOVAL_REL_PROPERY_ID,
                    'inverse_rel_type_id': REMOVAL_INV_REL_PROPERY_ID,
                },
            ],
            'skip_tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_comp3_type',
                    'targ_field': 'part_type',
                    'data_type': JSONB,
                    'value_transform': make_part_type_list_items,
                },
            ], 
        },
        {
            'raw_col': 'READY_comp3_dim_note',
            'targ_table': 'part_dimension',
            'stage_field_prefix': 'part_comp3_dim_',
            'value_transform': make_lang_dict_value, # Note a specific function for this.
            'targ_field': 'part_dimension_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_dimension_statement_type', JSONB, STATEMENT_PHYS_THING_MEASUREMENT_DESCRIPTION_LIST_ITEMS),
                ('part_dimension_statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_MATERIALS_TYPES_TRANSACTION_ID,),
            ],
            'related_tileid': {
                'source_tile_field': 'part_comp3_tileid',
                'targ_tile_field': 'has_part',
            },
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'READY_comp3_dim_value',
                    'targ_field': 'part_dimension_value_',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'READY_comp3_dim_unit',
                    'targ_field': 'part_dimension_unit',
                    'data_type': JSONB,
                    'value_transform': make_dimension_unit_list_items,
                },
                {
                    'raw_col': 'READY_comp3_dim_type',
                    'targ_field': 'part_dimension_type_',
                    'data_type': JSONB,
                    'value_transform': make_dimension_kind_list_items,
                },
            ], 
        },
    ],
}


# Name Types - Physical Thing list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce'
# pref_labels=['chemical name',]
NAME_TYPES_PHYSICAL_THING_CHEMICAL_NAME_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['chemical name',],
    list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce',
)

# Identifier Types - Physical Thing list_id='f4a87a34-8288-4cec-8e20-b28bd567ce0a'
# pref_labels=['Old Barcode',]
ID_TYPE_PHYSICAL_THING_OLD_BARCODE_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['Old Barcode',],
    list_id='f4a87a34-8288-4cec-8e20-b28bd567ce0a',
)

# Identifier Types - Physical Thing list_id='f4a87a34-8288-4cec-8e20-b28bd567ce0a'
# pref_labels=['Manufacturer Catalog Identifier (ID)',]
ID_TYPE_PHYSICAL_THING_MANUFACTURER_CATALOG_ID_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['Manufacturer Catalog Identifier (ID)',],
    list_id='f4a87a34-8288-4cec-8e20-b28bd567ce0a',
)

# Chemical Material Identifiers list_id='70b5d993-8545-4a07-a105-4bbeb40f362a'
# pref_labels=['CAS Registry Number (CAS RN®)',]
CHEMICAL_MATERIAL_IDS_CAS_REGISTRY_NUMBER_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['CAS Registry Number (CAS RN®)',],
    list_id='70b5d993-8545-4a07-a105-4bbeb40f362a',
)


# Identifier Types - Physical Thing list_id='993b2172-c101-46a8-9a56-7d2632ee5f89'
# pref_labels=['Certified Standard',]
STANDARD_TYPE_CERTIFIED_STANDARD_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['Certified Standard',],
    list_id='993b2172-c101-46a8-9a56-7d2632ee5f89',
)

# Identifier Types - Physical Thing list_id='993b2172-c101-46a8-9a56-7d2632ee5f89'
# pref_labels=['Not Standard',]
STANDARD_TYPE_CERTIFIED_NOT_STANDARD_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['Not Standard',],
    list_id='993b2172-c101-46a8-9a56-7d2632ee5f89',
)

def make_standard_type_list_items(value):
    """Make standard type list items for the 'Certified Standard' column. """
    if value and str(value) != 'nan':
        # Not empty, so it's a certified standard
        return STANDARD_TYPE_CERTIFIED_STANDARD_LIST_ITEMS
    return STANDARD_TYPE_CERTIFIED_NOT_STANDARD_LIST_ITEMS


RSCI_OTHER_IDS_TRANSACTION_ID = '8e3c0253-85a2-484a-9694-0bd309810b76'

RSCI_OTHER_IDS_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_other_ids',
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
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, RSCI_OTHER_IDS_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'Old Barcode',
            'targ_table': 'identifier',
            'stage_field_prefix': 'old_barcode_no_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'identifier_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('identifier_type', JSONB, ID_TYPE_PHYSICAL_THING_OLD_BARCODE_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_OTHER_IDS_TRANSACTION_ID,),
            ],
        },
        {
            'raw_col': 'Catalog No.',
            'targ_table': 'identifier',
            'stage_field_prefix': 'cat_id_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'identifier_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('identifier_type', JSONB, ID_TYPE_PHYSICAL_THING_MANUFACTURER_CATALOG_ID_LIST_ITEMS,),
                ('transactionid', UUID, RSCI_OTHER_IDS_TRANSACTION_ID,),
            ],
        },
        {
            'raw_col': 'Certified Standard?',
            'targ_table': 'standard_type',
            'stage_field_prefix': 'standard_type_',
            'value_transform': make_standard_type_list_items,
            'targ_field': 'standard_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, RSCI_OTHER_IDS_TRANSACTION_ID,),
            ],
        },
    ],
}







# FIELD MAPPINGS
IMPORT_FIELD_PLACES_CSV = os.path.join(DATA_DIR, 'gci-field-places.csv')

FIELD_PLACE_MAPPING_CONFIGS = {
    'model_id': PLACE_MODEL_UUID,
    'staging_table': f'etl_field_{PLACE_MODEL_NAME}',
    'model_staging_schema': PLACE_MODEL_NAME,
    'raw_pk_col': 'place_uuid',
    'load_path': IMPORT_FIELD_PLACES_CSV,
    'mappings': [
        {
            'raw_col': 'place_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, PLACE_MODEL_UUID,),
                ('graphpublicationid', UUID, 'a3e9145b-ba55-4793-a0fa-189e0f404ca7',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, FIELD_SAMPLES_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'specific_place',
            'targ_table': 'name',
            'stage_field_prefix': 'specific_place_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type', JSONB, NAME_TYPES_GENERIC_PREFERRED_LIST_ITEMS,),
                ('name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, FIELD_SAMPLES_TRANSACTION_ID,),
            ],
        },
        {
            'raw_col': 'statement',
            'targ_table': 'statement',
            'stage_field_prefix': 'statement_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('statement_type_metatype', JSONB, METATYPE_BRIEF_TEXT_LIST_ITEMS),
                ('statement_type', JSONB,  PLACE_STATEMENT_TYPE_LIST_ITEMS,),
                ('statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, FIELD_SAMPLES_TRANSACTION_ID,),
            ],
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
                ('transactionid', UUID, FIELD_SAMPLES_TRANSACTION_ID,),
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
                ('transactionid', UUID, FIELD_SAMPLES_TRANSACTION_ID,),
            ], 
        },
    ],
}



IMPORT_FIELD_RSCI_CSV = os.path.join(DATA_DIR, 'gci-field-all.csv')

ID_TYPE_PHYSICAL_THING_INTERNAL_ID_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['internal identifier',],
    list_id='f4a87a34-8288-4cec-8e20-b28bd567ce0a',
)

FACET_TYPE_BUILDING_MATERIALS_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['building materials',],
    list_id='44984237-b8fc-49c1-a94d-06fc8e38c48e',
)

def set_building_materials(value, default=FACET_TYPE_BUILDING_MATERIALS_LIST_ITEMS):
    if not value:
        return None
    return default



FIELD_RSCI_MAPPING_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_field_rsci',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'sample_uuid',
    'load_path': IMPORT_FIELD_RSCI_CSV,
    'mappings': [
        {
            'raw_col': 'sample_uuid',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, FIELD_SAMPLES_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'field_name',
            'targ_table': 'name',
            'stage_field_prefix': 'field_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type_', JSONB, NAME_TYPES_PHYSICAL_THING_PRIMARY_LIST_ITEMS,),
                ('name_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('transactionid', UUID, FIELD_SAMPLES_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'SampID',
            'targ_table': 'identifier',
            'stage_field_prefix': 'samp_id_id_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'identifier_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('identifier_type', JSONB, ID_TYPE_PHYSICAL_THING_INTERNAL_ID_LIST_ITEMS,),
                ('transactionid', UUID, FIELD_SAMPLES_TRANSACTION_ID,),
            ],
        },
        {
            # do this for each resourceinstance uuid
            'raw_col': 'sample_uuid',
            'targ_table': 'facet_type',
            'stage_field_prefix': '',
            'value_transform': set_building_materials,
            'targ_field': 'facet_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('facet_type_metatype', JSONB, METATYPE_FACET_TYPE_LIST_ITEMS,),
                ('transactionid', UUID, FIELD_SAMPLES_TRANSACTION_ID,),
            ],
        },
    ],
}


PHYS_THING_MODEL_ID = 'c08bae61-9726-47d8-9751-2dd04d20a7b4'
PHYS_THING_MODEL_NAME = 'physical_thing'
IMPORT_PHYS_THING_NAME_ID_CSV = os.path.join(DATA_DIR, 'gci-all-afs-physical-thing-name-identifier-object_type-current_location-current_owner-production-1.csv')
PHYS_THING_TRANSACTION_ID = '9dddda78-32b9-4a56-a293-ef08d0f700b2'


def make_language_list_items(value):
    if not value:
        return None
    value = value.strip()
    # Languages Types list_id='f7fc4f6d-fd46-4881-846f-4a08bc1a3fef'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=[value,],
        list_id='f7fc4f6d-fd46-4881-846f-4a08bc1a3fef',
    )

def make_statement_type_list_items(value):
    if not value:
        return None
    value = value.strip()
    # Languages Types list_id='1adae7ea-4884-432f-af44-c9aea3a48a3d'
    if ',' in value:
        raw_pref_labels = value.split(',')
    else:
        raw_pref_labels = [value]
    pref_labels = [v.strip() for v in raw_pref_labels]
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=pref_labels,
        list_id='1adae7ea-4884-432f-af44-c9aea3a48a3d',
    )

def phys_thing_transaction_id_for_non_blanks(value, transaction_id=PHYS_THING_TRANSACTION_ID):
    if not value:
        return None
    return transaction_id


def make_prod_type_list_items(value):
    if not value:
        return None
    value = value.strip()
    # Event Types - Production list_id='f0b85b97-f3ae-41dd-ac4e-6f1e249a9dbb'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=[value,],
        list_id='f0b85b97-f3ae-41dd-ac4e-6f1e249a9dbb',
    )


def make_prod_tech_list_items(value):
    if not value:
        return None
    value = value.strip()
    # Method Types - Production Technique list_id='1bb3db74-3dff-4e44-a629-7d6a0c1383e9'
    return get_controlled_list_objs_by_pref_labels(
        pref_labels=[value,],
        list_id='1bb3db74-3dff-4e44-a629-7d6a0c1383e9',
    )


AFS_PHYS_THING_NAME_ID_MAPPING_CONFIGS = {
    'model_id': PHYS_THING_MODEL_ID,
    'staging_table': 'etl_afs_phys_things',
    'model_staging_schema': PHYS_THING_MODEL_NAME,
    'raw_pk_col': 'resourceinstance_id',
    'load_path': IMPORT_PHYS_THING_NAME_ID_CSV,
    'mappings': [
        {
            'raw_col': 'resourceinstance_id',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, PHYS_THING_MODEL_ID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, PHYS_THING_TRANSACTION_ID,),
            ], 
        },
        {
            'raw_col': 'name__name_content',
            'targ_table': 'name',
            'stage_field_prefix': 'phys_things_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type_', JSONB, NAME_TYPES_PHYSICAL_THING_PRIMARY_LIST_ITEMS,),
                ('transactionid', UUID, PHYS_THING_TRANSACTION_ID,),
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'name__name_language_',
                    'targ_field': 'name_language_',
                    'data_type': JSONB,
                    'value_transform': make_language_list_items,
                },
            ],
        },
        {
            'raw_col': 'identifier__identifier_content',
            'targ_table': 'identifier',
            'stage_field_prefix': 'phys_things_id_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'identifier_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('identifier_type', JSONB, ID_TYPE_PHYSICAL_THING_INTERNAL_ID_LIST_ITEMS,),
                ('transactionid', UUID, PHYS_THING_TRANSACTION_ID,),
            ],
        },
        {
            # do this for each resourceinstance uuid
            'raw_col': 'object_type__object_type',
            'targ_table': 'object_type',
            'stage_field_prefix': 'phys_things_obj_type_',
            'value_transform': make_sample_object_type_list_items,
            'targ_field': 'object_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, PHYS_THING_TRANSACTION_ID,),
            ],
        },
        {
            # do this for each resourceinstance uuid
            'raw_col': 'resourceinstance_id',
            'targ_table': 'current_location',
            'stage_field_prefix': 'phys_things_cur_loc_',
            'value_transform': phys_thing_transaction_id_for_non_blanks,
            'targ_field': 'transactionid',
            'data_type': UUID,
            'make_tileid': True,
            'default_values': [
                # ('transactionid', UUID, PHYS_THING_TRANSACTION_ID,),
            ],
            'related_resources': [
                {
                    'targ_field': 'current_location',
                    'multi_value': True,
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'current_location__current_location',
                    'rel_type_id': REL_LINK_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_LINK_INVERSE_REL_TYPE_ID,
                },
            ]
        },
        {
            # do this for each resourceinstance uuid
            'raw_col': 'resourceinstance_id',
            'targ_table': 'current_owner',
            'stage_field_prefix': 'phys_things_cur_own_',
            'value_transform': phys_thing_transaction_id_for_non_blanks,
            'targ_field': 'transactionid',
            'data_type': UUID,
            'make_tileid': True,
            'default_values': [
                # ('transactionid', UUID, PHYS_THING_TRANSACTION_ID,),
            ],
            'related_resources': [
                {
                    'targ_field': 'current_owner',
                    'multi_value': True,
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'current_owner__current_owner',
                    'rel_type_id': REL_LINK_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_LINK_INVERSE_REL_TYPE_ID,
                },
            ]
        },
        {
            # do this for each resourceinstance uuid
            'raw_col': 'production__production_location_geo',
            'targ_table': 'production_',
            'stage_field_prefix': 'phys_things_prod_',
            'value_transform': copy_value,
            'targ_field': 'production_location_geo',
            'data_type': JSONB,
            'make_tileid': True,
            'source_geojson': True,
            'default_values': [
                ('transactionid', UUID, PHYS_THING_TRANSACTION_ID,),
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'production__production_type',
                    'targ_field': 'production_type',
                    'data_type': JSONB,
                    'value_transform': make_prod_type_list_items,
                },
                {
                    'raw_col': 'production__production_technique',
                    'targ_field': 'production_technique',
                    'data_type': JSONB,
                    'value_transform': make_prod_tech_list_items,
                },
            ],
            'related_resources': [
                {
                    'targ_field': 'production_location',
                    'multi_value': True,
                    'source_field_from_uuid': 'resourceinstanceid',
                    'source_field_to_uuid': 'production__production_location',
                    'rel_type_id': REL_LINK_REL_TYPE_ID,
                    'inverse_rel_type_id': REL_LINK_INVERSE_REL_TYPE_ID,
                },
            ]
        },
    ],
}


IMPORT_PHYS_THING_STATE_DIM_CSV = os.path.join(DATA_DIR, 'gci-all-afs-physical-thing-statement-dimension-2.csv')

AFS_PHYS_THING_STATE_DIM_MAPPING_CONFIGS = {
    'model_id': PHYS_THING_MODEL_ID,
    'staging_table': 'etl_afs_phys_things_state_dim',
    'model_staging_schema': PHYS_THING_MODEL_NAME,
    'raw_pk_col': 'row_num',
    'load_path': IMPORT_PHYS_THING_STATE_DIM_CSV,
    'mappings': [
        {
            'raw_col': 'resourceinstance_id',
            'targ_table': 'instances',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'resourceinstanceid',
            'data_type': UUID,
            'make_tileid': False,
            'do_distinct': True,
            'default_values': [
                ('graphid', UUID, PHYS_THING_MODEL_ID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
                ('transactionid', UUID, PHYS_THING_TRANSACTION_ID,),
            ], 
        },
        {
            # do this for each resourceinstance uuid
            'raw_col': 'statement__statement_content',
            'targ_table': 'statement',
            'stage_field_prefix': 'phys_things_state_cont_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, PHYS_THING_TRANSACTION_ID,),
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'statement__statement_type',
                    'targ_field': 'statement_type',
                    'data_type': JSONB,
                    'value_transform': make_statement_type_list_items,
                },
                {
                    'raw_col': 'statement__statement_language_',
                    'targ_field': 'statement_language_',
                    'data_type': JSONB,
                    'value_transform': make_language_list_items,
                },
            ],
        },
        {
            'raw_col': 'dimension__dimension_type_',
            'targ_table': 'dimension',
            'stage_field_prefix': 'phys_things_dim_',
            'value_transform': make_dimension_kind_list_items,
            'targ_field': 'dimension_type_',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('transactionid', UUID, PHYS_THING_TRANSACTION_ID,),
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'dimension__dimension_value_',
                    'targ_field': 'dimension_value_',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'dimension__dimension_lowest_possible_value',
                    'targ_field': 'dimension_lowest_possible_value',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'dimension__dimension_highest_possible_value',
                    'targ_field': 'dimension_highest_possible_value',
                    'data_type': Numeric,
                    'value_transform': copy_numeric_value,
                },
                {
                    'raw_col': 'dimension__dimension_unit',
                    'targ_field': 'dimension_unit',
                    'data_type': JSONB,
                    'value_transform': make_dimension_unit_list_items,
                },
            ], 
        },
    ],
    'tileid_unique_groups': {
        'phys_things_state_cont_tileid': [
            'phys_things_state_cont_statement_content',
            'phys_things_state_cont_statement_type',
        ],
        'phys_things_dim_tileid': [
            'phys_things_dim_dimension_type_',
            'phys_things_dim_dimension_value_',
        ],
    },
}








MAIN_ALL_MAPPING_CONFIGS = [
    # Create resource instances for different models
    RSCI_MAPPING_CONFIGS,
    PLACE_MAPPING_CONFIGS,
    GROUP_MAPPING_CONFIGS,
    PERSON_MAPPING_CONFIGS,
    SET_MAPPING_CONFIGS,

    RSCI_STATEMENTS_CONFIGS,
    RSCI_SAFETY_GROUP_MAPPINGS,

    RSCI_ACQUISITION_MAPPINGS,
    RSCI_PRODUCTION_CONFIGS,

    # Materials
    RSCI_CHEMICAL_MATERIAL_CONFIGS,

    # Colors
    RSCI_COLORS_CONFIGS,

    # Current location
    RSCI_CURRENT_LOCATION_CONFIGS,

    # Dimensions
    RSCI_DIMENSIONS_CONFIGS,

    RSCI_MATERIALS_TYPES_CONFIGS,

    RSCI_OTHER_IDS_CONFIGS,

    FIELD_PLACE_MAPPING_CONFIGS,
    FIELD_RSCI_MAPPING_CONFIGS,
]

ALL_MAPPING_CONFIGS = [
    # AFS physical things
    AFS_PHYS_THING_NAME_ID_MAPPING_CONFIGS,
    AFS_PHYS_THING_STATE_DIM_MAPPING_CONFIGS,
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
    SELECT __arches_create_resource_model_views('{SET_MODEL_UUID}');
    """,
    f"""
    SELECT __arches_create_resource_model_views('{PHYS_THING_MODEL_ID}');
    """,
]
