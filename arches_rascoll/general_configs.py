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
RAW_IMPORT_CSV = os.path.join(DATA_DIR, 'gci-all-orig.csv')
ARCHES_INSERT_SQL_PATH =  os.path.join(DATA_DIR, 'etl_sql.txt')

STAGING_SCHEMA_NAME = 'staging'
IMPORT_TABLE_NAME = 'rsci'

# For this demo, we're using the AfRSC resource and sample collection resource model.
# Alter this as needed to fit your own
RSCI_UUID = 'd4e956f7-9fad-4fd2-94e3-563a3b2c3585'
RSCI_MODEL_NAME = 'reference_and_sample_collection_item'


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
                ('name_type_', JSONB, NAME_TYPES_PHYSICAL_THING_PRIMARY_LIST_ITEMS,),
                ('name_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # ('nodegroupid', UUID, 'bda409e0-d376-11ef-a239-0275dc2ded29',),
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
                ('name_type_', JSONB, NAME_TYPES_PHYSICAL_THING_ALTERNATE_LIST_ITEMS,),
                ('name_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # ('nodegroupid', UUID, 'bda409e0-d376-11ef-a239-0275dc2ded29',),
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
                # ('nodegroupid', UUID, 'bda3962c-d376-11ef-a239-0275dc2ded29',),
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
                # ('nodegroupid', UUID, 'e9b8d73c-09b7-11f0-b84f-0275dc2ded29',),
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
                # ('nodegroupid', UUID, '3ddab19c-d771-11ef-825b-0275dc2ded29',),
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
                # ('nodegroupid', UUID, '3ddac588-d771-11ef-825b-0275dc2ded29',),
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
                # ('nodegroupid', UUID, '3ddaa8e6-d771-11ef-825b-0275dc2ded29',),
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
                # ('nodegroupid', UUID, '3ddabeda-d771-11ef-825b-0275dc2ded29',),
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
            'default_values': [
                ('graphid', UUID, GROUP_MODEL_UUID,),
                ('graphpublicationid', UUID, '166c7b56-b36e-4cfa-9dc0-7d232f147d69',), # updated 2025-11-04
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
                ('name_type', JSONB, NAME_TYPES_GENERIC_PREFERRED_LIST_ITEMS,),
                ('name_language',  JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # ('nodegroupid', UUID, '3695cff8-d770-11ef-8f5d-0275dc2ded29',),
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
                ('name_type', JSONB, PERSON_FULL_NAME_LIST_ITEMS,),
                ('name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # ('nodegroupid', UUID, 'e1d0f244-d770-11ef-8c40-0275dc2ded29',),
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
                ('name_type', JSONB, NAME_TYPES_GENERIC_PREFERRED_LIST_ITEMS,),
                ('name_language',  JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # ('nodegroupid', UUID, 'da0ef9d8-d771-11ef-af99-0275dc2ded29',),
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
                ('statement_type', JSONB, STATEMENT_PHYS_THING_NOTE_LIST_ITEMS,),
                ('statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # # ('nodegroupid', UUID, 'bda499a0-d376-11ef-a239-0275dc2ded29',),
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
                ('statement_type', JSONB, STATEMENT_PHYS_THING_SAMPLE_DESCRIPTION_LIST_ITEMS,),
                ('statement_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # # ('nodegroupid', UUID, 'bda499a0-d376-11ef-a239-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, 'bda499a0-d376-11ef-a239-0275dc2ded29',),
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
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
            ], 
        },
        {
            'raw_col': 'all_safety',
            'targ_table': 'safety_classification',
            'stage_field_prefix': '',
            'value_transform': copy_value,
            'targ_field': 'nfpa_safety_classifcation_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('nfpa_safety_classification_classification', JSONB, SAFETY_CLASSIFICATION_NFPA_LIST_ITEMS,),
                # # ('nodegroupid', UUID, 'bda455bc-d376-11ef-a239-0275dc2ded29',),
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
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
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

REL_RSCI_PLACE_REL_TYPE_ID = REL_LINK_REL_TYPE_ID
REL_RSCI_PLACE_INVERSE_REL_TYPE_ID = REL_LINK_INVERSE_REL_TYPE_ID
# REL_RSCI_PLACE_NODEID = 'bda5889c-d376-11ef-a239-0275dc2ded29'
REL_RSCI_PLACE_NODEID = None


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
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
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
                # # ('nodegroupid', UUID, PRODUCTION_NODE_ID,),
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
            'raw_col': 'origin_date_edtf',
            'targ_table': 'production_time',
            'stage_field_prefix': 'pt_',
            'value_transform': copy_value,
            'targ_field': 'production_time_edtf',
            'data_type': Text,
            'make_tileid': True,
            'default_values': [
                # # ('nodegroupid', UUID, 'bda37764-d376-11ef-a239-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, 'bda36f9e-d376-11ef-a239-0275dc2ded29',),
            ],
        },
    ],
}


#---------------------------------#
#- RSCI CHEMICAL MATERIAL CONFIGS #
#---------------------------------#


# Name Types - Physical Thing list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce'
# pref_labels=['chemical name',]
NAME_PHYSICAL_THINGS_CHEMICAL_NAMES_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['chemical name',],
    list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce',
)

IMPORT_RAW_RSCI_CHEMICAL_MATERIAL_CSV = os.path.join(DATA_DIR, 'gci-all-chemical-material.csv')

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
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
            ], 
        },
        {
            'raw_col': 'Chemical Name',
            'targ_table': 'chemical_material',
            'stage_field_prefix': 'chem_',
            'value_transform': copy_value,
            'targ_field': 'chemical_material_name_content',
            'data_type': Text,
            'make_tileid': True,
            'default_values': [
                ('chemical_material_name_type_', JSONB, NAME_PHYSICAL_THINGS_CHEMICAL_NAMES_LIST_ITEMS,),
                # ('material_data_assignment_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # ('nodegroupid', UUID, 'bda409e0-d376-11ef-a239-0275dc2ded29',),
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

# Statement Types - Physical Thing list_id='a16a4edc-c916-4293-af98-44d76ce6cba7'
# pref_labels=['biological taxonomy statement',]
STATEMENT_PHYS_THING_BIO_TAXON_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['biological taxonomy statement',],
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

IMPORT_RAW_RSCI_MATERIALS_OBJ_TYPE_CSV = os.path.join(DATA_DIR, 'gci-all-rsci-matterials-object-types.csv')


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


RSCI_MATERIALS_TYPE_CONFIGS = {
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
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
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
                # ('nodegroupid', UUID, 'bda409e0-d376-11ef-a239-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, 'bda499a0-d376-11ef-a239-0275dc2ded29',),
            ], 
        },
        {
            'raw_col': 'mixture_type_pref_label',
            'targ_table': 'mixture_type',
            'stage_field_prefix': '',
            'value_transform': make_mixture_type_list_items, # Note a specific function for this.
            'targ_field': 'mixture_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('mixture_type_metatype', JSONB, METATYPE_MATERIAL_COMPONENTS_LIST_ITEMS,),
                # # ('nodegroupid', UUID, '53f96a86-0908-11f0-9f70-0275dc2ded29',),
            ], 
        },
        {
            'raw_col': 'attributes_pref_label',
            'targ_table': 'attribute_type',
            'stage_field_prefix': 'atribs_',
            'value_transform': make_attribute_type_list_items,  # Note a specific function for this.
            'targ_field': 'attribute_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                # # ('nodegroupid', UUID, '398111dc-0907-11f0-9e45-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, '398111dc-0907-11f0-9e45-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, 'bda47b64-d376-11ef-a239-0275dc2ded29',),
            ], 
        },
    ],
}


#---------------------------------#
#- RSCI COMPONENT CONFIGS --------#
#---------------------------------#

IMPORT_RAW_RSCI_COMPONENTS_CSV = os.path.join(DATA_DIR, 'gci-all-rsci-components.csv')

# Name Types - Physical Thing list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce'
# pref_labels=['component name',]
NAME_PHYSICAL_THINGS_COMPONENT_NAME_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['component name',],
    list_id='9f1cf9a8-ce65-455f-ab1e-a9b36e9e23ce',
)

# Object Part Types list_id='73a6d800-669d-48b7-a1fb-2ecdaa0a4827'
# pref_labels=['component name',]
OBJECT_PART_TYPES_COMPONENTS_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['components (objects parts)',],
    list_id='73a6d800-669d-48b7-a1fb-2ecdaa0a4827',
)

def make_object_part_components_list_items(value):
    if not value:
        return None
    # Object Types - Physical Thing list_id='56991802-f539-4b22-b5a9-b1945fceb52b'
    return OBJECT_PART_TYPES_COMPONENTS_LIST_ITEMS

# Statement Types - Physical Thing list_id='a16a4edc-c916-4293-af98-44d76ce6cba7'
# pref_labels=['materials/technique description',]
STATEMENT_PHYS_THING_MATERIALS_DESCRIPTION_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['materials/technique description',],
    list_id='a16a4edc-c916-4293-af98-44d76ce6cba7',
)

# Dimension Types - Physical Thing list_id='c9e838f8-7660-4701-821c-721dac98a10b'
# pref_labels=['weight (heaviness attribute)',]
# NOTE TODO: this should be mass
DIMENSION_TYPES_PHYSICAL_THING_MASS_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['weight (heaviness attribute)',],
    list_id='c9e838f8-7660-4701-821c-721dac98a10b',
)

# Dimension Types - Physical Units list_id='44295221-1215-4359-a079-234b317f65b7'
# pref_labels=['grams (measurements)',]
DIMENSION_TYPES_PHYSICAL_UNITS_GRAMS_LIST_ITEMS = get_controlled_list_objs_by_pref_labels(
    pref_labels=['grams (measurements)',],
    list_id='44295221-1215-4359-a079-234b317f65b7',
)

# NOTE: TODO Add "Mixture Pigment" to the component has part mappings and etl.
# The specific pigment won't be a controlled vocab, but will go intop the part name.

RSCI_COMPONENT_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_components',
    'model_staging_schema': RSCI_MODEL_NAME,
    'raw_pk_col': 'rsci_uuid',
    'load_path': IMPORT_RAW_RSCI_COMPONENTS_CSV,
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
            'raw_col': 'comp1_p',
            'targ_table': 'has_part',
            'stage_field_prefix': 'comp1_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'part_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_name_type', JSONB, NAME_PHYSICAL_THINGS_COMPONENT_NAME_LIST_ITEMS,),
                ('part_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('part_type', JSONB, OBJECT_PART_TYPES_COMPONENTS_LIST_ITEMS,),
                # # ('nodegroupid', UUID, '6ee83c9c-08e4-11f0-81c1-0275dc2ded29',),
            ],
        },
        {
            'raw_col': 'Component1',
            'targ_table': 'part_statement',
            'stage_field_prefix': 'comp1_statement_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'part_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_statement_type', JSONB, STATEMENT_PHYS_THING_MATERIALS_DESCRIPTION_LIST_ITEMS,),
                ('part_statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # # ('nodegroupid', UUID, '6ee83f62-08e4-11f0-81c1-0275dc2ded29',),
            ],
            'related_tileid': {
                'source_tile_field': 'comp1_tileid',
                'targ_tile_field': 'has_part',
            },
        },
        {
            'raw_col': 'comp1_n',
            'targ_table': 'part_dimension',
            'stage_field_prefix': 'comp1_n_',
            'value_transform': copy_value,
            'targ_field': 'part_dimension_value_',
            'data_type': Numeric,
            'make_tileid': True,
            'default_values': [
                ('part_dimension_type_', JSONB, DIMENSION_TYPES_PHYSICAL_THING_MASS_LIST_ITEMS,),
                ('part_dimension_unit', JSONB, DIMENSION_TYPES_PHYSICAL_UNITS_GRAMS_LIST_ITEMS,),
                # # ('nodegroupid', UUID, '6ee8420a-08e4-11f0-81c1-0275dc2ded29',),
            ],
            'related_tileid': {
                'source_tile_field': 'comp1_tileid',
                'targ_tile_field': 'has_part',
            },
        },
        # The component 2 mappings are the same as component 1
        {
            'raw_col': 'comp2_p',
            'targ_table': 'has_part',
            'stage_field_prefix': 'comp2_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'part_name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_name_type', JSONB, NAME_PHYSICAL_THINGS_COMPONENT_NAME_LIST_ITEMS,),
                ('part_name_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                ('part_type', JSONB, OBJECT_PART_TYPES_COMPONENTS_LIST_ITEMS,),
                # # ('nodegroupid', UUID, '6ee83c9c-08e4-11f0-81c1-0275dc2ded29',),
            ],
        },
        {
            'raw_col': 'Component2',
            'targ_table': 'part_statement',
            'stage_field_prefix': 'comp2_statement_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'part_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_statement_type', JSONB, STATEMENT_PHYS_THING_MATERIALS_DESCRIPTION_LIST_ITEMS,),
                ('part_statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # # ('nodegroupid', UUID, '6ee83f62-08e4-11f0-81c1-0275dc2ded29',),
            ],
            'related_tileid': {
                'source_tile_field': 'comp2_tileid',
                'targ_tile_field': 'has_part',
            },
        },
        {
            'raw_col': 'comp2_n',
            'targ_table': 'part_dimension',
            'stage_field_prefix': 'comp2_n_',
            'value_transform': copy_value,
            'targ_field': 'part_dimension_value_',
            'data_type': Numeric,
            'make_tileid': True,
            'default_values': [
                ('part_dimension_type_', JSONB, DIMENSION_TYPES_PHYSICAL_THING_MASS_LIST_ITEMS,),
                ('part_dimension_unit', JSONB, DIMENSION_TYPES_PHYSICAL_UNITS_GRAMS_LIST_ITEMS,),
                # # ('nodegroupid', UUID, '6ee8420a-08e4-11f0-81c1-0275dc2ded29',),
            ],
            'related_tileid': {
                'source_tile_field': 'comp2_tileid',
                'targ_tile_field': 'has_part',
            },
        },
         # The component 3 mappings don't have names or dimensions
        {
            'raw_col': 'comp3_part_type_uuid',
            'targ_table': 'has_part',
            'stage_field_prefix': 'comp3_',
            'value_transform': make_object_part_components_list_items,
            'targ_field': 'part_type',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                # # ('nodegroupid', UUID, '6ee83594-08e4-11f0-81c1-0275dc2ded29',),
            ], 
        },
        {
            'raw_col': 'Component3',
            'targ_table': 'part_statement',
            'stage_field_prefix': 'comp3_statement_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'part_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('part_statement_type', JSONB, STATEMENT_PHYS_THING_MATERIALS_DESCRIPTION_LIST_ITEMS,),
                ('part_statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # # ('nodegroupid', UUID, '6ee83f62-08e4-11f0-81c1-0275dc2ded29',),
            ],
            'related_tileid': {
                'source_tile_field': 'comp3_tileid',
                'targ_tile_field': 'has_part',
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
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
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
                # # ('nodegroupid', UUID, '6ee83594-08e4-11f0-81c1-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, '6ee83f62-08e4-11f0-81c1-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, '6ee83f62-08e4-11f0-81c1-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, '6ee83f62-08e4-11f0-81c1-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, '6ee83f62-08e4-11f0-81c1-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, '6ee83f62-08e4-11f0-81c1-0275dc2ded29',),
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
            'default_values': [
                ('graphid', UUID, RSCI_UUID,),
                ('graphpublicationid', UUID, 'a4ea5a7a-d7f0-11ef-a75a-0275dc2ded29',),
                ('principaluser_id', Integer, 1,),
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
                # # ('nodegroupid', UUID, 'bda4a954-d376-11ef-a239-0275dc2ded29',),
            ], 
        },
        {
            'raw_col': 'current_location_statement',
            'targ_table': 'current_location_statement',
            'stage_field_prefix': 'cur_loc_statement_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'current_location_statement_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('current_location_statement_type', JSONB, PLACE_STATEMENT_TYPE_LIST_ITEMS,),
                ('current_location_statement_language', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # # ('nodegroupid', UUID, 'c7ab9e8a-08e1-11f0-a3e8-0275dc2ded29',),
            ],
            'related_tileid': {
                'source_tile_field': 'cur_loc_tileid',
                'targ_tile_field': 'current_location',
            },
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


RSCI_CHEMICALS_CONFIGS = {
    'model_id': RSCI_UUID,
    'staging_table': 'etl_rsci_chemical_attributes',
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
            'raw_col': 'Old Barcode',
            'targ_table': 'identifier',
            'stage_field_prefix': 'old_barcode_no_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'identifier_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('identifier_type', JSONB, ID_TYPE_PHYSICAL_THING_OLD_BARCODE_LIST_ITEMS,),
                # # ('nodegroupid', UUID, 'bda3962c-d376-11ef-a239-0275dc2ded29',),
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
                # # ('nodegroupid', UUID, 'bda3962c-d376-11ef-a239-0275dc2ded29',),
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
                
            ],
        },
        {
            'raw_col': 'Chemical Name',
            'targ_table': 'name',
            'stage_field_prefix': 'chem_name_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'name_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('name_type_', JSONB, NAME_TYPES_PHYSICAL_THING_CHEMICAL_NAME_LIST_ITEMS,),
                ('name_language_', JSONB, LANGUAGES_ENGLISH_LIST_ITEMS,),
                # # ('nodegroupid', UUID, '6ee83c9c-08e4-11f0-81c1-0275dc2ded29',),
            ],
        },
        {
            'raw_col': 'Chemical (CAS) No.',
            'targ_table': 'chemical_material',
            'stage_field_prefix': 'chem_no_',
            'value_transform': make_lang_dict_value,
            'targ_field': 'chemical_material_identifier_content',
            'data_type': JSONB,
            'make_tileid': True,
            'default_values': [
                ('chemical_material_identifier_type', JSONB, CHEMICAL_MATERIAL_IDS_CAS_REGISTRY_NUMBER_LIST_ITEMS,),
            ],
            'tile_other_fields': [
                # Mappings for other fields to include in the same tile
                {
                    'raw_col': 'Chemical Formula',
                    'targ_field': 'chemical_material_name_content',
                    'data_type': Text,
                    'value_transform': copy_value,
                },
            ],
        },
    ],
}



ALL_MAPPING_CONFIGS = [
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
    RSCI_MATERIALS_TYPE_CONFIGS,
    RSCI_COMPONENT_CONFIGS,

    # Colors
    RSCI_COLORS_CONFIGS,

    # Current location
    RSCI_CURRENT_LOCATION_CONFIGS,

    RSCI_CHEMICALS_CONFIGS,
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
]
