import json
import os
import pandas as pd
import uuid as GenUUID

from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import RDFS, SKOS, DCTERMS


from arches_rascoll import general_configs
from arches_rascoll import concepts
from arches_rascoll import utilities


"""
# testing

from arches_rascoll import general_configs
from arches_rascoll import concepts
from arches_rascoll import materials_object_types

df_rsci_mot = materials_object_types.prepare_rsci_materials_object_types_data()

"""



MIXTURE_TYPE_PREF_LABELS_DICT = {
    'Inorganic - inorganic': 'inorganic - inorganic mixture',
    'Organic - inorganic': 'organic - inorganic mixture',
    'Organic - organic': 'organic - organic mixture',
    'Other (i.e. many components)': 'multi-component mixture',
}

NATURAL_SYNTHETIC_MAPPING_TO_PREF_LABELS_DICT = {
    'Natural': 'natural',
    'Synthetic': 'synthetic',
}


def prepare_rsci_materials_object_types_data(
    df=None, 
    raw_path=general_configs.RAW_IMPORT_CSV,
    object_types_csv_path=general_configs.CONCEPTS_OBJECT_TYPE_CSV,
    rsci_materials_object_types_path=general_configs.IMPORT_RAW_RSCI_MATERIALS_OBJ_TYPE_CSV,
):
    """This prepares geospatial data specific to a given RSCI record."""
    if df is None:
        df = pd.read_csv(raw_path)
    df_con = pd.read_csv(object_types_csv_path, encoding='utf-8')
    keep_cols = [
        'rsci_uuid', 
        'Chemical Name',
        'gbif_canonical_name',
        'gbif_uri',
        'Mixture Type',
        'Natural/Synthetic',
        'Sample Type',
        'Typical Use',
    ]
    df_rsci_mot = df[keep_cols].copy()
    # Add a GBIF statement column
    df_rsci_mot['gbif_statement'] = ''
    act_index = ~df_rsci_mot['gbif_uri'].isnull()
    df_rsci_mot.loc[act_index, 'gbif_statement'] = df_rsci_mot[act_index].apply(
        lambda row: f"{row['gbif_canonical_name']} ({row['gbif_uri']})",
        axis=1
    )
    # Now add columns for value UUIDs to concepts.
    df_rsci_mot['sample_type_value_uuid'] = ''
    df_rsci_mot['typical_use_value_uuid'] = ''
    col_vals = [
        ('mixture_type_value_uuid', 'mixture_type_pref_label', 'Mixture Type', ),
        ('attributes_value_uuid', 'attributes_pref_label', 'Natural/Synthetic', ),
        ('sample_type_value_uuid', 'sample_type_pref_label', 'Sample Type', ),
        ('typical_use_value_uuid', 'typical_use_pref_label', 'Typical Use', ),
    ]
    for uuid_col, pref_col, str_col in col_vals:
        df_rsci_mot[uuid_col] = ''
        df_rsci_mot[pref_col] = ''
        vals = df_rsci_mot[str_col].unique().tolist()
        if len(vals) == 0:
            continue
        for val in vals:
            act_index = df_rsci_mot[str_col] == val
            pref_label = None
            if df_rsci_mot[act_index].empty:
                continue
            if uuid_col == 'mixture_type_value_uuid':
                # get the pref_label from a configuration dictionary
                pref_label = MIXTURE_TYPE_PREF_LABELS_DICT.get(val)
            elif uuid_col == 'attributes_value_uuid':
                # get the pref_label from a configuration dictionary
                pref_label = NATURAL_SYNTHETIC_MAPPING_TO_PREF_LABELS_DICT.get(val)
            else:
                con_index = (df_con['Sample Type Entry'] == val)
                if df_con[con_index].empty:
                    continue
                pref_label = df_con[con_index]['prefLabel'].values[0]
            if pref_label is None:
                continue
            df_rsci_mot.loc[act_index, pref_col] = pref_label
            con_dict = concepts.get_concept_values_by_preflabel(
                pref_label=pref_label,
            )
            if not con_dict:
                continue
            value_uuid = con_dict['preflabel_valueid']
            df_rsci_mot.loc[act_index, uuid_col] = str(value_uuid)
    # Make a list of the mixture type and the attributes value UUIDs
    single_val_to_list_cols = [
        ('mixture_type_value_uuid', 'mixture_type_value_uuids', ),
        ('attributes_value_uuid', 'attributes_value_uuids', ),
    ]
    for uuid_col, list_col in single_val_to_list_cols:
        df_rsci_mot[list_col] = ''
        act_index = df_rsci_mot[uuid_col].str.len() > 0
        df_rsci_mot.loc[act_index, list_col] = df_rsci_mot[act_index].apply(
            lambda row: [row[uuid_col]],
            axis=1
        )
        act_index = ~(df_rsci_mot[list_col] == '')
        df_rsci_mot.loc[act_index, list_col] = df_rsci_mot[act_index].apply(
            lambda row: json.dumps(row[list_col]), 
            axis=1
        )
    # Combine the sample type and typical use value UUIDs into a list
    df_rsci_mot['object_type_value_uuids'] = ''
    act_index = (
        (df_rsci_mot['sample_type_value_uuid'].str.len() > 0)
        & (df_rsci_mot['typical_use_value_uuid'].str.len() > 0)
        & (df_rsci_mot['sample_type_value_uuid'] != df_rsci_mot['typical_use_value_uuid'])
    )
    df_rsci_mot.loc[act_index, 'object_type_value_uuids'] = df_rsci_mot[act_index].apply(
        lambda row: [row['sample_type_value_uuid'], row['typical_use_value_uuid']], 
        axis=1
    )
    act_index = (
        (df_rsci_mot['sample_type_value_uuid'].str.len() > 0)
        & (df_rsci_mot['sample_type_value_uuid'] == df_rsci_mot['typical_use_value_uuid'])
    )
    df_rsci_mot.loc[act_index, 'object_type_value_uuids'] = df_rsci_mot[act_index].apply(
        lambda row: [row['sample_type_value_uuid']], 
        axis=1
    )
    act_index = (
        (df_rsci_mot['sample_type_value_uuid'].str.len() > 0)
        & (df_rsci_mot['typical_use_value_uuid'].str.len() < 1)
    )
    df_rsci_mot.loc[act_index, 'object_type_value_uuids'] = df_rsci_mot[act_index].apply(
        lambda row: [row['sample_type_value_uuid']], 
        axis=1
    )
    act_index = (
        (df_rsci_mot['sample_type_value_uuid'].str.len() < 1)
        & (df_rsci_mot['typical_use_value_uuid'].str.len() > 0)
    )
    df_rsci_mot.loc[act_index, 'object_type_value_uuids'] = df_rsci_mot[act_index].apply(
        lambda row: [row['typical_use_value_uuid']], 
        axis=1
    )
    act_index = ~(df_rsci_mot['object_type_value_uuids'] == '')
    df_rsci_mot.loc[act_index, 'object_type_value_uuids'] = df_rsci_mot[act_index].apply(
        lambda row: json.dumps(row['object_type_value_uuids']), 
        axis=1
    )
    df_rsci_mot.to_csv(rsci_materials_object_types_path, index=False)
    return df_rsci_mot