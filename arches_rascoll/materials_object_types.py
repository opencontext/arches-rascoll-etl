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
        'Sample Type',
        'Typical Use',
    ]
    df_rsci_mot = df[keep_cols].copy()
    df_rsci_mot['sample_type_value_uuid'] = ''
    df_rsci_mot['typical_use_value_uuid'] = ''
    col_vals = [
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
            if df_rsci_mot[act_index].empty:
                continue
            con_index = (df_con['Sample Type Entry'] == val)
            if df_con[con_index].empty:
                continue
            pref_label = df_con[con_index]['prefLabel'].values[0]
            df_rsci_mot.loc[act_index, pref_col] = pref_label
            con_dict = concepts.get_concept_values_by_preflabel(
                pref_label=pref_label,
            )
            if not con_dict:
                continue
            value_uuid = con_dict['preflabel_valueid']
            df_rsci_mot.loc[act_index, uuid_col] = str(value_uuid)
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