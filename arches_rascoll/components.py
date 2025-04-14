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
from arches_rascoll import components

df_rsci_comps = components.prepare_rsci_component_data()

"""

def prepare_rsci_component_data(
    df=None, 
    raw_path=general_configs.RAW_IMPORT_CSV,
    rsci_components_path=general_configs.IMPORT_RAW_RSCI_COMPONENTS_CSV,
):
    """This prepares geospatial data specific to a given RSCI record."""
    if df is None:
        df = pd.read_csv(raw_path)
    keep_cols = [
        'rsci_uuid', 
        'Component1',
        'comp1_n',
        'comp1_g',
        'comp1_p',
        'Component2',
        'comp2_n',
        'comp2_g',
        'comp2_p',
        'Component3',
    ]
    df_rsci_comps = df[keep_cols].copy()
    # Add the uuirs for the part type for the 3 components.
    part_type_cols = [
        ('comp1_part_type_uuid', ['Component1', 'comp1_n', 'comp1_p',],),
        ('comp2_part_type_uuid', ['Component2', 'comp2_n', 'comp2_p',],),
        ('comp3_part_type_uuid', ['Component3',],),
    ]
    for col, not_null_cols in part_type_cols:
        df_rsci_comps[col] = ''
        act_index = ~df_rsci_comps[not_null_cols[0]].isnull()
        if len(not_null_cols) > 1:
            for not_null_col in not_null_cols[1:]:
                act_index = act_index | ~df_rsci_comps[not_null_col].isnull()
        df_rsci_comps.loc[act_index, col] = general_configs.RSCI_PART_TYPE_VALUE_UUID
    good_index = (
        (df_rsci_comps['comp1_part_type_uuid'].notnull() & (df_rsci_comps['comp1_part_type_uuid'] != ''))
        | (df_rsci_comps['comp2_part_type_uuid'].notnull() & (df_rsci_comps['comp2_part_type_uuid'] != ''))
        | (df_rsci_comps['comp3_part_type_uuid'].notnull() & (df_rsci_comps['comp3_part_type_uuid'] != ''))
    )
    df_rsci_comps = df_rsci_comps[good_index].copy()
    df_rsci_comps.to_csv(rsci_components_path, index=False)
    return df_rsci_comps