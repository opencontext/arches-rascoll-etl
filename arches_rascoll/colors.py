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
from arches_rascoll import colors


df_colors = colors.prepare_rsci_color_data()

"""

def prepare_rsci_color_data(
    df=None, 
    raw_path=general_configs.RAW_IMPORT_CSV,
    color_concept_mapping_path=general_configs.COLOR_CONCEPT_MAPPINGS_CSV,
    rsci_color_path=general_configs.IMPORT_RAW_RSCI_COLORS_CSV,
):
    """This prepares geospatial data specific to a given RSCI record."""
    if df is None:
        df = pd.read_csv(raw_path)
    df_map = pd.read_csv(color_concept_mapping_path)
    keep_cols = [
        'rsci_uuid', 
        'Color',
        'color_use',
        'color_split 1',
        'color_split 2',
        'color_split 3',
    ]
    df_colors = df[keep_cols].copy()
    # Add the uuirs for the part type for the 3 components.
    color_type_cols = [
        ('color_a_type_uuid', 'color_use',),
        ('color_b_type_uuid', 'color_split 1',),
        ('color_c_type_uuid', 'color_split 2',),
        ('color_d_type_uuid', 'color_split 2',),
    ]
    for col, not_null_col in color_type_cols:
        df_colors[col] = ''
        not_null_index = (
            ~df_colors[not_null_col].isnull()
            &(df_colors[not_null_col] != '')
            &(df_colors[not_null_col] != 'NaN')
            &(df_colors[not_null_col] != 'nan')
            &(df_colors[not_null_col] != '""')
        )
        for _, row in df_colors[not_null_index].iterrows():
            color = row[not_null_col]
            color = color.strip()
            color_index = (df_map['entry'].str.strip() == color)
            if df_map[color_index].empty:
                print(f'No color mapping found for {color}')
                continue
            pref_label = df_map.loc[color_index, 'pref_label'].values[0]
            result = concepts.get_concept_values_by_preflabel(pref_label)
            if not result:
                print(f'No concept found for {color} pref_label: {pref_label}')
                continue
            act_index = df_colors[not_null_col] == color
            df_colors.loc[act_index, col] = result['preflabel_valueid']
    # Now keep rows that have at least one color
    good_index = (
        (df_colors['Color'].notnull() & (df_colors['Color'] != ''))
        | (df_colors['color_a_type_uuid'].notnull() & (df_colors['color_a_type_uuid'] != ''))
        | (df_colors['color_b_type_uuid'].notnull() & (df_colors['color_b_type_uuid'] != ''))
        | (df_colors['color_c_type_uuid'].notnull() & (df_colors['color_c_type_uuid'] != ''))
        | (df_colors['color_d_type_uuid'].notnull() & (df_colors['color_d_type_uuid'] != ''))
    )
    df_colors = df_colors[good_index].copy()
    df_colors.to_csv(rsci_color_path, index=False)
    return df_colors