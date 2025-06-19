import codecs
import copy
import datetime
import json
import os
import uuid as GenUUID

import numpy as np
import pandas as pd

from sqlalchemy.sql import text

from arches_rascoll import general_configs
from arches_rascoll import utilities

from arches_rascoll import general_configs

"""
# Add a column for the card name to the mapping summary table and reorder the columns.

from arches_rascoll import summarize
summarize.update_summary_table()
"""


MAPPING_SUMMARY_TABLE_FILE = 'gci-rascolls-mapping-summary.csv'
MAPPING_SUMMARY_TABLE_PATH = os.path.join(
    general_configs.DATA_DIR,
    MAPPING_SUMMARY_TABLE_FILE,
)

FIRST_COLS = [
    'Source Column',
]

LAST_COLS = [
    'Resource Model',
    'Target Table', 
    'Target Field', 
    'Model Staging Schema', 
    'Data Type',
    'Description', 
    'Additional Notes',
]

GRAPH_NAMES_DICT = {
    'reference_and_sample_collection_items': 'Reference and Sample Collection Item',
    'reference_and_sample_collection_item': 'Reference and Sample Collection Item',
    'place': 'Place',
    'person': 'Person',
    'group': 'Group',
    'provenance_activity': 'Provenance Activity',
}

def add_card_col_and_reorder_to_summary_table(
    df: pd.DataFrame,
    card_col: str = 'Card Name',
    first_cols: list = FIRST_COLS,
    last_cols: list = LAST_COLS,
):
    """
    Add a new column for the card name and reorder the DataFrame columns.
    """
    if card_col not in df.columns:
        df[card_col] = ''
    
    # Reorder columns
    cols = [c for c in first_cols if c in df.columns]
    cols.append(card_col)
    cols += [c for c in last_cols if c in df.columns]
    df = df[cols]
    return df


def update_summary_table(
    file_path: str = MAPPING_SUMMARY_TABLE_PATH,
):
    """
    Update the summary table with the provided summary data.
    """
    df = pd.read_csv(file_path)
    df = add_card_col_and_reorder_to_summary_table(df)
    for i, row in df.iterrows():
        if pd.isnull(row['Target Field']) or row['Target Field'] == "":
            continue
        graph_name = GRAPH_NAMES_DICT.get(row['Model Staging Schema'], row['Model Staging Schema'])
        card_result = utilities.get_card_data_for_node_in_graph(
            node_alias=row['Target Field'],
            graph_name=graph_name,
        )
        if not card_result:
            print(f"Card not found for {row['Target Field']} in {graph_name}")
            continue
        df.at[i, 'Card Name'] = card_result['card_name']
        df.at[i, 'Resource Model'] = graph_name
    df.to_csv(file_path, index=False)
