import json
import os
import pandas as pd
import uuid as GenUUID

from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import RDFS, SKOS, DCTERMS


from arches_rascoll import general_configs
from arches_rascoll import controlled_lists
from arches_rascoll import utilities

"""
from arches_rascoll import safety

df = safety.add_safety_list_items()

"""


def add_safety_list_items(
    safety_csv_path=general_configs.IMPORT_RSCI_GROUPS_SAFTEY_CSV,
):
    """Adds list item objects for safety"""
    df = pd.read_csv(safety_csv_path)
    configs = [
        ('Fire Safety', 'NFPA Flammability - ', '66fe06b4-9a61-4ee0-82cd-d33866e2be0c', 'nfpa_flammability', ),
        ('Health Safety', 'NFPA Health - ', '66fe06b4-9a61-4ee0-82cd-d33866e2be0c', 'nfpa_health', ),
        ('Other Safety', 'NFPA Special - ', '66fe06b4-9a61-4ee0-82cd-d33866e2be0c', 'nfpa_special', ),
        ('Reactivity Safety', 'NFPA Instability - ', '66fe06b4-9a61-4ee0-82cd-d33866e2be0c', 'nfpa_instability', ),
    ]
    safety_cols = []
    for col, prefix, list_id, new_col in configs:
        df[new_col] = ''
        safety_cols.append(new_col)
        act_index = ~df[col].isnull()
        for value in df[act_index][col].unique().tolist():
            if col == 'Other Safety':
                suffix = str(value).upper()
            else:
                suffix = str(int(value))
            value_index = df[col] == value
            act_value = f'{prefix}{suffix}'
            if col == 'Reactivity Safety' and suffix == '0':
                act_value = 'NFPA Instability - 0 '
            list_objs = general_configs.get_controlled_list_objs_by_pref_labels(
                pref_labels=[act_value,],
                list_id=list_id,
            )
            if not list_objs:
                print(f'Cannot find list objects "{act_value}"')
                if col == 'Reactivity Safety' and suffix == '0':
                    list_objs = controlled_lists.make_json_for_controlled_list_items_by_ids(
                        list_item_ids=['5888cb43-4233-4406-bf3d-329892321953',],
                        list_id=list_id,
                    )
            if not list_objs:
                continue
            list_objs_json = json.dumps(list_objs)
            df.loc[value_index, new_col] = list_objs_json
        if False:
            df[copy_col] = df[new_col]
            pass
    df['all_safety'] = ''
    for i, row in df.iterrows():
        safety_objs = []
        for col in safety_cols:
            if len(str(row[col])) < 4:
                continue
            col_objs = json.loads(row[col])
            safety_objs += col_objs
        safety_json = json.dumps(safety_objs)
        df.at[i, 'all_safety'] = safety_json
    df.to_csv(safety_csv_path, index=False)
    return df