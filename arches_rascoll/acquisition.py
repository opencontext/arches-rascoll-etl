import json
import pandas as pd
import uuid as GenUUID

from arches_rascoll import general_configs


"""
from arches_rascoll import acquisition

df_a = acquisition.add_notes_to_acquisition_csv()

"""


def add_notes_to_acquisition_csv(
    raw_path=general_configs.RAW_IMPORT_CSV,
    acquisition_path=general_configs.IMPORT_RSCI_ACQUISITION_CSV,
    type_list_items=general_configs.EVENT_TPYPE_ACQUISITION_TRANSFER_LIST_ITEMS,
):
    """Add acquisition notes to the acquisition data table."""
    df = pd.read_csv(raw_path)
    df_a = pd.read_csv(acquisition_path)
    configs = [
        ('Acquired From (NOTE)', 'acquisition_note')
    ]
    for from_col, to_col in configs:
        df_a[to_col] = ''
        note_index = ~df[from_col].isnull()
        for i, row in df[note_index].iterrows():
            note = str(row[from_col])
            if len(note) <= 3 or note == 'nan':
                continue
            uuid = row['rsci_uuid']
            act_index = df_a['rsci_uuid'] == uuid
            df_a.loc[act_index, to_col] = note
            print(f'To uuid: {uuid}; note: {note}')
    df_a['acquisition_type'] = json.dumps(type_list_items)
    df_a.to_csv(acquisition_path, index=False)
    return df_a

