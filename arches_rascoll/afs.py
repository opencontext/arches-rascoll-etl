import json
import os
import pandas as pd
import uuid as GenUUID

from arches_rascoll import general_configs
from arches_rascoll import utilities

"""

from arches_rascoll import afs
afs.prepare_afs_csv_files()

"""


XSLX_TO_CSV_FILES_DICT = {
    'AfSdemo_PhysicalThing_20260813ale.xlsx': 'gci-all-afs-physical-thing.csv',
    'AfSdemo_DigitalResources_20260813ale.xlsx': 'gci-all-afs-digital-resources.csv',
    'AfSdemo_RaSColl_Item_20260813ale.xlsx': 'gci-all-afs-rsci.csv',
}


def make_wide_csv_from_excel(excel_filepath, csv_filepath):
    dfs = utilities.read_excel_to_dataframes(excel_filepath=excel_filepath)
    df_all = None
    for key, df in dfs.items():
        cols = df.columns.tolist()
        rename_cols = {c: f'{key}__{c}' for c in cols}
        for key_col in ['resourceinstance_id', 'resourceinstanceid']:
            rename_cols[key_col] = 'resourceinstance_id'
        df.rename(columns=rename_cols, inplace=True)
        if df_all is None:
            df_all = df.copy()
            continue
        print(f'Merge sheet df with columns: {df.columns.tolist()}')
        df_all = pd.merge(left=df_all, right=df, on='resourceinstance_id', how='outer')
    df_all.to_csv(csv_filepath, index=False)
    return df_all


def prepare_afs_csv_files():
    for excel_key, csv_file in XSLX_TO_CSV_FILES_DICT.items():
        excel_filepath = os.path.join(general_configs.DATA_DIR, excel_key)
        csv_filepath = os.path.join(general_configs.DATA_DIR, csv_file)
        _ = make_wide_csv_from_excel(excel_filepath, csv_filepath)
