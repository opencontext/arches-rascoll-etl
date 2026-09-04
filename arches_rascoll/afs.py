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
    'AfSdemo_PhysicalThing_20260813ale.xlsx': 'gci-all-afs-physical-thing',
    'AfSdemo_DigitalResources_20260813ale.xlsx': 'gci-all-afs-digital-resources',
    'AfSdemo_RaSColl_Item_20260813ale.xlsx': 'gci-all-afs-rsci',
}


def make_dfs_keyed_by_lens_from_excel(excel_filepath):
    dfs = utilities.read_excel_to_dataframes(excel_filepath=excel_filepath)
    dfs_keyed_by_lens = {}
    for key, df in dfs.items():
        df_len = len(df.index)
        if not df_len in dfs_keyed_by_lens:
            dfs_keyed_by_lens[df_len] = {
                'sheets': [],
                'dfs': {},
            }
        dfs_keyed_by_lens[df_len]['sheets'].append(key.strip('_'))
        dfs_keyed_by_lens[df_len]['dfs'][key] = df
    return dfs_keyed_by_lens


def make_wide_csvs_from_excel(excel_filepath, csv_file_prefix):
    dfs_keyed_by_lens = make_dfs_keyed_by_lens_from_excel(excel_filepath)
    df_alls = []
    for df_len, df_dict in dfs_keyed_by_lens.items():
        sheet_names = "-".join(df_dict["sheets"])
        csv_file_name = f'{csv_file_prefix}-{sheet_names}-{df_len}.csv'
        csv_filepath = os.path.join(general_configs.DATA_DIR, csv_file_name)
        df_all = None
        for key, df in df_dict['dfs'].items():
            cols = df.columns.tolist()
            rename_cols = {c: f'{key.strip("_")}__{c}' for c in cols}
            for key_col in ['resourceinstance_id', 'resourceinstanceid']:
                rename_cols[key_col] = 'resourceinstance_id'
            df.rename(columns=rename_cols, inplace=True)
            df.sort_values(by=['resourceinstance_id'], inplace=True)
            df.reset_index(drop=True, inplace=True)
            if df_all is None:
                df_all = df.copy()
                continue
            print(f'Merge sheet df with columns: {df.columns.tolist()}')
            # df_all = pd.merge(left=df_all, right=df, on='resourceinstance_id', how='left')
            # df_all = pd.merge(df_all, df, on='resourceinstance_id', how='inner')
            df_all.set_index('resourceinstance_id')
            df.set_index('resourceinstance_id')
            df_all = pd.concat([df_all, df], axis=1)
        # cols = df_all.columns.tolist()
        # df_all.drop_duplicates(subset=cols, inplace=True)
        df_all.reset_index(drop=True, inplace=True)
        df_all['row_num'] = df_all.index + 1
        df_all.reset_index(drop=True, inplace=True)
        df_all = df_all.loc[:,~df_all.columns.duplicated()].copy()
        print(f'Saving CSV {csv_filepath} with columns: {df_all.columns.tolist()}')
        df_all.to_csv(csv_filepath, index=False)
        df_alls.append(df_all)
    return df_alls


def prepare_afs_csv_files():
    for excel_key, csv_file_prefix in XSLX_TO_CSV_FILES_DICT.items():
        excel_filepath = os.path.join(general_configs.DATA_DIR, excel_key)
        _ = make_wide_csvs_from_excel(excel_filepath, csv_file_prefix)
