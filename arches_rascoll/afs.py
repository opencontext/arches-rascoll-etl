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


FILE_UUID_DICT = {
    'Biolk_XRF__001.txt': '37d8bc63-dd9c-4bf6-90cd-41e947532537',
    'Biolk_XRF__005.txt': 'a1d5ce40-7cb4-452c-afa6-4796cd795302',
    'Biolk_XRF__006.txt': '22dfd757-4142-49a8-ba15-383370bbb85e',
    'Biolk_XRF__004.txt': 'b6666138-7306-4fe0-b0f5-aeed7e3c75e4',
    'Biolk_XRF__008.txt': '75dba810-82d0-4349-b9f2-c0073c974852',
    'Biolk_XRF__002.txt': 'a88f5b03-4ae4-443d-ae45-2269e268a738',
    'Biolk_XRF__003.txt': '5b9d5bc8-87f4-43d5-be8d-492720d16fd9',
    'Biolk_XRF__007.txt': '4dc73d16-5089-40d2-9936-e94bf4e6e32e',
    'biolk_sC_100x_spot6.txt': 'd916fea3-41f1-4029-a86f-0db99b993f4f',
    'biolk_sC_100x_spot6.txt': '9eba1281-1db0-4623-b918-bd6862ebd3a4',
    'biolk_sA2_20x_spot1.txt': '81b996b9-b7d3-44e7-acc7-17182b5e1d2d',
    'biolk_sA2_20x_spot1.txt': '3e1b0909-651e-4444-8285-d47a8e2e271a',
    'biolk_sA2_50x_spot1.txt': '8859f761-5c51-4d29-85b1-8064c9cdc563',
    'biolk_sA2_50x_spot1.txt': '6150f5d5-0c88-4376-9757-292e6efcde9b',
    'biolk_sC_100x_spot5.txt': '0d518d89-35e2-4319-aa0f-4d97c0fc0782',
    'biolk_sC_100x_spot5.txt': 'eb8f81be-dc41-47de-b1ea-c7ab5877ef29',
    'biolk_sA2_50x_spot2.txt': 'b88a68aa-337a-4467-8470-f4d885064a61',
    'biolk_sA2_50x_spot2.txt': '9ef5f363-e1e9-431e-b8f1-9a99352ffd89',
    'biolk_sA2_50x_spot3.txt': 'dde74c8a-5357-4d8f-b6cd-d81208ed05ea',
    'biolk_sA2_50x_spot3.txt': 'a95f55ab-350f-4234-82b8-e7ba061b65c8',
    'biolk_sC_100x_spot4.txt': '3f847aa1-441f-4063-ace6-947657c22b1d',
    'biolk_sC_100x_spot4.txt': 'a38326d2-3980-40c1-a34a-c20931def52e',
    'biolk_sC_100x_spot1.txt': '593a1ffe-68ae-48d5-8a42-341a60ecfe3c',
    'biolk_sC_100x_spot1.txt': '0e04427c-6a89-45fe-bd6c-ac8926263b4e',
    'biolk_sC_100x_spot3.txt': 'e091917a-3465-4de1-bffb-ef4b295a1f85',
    'biolk_sC_100x_spot3.txt': '09452bc1-c53d-4c01-93a7-cf838c762e9f',
    'biolk_sC_100x_spot2.txt': 'e5a969d3-bdc5-450b-a050-675c66ff1dda',
    'biolk_sC_100x_spot2.txt': 'af157539-4908-460c-9f76-099fc56cf3b7',
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
