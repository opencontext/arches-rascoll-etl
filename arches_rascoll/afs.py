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

    # for digital resources files
    'TSR_FORS_biolk_00003.asd.sco.abs.dx': 'b457ba6f-98ea-488f-90d9-69263c5f16fb', 
    'biolk_sC_100x_spot3.dx': 'aeb2f3e0-72fe-486c-8b87-be52dc1135d9', 
    'biolk_sC_100x_spot3.wxd': '62c68a62-4d3f-45fc-99eb-c69ff6ba31d1', 
    'biolk_sC_100x_spot1.wxd': '009fbff5-890d-4a7f-acd1-aa2764d5cf2f', 
    'biolk_sC_100x_spot1.dx': '6c4808b3-edab-44b5-96eb-f7512006153a', 
    'TSR_FORS_biolk_00007.asd.sco.abs.dx': 'f229c70f-a3d2-4ea0-b398-060102faa3e8', 
    'biolk_sC_100x_spot4.wxd': '687e536f-9ea8-4b9f-9fd9-e8f9ec8ea35f', 
    'biolk_sC_100x_spot4.dx': 'c34a0b2b-3cd4-4c85-aa4f-1819ab2f71a6', 
    'biolk_sA2_20x_spot1.dx': '6dbf2393-3721-4c89-9136-03ff7d035e0e', 
    'biolk_sA2_50x_spot1.dx': '65ababbc-d014-40a6-85ab-e31c227cdf70', 
    'biolk_sA2_50x_spot1.wxd': '3f04955c-82fb-4946-a972-e4b735db4c43', 
    'biolk_sA2_20x_spot1.wxd': '37f6ac4e-c3bf-45ca-a170-746f156a9cdd', 
    'biolk_sC_100x_spot5.wxd': 'c24ca4d2-bcb2-4c40-a20c-642be1981fd5', 
    'biolk_sC_100x_spot5.dx': 'd58d53df-c0ef-40a0-b60d-ef21361591c9', 
    'biolk_sC_100x_spot6.dx': '5a36969a-3181-40c8-9cdb-0ba137bf08e6', 
    'biolk_sC_100x_spot6.wxd': 'a75ce363-5457-4e6f-9e81-98df0fe7f92b', 
    'biolk_sA2_50x_spot3.wxd': 'edaecc1d-97ce-490d-95d3-0b3a4deea1d2', 
    'biolk_sA2_50x_spot3.dx': 'b763a9ae-b23f-49d4-9da8-e1d3fca72f2d', 
    'TSR_FORS_biolk_00008.asd.sco.abs.dx': 'fe5b89b6-234b-45a1-bf42-6d184449fa8a', 
    'TSR_FORS_biolk_00009.asd.sco.abs.dx': '52bbd3cc-06ce-45e0-85b7-509a0e9392de', 
    'Biolk_XRF__007minus008.txt': '350bd7a3-f375-45db-b9f5-6ac9502796d1', 
    'Biolk_XRF__007minus008.spx': '7c65403f-c53d-44a9-b5ff-fb4ab4c65dd6', 
    'Biolk_XRF__007.spx': 'a4c29519-189e-464c-bf94-6ea5a33bb7a4', 
    'Biolk_XRF__004minus008.spx': '39374ef6-e1b1-4a58-b277-fac9a4919ffb', 
    'Biolk_XRF__004.spx': '725da319-238c-4f66-b93e-3809ef13c4a5', 
    'Biolk_XRF__004minus008.txt': '3b9cbc5c-01b5-4a59-a0dc-f0dfdcc5981b', 
    'Biolk_XRF__003minus008.txt': 'beaedb2b-c3f8-4a3e-8e12-37e223b3ffa9', 
    'Biolk_XRF__003minus008.spx': 'c08cf678-6780-4bd2-a861-2fc1e4416574', 
    'Biolk_XRF__003.spx': 'e344d5b1-00da-4953-9145-ecea3d92f679', 
    'TSR_FORS_biolk_00010.asd.sco.abs.dx': 'ad209f79-c87c-4e4a-ba1a-048e5ad1e882', 
    'biolk_sA2_50x_spot2.dx': '4fd97f19-c884-44d5-a63c-497ec66c79e8', 
    'biolk_sA2_50x_spot2.wxd': '133e2107-5e64-47ab-a588-48955ef1d427', 
    'Biolk_XRF__002.spx': '7666c8ce-da9f-40d7-a101-937f85a50b0d', 
    'Biolk_XRF__002minus008.spx': '0bde8e88-72e3-4e98-a0c6-43a08903ec05', 
    'Biolk_XRF__002minus008.txt': '2f0d3d0b-8961-469c-a2c4-c2d89aac7035', 
    'biolk_sC_100x_spot2.dx': '69ca77fc-38e2-40be-9318-7079116d090f', 
    'biolk_sC_100x_spot2.wxd': 'a2be6af6-c9ad-40f0-b8ac-9569a7784fad', 
    'Biolk_XRF__008.spx': 'f5506aac-a6ef-4fd5-a2dd-cba70afbe61e', 
    'Biolk_XRF__001.spx': 'ed2d2e43-74b2-409b-8c95-4a770ad38bc4', 
    'Biolk_XRF__001minus008.txt': '49a392bf-096a-4469-bde4-b72320b87e03', 
    'Biolk_XRF__001minus008.spx': 'b7b78332-37b6-402c-a76d-e843ad5e2880', 
    'Biolk_XRF__006minus008.txt': '880c0d00-d727-4859-a61d-9524335cc12c', 
    'Biolk_XRF__006minus008.spx': '8170ccfd-d00c-4d69-9a39-29d7ce5e3057', 
    'Biolk_XRF__006.spx': 'b2e5bad4-df49-4adc-83d5-150a474b9ef4', 
    'VioBiolk_flat.bcf': 'c0c0dc86-4f10-4cc1-af1b-adde2b0782fc', 
    'VioBiolk_flat.raw': 'cea6aefd-6001-4915-93e3-910a71164a0e', 
    'VioBiolk_flat.rpl': '33b8b921-034d-4422-8841-d30176ad995e', 
    'Biolk_XRF__005.spx': '20ab2573-85cb-4042-922a-bcb1511ba625', 
    'Biolk_XRF__005minus008.txt': '198efb7a-bee3-4c0b-aa03-4c124e04519c', 
    'Biolk_XRF__005minus008.spx': 'c0b934e7-196b-4f9a-b10f-311e2469ca7f', 

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
