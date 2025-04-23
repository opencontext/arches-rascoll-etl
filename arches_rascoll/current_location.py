import pandas as pd
import uuid as GenUUID

from arches_rascoll import general_configs



"""
# testing

from arches_rascoll import general_configs
from arches_rascoll import current_location


df_cl = current_location.prepare_save_current_location_data()

"""



def prepare_save_current_location_data(
    df=None,
    raw_path=general_configs.RAW_IMPORT_CSV,
    save_path=general_configs.IMPORT_RAW_RSCI_CURRENT_LOCATION_CSV,
):
    """Prepare and save the persons data for import."""
    if df is None:
        df = pd.read_csv(raw_path)
    keep_cols = [
        'rsci_uuid', 
        'Grid Location'
    ]
    df_cl = df[keep_cols].copy()
    df_cl['current_location_place_resourceid'] = general_configs.GETTY_CENTER_PLACE_UUID 
    act_index = (
        ~df_cl['Grid Location'].isnull()
        &(df_cl['Grid Location'] != '')
        &(df_cl['Grid Location'] != 'NaN')
        &(df_cl['Grid Location'] != 'nan')
        &(df_cl['Grid Location'] != '""')
    )
    df_cl['current_location_statement'] = ''
    df_cl.loc[act_index, 'current_location_statement'] = df_cl[act_index].apply(
        lambda row: f"Grid Location: {row['Grid Location']}",
        axis=1
    )
    df_cl.to_csv(save_path, index=False)
    return df_cl

