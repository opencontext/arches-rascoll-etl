import os
import pandas as pd
import uuid as GenUUID

from arches_rascoll import general_configs


def prepare_save_sets_data(
    df=None,
    raw_path=general_configs.RAW_IMPORT_CSV,
    data=general_configs.SET_DATA, 
    save_path=general_configs.IMPORT_RAW_SET_CSV
):
    if df is None:
        if not os.path.exists(save_path):
            df = pd.DataFrame(data)
        else:
            df = pd.read_csv(save_path)
    df.to_csv(save_path, index=False)
    return df

