import pandas as pd

from arches_rascoll import general_configs


def prepare_save_groups_data(
    df=None,
    data=general_configs.GROUP_DATA, 
    save_path=general_configs.IMPORT_RAW_GROUP_CSV
):
    if df is None:
        df = pd.DataFrame(data)
    df.to_csv(save_path, index=False)
    return df

