import pandas as pd
import uuid as GenUUID

from arches_rascoll import general_configs


def prepare_save_manufacturer_data(
    df=None,
    raw_path=general_configs.RAW_IMPORT_CSV,
    groups_path=general_configs.IMPORT_RAW_GROUP_CSV,
    save_path=general_configs.IMPORT_RAW_MANU_CSV,
):
    """Prepare and save the persons data for import."""
    if df is None:
        df = pd.read_csv(raw_path)

    group_name_id_cols = [
        ('Manufacturer (CLEAN)', 'manu_group_uuid'),
    ]

    groups_cols = [c for c, _ in group_name_id_cols]
    
    man_cols = [
        'rsci_uuid',
        'Barcode No.',
    ] + groups_cols
    first_cols = [
        'rsci_uuid',
        'manu_label',
    ]
    end_cols = [c for _, c in group_name_id_cols]
    df_manu = df[man_cols].copy()
    df_manu['manu_label'] = 'Production of Barcode No. ' + df_manu['Barcode No.'].astype(str)
    df_groups = pd.read_csv(groups_path)
    for name_col, id_col in group_name_id_cols:
        df_manu = df_manu.merge(df_groups, how='left', left_on=name_col, right_on='group_name')
        df_manu.drop(columns=['group_name'], inplace=True)
        df_manu.rename(columns={'group_uuid': id_col}, inplace=True)
        null_index = df_manu[id_col].isnull()
        df_manu.loc[null_index, id_col] = ''
    df_manu = df_manu[first_cols + end_cols].copy()
    # Let's only keep rows with at least some acquisition data
    good_index = (
        df_manu[end_cols[0]].notnull()
        & (df_manu[end_cols[0]] != '')
    )
    if len(end_cols) > 1:
        for col in end_cols[1:]:
            good_index = good_index | (
                df_manu[col].notnull()
                & (df_manu[col] != '')
            )
    df_manu = df_manu[good_index].copy()
    df_manu.to_csv(save_path, index=False)
    return df_manu

