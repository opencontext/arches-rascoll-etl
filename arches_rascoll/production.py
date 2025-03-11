import pandas as pd
import uuid as GenUUID

from arches_rascoll import general_configs
from arches_rascoll import places


def prepare_rsci_place_data(
    df=None, 
    raw_path=general_configs.RAW_IMPORT_CSV,
    geo_path=general_configs.IMPORT_PLACES_CSV,
    rsci_geo_path=general_configs.IMPORT_RSCI_PLACES_CSV,
):
    """This prepares geospatial data specific to a given RSCI record."""
    if df is None:
        df = pd.read_csv(raw_path)
    if not os.path.exists(geo_path):
        df_all_geo = places.prepare_save_geo_data(
            df, 
            raw_path=raw_path,
            save_path=geo_path,
        )
    else:
        df_all_geo = pd.read_csv(geo_path)
    keep_cols = ['rsci_uuid', 'specific_place_uri', 'specific_place_uri_2']
    df_rsci_geo = df[keep_cols].copy()
    df_rsci_geo.rename(columns={'specific_place_uri': 'specific_place_uri_1'}, inplace=True)
    copy_geo_cols = ['place_uuid', 'specific_place_uri', 'geo_point']
    df_geo_trim = df_all_geo[copy_geo_cols].copy()
    for i in [1, 2]:
        renames = {col: f'{col}_{i}' for col in df_geo_trim.columns.tolist()}
        df_act_geo = df_geo_trim.copy()
        df_act_geo.rename(columns=renames, inplace=True)
        join_col = f'specific_place_uri_{i}'
        df_rsci_geo = df_rsci_geo.merge(df_act_geo, how='left', left_on=join_col, right_on=join_col)
    good_index = (df_rsci_geo['place_uuid_1'].notnull() | df_rsci_geo['place_uuid_2'].notnull())
    df_rsci_geo = df_rsci_geo[good_index].copy()
    df_rsci_geo.to_csv(rsci_geo_path, index=False)
    return df_rsci_geo



def prepare_save_production_data(
    df=None,
    raw_path=general_configs.RAW_IMPORT_CSV,
    groups_path=general_configs.IMPORT_RAW_GROUP_CSV,
    rsci_geo_path=general_configs.IMPORT_RSCI_PLACES_CSV,
    save_path=general_configs.IMPORT_RAW_PROD_CSV,
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
        'origination_date_statement',
        'origin_date_begin_of_begin',
        'origin_date_end_of_begin',
        'origin_date_begin_of_end',
        'origin_date_end_of_end',
    ] + groups_cols
    first_cols = [
        'rsci_uuid',
        'prod_label',
    ]
    end_cols = [c for _, c in group_name_id_cols]
    end_cols += [
        'origination_date_statement',
        'origin_date_begin_of_begin',
        'origin_date_end_of_begin',
        'origin_date_begin_of_end',
        'origin_date_end_of_end',
    ]
    df_prod = df[man_cols].copy()
    df_groups = pd.read_csv(groups_path)
    for name_col, id_col in group_name_id_cols:
        df_prod = df_prod.merge(df_groups, how='left', left_on=name_col, right_on='group_name')
        df_prod.drop(columns=['group_name'], inplace=True)
        df_prod.rename(columns={'group_uuid': id_col}, inplace=True)
        null_index = df_prod[id_col].isnull()
        df_prod.loc[null_index, id_col] = ''

    # Get the geospatial data for the production data.
    df_rsci_geo = pd.read_csv(rsci_geo_path)
    mid_cols = [c for c in df_rsci_geo.columns.tolist() if c not in df_prod.columns.tolist()]

    # Merge the geospatial data into the rest of the production data.
    df_prod = df_prod.merge(df_rsci_geo, how='outer', left_on='rsci_uuid', right_on='rsci_uuid')
    # Make a barcode label for all the rows. 
    df_prod['prod_label'] = 'Production of Barcode No. ' + df_prod['Barcode No.'].astype(str)

    # Reorder the columns.
    df_prod = df_prod[first_cols + mid_cols + end_cols].copy()
    
    # Let's only keep rows with at least some production data
    not_null_cols = mid_cols + end_cols
    good_index = (
        df_prod[not_null_cols[0]].notnull()
        & (df_prod[not_null_cols[0]] != '')
    )
    if len(not_null_cols) > 1:
        for col in not_null_cols[1:]:
            good_index = good_index | (
                df_prod[col].notnull()
                & (df_prod[col] != '')
            )
    df_prod = df_prod[good_index].copy()
    df_prod.to_csv(save_path, index=False)
    return df_prod

