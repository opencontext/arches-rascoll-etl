import pandas as pd
import uuid as GenUUID

from arches_rascoll import general_configs


def prepare_save_prov_acts_data(
    df=None,
    raw_path=general_configs.RAW_IMPORT_CSV,
    persons_path=general_configs.IMPORT_RAW_PERSON_CSV,
    groups_path=general_configs.IMPORT_RAW_GROUP_CSV,
    save_path=general_configs.IMPORT_RAW_PROV_ACT_CSV,
):
    """Prepare and save the persons data for import."""
    if df is None:
        df = pd.read_csv(raw_path)

    person_name_id_cols = [
        ('Acquired By (CLEAN_1)', 'acq_by_person_1_uuid'),
        ('Acquired By (CLEAN_2)', 'acq_by_person_2_uuid'),
    ]
    group_name_id_cols = [
        ('Acquired By (Institution_1)', 'acq_by_group_1_uuid'),
        ('Acquired By (Institution_2)', 'acq_by_group_2_uuid'),
        ('Acquired From (CLEAN_1)', 'acq_from_group_1_uuid'),
        ('Acquired From (CLEAN_2)', 'acq_from_group_2_uuid'),
    ]

    persons_cols = [c for c, _ in person_name_id_cols]
    groups_cols = [c for c, _ in group_name_id_cols]
    
    date_cols = [
        'Acquisition Date__begin_of_the_begin',
        'Acquisition Date__end_of_the_begin',
        'Acquisition Date__begin_of_the_end',
        'Acquisition Date__end_of_the_end',
    ]
    prov_act_cols = [
        'rsci_uuid',
        'Barcode No.',
    ] + persons_cols + groups_cols + date_cols
    first_cols = [
        'rsci_uuid',
        'set_uuid',
        'prov_act_uuid',
        'prov_act_name',
    ]
    end_cols = date_cols + [c for _, c in person_name_id_cols] + [c for _, c in group_name_id_cols]
    df_prov_acts = df[prov_act_cols].copy()
    df_prov_acts['set_uuid'] = general_configs.GCI_REF_COL_SET_UUID
    df_prov_acts['prov_act_uuid'] = ''
    df_prov_acts['Barcode No.'] = df_prov_acts['Barcode No.'].astype(str)
    df_prov_acts['prov_act_uuid'] = df_prov_acts['prov_act_uuid'].apply(lambda x: str(GenUUID.uuid4()))
    df_prov_acts['prov_act_name'] = 'Acquisition of Barcode ' + df_prov_acts['Barcode No.'].str.strip().replace('.0', '', regex=True)  
    df_persons = pd.read_csv(persons_path)
    for name_col, id_col in person_name_id_cols:
        df_prov_acts = df_prov_acts.merge(df_persons, how='left', left_on=name_col, right_on='person_name')
        df_prov_acts.drop(columns=['person_name'], inplace=True)
        df_prov_acts.rename(columns={'person_uuid': id_col}, inplace=True)
        null_index = df_prov_acts[id_col].isnull()
        df_prov_acts.loc[null_index, id_col] = ''
    df_groups = pd.read_csv(groups_path)
    for name_col, id_col in group_name_id_cols:
        df_prov_acts = df_prov_acts.merge(df_groups, how='left', left_on=name_col, right_on='group_name')
        df_prov_acts.drop(columns=['group_name'], inplace=True)
        df_prov_acts.rename(columns={'group_uuid': id_col}, inplace=True)
        null_index = df_prov_acts[id_col].isnull()
        df_prov_acts.loc[null_index, id_col] = ''
    df_prov_acts = df_prov_acts[first_cols + end_cols].copy()
    # Let's only keep rows with at least some acquisition data
    good_index = (
        df_prov_acts[end_cols[0]].notnull()
        & (df_prov_acts[end_cols[0]] != '')
    )
    for col in end_cols[1:]:
        good_index = good_index | (
            df_prov_acts[col].notnull()
            & (df_prov_acts[col] != '')
        )
    df_prov_acts = df_prov_acts[good_index].copy()
    df_prov_acts.to_csv(save_path, index=False)
    return df_prov_acts

