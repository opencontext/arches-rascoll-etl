import os
import pandas as pd
import uuid as GenUUID

from arches_rascoll import general_configs

# The following are the value UUIDs for the NFPA Safety Classification concept prefLabels.
NFPA_SAFETY_CLASSIFICATION_VALUE_UUIDS = {
    'NFPA Flammability - 0': 'b1e26e3a-b178-4e51-8ae1-261a9937a099',
    'NFPA Flammability - 1': '02424d5d-2e33-4b0b-9ac2-aba7a2be7d4a',
    'NFPA Flammability - 2': '141fa84a-489b-4897-a9ee-3e9515517578',
    'NFPA Flammability - 3': 'e985b150-5d14-4cf6-81f4-6429113312f7',
    'NFPA Flammability - 4': '2135b772-2f1f-4e61-87e7-362d16f5e026',
    'NFPA Health - 0': '32ee5079-c1a0-4528-b2b9-a8559cce25e4',
    'NFPA Health - 1': '4fc13889-bc88-4fc5-8ae8-56748363caf4',
    'NFPA Health - 2': 'ff7fa2f3-87ef-45dc-9d92-cb6848b776fc',
    'NFPA Health - 3': 'e353d9b5-24e5-42bb-a4f5-12e1a4e1926b',
    'NFPA Health - 4': 'a18f4907-cf94-4b18-883b-4270ea3255e0',
    'NFPA Instability - 0': 'd4bc692c-954e-4615-bdc6-f65fa9f01a47',
    'NFPA Instability - 1': 'c5bf3543-f3a4-4050-bf90-9ee01f143773',
    'NFPA Instability - 2': 'a1c82920-1daa-4a59-9f28-20fdf8fed34a',
    'NFPA Instability - 3': 'dadb52af-51e2-48c7-882c-a393fc1e9614',
    'NFPA Instability - 4': '77d07209-6b39-4726-9c54-66b2c0969905',
    'NFPA Special - OX': '8063859d-738e-4177-8c30-82ef6f2c86fe',
    'NFPA Special - SA': 'fff9f26d-6518-4fde-abb1-565e92be3843',
    'NFPA Special - W': '5ec9597f-5690-45c2-b55e-685454b08566',
}

# The following configs help define how to map between the raw data
# and the NFPA Safety Classification concept prefLabel value UUIDs.
NFPA_FIELD_CONFIGS = [
    ('Fire Safety', 'fire_safety_value_uuid', 'NFPA Flammability - ', {}), 
    ('Health Safety', 'health_safety_value_uuid', 'NFPA Health - ', {}),
    ('Other Safety', 'other_safety_value_uuid', 'NFPA Special - ', {'C': 'OX', 'OX': 'OX', 'W': 'W'}),
    ('Reactivity Safety', 'reactivity_safety_value_uuid', 'NFPA Instability - ', {}),
]


def get_groups_from_raw_data(
    raw_path=general_configs.RAW_IMPORT_CSV,
    data=general_configs.GROUP_DATA,
):
    """Add groups from raw data."""
    group_cols = [
        'Acquired By (Institution_1)',
        'Acquired By (Institution_2)',
        'Acquired From (CLEAN_1)',
        'Acquired From (CLEAN_2)',
        'Manufacturer (CLEAN)',
    ]
    df = pd.read_csv(raw_path)
    group_vals = []
    for col in group_cols:
        index = df[col].notnull()
        group_vals += df[index][col].unique().tolist()
    group_vals = list(set(group_vals))
    group_data = data.copy()
    for group_val in group_vals:
        group_data.append(
            {
                'group_uuid': str(GenUUID.uuid4()),
                'group_name': group_val,
            }
        )
    df_groups = pd.DataFrame(group_data)
    return df_groups


def prepare_save_groups_data(
    df=None,
    raw_path=general_configs.RAW_IMPORT_CSV,
    data=general_configs.GROUP_DATA, 
    save_path=general_configs.IMPORT_RAW_GROUP_CSV
):
    if df is None:
        if not os.path.exists(save_path):
            df = get_groups_from_raw_data(
                raw_path=raw_path,
                data=data,
            )
        else:
            df = pd.read_csv(save_path)
    df.to_csv(save_path, index=False)
    return df


def prepare_rsci_group_safety_data(
    df=None, 
    raw_path=general_configs.RAW_IMPORT_CSV,
    rsci_safety_path=general_configs.IMPORT_RSCI_GROUPS_SAFTEY_CSV,
    field_configs=NFPA_FIELD_CONFIGS,
):
    if df is None:
        df = pd.read_csv(raw_path)
    
    keep_cols = ['rsci_uuid'] + [field_config[0] for field_config in field_configs]
    df_rsci_safety = df[keep_cols].copy()
    # Add the group_uuid column with the uuid for the NFPA, which is the same for all rows.
    # It is the authority for the NFPA Safety Classification concept.
    df_rsci_safety['group_uuid'] = general_configs.NFPA_GROUP_UUID
    for field_name, value_uuid_name, value_prefix, suffix_map in field_configs:
        df_rsci_safety[value_uuid_name] = ''
        field_index = df_rsci_safety[field_name].notnull()
        field_vals = df_rsci_safety[field_index][field_name].unique().tolist()
        for raw_field_val in field_vals:
            field_val = str(raw_field_val).upper()
            field_val = field_val.strip()
            field_val = field_val.replace('.0', '')
            field_val = suffix_map.get(field_val, field_val)
            label = f"{value_prefix}{field_val}"
            value_uuid = NFPA_SAFETY_CLASSIFICATION_VALUE_UUIDS.get(label)
            print(f"label: {label}, value_uuid: {value_uuid}")
            if not value_uuid:
                continue
            index = df_rsci_safety[field_name] == raw_field_val
            df_rsci_safety.loc[index, value_uuid_name] = value_uuid
    # Let's only keep rows with at least one safety value.
    good_index = (
        df_rsci_safety[field_configs[0][1]].notnull()
        & (df_rsci_safety[field_configs[0][1]] != '')
    )
    for _, value_uuid_name, _, _ in field_configs[1:]:
        good_index = good_index | (
            df_rsci_safety[value_uuid_name].notnull()
            & (df_rsci_safety[value_uuid_name] != '')
        )
    df_rsci_safety = df_rsci_safety[good_index].copy()
    df_rsci_safety.to_csv(rsci_safety_path, index=False)
    return df_rsci_safety