import pandas as pd
import uuid as GenUUID

from arches_rascoll import general_configs


def prepare_save_persons_data(
    df=None,
    raw_path=general_configs.RAW_IMPORT_CSV, 
    save_path=general_configs.IMPORT_RAW_PERSON_CSV
):
    """Prepare and save the persons data for import."""
    if df is None:
        df = pd.read_csv(raw_path)
    person_cols = [
        'Acquired By (CLEAN_1)',
        'Acquired By (CLEAN_2)',
    ]
    rows = []
    person_vals = []
    for col in person_cols:
        index = df[col].notnull()
        person_vals += df[index][col].unique().tolist()
    person_vals = list(set(person_vals))
    for person_val in person_vals:
        rows.append(
            {
                'person_uuid': str(GenUUID.uuid4()),
                'person_name': person_val,
            }
        )
    df_persons = pd.DataFrame(rows)
    df_persons.to_csv(save_path, index=False)
    return df_persons

