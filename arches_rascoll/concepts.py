import os
import pandas as pd
import uuid as GenUUID

from arches_rascoll import general_configs
from arches_rascoll import utilities


def get_concept_values_by_uri(
    concept_uri,
    db_url=general_configs.ARCHES_DB_URL
):
    """Get a conceptid, concept_uri, and preflabel
    and various valueids by a concept_uri."""
    engine = utilities.create_engine(db_url)
    sql = f"""
    SELECT
        vi.conceptid,
        vi.valueid AS concept_uri_valueid, 
        vi.value AS concept_uri, 
        vpf.valueid AS preflabel_valueid,
        vpf.value AS preflabel
    FROM public.values AS vi
    LEFT JOIN public.values AS vpf ON (
        vi.conceptid = vpf.conceptid
        AND vpf.valuetype = 'prefLabel'
    )
    WHERE vi.value = '{concept_uri}'
    AND vi.valuetype = 'identifier'
    LIMIT 1;
    """
    df = pd.read_sql(sql, engine)
    d_list = df.to_dict(orient='records')
    if len(d_list) == 0:
        return None
    return d_list[0]



def get_concept_values_by_preflabel(
    pref_label,
    db_url=general_configs.ARCHES_DB_URL
):
    """Get a conceptid, concept_uri, and preflabel
    and various valueids by a preflabel."""
    engine = utilities.create_engine(db_url)
    sql = f"""
    SELECT
        vpf.conceptid,
        vi.valueid AS concept_uri_valueid, 
        vi.value AS concept_uri, 
        vpf.valueid AS preflabel_valueid,
        vpf.value AS preflabel
    FROM public.values AS vpf
    LEFT JOIN public.values AS vi ON (
        vi.conceptid = vpf.conceptid
        AND vpf.valuetype = 'identifier'
    )
    WHERE vpf.value = '{pref_label}'
    AND vpf.valuetype = 'prefLabel'
    LIMIT 1;
    """
    df = pd.read_sql(sql, engine)
    d_list = df.to_dict(orient='records')
    if len(d_list) == 0:
        return None
    return d_list[0]
