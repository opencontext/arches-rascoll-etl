import os
import pandas as pd
import uuid as GenUUID

from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import RDFS, SKOS, DCTERMS


from arches_rascoll import general_configs
from arches_rascoll import utilities


"""
# testing

from arches_rascoll import general_configs
from arches_rascoll import controlled_lists

controlled_lists.get_controlled_list_item_by_preflabel('adhesive')
controlled_lists.get_controlled_list_item_by_preflabel(
    'internal identifier', 
    list_id='7a8d3213-0a64-4f31-92a4-191b41236ae9',
)
uri = 'http://localhost:8000/plugins/controlled-list-manager/item/55801709-9719-473f-9608-949f08b32eb7'
controlled_lists.get_controlled_list_item_by_uri(uri)
"""

def get_controlled_list_item_by_uri(
    uri,
    db_url=general_configs.ARCHES_DB_URL
):
    """Get a conceptid, concept_uri, and preflabel
    and various valueids by a concept_uri."""
    engine = utilities.create_engine(db_url)
    uri = str(uri).strip()
    sql = f"""
    SELECT
        l.id AS list_id,
        l.name AS list_name,
        li.id,
        li.uri AS uri, 
        liv.id AS preflabel_valueid,
        liv.value AS preflabel
    FROM public.arches_controlled_lists_listitem AS li    
    LEFT JOIN public.arches_controlled_lists_listitemvalue AS liv ON (
        liv.list_item_id = li.id
        AND liv.valuetype_id = 'prefLabel'
    )
    LEFT JOIN public.arches_controlled_lists_list AS l ON (
        li.list_id = l.id
    )
    WHERE li.uri = '{uri}'
    LIMIT 1;
    """
    df = pd.read_sql(sql, engine)
    d_list = df.to_dict(orient='records')
    if len(d_list) == 0:
        return None
    return d_list[0]



def get_controlled_list_item_by_preflabel(
    pref_label,
    list_id=None,
    db_url=general_configs.ARCHES_DB_URL
):
    """Get a conceptid, concept_uri, and preflabel
    and various valueids by a preflabel."""
    engine = utilities.create_engine(db_url)
    pref_label = str(pref_label).strip()
    pref_label = pref_label.replace("'", "''")
    if not list_id:
        sql = f"""
        SELECT
            l.id AS list_id,
            l.name AS list_name,
            li.id,
            li.uri AS uri, 
            liv.id AS preflabel_valueid,
            liv.value AS preflabel
        FROM public.arches_controlled_lists_listitemvalue AS liv
        LEFT JOIN public.arches_controlled_lists_listitem AS li ON (
            liv.list_item_id = li.id
        )
        LEFT JOIN public.arches_controlled_lists_list AS l ON (
           li.list_id = l.id
        )
        WHERE liv.value = '{pref_label}'
        AND liv.valuetype_id = 'prefLabel'
        LIMIT 1;
        """
    else:
        sql = f"""
        SELECT
            l.id AS list_id,
            l.name AS list_name,
            li.id,
            li.uri AS uri, 
            liv.id AS preflabel_valueid,
            liv.value AS preflabel
        FROM public.arches_controlled_lists_listitemvalue AS liv
        LEFT JOIN public.arches_controlled_lists_listitem AS li ON (
            liv.list_item_id = li.id
        )
        LEFT JOIN public.arches_controlled_lists_list AS l ON (
           li.list_id = l.id
        )
        WHERE liv.value = '{pref_label}'
        AND liv.valuetype_id = 'prefLabel'
        AND li.list_id = '{list_id}'
        LIMIT 1;
        """
    df = pd.read_sql(sql, engine)
    d_list = df.to_dict(orient='records')
    if len(d_list) == 0:
        return None
    return d_list[0]


