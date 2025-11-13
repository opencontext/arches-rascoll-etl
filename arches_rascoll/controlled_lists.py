import json
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
controlled_lists.make_json_for_controlled_list_items(
    pref_labels=['brief text', 'sources (general concept)',],
    list_id='f44f7240-35c5-49e8-a0e3-2ffe308f0862',
)
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
        liv.value AS preflabel,
        liv.languageid AS language_id,
        liv.valuetype_id AS valuetype_id
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
            liv.value AS preflabel,
            liv.languageid AS language_id,
            liv.valuetype_id AS valuetype_id
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
            liv.value AS preflabel,
            liv.languageid AS language_id,
            liv.valuetype_id AS valuetype_id
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


def get_controlled_list_item_by_list_item_id(
    list_item_id,
    list_id=None,
    db_url=general_configs.ARCHES_DB_URL
):
    """Get a conceptid, concept_uri, and preflabel
    and various valueids by a preflabel."""
    engine = utilities.create_engine(db_url)
    if not list_id:
        sql = f"""
        SELECT
            l.id AS list_id,
            l.name AS list_name,
            li.id,
            li.uri AS uri, 
            liv.id AS preflabel_valueid,
            liv.value AS preflabel,
            liv.languageid AS language_id,
            liv.valuetype_id AS valuetype_id
        FROM public.arches_controlled_lists_listitemvalue AS liv
        LEFT JOIN public.arches_controlled_lists_listitem AS li ON (
            liv.list_item_id = li.id
        )
        LEFT JOIN public.arches_controlled_lists_list AS l ON (
           li.list_id = l.id
        )
        WHERE liv.list_item_id = '{list_item_id}'
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
            liv.value AS preflabel,
            liv.languageid AS language_id,
            liv.valuetype_id AS valuetype_id
        FROM public.arches_controlled_lists_listitemvalue AS liv
        LEFT JOIN public.arches_controlled_lists_listitem AS li ON (
            liv.list_item_id = li.id
        )
        LEFT JOIN public.arches_controlled_lists_list AS l ON (
           li.list_id = l.id
        )
        WHERE liv.list_item_id = '{list_item_id}'
        AND liv.valuetype_id = 'prefLabel'
        AND li.list_id = '{list_id}'
        LIMIT 1;
        """
    df = pd.read_sql(sql, engine)
    d_list = df.to_dict(orient='records')
    if len(d_list) == 0:
        return None
    return d_list[0]



def make_json_for_controlled_list_items(
    pref_labels,
    list_id=None,
    as_string=False,
    db_url=general_configs.ARCHES_DB_URL,
):
    if not isinstance(pref_labels, list):
        pref_labels = [pref_labels]
    output = []
    for pref_label in pref_labels:
        r = get_controlled_list_item_by_preflabel(
            pref_label=pref_label,
            list_id=list_id,
            db_url=db_url,
        )
        if not r:
            continue
        item = {
            'uri': r.get('uri'),
            'labels': [
                {
                    'id': str(r.get('preflabel_valueid')),
                    'value': str(r.get('preflabel')),
                    'language_id': str(r.get('language_id')),
                    'list_item_id': str(r.get('id')),
                    'valuetype_id': str(r.get('valuetype_id')),
                },
            ],
            'list_id': str(r.get('list_id')),
        }
        output.append(item)
    if as_string:
        return json.dumps(output, indent=4)
    return output


def make_json_for_controlled_list_items_by_ids(
    list_item_ids,
    list_id=None,
    as_string=False,
    db_url=general_configs.ARCHES_DB_URL,
):
    if not isinstance(list_item_ids, list):
        list_item_ids = [list_item_ids]
    output = []
    for list_item_id in list_item_ids:
        r = get_controlled_list_item_by_list_item_id(
            list_item_id=list_item_id,
            list_id=list_id,
            db_url=db_url,
        )
        if not r:
            continue
        item = {
            'uri': r.get('uri'),
            'labels': [
                {
                    'id': str(r.get('preflabel_valueid')),
                    'value': str(r.get('preflabel')),
                    'language_id': str(r.get('language_id')),
                    'list_item_id': str(r.get('id')),
                    'valuetype_id': str(r.get('valuetype_id')),
                },
            ],
            'list_id': str(r.get('list_id')),
        }
        output.append(item)
    if as_string:
        return json.dumps(output, indent=4)
    return output