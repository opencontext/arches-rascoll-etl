import os
import pandas as pd
import uuid as GenUUID

from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import RDFS, SKOS

from arches_rascoll import general_configs
from arches_rascoll import utilities



def prepare_skos_graph_from_concepts_csv(
    csv_file_path,
    scheme_uri=None,
    scheme_label=None,
):
    """Prepare a SKOS graph from a CSV file of concepts.""" 
    df = pd.read_csv(csv_file_path, encoding='utf-8')

    if not scheme_uri:
        scheme_uri = f"http://localhost/{str(GenUUID.uuid4())}"
    if not scheme_label:
        scheme_label = f"Scheme from CSV {os.path.basename(csv_file_path)}"

    g = Graph()

    scheme = URIRef(scheme_uri)
    g.add((scheme, RDF.type, SKOS.ConceptScheme))
    g.add((scheme, SKOS.prefLabel, Literal(scheme_label, lang="en")))

    entities = {
        scheme_uri: scheme,
    }
    # add all of the parent concepts to the graph
    p_index = (
        ~df['parent_prefLabel'].isnull()
        & ~df['parent_uri'].isnull()
    )
    for i, row in df[p_index].iterrows():
        parent_concept_uri = row['parent_uri']
        parent_pref_label = row['parent_prefLabel']
        if parent_concept_uri in entities:
            # this concept has already been added to the graph
            continue
        parent_concept = URIRef(parent_concept_uri)
        g.add((parent_concept, RDF.type, SKOS.Concept))
        g.add((parent_concept, SKOS.prefLabel, Literal(parent_pref_label, lang="en")))
        g.add((parent_concept, SKOS.inScheme, scheme))
        entities[parent_concept_uri] = parent_concept
    
    # Now check to see if the parent concept has a parent concept
    for i, row in df[p_index].iterrows():
        parent_concept_uri = row['parent_uri']
        p_p_index = df['uri'] == parent_concept_uri
        if df[p_p_index].empty:
            # this concept has no parent concept, add it as a top level concept
            g.add((parent_concept, SKOS.topConceptOf, scheme))
            continue
        # this concept has a parent concept, add it as a child of the parent concept
        p_p_concept_uri = df[p_p_index]['parent_uri'].values[0]
        if not p_p_concept_uri or str(p_p_concept_uri) == 'nan':
            continue
        p_p_concept = entities.get(p_p_concept_uri)
        if not p_p_concept:
            raise ValueError(f"Parent concept {p_p_concept_uri}, a parent of {parent_concept_uri} not found in entities")
        g.add((parent_concept, SKOS.broader, p_p_concept))
        g.add((p_p_concept, SKOS.narrower, parent_concept))

    # add all of the main concepts to the graph
    act_index = (
        ~df['prefLabel'].isnull()
        & ~df['uri'].isnull()
    )
    for i, row in df[act_index].iterrows():
        concept_uri = row['uri']
        pref_label = row['prefLabel']
        if concept_uri in entities:
            # this concept has already been added to the graph
            continue
        act_concept = URIRef(concept_uri)
        g.add((act_concept, RDF.type, SKOS.Concept))
        g.add((act_concept, SKOS.prefLabel, Literal(pref_label, lang="en")))
        g.add((act_concept, SKOS.inScheme, scheme))
        entities[concept_uri] = act_concept
        # check to see if the concept has a parent concept
        parent_concept_uri = row['parent_uri']
        if parent_concept_uri and len(str(parent_concept_uri)) > 0 and entities.get(parent_concept_uri):
            parent_concept = entities[parent_concept_uri]
            g.add((act_concept, SKOS.broader, parent_concept))
            g.add((parent_concept, SKOS.narrower, act_concept))
            continue
        else:
            # this concept has no parent concept, add it as a top level concept
            g.add((act_concept, SKOS.topConceptOf, scheme))
    return g


def prepare_save_skos_rdf_graph_from_csv(
    csv_file_path,
    rdf_file_path,
    scheme_uri=None,
    scheme_label=None,
    overwrite_rdf=False,
    format='turtle',
):
    """Prepare and save the RDF graph."""
    if os.path.exists(rdf_file_path) and not overwrite_rdf:
        g = Graph()
        g.parse(rdf_file_path)
        return g
    g = prepare_skos_graph_from_concepts_csv(
        csv_file_path,
        scheme_uri=scheme_uri,
        scheme_label=scheme_label,
    )
    g.serialize(rdf_file_path, format=format)
    return g



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
