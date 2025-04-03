import os
import pandas as pd
import uuid as GenUUID

from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import RDFS, SKOS, DCTERMS


from arches_rascoll import general_configs
from arches_rascoll import utilities


def get_parent_concept_uri_for_concept_uri(
    df,
    concept_uri,
):
    p_index = df['uri'] == concept_uri
    if df[p_index].empty:
        return None
    parent_concept_uri = df[p_index]['parent_uri'].values[0]
    if parent_concept_uri == concept_uri:
        # The parent concept is the same as the concept
        return None
    if len(str(parent_concept_uri)) == 0 or str(parent_concept_uri) == 'nan':
        # The parent concept is empty or NaN
        return None
    return parent_concept_uri


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
    g.add((scheme, DCTERMS.title, Literal(scheme_label, lang="en")))

    entities = {}

    # Add all the child concepts to the graph
    c_index = (
        ~df['prefLabel'].isnull()
        & ~df['uri'].isnull()
    )
    for i, row in df[c_index].iterrows():
        concept_uri = row['uri']
        concept_uri = str(concept_uri).strip()
        pref_label = row['prefLabel']
        if concept_uri in entities:
            # this concept has already been added to the graph
            pass
        act_concept = URIRef(concept_uri)
        g.add((act_concept, RDF.type, SKOS.Concept))
        g.add((act_concept, SKOS.prefLabel, Literal(pref_label, lang="en")))
        g.add((act_concept, SKOS.inScheme, scheme))
        entities[concept_uri] = act_concept

    # add all of the parent concepts to the graph
    p_index = (
        ~df['parent_prefLabel'].isnull()
        & ~df['parent_uri'].isnull()
    )
    for i, row in df[p_index].iterrows():
        parent_concept_uri = row['parent_uri']
        parent_concept_uri = str(parent_concept_uri).strip()
        parent_pref_label = row['parent_prefLabel']
        if parent_concept_uri in entities:
            # this concept has already been added to the graph
            continue
        parent_concept = URIRef(parent_concept_uri)
        g.add((parent_concept, RDF.type, SKOS.Concept))
        g.add((parent_concept, SKOS.prefLabel, Literal(parent_pref_label, lang="en")))
        g.add((parent_concept, SKOS.inScheme, scheme))
        entities[parent_concept_uri] = parent_concept

    # iterate over all the concepts to see if they have a parent concept
    for concept_uri, concept in entities.items():
        parent_concept = None
        parent_concept_uri = get_parent_concept_uri_for_concept_uri(
            df=df,
            concept_uri=concept_uri,
        )
        if parent_concept_uri:
            parent_concept = entities.get(parent_concept_uri)
        if not parent_concept:
            # this concept has no parent concept, add it as a top level concept
            g.add((concept, SKOS.topConceptOf, scheme))
            g.add((scheme, SKOS.hasTopConcept, concept))
            g.add((concept, SKOS.inScheme, scheme))
        else:
            # this concept has a parent concept, so add the relationship
            g.add((concept, SKOS.broader, parent_concept))
            g.add((parent_concept, SKOS.narrower, concept))
            g.add((parent_concept, SKOS.inScheme, scheme))
    return g


def prepare_save_skos_rdf_graph_from_csv(
    csv_file_path,
    rdf_file_path,
    scheme_uri=None,
    scheme_label=None,
    overwrite_rdf=False,
    format='xml',
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
