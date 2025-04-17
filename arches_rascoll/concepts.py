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
from arches_rascoll import concepts

csv_file_path = general_configs.CONCEPTS_OBJECT_TYPE_CSV
rdf_file_path = general_configs.CONCEPTS_OBJECT_TYPE_RDF

g, new_entities = concepts.prepare_save_skos_rdf_graph_from_csv(
    csv_file_path=csv_file_path,
    rdf_file_path=rdf_file_path,
    format='xml',
)
ent_str = concepts.output_new_skos_concepts(new_entities)
"""

def output_new_skos_concepts(
    new_entities,
    output_path=general_configs.NEW_CONCEPTS_OBJECT_TYPE_RDF,
):
    """Output the new SKOS concepts to a CSV file."""
    if not os.path.exists(output_path):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
    ent_strs = []
    for new_entity in new_entities:
        ent_str = f"""
        <skos:narrower>
            <skos:Concept rdf:about="{new_entity['uri']}">
            <skos:prefLabel xml:lang="en">{new_entity['pref_label']}</skos:prefLabel>
            <skos:inScheme rdf:resource="http://localhost:8000/b73e741b-46da-496c-8960-55cc1007bec4"/>
            </skos:Concept>
        </skos:narrower>
        """
        ent_strs.append(ent_str)
    output = '\n'.join(ent_strs)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(output)
    f.close()
    return output



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
    entities_pref_labels = {}
    # Add all the child concepts to the graph
    c_index = (
        ~df['prefLabel'].isnull()
        & ~df['uri'].isnull()
    )
    for i, row in df[c_index].iterrows():
        concept_uri = row['uri']
        concept_uri = str(concept_uri).strip()
        pref_label = row['prefLabel']
        entities_pref_labels[concept_uri] = pref_label
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
        entities_pref_labels[parent_concept_uri] = parent_pref_label 
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
    
    new_entities = []
    for uri, pref_label in entities_pref_labels.items():
        # check to see if the uri already exists in Arches
        exist_obj = get_concept_values_by_uri(
            concept_uri=uri,
            db_url=general_configs.ARCHES_DB_URL,
        )
        if exist_obj:
            # this concept already exists in Arches, so skip it
            continue
        new_entities.append(
            {
                'uri': uri,
                'pref_label': pref_label,
            }
        )
    return g, new_entities


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
        return g, None
    g, new_entities = prepare_skos_graph_from_concepts_csv(
        csv_file_path,
        scheme_uri=scheme_uri,
        scheme_label=scheme_label,
    )
    g.serialize(rdf_file_path, format=format)
    return g, new_entities


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
    pref_label = str(pref_label).strip()
    pref_label = pref_label.replace("'", "''")
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


def get_all_configs_concept_prelabel_value_ids():
    """Gets a list of all of the preflabel valueids from the mapping configs."""
    check_ids = [
        general_configs.ENG_VALUE_UUID,
        general_configs.PREFERRED_TERM_TYPE_UUID,
        general_configs.ALT_NAME_TYPE_UUID,
        general_configs.RSCI_FACET_METATYPE_UUID,
        general_configs.RSCI_FACET_TYPE_UUID,
    ]
    check_ids += general_configs.PLACE_STATEMENT_TYPE_UUIDS
    check_ids += [
        general_configs.FULLNAME_TYPE_VALUE_UUID,
    ]
    check_ids += general_configs.RSCI_NOTES_STATEMENT_TYPE_IDS 
    check_ids += general_configs.RSCI_PHYS_FORM_STATEMENT_TYPE_IDS
    check_ids += [
        general_configs.PROV_ACT_EVENT_TYPE_TRANSFERED_VALUE_UUID,
    ]
    check_ids += general_configs.RSCI_PLACE_PRODUCTION_STATEMENT_TYPE_IDS
    check_ids += general_configs.RSCI_MATERIAL_CHEM_NAME_TYPES
    check_ids += general_configs.RSCI_GBIF_NAME_TYPES
    check_ids += general_configs.RSCI_PART_STATEMENT_TYPES
    check_ids += [
        general_configs.RSCI_PART_DIMENSION_TYPE,
        general_configs.RSCI_PART_GRAMS_VALUE_UUID,
        general_configs.RSCI_PART_TYPE_VALUE_UUID,
    ]
    # Add value_ids from the mapping configs
    for config in general_configs.ALL_MAPPING_CONFIGS:
        for field, dtype, vals in config.get('default_values', []):
            skip = True
            for check in ['_type', '_language', '_unit']:
                if check in field:
                    skip = False
            if skip:
                continue
            if not isinstance(vals, list):
                vals = [vals]
            for val in vals:
                if not val in check_ids:
                    check_ids.append(val)
    return check_ids


def validate_prelabel_value_id(value_id, db_url=general_configs.ARCHES_DB_URL):
    """Check if a value id exists in the database."""
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
    WHERE vpf.valueid = '{value_id}'
    AND vpf.valuetype = 'prefLabel'
    LIMIT 1;
    """
    df = pd.read_sql(sql, engine)
    d_list = df.to_dict(orient='records')
    if len(d_list) == 0:
        return None
    return d_list[0]


def validate_all_configs_concept_prelabel_value_ids():
    check_ids = get_all_configs_concept_prelabel_value_ids()
    for value_id in check_ids:
        result = validate_prelabel_value_id(value_id)
        if not result:
            print('-' * 80)
            print(f"Value id {value_id} does not exist in the database.")
            print('-' * 80)
            continue
        print(f"Value id {value_id} exists in the database.")
        print(str(result))
