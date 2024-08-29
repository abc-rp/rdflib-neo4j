from test.integration.constants import RDFLIB_DB
from test.integration.utils import read_file_n10s_and_rdflib, records_equal

from rdflib import Graph, Namespace

from rdflib_neo4j.config.const import (
    HANDLE_MULTIVAL_STRATEGY,
    HANDLE_VOCAB_URI_STRATEGY,
)
from rdflib_neo4j.config.Neo4jStoreConfig import Neo4jStoreConfig
from rdflib_neo4j.Neo4jStore import Neo4jStore


def test_read_file_multival_with_strategy_no_predicates(
    neo4j_driver, neo4j_connection_parameters
):
    """Compare data imported with n10s procs and n10s + rdflib in single add mode for multivalues"""

    auth_data = neo4j_connection_parameters

    # Define your prefixes
    prefixes = {}

    # Define your custom mappings
    custom_mappings = []

    multival_props_names = []

    config = Neo4jStoreConfig(
        auth_data=auth_data,
        custom_prefixes=prefixes,
        custom_mappings=custom_mappings,
        multival_props_names=multival_props_names,
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
        handle_multival_strategy=HANDLE_MULTIVAL_STRATEGY.ARRAY,
        batching=False,
    )

    graph_store = Graph(store=Neo4jStore(config=config))

    n10s_params = {"handleVocabUris": "IGNORE", "handleMultival": "ARRAY"}

    records_from_rdf_lib, records, _, _ = read_file_n10s_and_rdflib(
        neo4j_driver, graph_store, n10s_params=n10s_params
    )
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])


def test_read_file_multival_with_strategy_and_predicates(
    neo4j_driver, neo4j_connection_parameters
):
    """Compare data imported with n10s procs and n10s + rdflib in single add mode for multivalues"""
    auth_data = neo4j_connection_parameters

    # Define your prefixes
    prefixes = {"neo4voc": Namespace("http://neo4j.org/vocab/sw#")}

    # Define your custom mappings
    custom_mappings = []

    multival_props_names = [("neo4voc", "author")]

    config = Neo4jStoreConfig(
        auth_data=auth_data,
        custom_prefixes=prefixes,
        custom_mappings=custom_mappings,
        multival_props_names=multival_props_names,
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
        handle_multival_strategy=HANDLE_MULTIVAL_STRATEGY.ARRAY,
        batching=False,
    )

    graph_store = Graph(store=Neo4jStore(config=config))

    n10s_params = {
        "handleVocabUris": "IGNORE",
        "handleMultival": "ARRAY",
        "multivalPropList": ["http://neo4j.org/vocab/sw#author"],
    }
    records_from_rdf_lib, records, _, _ = read_file_n10s_and_rdflib(
        neo4j_driver, graph_store, n10s_params=n10s_params
    )
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])


def test_read_file_multival_with_no_strategy_and_predicates(
    neo4j_driver, neo4j_connection_parameters
):
    """Compare data imported with n10s procs and n10s + rdflib in single add mode for multivalues"""
    auth_data = neo4j_connection_parameters

    # Define your prefixes
    prefixes = {"neo4voc": Namespace("http://neo4j.org/vocab/sw#")}

    # Define your custom mappings
    custom_mappings = []

    multival_props_names = [("neo4voc", "author")]

    config = Neo4jStoreConfig(
        auth_data=auth_data,
        custom_prefixes=prefixes,
        custom_mappings=custom_mappings,
        multival_props_names=multival_props_names,
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
        batching=False,
    )

    graph_store = Graph(store=Neo4jStore(config=config))

    n10s_params = {
        "handleVocabUris": "IGNORE",
        "multivalPropList": ["http://neo4j.org/vocab/sw#author"],
    }
    records_from_rdf_lib, records, _, _ = read_file_n10s_and_rdflib(
        neo4j_driver, graph_store, n10s_params=n10s_params
    )
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])


def test_read_file_multival_array_as_set_behavior(
    neo4j_driver, neo4j_connection_parameters
):
    """When importing the data, if a triple will add the same value to a multivalued property it won't be added"""
    auth_data = neo4j_connection_parameters

    prefixes = {"music": Namespace("neo4j://graph.schema#")}

    custom_mappings = []

    multival_props = [("rdfs", "label")]
    config = Neo4jStoreConfig(
        auth_data=auth_data,
        custom_prefixes=prefixes,
        custom_mappings=custom_mappings,
        multival_props_names=multival_props,
        handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.IGNORE,
        handle_multival_strategy=HANDLE_MULTIVAL_STRATEGY.ARRAY,
        batching=False,
    )

    graph_store = Graph(store=Neo4jStore(config=config))

    payload1 = """<http://dbpedia.org/resource/Cable_One>	<http://dbpedia.org/property/name>	"Sparklight"@en .\
    <http://dbpedia.org/resource/Donald_E._Graham>	<http://www.w3.org/2000/01/rdf-schema#label>	"Donald Ernest. Graham II" .\
    <http://dbpedia.org/resource/Cable_One>	<http://dbpedia.org/ontology/owner>	<http://dbpedia.org/resource/Donald_E._Graham> .\
    <http://dbpedia.org/resource/Cable_One>	<http://dbpedia.org/ontology/netIncome>	"3.04391E8"^^<http://dbpedia.org/datatype/usDollar> .\
    """
    payload2 = """ <http://dbpedia.org/resource/Donald_E._Graham>	<http://www.w3.org/2000/01/rdf-schema#label>	"Donald Ernest. Graham II" . """

    payload3 = """ <http://dbpedia.org/resource/Donald_E._Graham>	<http://www.w3.org/2000/01/rdf-schema#label>	"Donald Ernest. Graham II" . """

    for p in [payload1, payload2, payload3]:
        graph_store.parse(data=p, format="ttl")

        records, summary, keys = neo4j_driver.execute_query(
            "MATCH (n) WHERE size(n.label) > 1 RETURN n", database_=RDFLIB_DB
        )

        assert len(records) == 0
