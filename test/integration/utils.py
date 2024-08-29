import os
from test.integration.constants import GET_NODES_PROPS_QUERY, GET_RELS_QUERY, RDFLIB_DB

from neo4j import Record
from rdflib import Graph


def records_equal(record1: Record, record2: Record, rels=False):
    """
    Used because a test is failing because the sorting of the labels is different:
    Full diff:
    - <Record uri='http://neo4j.org/ind#neo4j355' labels=['Resource', 'AwesomePlatform', 'GraphPlatform'] props={'name': 'neo4j', 'version': '3.5.5', 'uri': 'http://neo4j.org/ind#neo4j355'}>
    ?                                                                                  -----------------
    + <Record uri='http://neo4j.org/ind#neo4j355' labels=['Resource', 'GraphPlatform', 'AwesomePlatform'] props={'name': 'neo4j', 'version': '3.5.5', 'uri': 'http://neo4j.org/ind#neo4j355'}>
    ?                                                                 +++++++++++++++++
    """
    if not rels:
        for key in record1.keys():
            if key == "props":
                for prop_name in record1[key]:
                    if not sorted(record1[key][prop_name]) == sorted(
                        record2[key][prop_name]
                    ):
                        return False
            elif key == "labels":
                if not sorted(record1[key]) == sorted(record2[key]):
                    return False
            else:
                if not record1[key] == record2[key]:
                    return False
    else:
        for key in record1.keys():
            if record1[key] != record2[key]:
                return False
    return True


def read_file_n10s_and_rdflib(
    neo4j_driver,
    graph_store,
    batching=False,
    n10s_params=None,
    n10s_mappings=None,
    get_rels=False,
    file_path="../test_files/n10s_example.ttl",
    n10s_file_format="'Turtle'",
    rdflib_file_format="ttl",
):
    """Compare data imported with n10s procs and n10s + rdflib"""
    if n10s_mappings is None:
        n10s_mappings = []
    if n10s_params is None:
        n10s_params = {"handleVocabUris": "IGNORE"}

    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), file_path))
    rdf_payload = g.serialize(format=rdflib_file_format)

    neo4j_driver.execute_query(
        "CALL n10s.graphconfig.init($params)", params=n10s_params
    )
    for prefix, mapping in n10s_mappings:
        neo4j_driver.execute_query(prefix)
        neo4j_driver.execute_query(mapping)

    records = neo4j_driver.execute_query(
        f"CALL n10s.rdf.import.inline($payload, {n10s_file_format})",
        payload=rdf_payload,
    )
    assert records[0][0]["terminationStatus"] == "OK"

    graph_store.parse(data=rdf_payload, format=rdflib_file_format)
    # When batching we need to close the store to check that all the data is flushed
    if batching:
        graph_store.close(True)
    records, summary, keys = neo4j_driver.execute_query(GET_NODES_PROPS_QUERY)
    records_from_rdf_lib, summary, keys = neo4j_driver.execute_query(
        GET_NODES_PROPS_QUERY, database_=RDFLIB_DB
    )
    n10s_rels, rdflib_rels = None, None
    if get_rels:
        n10s_rels, summary, keys = neo4j_driver.execute_query(GET_RELS_QUERY)
        rdflib_rels, summary, keys = neo4j_driver.execute_query(
            GET_RELS_QUERY, database_=RDFLIB_DB
        )
    return records_from_rdf_lib, records, rdflib_rels, n10s_rels
