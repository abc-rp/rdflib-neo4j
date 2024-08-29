from test.integration.utils import read_file_n10s_and_rdflib, records_equal


def test_read_json_ld_file(neo4j_driver, graph_store):
    """Compare data imported with n10s procs and n10s + rdflib in single add mode"""
    records_from_rdf_lib, records, _, _ = read_file_n10s_and_rdflib(
        neo4j_driver,
        graph_store,
        file_path="../test_files/n10s_example.json",
        n10s_file_format="'JSON-LD'",
        rdflib_file_format="json-ld",
    )
    assert len(records_from_rdf_lib) == len(records)
    for i in range(len(records)):
        assert records_equal(records[i], records_from_rdf_lib[i])
