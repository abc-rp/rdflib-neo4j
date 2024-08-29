from rdflib import Graph, Namespace

from rdflib_neo4j import HANDLE_VOCAB_URI_STRATEGY, Neo4jStore, Neo4jStoreConfig

# Define your new prefixes
prefixes = {
    "brcomp": Namespace("https://w3id.org/brcomp#"),
    "brot": Namespace("https://w3id.org/brot#"),
    "brstr": Namespace("https://w3id.org/brstr#"),
    "dot": Namespace("https://w3id.org/dot#"),
    "ins": Namespace("http://example.org/sibbw79366620/RelocBridgeModel#"),
    "owl": Namespace("http://www.w3.org/2002/07/owl#"),
    "rdfs": Namespace("http://www.w3.org/2000/01/rdf-schema#"),
    "reloc": Namespace("https://w3id.org/reloc#"),
    "sib": Namespace("http://example.org/sibbw79366620#"),
    "asb": Namespace("https://w3id.org/asbingowl/core#"),
    "xsd": Namespace("http://www.w3.org/2001/XMLSchema#"),
}

# Neo4j connection credentials
auth_data = {
    "uri": "bolt://localhost:7687",  # Update this if your Neo4j is hosted differently
    "database": "neo4j",
    "user": "neo4j",
    "pwd": "password",
}

# Define your Neo4jStoreConfig
config = Neo4jStoreConfig(
    auth_data=auth_data,
    custom_prefixes=prefixes,
    handle_vocab_uri_strategy=HANDLE_VOCAB_URI_STRATEGY.KEEP,
    batching=True,
)

# Update the file_path to point to the local TTL file
file_path = "./MaintenanceBridgeModel.ttl"

# Initialize the graph store with the Neo4jStore configuration
graph_store = Graph(store=Neo4jStore(config=config))

# Parse the TTL file into the graph store
graph_store.parse(file_path, format="ttl")

# Close the graph store and persist any changes
graph_store.close(True)
