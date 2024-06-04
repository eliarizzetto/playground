from SPARQLWrapper import SPARQLWrapper, JSON

# Set up the SPARQL endpoint
sparql = SPARQLWrapper("https://opencitations.net/meta/sparql")

# Set the SPARQL query
# sparql.setQuery("""
#     PREFIX fabio: <http://purl.org/spar/fabio/>
#     PREFIX datacite: <http://purl.org/spar/datacite/>
#
#     SELECT (COUNT(?br) as ?count) WHERE
#     {
#       {
#         SELECT ?br WHERE {
#           ?identifier datacite:usesIdentifierScheme datacite:openalex ;
#                        ^datacite:hasIdentifier ?br .
#         }
#       }
#       ?br a fabio:Expression .
#     }
# """)

sparql.setQuery("""
    PREFIX fabio: <http://purl.org/spar/fabio/>
    PREFIX datacite: <http://purl.org/spar/datacite/>

    SELECT (COUNT(?identifier) as ?count) WHERE
    {
        ?identifier datacite:usesIdentifierScheme datacite:openalex .
    }
""")

# Set timeout (in milliseconds)
sparql.setTimeout(2700)  # 45 min timeout

# Set return format
sparql.setReturnFormat(JSON)

# Execute the query and print the result
try:
    result = sparql.query().convert()
    count = result['results']['bindings'][0]['count']['value']
    print("Total count:", count)
except Exception as e:
    print("An error occurred:", e)
