from SPARQLWrapper import SPARQLWrapper, JSON

newtypes = [
    'abstract',
    'archival document',
    'audio document',
    'computer program',
    # '',
    'data management plan',
    'editorial',
    'journal editorial',
    'newspaper',
    'newspaper article',
    'newspaper issue',
    'preprint',
    'presentation',
    'retraction notice'
]

sparql = SPARQLWrapper("https://opencitations.net/meta/sparql")
for t in newtypes:
    t = ''.join([w.capitalize() for w in t.split()])
    print(t)
    sparql.setQuery(f"PREFIX fabio: <http://purl.org/spar/fabio/> ASK  {{ ?x a fabio:{t} }}")
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    print(results)