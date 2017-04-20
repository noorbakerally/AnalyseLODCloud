from rdflib import Graph
g = Graph()

data = '''@prefix da:    <http://example.com/data/> .
@prefix on:    <http://example.com/on/> .

da:Tom  on:sister  da:James .

da:Jaabir  on:grandFather  da:Rafick .

da:Noor  on:sister  da:Shenaz .

da:Rita  on:friend  da:Noor ;
        on:sister  da:Tom .

da:Javed  on:child  da:Jaabir .

da:Shenaz  on:husband  da:Javed .
'''
g.parse("http://dbpedia.org/data/Paris.ttl",format="n3")
query = '''
PREFIX da: <http://example.com/data/> 
PREFIX on: <http://example.com/on/> 
SELECT (count(*) as ?lt)
WHERE {
    ?s ?p ?o .
  {{?s ((<>|!<>)|^(<>|!<>))* <Resource> } UNION {?o ((<>|!<>)|^(<>|!<>))* <Resource> } }
  FILTER (?s!=<Resource>)
  FILTER (?o!=<Resource>)
}'''
query = query.replace("Resource","http://dbpedia.org/resource/Paris")
r = g.query(query)
for p in r:
	print p
