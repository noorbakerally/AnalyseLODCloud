t = '''_:httpx3Ax2Fx2Flovx2Eokfnx2Eorgx2Fdatasetx2Flovx2Flovx2Erdfxxnode185mt8qcdx78743 <http://purl.org/stuff/rev#text> "[ADD_PUBLISHER: http://swan.mindinformatics.org]"@EN <http://lov.okfn.org/dataset/lov/lov.rdf> . '''
from rdflib import ConjunctiveGraph
cg = ConjunctiveGraph()
cg.parse(data = t, format = "nquads")
for s,p,o in cg:
	print s,p,o
