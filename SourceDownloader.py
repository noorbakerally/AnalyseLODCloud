import time
import requests
import traceback
from rdflib import Graph
import threading
caseGT = threading.Lock()
caseST = threading.Lock()
caseOT = threading.Lock()
caseSTOT = threading.Lock()
caseSTGT = threading.Lock()
caseOTGT = threading.Lock()
caseSTOTGT = threading.Lock()
caseEmpty = threading.Lock()

def getST(g,rlink):
	st = 0
	gt = 0
	at = 0
	ot = 0

	fquery = ''' Select ?at ?st ?ot ?gt where {
		  {SELECT (count(*) as ?zt) where {
                    ?s ?p ?o .
                  }}
		  {SELECT (count(*) as ?st) where {
                    ?s ?p ?o .
                      FILTER (?s=<Resource>)
                  }}
                  {SELECT (count(*) as ?at) where {
                    ?s ?p ?o .
                  }}
                  {SELECT (count(*) as ?ot) where {
                    ?s ?p ?o .
                      FILTER (?o=<Resource>)
                  }}
                  {SELECT (count(*) as ?gt) where {
                    ?s ?p ?o .
                      FILTER (?o!=<Resource> && ?s!=<Resource>)
                  }}
                } '''
	query = fquery.replace("Resource",rlink.replace("\n",""))
        result = g.query(query)
        for p in result:
                at = str(p.at)
                st = str(p.st)
                ot = str(p.ot)
                gt = str(p.gt)
        return [st,ot,gt]


def setVal(sCases,st,ot,gt):
	st = int(st)
	ot = int(ot)
	gt = int(gt)
        if st == 0 and ot == 0 and gt > 0:
		with caseGT:
			sCases["GT"]["NumCase"] = sCases["GT"]["NumCase"] + 1

        elif st > 0 and ot == 0 and gt == 0:
		with caseST:
			sCases["ST"]["NumCase"] = sCases["ST"]["NumCase"] + 1

        elif st == 0 and ot > 0 and gt == 0:
		with caseOT:
			sCases["OT"]["NumCase"] = sCases["OT"]["NumCase"] + 1

        elif st > 0 and ot > 0 and gt == 0:
		with caseSTOT:
			sCases["ST,OT"]["NumCase"] = sCases["ST,OT"]["NumCase"] + 1

        elif st > 0 and ot == 0 and gt > 0:
		with caseSTGT:
			sCases["ST,GT"]["NumCase"] = sCases["ST,GT"]["NumCase"] + 1

        elif st == 0 and ot > 0 and gt > 0:
		with caseOTGT:
			sCases["OT,GT"]["NumCase"] = sCases["OT,GT"]["NumCase"] + 1

        elif st > 0 and ot > 0 and gt > 0:
		with caseSTOTGT:
			sCases["ST,OT,GT"]["NumCase"] = sCases["ST,OT,GT"]["NumCase"] + 1

        elif st == 0 and ot == 0 and gt == 0:
                with caseEmpty:
			sCases["Empty"]["NumCase"] = sCases["Empty"]["NumCase"] + 1

        return sCases

def evaluateRDF(fileurl,allstats,statusFile):
	headers={'Content-type': 'text/turtle,application/rdf+xml,text/ntriples,application/ld+json'}
	r = requests.get(fileurl,headers=headers,timeout=1)
	code = r.status_code
        try:
                content = r.text
                contentType =r.headers.get('Content-Type')
		print contentType
                if "xml" in contentType:
                        contentType = "xml"
                elif "turtle" in contentType:
                        contentType = "n3"
                elif "n3" in contentType:
                        contentType = "n3"
                elif "nt" in contentType:
                        contentType = "nt"
		else:
			contentType = "xml"
                g = Graph()
                g.parse(data=content,format=contentType)
		print "finished loading"
                stats = getST(g,fileurl)
                allstats = setVal(allstats,stats[0],stats[1],stats[2])
		
		statusFile.write("success:"+fileurl+","+str(code)+","+r.headers.get('Content-Type'))
		
		#print stats
		#print allstats
		return allstats

        except:
		traceback.print_exc()
		statusFile.write("error:"+fileurl+","+str(code)+","+r.headers.get('Content-Type'))
                return "error:" + " "+fileurl


class Source:
	sfile = ""

	def __init__(self,sName,sCode,sNumSources,sfile,allStats,statusFile):
		self.sfile = sfile
		self.f = open(sfile,"r")
		self.allStats = allStats
		self.statusFile = statusFile	
	
	def evaluate(self):
		for url in self.f:
			print url
			stats = evaluateRDF(url,self.allStats,self.statusFile) 
			time.sleep(2)
			#print url + str(stats)
#allstats = {
#          "GT":{"ST":"=0","OT":"=0","GT":">0","NumCase":0,"PNumCase":0},
#          "ST":{"ST":">0","OT":"=0","GT":"=0","NumCase":0,"PNumCase":0},
#          "OT":{"ST":"=0","OT":">0","GT":"=0","NumCase":0,"PNumCase":0},
#          "ST,OT":{"ST":">0","OT":">0","GT":"=0","NumCase":0,"PNumCase":0},
#          "ST,GT":{"ST":">0","OT":"=0","GT":">0","NumCase":0,"PNumCase":0},
#          "OT,GT":{"ST":"=0","OT":">0","GT":">0","NumCase":0,"PNumCase":0},
#          "ST,OT,GT":{"ST":">0","OT":">0","GT":">0","NumCase":0,"PNumCase":0},
#          "Empty":{"ST":"=0","OT":"=0","GT":"=0","NumCase":0,"PNumCase":0}
#          }
#evaluateRDF("http://www.semanlink.net/tag/mp3.rdf",allstats)
#evaluateRDF("http://www.semanlink.net/tag/mp3.rdf",allstats)
