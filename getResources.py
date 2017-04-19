from rdflib import Graph
import json
import pickle
from urlparse import urlparse
import validators
from datetime import datetime
import sys
import urllib2
import threading

numResourcesLock = threading.Lock()
numResources = 0

dindexLock = threading.Lock()
dindex = {}

durlsLock = threading.Lock()
durls = {}

triplesProcessedLock = threading.Lock()
triplesProcessed = 0

workers = []
def writeToFile():
	global dindex
	indexCounter = 1
	index = open("index","w") 
	for host in dindex.keys():
		line = host + ","+ str(indexCounter) + "\n"
		index.write(line)

		rfile = open(str(indexCounter),"w")
		for url in dindex[host]:
			rfile.write(url+"\n")
		rfile.close()
		
		indexCounter = indexCounter + 1
	index.close()

def getDataType(word):
	word = word[1:]
	if word == "" or word=="/":
		return "NULL"
	if word.isdigit():
		return "NUMBER"
	if validators.url(word):
		return "URL"
	return "STRING"

def getURLKey(rURL):
	sep = "/"
	sepType = "NULL"
	urlParts = urlparse(rURL)
	namespace = urlParts.scheme+"://"+urlParts.netloc+urlParts.path
	onamespace = urlParts.scheme+"://"+urlParts.netloc
	
	if (len(urlParts.query) == 0 and len(urlParts.fragment) == 0):
		slindex = urlParts.path.rfind("/")
		sl = urlParts.path[slindex:]
		if len(sl.replace(" ", "")) != 0:
			sepType = getDataType(sl)
			namespace = namespace[:namespace.rfind("/")] + "/" + getDataType(sl)
			onamespace = namespace[:namespace.rfind("/")] + "/"
	else:
		onamespace = onamespace + urlParts.path
        
	if len(urlParts.query) > 0:
                query = urlParts.query
                qparts = sorted(query.split("&"))
                namespace = namespace + "?"
                for qp in qparts:
                        if "=" in qp:
                                qps = qp.split("=")     
                                qp1 = qps[0]
                                qp2 = getDataType(qps[1])
                                namespace = namespace + qp1+"="+qp2 + "&"
                        else:
                                namespace = namespace + qp + "&"

        if len(urlParts.fragment) > 0:
                namespace = namespace + "#" + getDataType(urlParts.fragment)
		sep = "#"	
	return [namespace,onamespace,sep,sepType]

def addURL(rURL,role):
	global durls
	URLKey,namespace,sep,sepType = getURLKey(rURL)
	
	with durlsLock:
		if URLKey not in durls.keys():
			durls[URLKey] = {"subject":0,"predicate":0,"object":0,"sep":sep,"sepType":sepType,"sampleURL":rURL}
		durls[URLKey][role] =  durls[URLKey][role] + 1	
	

def addResource(rURL,role):
	global numResources

	urlParts = urlparse(rURL)
	host = urlParts.netloc
	addURL(rURL,role)

	with dindexLock:
		if host not in dindex.keys():
			dindex[host] = []

		if rURL not in dindex[host]:
			dindex[host].append(rURL)
			numResources = numResources + 1

def processTriple(line):
	global triplesProcessed
	t = line.split(" ")	
	s = t[0]
	s = s[1:len(s)-1]
	p = t[1]
	p = p[1:len(p)-1]
	o = t[2]
	o = o[1:len(o)-1]
	if (validators.url(s)):
		addResource(s,"subject")
	if (validators.url(o)):
		addResource(o,"object")
	addURL(p,"predicate")
	
	with triplesProcessedLock:
		triplesProcessed = triplesProcessed + 1
		counter = open("counter","w")
		counter.write(str(triplesProcessed)+"\n")
		counter.close()
		print "Triples Processed:"+str(triplesProcessed)+" Resources added:"+str(numResources)

def main():
	global triplesProcessed
	f = open("test.nq","r")
	#f = urllib2.urlopen("http://ci.emse.fr/dump/dmp/dump.nq")
	for line in f:
		worker = threading.Thread(target=processTriple, args=(line,))
		workers.append(worker)
		worker.start()
	for worker in workers:
    		worker.join()
	writeToFile()
	with open('URLTemplate', 'w') as outfile:
    		json.dump(durls, outfile)

main()
