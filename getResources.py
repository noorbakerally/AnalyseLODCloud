from rdflib import Graph
import json
import pickle
from urlparse import urlparse
import validators
from datetime import datetime
import sys

numResources = 0
dindex = {}
durls = {}
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
	if URLKey not in durls.keys():
		durls[URLKey] = {"subject":0,"object":0,"sep":sep,"sepType":sepType,"sampleURL":rURL}
	durls[URLKey][role] =  durls[URLKey][role] + 1	
	

def addResource(rURL,role):
	global numResources

	urlParts = urlparse(rURL)
	host = urlParts.netloc
	addURL(rURL,role)

	if host not in dindex.keys():
		dindex[host] = []

	if rURL not in dindex[host]:
		dindex[host].append(rURL)
		numResources = numResources + 1

def iterTriples():
	f = open(sys.argv[1],"r")
	tp = 1
	for line in f:
		t = line.split(" ")	
		s = t[0]
		s = s[1:len(s)-1]
		o = t[2]
		o = o[1:len(o)-1]
		if (validators.url(s)):
			addResource(s,"subject")
		if (validators.url(o)):
			addResource(o,"object")
		#print "Triples Processed:"+str(tp)
		#print "Resources Added:"+str(numResources)
		counter = open("counter","w")
		counter.write(str(tp)+"\n")
		counter.close()
		tp = tp + 1
iterTriples()
writeToFile()

with open('URLTemplate', 'w') as outfile:
    json.dump(durls, outfile)
