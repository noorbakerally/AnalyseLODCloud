from rdflib import Graph,ConjunctiveGraph
import json
import pickle
import validators
from datetime import datetime
import sys
import urllib2
import threading
from urlparse import urlparse

numResources = 0

dindex = {}

durls = {}
gurls = {}

triplesProcessed = 0
inputFilename = ""
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
	
	if URLKey not in durls.keys():
		durls[URLKey] = {"subject":0,"predicate":0,"object":0,"sep":sep,"sepType":sepType,"sampleURL":rURL}
	durls[URLKey][role] =  durls[URLKey][role] + 1	
	
def addGURL(rURL):
	role = "occurence"
        global gurls
        URLKey,namespace,sep,sepType = getURLKey(rURL)

        if URLKey not in gurls.keys():
                gurls[URLKey] = {"occurence":0,"sep":sep,"sepType":sepType,"sampleURL":rURL}
        gurls[URLKey][role] =  gurls[URLKey][role] + 1


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

def processTriple(line):
	global triplesProcessed
	global inputFilename

	g = ConjunctiveGraph()
	g.parse(data=line,format="nquads")
	for s,p,o in g:
		if (validators.url(s)):
			addResource(s,"subject")
		if (validators.url(o)):
			addResource(o,"object")
		addURL(p,"predicate")
	
	for s in g.contexts():
		addGURL(s._Graph__identifier)
	triplesProcessed = triplesProcessed + 1
	counter = open(inputFilename+"counter","w")
	counter.write(inputFilename+" "+str(triplesProcessed)+"\n")
	counter.close()
	print "Triples Processed:"+str(triplesProcessed)+" Resources added:"+str(numResources)

def main():
	global triplesProcessed
	global inputFilename
	inputFilename = sys.argv[1]
	inputFilename = inputFilename[:inputFilename.rfind("/")+1]
	f = open(sys.argv[1],"r")
	directory = inputFilename[:inputFilename.rfind("/")+1] 
	#f = urllib2.urlopen("http://ci.emse.fr/dump/dmp/dump.nq")
	for line in f:
		processTriple(line)
		#if (triplesProcessed > 100):
		#	break
	writeToFile()
	with open('URLTemplate', 'w') as outfile:
    		json.dump(durls, outfile)
	with open('GURLTemplate', 'w') as goutfile:
    		json.dump(gurls, goutfile)

main()
