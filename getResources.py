from rdflib import Graph
import pickle
from urlparse import urlparse
import validators

numResources = 0
dindex = {}

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

def addResource(rURL):
	global numResources
	urlParts = urlparse(rURL)
	host = urlParts.netloc
	if host not in dindex.keys():
		dindex[host] = []
	if rURL not in dindex[host]:
		dindex[host].append(rURL)
		numResources = numResources + 1
		print rURL,numResources	

def iterTriples():
	f = open("test.nq","r")
	tp = 1
	for line in f:
		t = line.split(" ")	
		s = t[0]
		s = s[1:len(s)-1]
		o = t[2]
		o = o[1:len(o)-1]
		if (validators.url(s)):
			addResource(s)
		if (validators.url(o)):
			addResource(o)
		print
		print "Triples Processed:"+str(tp)
		print "Resources Added:"+str(numResources)
		tp = tp + 1
iterTriples()
writeToFile()
