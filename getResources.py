from rdflib import Graph,ConjunctiveGraph
import json
import pickle
import validators
from datetime import datetime
import urllib2
import threading
from urlparse import urlparse
import traceback

import sys
reload(sys)
sys.setdefaultencoding('utf8')


numResources = 0

dindex = {}

durls = {}
gurls = {}

triplesProcessed = 0
inputFilename = ""
logFile = ""
workers = []

def writeError(err,traceback):
	global logFile
	lg = "Error: Triple:"+str(triplesProcessed)+"\n"
	lg = lg + traceback.format_exc()
	lg = lg + "\n"
        logFile.write(lg)


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

	try:
		urlParts = urlparse(rURL)
	except Exception as err:
		writeError(err,traceback)
		return None
		
	namespace = urlParts.scheme+"://"+urlParts.netloc+urlParts.path
	onamespace = urlParts.scheme+"://"+urlParts.netloc

	try:
		if (len(urlParts.query) == 0 and len(urlParts.fragment) == 0):
			slindex = urlParts.path.rfind("/")
			sl = urlParts.path[slindex:]
			if len(sl.replace(" ", "")) != 0:
				sepType = getDataType(sl)
				namespace = namespace[:namespace.rfind("/")] + "/" + getDataType(sl)
				onamespace = namespace[:namespace.rfind("/")] + "/"
		else:
			onamespace = onamespace + urlParts.path
      	except Exception as err:
                writeError(err,traceback)
                return None

	 
	try:
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
	except Exception as err:
                writeError(err,traceback)
                return None

	try:
		if len(urlParts.fragment) > 0:
			namespace = namespace + "#" + getDataType(urlParts.fragment)
			sep = "#"	
	except Exception as err:
                writeError(err,traceback)
                return None

	return [namespace,onamespace,sep,sepType]

def addURL(rURL,role):
	global durls
	
	URLKeys = getURLKey(rURL)
	if URLKeys == None:
		return

	URLKey,namespace,sep,sepType = URLKeys
	
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

	try:
		urlParts = urlparse(rURL)
	except Exception as err:
                writeError(err,traceback)
                return

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
	try:
		g.parse(data=line,format="nquads")
		for s,p,o in g:
			if (validators.url(s)):
				addResource(s,"subject")
			if (validators.url(o)):
				addResource(o,"object")
			addURL(p,"predicate")
	except Exception as err:
		writeError(err,traceback)
		return 
	try:
		for s in g.contexts():
			addGURL(s._Graph__identifier)
	except Exception as err:
		writeError(err,traceback)
		return
		
	triplesProcessed = triplesProcessed + 1
	counter = open(inputFilename+"counter","w")
	counter.write(inputFilename+" "+str(triplesProcessed)+"\n")
	counter.close()
	print "Triples Processed:"+str(triplesProcessed)+" Resources added:"+str(numResources)

def main():
	global triplesProcessed
	global inputFilename
	global logFile

	inputFilename = sys.argv[1]
	inputFilename = inputFilename[:inputFilename.rfind("/")+1]
	f = open(sys.argv[1],"r")
	directory = inputFilename[:inputFilename.rfind("/")+1] 
	#f = urllib2.urlopen("http://ci.emse.fr/dump/dmp/dump.nq")
	logFile = open(directory+"log","w")
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
