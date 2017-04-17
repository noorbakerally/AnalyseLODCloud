from threading import Thread
from SourceDownloader import Source
import urllib2
from rdflib import Graph
import sys
reload(sys)
sys.setdefaultencoding('utf8')

allstats = {
          "GT":{"ST":"=0","OT":"=0","GT":">0","NumCase":0,"PNumCase":0},
          "ST":{"ST":">0","OT":"=0","GT":"=0","NumCase":0,"PNumCase":0},
          "OT":{"ST":"=0","OT":">0","GT":"=0","NumCase":0,"PNumCase":0},
          "ST,OT":{"ST":">0","OT":">0","GT":"=0","NumCase":0,"PNumCase":0},
          "ST,GT":{"ST":">0","OT":"=0","GT":">0","NumCase":0,"PNumCase":0},
          "OT,GT":{"ST":"=0","OT":">0","GT":">0","NumCase":0,"PNumCase":0},
          "ST,OT,GT":{"ST":">0","OT":">0","GT":">0","NumCase":0,"PNumCase":0},
          "Empty":{"ST":"=0","OT":"=0","GT":"=0","NumCase":0,"PNumCase":0}
          }

directory = "/home/bakerally/Documents/repositories/emse_gitlab/LODRDFAnalysis/test/"
f = open(directory+"repstat","r")
statusFileName = directory+"downloadStatus"
statusFile = open(statusFileName,"a")
workers = []
for line in f:
	if ">" in line:
		continue
	print line
	lineParts = line.split(",")
	sourceName = lineParts[0]
	code = lineParts[1]
	numSources = lineParts[2]
	sourceFilename = directory + code
	
	s = Source(sourceName,code,numSources,sourceFilename,allstats,statusFile)
	t = Thread(target=s.evaluate)
	workers.append(t)
	#statusFile.close()
for t in workers:
	t.start()
for t in workers:
	t.join()
#print allstats
