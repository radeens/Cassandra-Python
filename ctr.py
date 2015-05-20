#!/usr/bin/env python

import sys

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import dict_factory
from collections import defaultdict

KEYSPACE = "ctr"

def main():
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()
	
	session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % KEYSPACE)

	session.set_keyspace(KEYSPACE)
	
	session.execute("""
        CREATE TABLE IF NOT EXISTS ctrImp (
		OwnerId int,
		AdId int,
		numClicks int,
		numImpressions int,
		PRIMARY KEY (OwnerId, AdId)
	);
        """)


	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (1,1,1)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (1,2,0)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (1,3,1)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (1,4,0)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (2,1,0)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (2,2,0)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (2,3,0)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (2,4,0)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (3,1,1)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (3,2,0)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (3,3,2)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numClicks) VALUES (3,4,1)
""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (1,1,10)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (1,2,5)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (1,3,20)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (1,4,15)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (2,1,10)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (2,2,55)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (2,3,13)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (2,4,21)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (3,1,32)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (3,2,23)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (3,3,44)""")
	session.execute("""
INSERT INTO ctrImp (OwnerId, AdId, numImpressions) VALUES (3,4,36)""")

	rows = session.execute("""Select * from ctrImp""")
	print " OwnerId\t", "AdId\t", "Clicks\t", "Impressions\t"
	for row in rows:
        	print "  ", row[0],"\t\t", row[1],"\t", row[2],"\t", row[3]

	d = dict()
	for row in rows:
		if row[0] in d:
			d[row[0]][0]+= row[2]
			d[row[0]][1]+= row[3]
		else:
			d[row[0]] = [row[2],row[3]]
	
	print " OwnerId\t", "Clicks\t", "Impressions\t"

	for key in d.keys():
		print "  ", key,"\t\t", d[key][0],"\t", d[key][1]

	for row in rows:
		if row[0] == 1 and row[1] == 3:
			print " OwnerId\t", "AdId\t", "Clicks\t", "Impressions\t"
			print "  ", row[0],"\t\t", row[1],"\t", row[2],"\t", row[3]

	print " OwnerId\t", "Clicks\t", "Impressions\t"
	print "  2\t\t", d[2][0],"\t", d[2][1]





if __name__ == "__main__":
    main()


	
	
	
	
