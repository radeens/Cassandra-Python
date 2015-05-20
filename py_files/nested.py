#!/usr/bin/env python

# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import sys

log = logging.getLogger()
#log.setLevel('DEBUG')
log.setLevel('WARN')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import dict_factory
from collections import defaultdict


def main():
    	cluster = Cluster(['127.0.0.1'])
    	session = cluster.connect()

    	session.row_factory = dict_factory

	if len(sys.argv) != 5:
		print("USAGE: nested.py <keyspace> <table1> <table2> <eq col>")
		sys.exit(1)

	_, keyspace, table1, table2, eqcol = sys.argv
	
	
	# your code here
	session.set_keyspace(keyspace)
	
	eq_col = eqcol
	rows1 = session.execute("""Select * from %s""" % table1)
	rows2 = session.execute("""Select * from %s""" % table2)
	
	counter = 0
	for r1 in rows1:
		for r2 in rows2:
			if r1[eq_col] == r2[eq_col]:
				x1 = r1.copy()
				x2 = r2.copy()
				x1.pop(eq_col)
				x2.pop(eq_col)
				a = dict()
				a[eq_col]=r1[eq_col]
				print a,x1, x2	
				counter+=1
	print "Count:",counter


if __name__ == "__main__":
    main()
