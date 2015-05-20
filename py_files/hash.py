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
        print("USAGE: hash.py <keyspace> <table1> <table2> <eq col>")
        sys.exit(1)

    _, keyspace, table1, table2, eqcol = sys.argv
	
    # your code here

    session.set_keyspace(keyspace)
	
    rows = session.execute("""Select * from table1""")
	

if __name__ == "__main__":
    main()
    
