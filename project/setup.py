#!/usr/bin/python

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.cqlengine import connection

KEYSPACE = "test"

class cassandra(object):

    def __init__(self):

        global KEYSPACE

        self.cluster = Cluster(["10.244.35.35"])
        print("start a cluster")

        self.session = self.cluster.connect(KEYSPACE)
        #self.session.execute("CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = \
        #            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
        #print("create demo keyspace.")
    	self.session.default_timeout = 30
        self.session.set_keyspace(KEYSPACE)
        print("start a demo session")

        ''' config cqlengine connection'''
        self.session.row_factory = dict_factory
        connection.set_session(self.session)
        print("cassandra setup completed.")

def main():
    cass = cassandra()

if __name__ == "__main__":
    main()

