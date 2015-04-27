#!/usr/bin/python
# description       :A worker process map/reduce input files.
# author            :Yuemin Li
# python_version    :2.7.6

import socket
import string
import re
import glob
import pickle
import logging
import threading
import sys

import uuid
from cassandra.cqlengine import connection
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine import columns
from cassandra.cqlengine.query import LWTException

KEYSPACE = "test"


class Map_0(Model):
    __keyspace__ = KEYSPACE
    Index = columns.UUID(primary_key = True, default=uuid.uuid4)
    Word = columns.Text(index = True)
#print("create a Map table")

class Reduce_0(Model):
    __keyspace__ = KEYSPACE
    Word = columns.Text(primary_key = True)
    Count = columns.Integer()
#print("create a Reduce table")

class worker(object):
    '''worker process map/reduce input files.'''

    def __init__(self, master_ip, master_port, worker_ip, worker_port):
        self.master_addr = (master_ip, master_port)
        self.worker_addr = (worker_ip, worker_port)
        self.sock = None
        
        
    def tokenize(self, input_file):
        '''return a list of words splited from a input file.'''
        with open(input_file, "r") as f:
            text = f.read().lower()
            regex = re.compile('[^a-z0-9]+')
            words = regex.split(text)
            words = filter(None, words)
        logging.info("tokenize a file...")
        return words

    def map(self, word):
        Map_0.create(Word = word)
        #print("create [%s] in Map table." % word )
        
    # if_not_exists()
    def reduce(self):
	print("start reduceing.")
        p = Map_0.objects.all()
	for row in p:
	    word = row.Word
	    q = Map_0.objects.filter(Word = word).consistency(1)
	    count = q.count()
	    try:
		Reduce_0.if_not_exists().create(Word = word, Count = count)
        	#print("create [%s]:[%i] in Reduce table." % (word, count))
	    except LWTException as e:
		pass
    
    def cleanup(self):
        '''delete all rows in map table.'''
        objects_map = Map_0.objects.all()
        objects_reduce = Reduce_0.objects.all()  
        for o in objects_map:
            Map_0.delete(o)
            print("delete row [%s] in Map table." % o.Word)
        for o in objects_reduce:
            Reduce_0.delete(o)
            print("delete row [%s] in Reduce table." % o.Word)

    def reduced(self):
        '''return a tuple list of (word, count)'''
        list_t = []
        objects = Reduce_0.objects.all()
        for o in objects:
           list_t.append((o.Word, o.Count))
        #print list_t
        return list_t 
    
    def processing(self):
	print("start processing...")
	with open("./worker_file.txt", 'rU') as f:
            for eachline in f:
		word = eachline.rstrip()  
            	self.map(word)
            	#self.reduce(eachword)
	print("start reducing...")
	self.reduce()
        reduced_res = self.reduced()
        return reduced_res

    def sendtomaster(self, wordcount):
        '''send each (word, count) tuple to master collection'''
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.master_addr)
        logging.debug("connected to ", self.master_addr)
        try:
            sock.sendall(pickle.dumps(wordcount))
            logging.debug("sending to master", wordcount)
        except Exception as msg:
            logging.error("got an exception!")
            print(msg)
        finally:
            sock.close()  
    
    def reply(self, msg):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.master_addr)
        sock.sendall(msg)
        print("send msg", msg, "to", self.master_addr)
        sock.close()
    
    def listen(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #reuse socket, avoid already in use error introduced by TCP
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(self.worker_addr)
        self.sock.listen(5)
        print ("worker is listening on ", self.worker_addr) 
        

    def stop_listen(self):
        self.sock.close()
        print("close listening...")

    def echo_hello(self):
        connection, src_addr = self.sock.accept()
        msg = connection.recv(64)
        print("recv msg", msg)
        if msg == "hello":
            self.reply("hello")
        #elif msg == "terminate":
        #    self.stop_listen(self.sock)
        #    print("this worker is terminating....")
        #    sys.exit(0)


     
    def recvfile(self):
        connection, src_addr = self.sock.accept() 
        with open("./worker_file.txt", "w") as f:            
            while True:
                data = connection.recv(4096)
                if not data:
                    break
                f.write(data)
        print("a file is received.")
        

class echo_alive(threading.Thread):
    def __init__(self, master_ip, worker_ip):
        super(echo_alive, self).__init__()
        self.echo_master_addr = (master_ip, 9999)
        self.echo_worker_addr = (worker_ip, 6666)
        self.echo_sock = None
    
    def listen(self):
        self.echo_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #reuse socket, avoid already in use error introduced by TCP
        self.echo_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.echo_sock.bind(self.echo_worker_addr)
        self.echo_sock.listen(5)
        print ("worker is echoing on ", self.echo_worker_addr) 

    def run(self):
        self.listen()
        while True:
            connection, src_addr = self.echo_sock.accept()
            msg = connection.recv(64)
            #print("recv msg", msg)
            if msg == "alive":
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(self.echo_master_addr)
                sock.sendall("alive")
                sock.close()
            elif msg == "stop":
                print("get stop echo msg from master")
                self.echo_sock.close()
                break
                 

def main():

    #cass = cassandra()
    #print("creating cassandra object.")
    connection.setup(['10.244.35.35'], "test", 1)
    print("set up a connection to cassandra cluster")
   
    echo = echo_alive("10.244.35.35", "10.244.35.35") 
    echo.start() 
    while True:
	drop_table(Map_0)
    	print("droping map table if exists.")
	drop_table(Reduce_0)
    	print("droping reduce table if exists.")
    	sync_table(Map_0)
    	print("creating map table.")
    	sync_table(Reduce_0)
    	print("creating reduce table.")
	print("start a worker task")
        wk = worker("10.244.35.35", 54321, "10.244.35.35", 12345)
        #wk.cleanup()
        wk.listen()
        wk.echo_hello()
        wk.recvfile()
        #wk.stop_listen()
        
        word_counts = wk.processing()
        print("word count to be send: ", word_counts)
        for tup in word_counts:
            wk.sendtomaster(tup)
        end_msg = ("###done###", 0)
        wk.sendtomaster(end_msg)

        print("all sent, end of this worker task") 

        
if __name__ == "__main__":
    main()
    
    
