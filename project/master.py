#!/usr/bin/python
# description       :A master process distribute work and gathering result.
# author            :Yuemin Li
# python_version    :2.7.6

import socket
import operator
import argparse
import threading
import pickle
import logging
import time
import glob
import random
import os

wordcount_all = {}
lock = threading.Lock()
ms_wk = {   0   :{  "master"    :("10.244.35.35", 54321), \
                    "worker"    :("10.244.35.35", 12345), \
                    "status"    :True,\
                    "files"     :[]},\
	1   :{  "master"    :("10.244.35.35", 54322), \
                "worker"    :("10.244.35.91", 12345), \
                "status"    :True,\
                "files"     :[]},\
	2   :{  "master"    :("10.244.35.35", 54323), \
                "worker"    :("10.244.35.104", 12345), \
                "status"    :True,\
                "files"     :[]},\
	3   :{  "master"    :("10.244.35.35", 54324), \
                "worker"    :("10.244.35.119", 12345), \
                "status"    :True,\
                "files"     :[]},\
	4   :{  "master"    :("10.244.35.35", 54325), \
                "worker"    :("10.244.35.133", 12345),\
                "status"    :True,\
                "files"     :[]}}
file_status = {}



class monitor(threading.Thread):
    def __init__(self):
        super(monitor, self).__init__()
        global ms_wk
        global file_status
        self.echo_master_addr = (ms_wk[0]["master"][0], 9999)
        self.sock = None
   
    
    def listen(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(self.echo_master_addr)
        self.sock.listen(5)
        print("start listening echo on  ", self.echo_master_addr)
    
    def checkfilestatus(self):
        '''return true when all file has been done.'''
        for f in file_status:
            if not file_status[f]:
                return False
        return True
    
    def countalive(self):
        '''return true is all workers are alive.'''
        count = 0
        for num in ms_wk:
            if ms_wk[num]["status"] is True:
                count += 1
        return count == len(ms_wk)
            
    def run(self):
        self.listen()
        while self.countalive() is True and self.checkfilestatus() is not True:
            for num in ms_wk:
                echo_worker_addr = (ms_wk[num]["worker"][0], 6666)
                time.sleep(3)
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(echo_worker_addr)
                    sock.sendall("alive")
                    #print("say alive to worker", echo_worker_addr)
                    connection, client_addr = self.sock.accept()
                    msg = connection.recv(64)
                    if msg == "alive":
                        #print("recv alive from worker", num)
                        sock.close()
                except:
                    ms_wk[num]["status"]=False
                    sock.close()
                    print("detected a worker is dead", num)
                    break
        
        #self.checkfilestatus is True
        print("all work has been done/found a dead worker, stop monitor.",\
                 self.countalive(), self.checkfilestatus()) 
 
    
    
class master(threading.Thread):
    '''master thread updating a global wordcount dict.'''

    def __init__(self, num):
        super(master, self).__init__()
        global lock
        global wordcount_all
        global file_status
        global ms_wk
        self.master_addr = ms_wk[num]["master"]
        self.worker_addr = ms_wk[num]["worker"]
        #self.filename = filename
        self.wordcounts = {}
        self.num = num
        self.sock = None
    
    def sayhello(self):
        '''return true on active worker, false on timeout/dead worker.'''
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(self.worker_addr)
            sock.sendall("hello")
            print("say hello to worker", self.worker_addr)
            connection, client_addr = self.sock.accept()
            msg = connection.recv(64)
            if msg == "hello":
                print("recv hello from worker")
                sock.close()
                return True
            sock.close()
        except:
            print("worker is dead.", self.worker_addr)
            sock.close()
            return False
    
     
    def sendfile(self):
        if ms_wk[self.num]["status"]:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(self.worker_addr)
            for filename in ms_wk[self.num]["files"]:
                with open(filename, "r") as f:
                    while True:
                        data = f.read(4096)
                        if not data:
                            break
                        sock.sendall(data)
                print(filename, "has been sent to ", self.worker_addr)
            sock.close()
        else:
            print("worker is dead, do nothing")
    
    def listen(self):
        try: 
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(self.master_addr)
            self.sock.listen(5)
            print("start listening on  ", self.master_addr)
            return True
        except:
            print("worker has been occupied, try another one")
            return False
    
    def stop_listen(self):
        self.sock.close()
        print("stop listening...")
        
 
    def recvcount(self):
        print("waiting for results, set timeout")
        self.sock.settimeout(2) 
        while True:
            if ms_wk[self.num]["status"]:
                try:
                    connection, client_addr = self.sock.accept()
                    buff = connection.recv(64)
                    word_t = pickle.loads(buff)
                    #print("received ", word_t)
                except:
                    #print("socket.timeout")
                    continue
                
                if word_t[0] == "###done###":
                    print("all word count received.")
                    break 
                if word_t[0] in self.wordcounts:
                    self.wordcounts[word_t[0]] += word_t[1]
                else:
                    self.wordcounts[word_t[0]] = word_t[1]
            else:
                print("this worker is dead, do nothing.")
                break

    def addtoglobalcount(self):
        if ms_wk[self.num]["status"]:
            for w in self.wordcounts:
                with lock:
                    if w in wordcount_all:
                        wordcount_all[w] += self.wordcounts[w]
                    else:
                        wordcount_all[w] = self.wordcounts[w]
        else:
            print("this worker is dead, do nothing.")
                
    def changefilestatus(self):
        if ms_wk[self.num]["status"]:
            for filename in ms_wk[self.num]["files"]:
                 file_status[filename] = True
            print("mark all file in this task to be true.")
        else:
            print("file status not changed.")

    def run(self):
        '''set worker available when thread is done.'''
        self.listen()
        if not self.sayhello():
            self.stop_listen()
            ms_wk[self.num]["status"] = False
            print("thread fail.")
        else:
            self.sendfile()
            self.recvcount()
            self.addtoglobalcount()
            self.stop_listen()
            self.changefilestatus()
            print("thread success, release the status.")
            #self.changefilestatus("done")
        print("this thread is stopping...")


def checkfilestatus(files):
    '''return true on all file in files are done.'''
    for f in files:
        if not file_status[f]:
            return False
    return True
        

def getfiles(directory):
    global ms_wk
    files = glob.glob(directory)
    return files

def allocatework(files):
	global ms_wk
	allo = {}
	for num in ms_wk:
		allo[num] = 0
	for f in files:
		size = os.path.getsize(f)
		sorted_allo = sorted(allo.items(), key=operator.itemgetter(1))
		index = sorted_allo[0][0]
		ms_wk[index]["files"].append(f)
		allo[index] += size
	print("files are allocated")

def createfilestatus(files):
    for filename in files:
        file_status[filename] = False
    print("file status has been created.")

def terminater():
    '''terminating the alive workers'''
    pass


def finddead():
    for num in ms_wk:
        if ms_wk[num]["status"] is False:
            return num
    return -1
    

def main():
    t0 = time.time()
   
    files = getfiles("./demo/*.txt")
    createfilestatus(files) 
    allocatework(files)
    
    threads = [] 
    for num in ms_wk:
        if ms_wk[num]["status"]:
            thread = master(num)
            thread.start()
            threads.append(thread)
            print("start a thread at worker", num)
        else:
            #do nothing
            pass

    thread_monitor = monitor()
    thread_monitor.start()
    threads.append(thread_monitor)
    
    for t in threads:
        t.join()
   
     
    dead = finddead()
    if dead is -1:   
        with open("output.txt", 'w') as f:
            for w in wordcount_all:
                f.write("%s %i\n" % (w, wordcount_all[w]))
        
        total_time = time.time()-t0
        print("use time in seconds", total_time)
        
        logging.info("Master is terminating...")
        print("Master is terminating...")
        return 
    else:
        files_remain = ms_wk[dead]["files"]
        del ms_wk[dead]
        print("delete worker in ms_wk", dead)
        for num in ms_wk:
            ms_wk[num]["files"] = []
        print("clean up file tasks in ms_wk")
        allocatework(files_remain)
        
        time.sleep(20)#wait for cleanup in worker 
        threads_remain = [] 
        for num in ms_wk:
            if ms_wk[num]["status"]:
                thread = master(num)
                thread.start()
                threads_remain.append(thread)
                print("start a thread at worker again", num)
            else:
                #do nothing
                pass
        for t in threads_remain:
            t.join()
        
        with open("output.txt", 'w') as f:
            for w in wordcount_all:
                f.write("%s %i\n" % (w, wordcount_all[w]))
        
        total_time = time.time()-t0
        print("use time in seconds", total_time)
        
        logging.info("Master is terminating...")
        print("Master is terminating...")
        return 
    
     

if __name__ == "__main__":
    main()
