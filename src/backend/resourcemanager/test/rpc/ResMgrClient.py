# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import socket, time, random, math
from optparse import OptionParser
from threading import Thread
import struct

################################################################################
# Functions for network communication.
################################################################################
def connectToRM():
    (opts,args) = parseCLIArgs()
    try:
        if opts.socktype == "domain" :
            conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            conn.connect(opts.sockdomainfile)
        else :
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect(opts.sockserver, opts.sockport)
    except socket.error, msg:
        print "ERROR : Fail to connect to server." + str(msg[0]) + " error message : " + msg[1]
        return None
    print "Connected to server."
    return conn

def sendMessageHead(conn,msgid,msgsize):
    header = struct.pack('<8sBBHI', 'MSGSTART', 0x80, 0, msgid, msgsize)
    try:
        conn.send(header)
    except socket.error, msg:
        print "ERROR : Fail to send message head." + str(msg[0]) + " error message : " + msg[1]
        return -1
    print "Sent message head, size " + str(len(header)) + " message id " + str(msgid) + " message size " + str(msgsize)
    return len(header)

def sendMessageEnd(conn):
    try:
        conn.send(struct.pack('<8s','MSGENDS!'))
    except socket.error, msg:
        print "ERROR : Fail to send message end tag." + str(msg[0]) + " error message : " + msg[1]
        return -1
    print "Sent message end tag."
    return 8

def sendMessage(conn, content):
    try:
        conn.send(content)
    except socket.error, msg:
        print "ERROR : Fail to send message content." + str(msg[0]) + " error message : " + msg[1]
        return -1
    return len(content)

def recvMessageHead(conn):
    try:
        recvheader = conn.recv(16)
    except socket.error, msg:
        print "ERROR : Fail to receive message head." + str(msg[0]) + " error message : " + msg[1]
        return (-1,-1)
    if len(recvheader) != 16 :
        print "ERROR : Fail to receive message head due to wrong size. ", len(recvheader)
        return (-1,-1)
    (message_begin,id1,id2,msgid,msgsize) = struct.unpack('<8sBBHI', recvheader)
    if  message_begin == "MSGSTART" and id1 == 0x80 and id2 == 0 :
        print "Received response message head message id " + str(msgid) + " message size " + str(msgsize)
        return (msgid,msgsize)
    else :
        print "ERROR : Fail to receive message head due to wrong content. ", message_begin, id1, id2
        return (-1,-1)

def recvMessageTail(conn):
    recv_tail = conn.recv(8)
    unpack_tail = struct.unpack('<8s', recv_tail)
    (message_end,) = unpack_tail
    if message_end == "MSGENDS!":
        return True
    else :
        print "ERROR: Wrong message tail. ", message_end
        return False

def recvMessage(conn, msgsize) :
    try:
        content = conn.recv(msgsize)
    except socket.error, msg :
        print "ERROR : Fail to receive message content." + str(msg[0]) + " error message : " + msg[1]
        return None
    print "Received response message content. size " + str(len(content))
    return content

################################################################################
# Functions for value check.
################################################################################
def expectRightHead(msgid,msgsize,expmsgid,expmsgsize):
    if expmsgid != -1 :
        if msgid != expmsgid :
            print "ERROR : Wrong message head message id ", msgid, " expect ", expmsgid
            return False
    if expmsgsize != -1 :
        if expmsgsize != msgsize :
            print "ERROR : Wrong message head message size ", msgsize, " expect ", expmsgsize
            return False
    # The message size must be 64bit aligned.
    if msgsize % 8 != 0 :
        print "ERROR : Wrong message head message size ", msgsize, " expect mod 8 value as 0"
        return False
    print "Passed message head check."
    return True

def expectRightContentSize(msgsize,expmsgsize) :
    # The message size must be 64bit aligned.
    if msgsize % 8 != 0 :
        print "ERROR : Wrong received message content message size ", msgsize, " expect mod 8 value as 0"
        return False
    if expmsgsize != msgsize :
        print "ERROR : Wrong received message content message size ", msgsize, " expect ", expmsgsize
        return False
    print "Passed content size check."
    return True

def expectValue(val1, val2, msg):
    if  val1 != val2 :
        print "ERROR : Wrong value checked " + str(val1) + " expect " + str(val2) + "(" + msg + ")"
        return False
    print "Value check passed. " + str(val1) + "(" + msg + ")"
    return True

################################################################################
# Functions for specific tests.
################################################################################
def sendDummyRequest(conn,dummysize):
    content = struct.pack('<BBBB',0, 0, 0, 0)
    if sendMessageHead(conn,269,dummysize) < 0 :
        return False
    sentsize = 0
    while sentsize < dummysize :
        sentsize += 4
        if sendMessage(conn,content) < 0 :
            return False
    if sendMessageEnd(conn) < 0 :
        return False
    print "Sent dummy request."
    return True

def recvDymmyResponse(conn, expmsgsize):
    (msgid,msgsize)=recvMessageHead(conn)
    if not expectRightHead(msgid,msgsize, 2317, 8) :
        return False
    (content) = recvMessage(conn,msgsize)
    if not expectRightContentSize(msgsize,expmsgsize) :
        return False
    (result,reserved) = struct.unpack('<II',content)
    if not expectValue(result,0, "Dummy response result") :
        return False
    if not expectValue(reserved,0, "Dummy response reserved field") :
        return False
    tailresult = recvMessageTail(conn)
    if  not expectValue(tailresult, True, "Received end tag.") :
        return False
    print "Received dummy response."
    return True

def testRPCWithoutContent():
    print "Start testing resource manager rpc : without content context."
    time1 = time.time()
    conncount = 0
    conncountmax = 5000
    while conncount < conncountmax :
        conncount += 1
        conn = connectToRM()
        if conn == None :
            continue
        if not sendDummyRequest(conn, 8) :
            conn.close()
            continue
        if not recvDymmyResponse(conn, 8) :
            conn.close()
            continue
        conn.close()
    print "End of testing resource manager rpc : without content context." , time.time() - time1

def testRPCWithoutContentMultiThread():
    threadarray = []
    print "Start testing resource manager rpc : without content context multithread workload."
    time1 = time.time()
    for i in range(50):
        threadarray.append(Thread(target=testRPCWithoutContent,name='test', args=()))
    for thr in threadarray:
        thr.start()
    for thr in threadarray:
        thr.join()
    print "End of testing resource manager rpc : without content context multithread workload." , time.time() - time1

def testRPCWithoutContentRandomAbort():
    print "Start testing resource manager rpc : without content context, random abort."
    conn = None
    for i in range(5000):
        if conn == None :
            conn = connectToRM()
        if conn == None :
            time.sleep(0.1)
            continue
        if random.uniform(0,1) < 0.1 :
            conn.close()
            conn = None
            continue
        msgsize = math.ceil(random.uniform(1,500)) * 8
        if not sendDummyRequest(conn, msgsize) :
            conn.close()
            continue
        if  random.uniform(0,1) < 0.1 :
            conn.close()
            conn = None
            continue
        if not recvDymmyResponse(conn, 8) :
            conn.close()
            continue
        if random.uniform(0,1) < 0.5 :
            conn.close()
            conn = None
    print "End of testing resource manager rpc : without content context, random abort."

def parseCLIArgs():
    parser = OptionParser(usage="HAWQ RM RPC test options.")
    parser.add_option("-u", "--userid", dest="userid", action="store", default="gpadmin", help="Set user id to register for rpc")
    parser.add_option("-t", "--socktype", dest="socktype", action="store", default="domain", help="Set socket connection type : domain or inet, default is domain")
    parser.add_option("-S", "--server", dest="sockserver", action="store", default="localhost", help="Set socket server address, default is localhost")
    parser.add_option("-P", "--port", dest="sockport", action="store", default=5438, help="Set socket server port, default is 5438")
    parser.add_option("-D", "--domainfile", dest="sockdomainfile", action="store", default="/tmp/.s.PGSQL.5436", help="Set domain socket file name, default is /tmp/.s.PGSQL.5436")
    (options, args) = parser.parse_args()
    return (options, args)

# Main entry
if __name__ == '__main__':
    print "Start testing resource manager rpc."
    (opts,args) = parseCLIArgs()
    print "Use user id " + opts.userid
    if opts.socktype == "domain" :
        print "Set Domain socket to connect " + opts.sockdomainfile
    else :
        print "Set Inet socket to connect " + opts.sockserver + ":" + '%d' %opts.sockport

    testRPCWithoutContent()
    testRPCWithoutContentMultiThread()
    testRPCWithoutContentRandomAbort()

    print "End of testing resource manager rpc."
