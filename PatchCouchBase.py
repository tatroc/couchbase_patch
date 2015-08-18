__author__ = 'tatroc'

import sys
import getopt
import requests
import json
import time
import addNode
import patch
from datetime import datetime
import syslog



def main(argv):
    try:
        opts, args = getopt.getopt(argv, "s:cp:cu:op:ou:rt", ["serverlist=", "cpassword=", "cuser=", "ospassword=", "osuser=", "rtimeout="])
    except getopt.GetoptError:
        print "error in arguments"
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-s', '--serverlist'):
            global serverlist
            serverlist = arg
        elif opt in ('-cp', '--cpassword'):
            global cpassword
            cpassword = arg
        elif opt in ('-cu', '--cuser'):
            global cuser
            cuser = arg
        elif opt in ('-op', '--ospassword'):
            global ospassword
            ospassword = arg
        elif opt in ('-ou', '--osuser'):
            global osuser
            osuser = arg
        elif opt in ('-rt', '--rtimeout'):
            global rtimeout
            rtimeout = int(arg)
        else:
            print "Usage --serverlist"

if __name__ == "__main__":
    main(sys.argv[1:])




couchbase2 = serverlist.split(',')





l = [None]



# Create server object
class Server(object):
    hostname = ""
    patched = 0
    ns_hostname = 'ns_1@' + ""

    # The class "constructor" - It's actually an initializer
    def __init__(self, hostname, patched, ns_hostname):
        self.hostname = hostname
        self.patched = patched
        self.ns_hostname = 'ns_1@' + ns_hostname


def make_server(hostname, patched):

    ns_hostname = 'ns_1@' + hostname
    server = Server(hostname, patched, ns_hostname)
    return server


a = []
for i in couchbase2:
    x = Server(i, 0, i)
    a.append(x)

# verify login credentials to couch base nodes
for sv in a:
    try:
        url = 'http://' + sv.hostname + ':8091/pools/nodes'
        req_status = requests.get(url, auth=(cuser, cpassword))
        if req_status.status_code == 200:
            msg = "status is " + str(req_status.status_code) + " for " + sv.hostname + " login was successful"
            print msg
            syslog.syslog(syslog.LOG_INFO, msg)
        else:
            msg = "Connection failed to node: " + sv.hostname + " status code is " + str(req_status.status_code) + " login failed"
            print msg
            syslog.syslog(syslog.LOG_ERR, msg)
            sys.exit(-1)
    except Exception as e:
        msg = "Connection failed"
        print msg
        syslog.syslog(syslog.LOG_ERR, msg)





# easy_install requests

headers = {'content-type': 'application/json'}

serverArrayLen = len(a)
knownNodesString = ""
count = 0

for s in a:
    knownNodesString += s.ns_hostname + ','

for srv in a:

    # eject node from cluster
    payload = {'ejectedNodes': a[count].ns_hostname, 'knownNodes': knownNodesString}
    try:
        url = 'http://' + srv.hostname + ':8091/controller/rebalance'
        r2 = requests.post(url, data=payload, auth=(cuser, cpassword))
    except Exception as e:
        msg = "Connection failed for re-balance operation"
        print msg
        syslog.syslog(syslog.LOG_ERR, msg)

    # get re-balance progress
    try:
        url = 'http://' + srv.hostname + ':8091/pools/default/rebalanceProgress'
        rebalanceprogress = requests.get(url, auth=(cuser, cpassword))
    except Exception as e:
        msg = "Connection failed"
        print msg
        syslog.syslog(syslog.LOG_ERR, msg)

    json_data = json.loads(rebalanceprogress.text)

    while json_data["status"] == "running":
        time.sleep(5)
        msg = str(datetime.now()) + ": re-balance running on " + str(srv.hostname)
        print msg
        syslog.syslog(syslog.LOG_INFO, msg)
        try:
            url = 'http://' + srv.hostname + ':8091/pools/default/rebalanceProgress'
            rebalanceprogress = requests.get(url, auth=(cuser, cpassword))
            json_data = json.loads(rebalanceprogress.text)
        except Exception as e:
            pass
            msg = "Connection failed to : " + str(srv.hostname)
            print msg
            syslog.syslog(syslog.LOG_ERR, msg)

    msg = "re-balance complete " + str(srv.hostname)
    print msg
    syslog.syslog(syslog.LOG_INFO, msg)

    srv.patched = 1
    count = count + 1

    patch.patch_and_reboot(srv.hostname, osuser, ospassword, rtimeout)
    msg = str(datetime.now()) + " : Operating system patched, rebooting : " + str(srv.hostname)
    print msg
    syslog.syslog(syslog.LOG_INFO, msg)

    # re-add node to cluster
    addNode.add(srv.hostname, knownNodesString, cuser, cpassword, a)
    msg = str(datetime.now()) + " : re-add node " + str(srv.hostname) + " is complete!"
    print msg
    syslog.syslog(syslog.LOG_INFO, msg)






