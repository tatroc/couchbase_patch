__author__ = 'tatroc'

import requests
import json
import time
from datetime import datetime
import syslog

def add(hostname, knownNodes, cuser, cpassword, a):

    # Add node to cluster
    payload = {'hostname': hostname}
    try:
        # find member of cluster
        for servers in a:
            if servers.hostname != hostname:
                cluster_member = servers.hostname
                print cluster_member
                break

        url = 'http://' + hostname + ':8091/controller/addNode'
        resp = requests.post('http://' + cluster_member + ':8091/controller/addNode', data=payload, auth=(cuser, cpassword))
    except Exception as e:
        msg = "Adding node to cluster failed"
        print msg
        syslog.syslog(syslog.LOG_ERR, msg)

    # re-balance cluster
    payload = {'knownNodes': knownNodes}
    try:
        url = 'http://' + hostname + ':8091/controller/rebalance'
        r2 = requests.post(url, data=payload, auth=(cuser, cpassword))
    except Exception as e:
        msg = "Cluster rebalance operation failed"
        print msg
        syslog.syslog(syslog.LOG_ERR, msg)


    # get re-balance progress
    try:
        url = 'http://' + hostname + ':8091/pools/default/rebalanceProgress'
        rebalanceprogress = requests.get(url, auth=(cuser, cpassword))
    except Exception as e:
        msg = "Connection failed"
        print msg
        syslog.syslog(syslog.LOG_ERR, msg)

    json_data = json.loads(rebalanceprogress.text)

    while json_data["status"] == "running":
        time.sleep(5)
        msg = str(datetime.now()) + " : node " + hostname + " added to cluster, re-balance running"
        print msg
        syslog.syslog(syslog.LOG_INFO, msg)
        try:
            url = 'http://' + hostname + ':8091/pools/default/rebalanceProgress'
            rebalanceprogress = requests.get(url, auth=(cuser, cpassword))
            json_data = json.loads(rebalanceprogress.text)
        except Exception as e:
            msg = "Connection failed"
            print msg
            syslog.syslog(syslog.LOG_ERR, msg)

    msg = str(datetime.now()) + " : Node added and re-balance complete"
    print msg
    syslog.syslog(syslog.LOG_INFO, msg)