__author__ = 'tatroc'

from fabric.api import run, env, settings
import os
import time
import requests
from datetime import datetime
import syslog
from datetime import timedelta
import sys


def get_couch_status(hostname, rtimeout):
    status = 400
    now = datetime.now()
    future_time = now + timedelta(seconds=rtimeout)

    while status != 200:
        time.sleep(5)
        try:
            url = 'http://' + hostname + ':8091'
            resp = requests.get(url, params=None)
            status = resp.status_code
            msg = str(datetime.now()) + " : Connection success, status: " + str(status)
            print msg
            syslog.syslog(syslog.LOG_INFO, msg)

            if future_time > datetime.now():
                msg = str(datetime.now()) + " : Reboot timeout " + str(rtimeout) + "seconds exceeded, exiting script!"
                print msg
                syslog.syslog(syslog.LOG_INFO, msg)
                sys.exit(-1)

        except Exception as e:
            pass
            msg = str(datetime.now()) + " : Connection failed, status: " + str(status)
            print msg
            syslog.syslog(syslog.LOG_ERR, msg)



def patch_and_reboot(hostname, osuser, ospassword, rtimeout):
    env.hosts = [hostname]
    env.user = osuser
    env.password = ospassword
    for host in env.hosts:
        #print host
        env.host_string = host
        run('yum update -y')
        run('shutdown -r now')
        time.sleep(20)
        get_couch_status(hostname, rtimeout)


