#!/usr/bin/env python26
#
# Copyright [2012] yczealot.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Drop big table, use hard link + truncate file to avoid IO impact

writelog(log,msg): write exec log

"""

__authors__ = [ '"yczealot" <yczealot@gmail.com>' ]
__version__ = 1.1
__create__  = "2012-11-29"
__update__  = "2012-11-30"

import os
import sys
import time
import datetime 
from mysqlbaseop import *
from optparse import OptionParser

def parse_options():
    global usage
    usage = "usage: %prog -P port" 
    global parser 
    parser = OptionParser(usage=usage,version=str(__version__))

    parser.add_option("-P", "--port", dest="port", type="int", default=0, help="TCP/IP port (default: 0)")

    return parser.parse_args()

def writelog(log,msg):
    dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = dt+": "+msg 
    print >> log, msg     

def main():
    (options, args) = parse_options()
    if options.port == 0 or options.port > 10003:
        parser.error("Port can't be null or >10003")

    port = str(options.port)

    n = 2 
    oneday = datetime.timedelta(days=1)
    day = datetime.datetime.today() - n*oneday
    dt = day.strftime('%y%m%d')

    socket = "/tmp/mysql"+str(port)+".sock"
    dbname = "firehose"
    tbprefix = "msgs"
    tbname = tbprefix+"_"+dt

    logname = "/var/log/log_drop_table_"+dt+".log"
    log = open(logname,"a")
    
    writelog(log,"-"*100)
    print port
    writelog(log,port)

# hard link
    command = "cd /data1/mysql"+port+"/"+dbname+"; ln "+tbname+".ibd "+tbname+".ibd.hdlk"
    print command 
    writelog(log,command)

    try:
        result = os.system(command)
        if result != 0:
            msg = "ln failed, exit reason:rtcode="+str(result)
            print msg
            writelog(log,msg)
            sys.exit()

    except Exception, err:
        msg = "ln failed, exit reason:%s" %err
        print msg
        writelog(log,msg)
        sys.exit() 

# drop table
    sql = "drop table if exists "+dbname+"."+tbname
    print sql
    writelog(log,sql)

    try:
        conn =  MySQLdb.connect(
                host = "localhost",
                user = "root", 
                passwd = "TR4anis@xtooRz",
                unix_socket = socket)
    except Exception, err:
        msg= "conn failed, exit reason:%s" %err
        print msg
        write(log,msg)
        sys.exit()

    try:
        result = act_query(conn,sql)
    except Exception, err:
        msg = "drop failed, exit reason:%s" %s
        print msg
        write(log,msg)
        sys.exit()

# truncate file
    splitsize = 1024*1024*64
    file = "/data1/mysql"+port+"/"+dbname+"/"+tbname+".ibd.hdlk"
    try:
        size = os.path.getsize(file)
    except Exception, err:
        msg = "get size failed, exit reason:%s" %err
        print msg
        writelog(log,msg)
        sys.exit()

    count = size/splitsize
    
    msg = "size="+str(size)+",splitsize="+str(splitsize)+",count="+str(count)
    print msg
    writelog(log,msg)

    for i in range(1,count+1):
        command1 = "truncate -s -"+str(splitsize)+" "+file
        print command1 
        writelog(log,command1)
        os.popen(command1)

        time.sleep(2)

# rm file
    command = "rm -rf "+file
    print command
    writelog(log,command)

    try:
        os.popen(command)
    except Exception, err:
        msg = "rm failed, exit reason:%s" %err
        print msg
        writelog(log,msg)
        
        sys.exit() 

main()
