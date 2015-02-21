import sys, socket, time, logging
import subprocess
from kazoo.client import KazooClient

def zkConnect(conn_str):
  zk = KazooClient(hosts=conn_str)
  zk.start()
  return zk

def zkRemoveJobDir(zk, job_name):
  zk.delete("/napper/%s" % (job_name), recursive=True)

logging.basicConfig()

if len(sys.argv) < 2:
  print "usage: cleanup <Zookeeper hostname:port> <job name>"
  sys.exit(1)

hostport = sys.argv[1]
job_name = sys.argv[2]

client = zkConnect(hostport)
zkRemoveJobDir(client, job_name)

