import sys, socket, time, logging
import subprocess
from kazoo.client import KazooClient

def zkConnect(conn_str):
  zk = KazooClient(hosts=conn_str)
  zk.start()
  return zk

def zkRemove(zk, path):
  zk.delete("/napper/%s" % (path), recursive=True)

logging.basicConfig()

if len(sys.argv) < 2:
  print "usage: cleanup <Zookeeper hostname:port> <path>"
  sys.exit(1)

hostport = sys.argv[1]
path = sys.argv[2]

client = zkConnect(hostport)
zkRemove(client, path)

