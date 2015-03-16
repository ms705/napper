import sys, socket, time, logging
import shlex, subprocess
from kazoo.client import KazooClient

def zkConnect(conn_str):
  zk = KazooClient(hosts=conn_str)
  zk.start()
  return zk

logging.basicConfig()

if len(sys.argv) < 6:
  print "usage: dump <Zookeeper hostname:port> <path>"
  sys.exit(1)

hostport = sys.argv[1]
zk_path = sys.argv[2]

client = zkConnect(hostport)
zkCreateJobDir(client, job_name)

children = client.get_children("/napper/%s" % (zk_path))
for c in children:
  data, stat = client.get("/napper/%s/%s" % (zk_path, c))
  print "%s: %s" % (c, data)

sys.exit(0)
