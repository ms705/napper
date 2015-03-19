import sys, socket, time, logging
import shlex, subprocess
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

def zkConnect(conn_str):
  zk = KazooClient(hosts=conn_str)
  zk.start()
  return zk

def zkCreateJobDir(zk, job_name):
  zk.ensure_path("/napper/memaslap/%s" % (job_name))

def zkRemoveJobDir(zk, job_name):
  zk.delete("/napper/memaslap/%s" % (job_name), recursive=True)

def zkRegisterWorker(zk, job_name, hostname, port):
  print "Registering myself as %s:%d" % (hostname, port)
  zk.create("/napper/memaslap/%s/%s:%d" % (job_name, hostname, port), "%d" % (port), ephemeral=True)
  return port

logging.basicConfig()

if len(sys.argv) < 6:
  print "usage: napper_memaslap <Zookeeper hostname:port> <job name> <worker ID> <num workers> <executable>"
  sys.exit(1)

hostport = sys.argv[1]
job_name = sys.argv[2]
worker_id = int(sys.argv[3])
num_workers = int(sys.argv[4])
memaslap_path = " ".join(sys.argv[5:])

client = zkConnect(hostport)
zkCreateJobDir(client, job_name)

hosts = []
done = False
children = client.get_children("/napper/memcached/")
for c in children:
  if not ":" in c:
    continue
  data, stat = client.get("/napper/memcached/%s" % (c))
  print "%s:%s" % (c, data)
  hosts.append("%s" % (c))

# randomly select T hosts
num_hosts = min(len(hosts), 8)
sampled_servers = random.sample(hosts, num_hosts)

# execute program
command = "%s -s %s" % (memaslap_path, ",".join(sampled_servers))
print "RUNNING: %s" % (command)
subprocess.call(shlex.split(command))

# this will implicitly clean up afterwards
client.stop()

sys.exit(0)
