import sys, socket, time, logging
import shlex, subprocess
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

def zkConnect(conn_str):
  zk = KazooClient(hosts=conn_str)
  zk.start()
  return zk

def zkCreateJobDir(zk, job_name):
  zk.ensure_path("/napper/memcached/%s" % (job_name))

def zkRemoveJobDir(zk, job_name):
  zk.delete("/napper/memcached/%s" % (job_name), recursive=True)

def zkRegisterWorker(zk, job_name, hostname, port):
  print "Registering myself as %s:%d" % (hostname, port)
  while zk.exists("/napper/memcached/%s:%d" % (hostname, port)):
    port += 1
  zk.create("/napper/memcached/%s:%d" % (hostname, port), "%d" % (port), ephemeral=True)
  zk.create("/napper/memcached/%s/%s:%d" % (job_name, hostname, port), "%d" % (port), ephemeral=True)
  return port

logging.basicConfig()

if len(sys.argv) < 6:
  print "usage: napper_memcached <Zookeeper hostname:port> <job name> <worker ID> <num workers> <executable>"
  sys.exit(1)

hostport = sys.argv[1]
job_name = sys.argv[2]
worker_id = int(sys.argv[3])
num_workers = int(sys.argv[4])
memcached_path = " ".join(sys.argv[5:])

client = zkConnect(hostport)
zkCreateJobDir(client, job_name)

done = False

while not done:
  try:
    actual_port = zkRegisterWorker(client, job_name, socket.gethostname(), 11211)
    done = True
  except NodeExistsError:
    pass

hosts = []
done = False
while not done:
  children = client.get_children("/napper/memcached/%s" % (job_name))
  if len(children) == num_workers:
    print "All tasks are here!"
    for c in children:
      data, stat = client.get("/napper/memcached/%s/%s" % (job_name, c))
      print "%s:%s" % (c, data)
    done = True
  time.sleep(1)

# execute program
command = "%s -m 1024 -p %d" % (memcached_path, actual_port)
print "RUNNING: %s" % (command)
subprocess.call(shlex.split(command))

# this will implicitly clean up afterwards
client.stop()

sys.exit(0)
