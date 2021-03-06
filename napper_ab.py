import os, sys, socket, time, logging, random
import shlex, subprocess
import tempfile
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

def createLocalScratchDir():
  if 'FLAGS_task_data_dir' in os.environ:
    working_dir = os.environ['FLAGS_task_data_dir']
    if not os.path.exists(working_dir):
      os.makedirs(working_dir)
  else:
    working_dir = tempfile.mkdtemp(dir="/mnt/scratch/")
    print "Working dir is: %s" % (working_dir)
    os.chmod(working_dir, 0777)
  return working_dir

def zkConnect(conn_str):
  zk = KazooClient(hosts=conn_str)
  zk.start()
  return zk

def zkCreateJobDir(zk, job_name):
  zk.ensure_path("/napper/ab/%s" % (job_name))

def zkRemoveJobDir(zk, job_name):
  zk.delete("/napper/ab/%s" % (job_name), recursive=True)

def zkRegisterWorker(zk, job_name, hostname, port):
  print "Registering myself as %s:%d" % (hostname, port)
  zk.create("/napper/ab/%s/%s:%d" % (job_name, hostname, port), "%d" % (port), ephemeral=True)
  return port

logging.basicConfig()

if len(sys.argv) < 6:
  print "usage: napper_ab <Zookeeper hostname:port> <job name> <worker ID> <num workers> <executable>"
  sys.exit(1)

hostport = sys.argv[1]
job_name = sys.argv[2]
worker_id = int(sys.argv[3])
num_workers = int(sys.argv[4])
ab_path = " ".join(sys.argv[5:])

client = zkConnect(hostport)
zkCreateJobDir(client, job_name)

hosts = []
done = False
children = client.get_children("/napper/nginx/")
for c in children:
  if not ":" in c:
    continue
  data, stat = client.get("/napper/nginx/%s" % (c))
  print "%s:%s" % (c, data)
  hosts.append("%s" % (c))

working_dir = createLocalScratchDir()
i = 0
# randomly select T hosts
while True:
  if len(hosts) == 0:
    print "No nginx servers found!"
    sys.exit(1)
  sampled_server = random.sample(hosts, 1)

  # execute program
  command = "%s -e %s/req_cdf_%d.csv http://%s/" % (ab_path, working_dir, i, sampled_server[0])
  print "RUNNING: %s" % (command)
  subprocess.call(shlex.split(command))
  i += 1

# this will implicitly clean up afterwards
client.stop()

sys.exit(0)
