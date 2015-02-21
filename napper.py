import sys, socket, time, logging
import subprocess
from kazoo.client import KazooClient

def zkConnect(conn_str):
  zk = KazooClient(hosts=conn_str)
  zk.start()
  return zk

def zkCreateJobDir(zk, job_name):
  zk.ensure_path("/napper/%s" % (job_name))

def zkRemoveJobDir(zk, job_name):
  zk.delete("/napper/%s" % (job_name), recursive=True)

def zkRegisterWorker(zk, job_name, hostname, port):
  zk.create("/napper/%s/%s" % (job_name, hostname), "%d" % (port))

logging.basicConfig()

if len(sys.argv) < 6:
  print "usage: napper <Zookeeper hostname:port> <job name> <worker ID> <num workers> <executable>"
  sys.exit(1)

hostport = sys.argv[1]
job_name = sys.argv[2]
worker_id = int(sys.argv[3])
num_workers = int(sys.argv[4])
naiad_path = sys.argv[5]

client = zkConnect(hostport)
zkCreateJobDir(client, job_name)

zkRegisterWorker(client, job_name, socket.gethostname(), 2000 + worker_id)

done = False
while not done:
  children = client.get_children("/napper/%s" % (job_name))
  if len(children) == num_workers:
    print "All workers are here!"
    for c in children:
      data, stat = client.get("/napper/%s/%s" % (job_name, c))
      print "%s:%s" % (c, data)
    done = True
  time.sleep(1)

if worker_id == 0:
  zkRemoveJobDir(client, job_name)
  print "Deleted nodes for %s" % (job_name)
client.stop()
# execute program
subprocess.call(naiad_path)
sys.exit(0)
