import sys, socket, time, logging
import shlex, subprocess
from kazoo.client import KazooClient

def zkConnect(conn_str):
  zk = KazooClient(hosts=conn_str)
  zk.start()
  return zk

def zkCreateJobDir(zk, job_name):
  zk.ensure_path("/napper/naiad/%s" % (job_name))

def zkRemoveJobDir(zk, job_name):
  zk.delete("/napper/naiad/%s" % (job_name), recursive=True)

def zkRegisterWorker(zk, job_name, worker_id, hostname, port):
  print "Registering myself as %s:%d on %s:%d" % (job_name, worker_id, hostname, port)
  zk.create("/napper/naiad/%s/%d" % (job_name, worker_id), "%s:%d" % (hostname, port))

logging.basicConfig()

if len(sys.argv) < 6:
  print "usage: napper_naiad <Zookeeper hostname:port> <job name> <worker ID> <num workers> <executable>"
  sys.exit(1)

hostport = sys.argv[1]
job_name = sys.argv[2]
worker_id = int(sys.argv[3])
num_workers = int(sys.argv[4])
naiad_path = " ".join(sys.argv[5:])

client = zkConnect(hostport)
zkCreateJobDir(client, job_name)

zkRegisterWorker(client, job_name, worker_id, socket.gethostname(), 2100 + worker_id)

done = False
hosts = []
while not done:
  children = client.get_children("/napper/naiad/%s" % (job_name))
  if len(children) == num_workers:
    print "All workers are here!"
    for c in sorted(children,
                    key=lambda item: (int(item.partition(' ')[0])
                                      if item[0].isdigit()
                                      else float('inf'), item)):
      data, stat = client.get("/napper/%s/%s" % (job_name, c))
      print "%s @ %s" % (c, data)
      hosts.append("%s" % (data))
    done = True
  time.sleep(1)

if worker_id == 0:
  zkRemoveJobDir(client, job_name)
  print "Deleted nodes for %s" % (job_name)
client.stop()
# execute program
command = "mono-sgen %s -p %d -n %d -t 1 -h %s --inlineserializer" % (naiad_path, worker_id, num_workers, " ".join(hosts))
print "RUNNING: %s" % (command)
subprocess.call(shlex.split(command))
sys.exit(0)
