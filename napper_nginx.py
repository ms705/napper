import os, sys, socket, time, logging
import shlex, subprocess
import shutil
import netifaces as ni
import tempfile
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

def createLocalScratchDir():
  if 'FLAGS_task_data_dir' in os.environ:
    working_dir = os.environ(['FLAGS_task_data_dir'])
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
  zk.ensure_path("/napper/nginx/%s" % (job_name))

def zkRemoveJobDir(zk, job_name):
  zk.delete("/napper/nginx/%s" % (job_name), recursive=True)

def zkRegisterWorker(zk, job_name, hostname, port):
  while zk.exists("/napper/nginx/%s:%d" % (hostname, port)):
    port += 1
  print "Registering myself as %s:%d" % (hostname, port)
  zk.create("/napper/nginx/%s:%d" % (hostname, port), "%d" % (port), ephemeral=True)
  zk.create("/napper/nginx/%s/%s:%d" % (job_name, hostname, port), "%d" % (port), ephemeral=True)
  return port

logging.basicConfig()

if len(sys.argv) < 6:
  print "usage: napper_nginx <Zookeeper hostname:port> <job name> <worker ID> <num workers> <executable>"
  sys.exit(1)

hostport = sys.argv[1]
job_name = sys.argv[2]
worker_id = int(sys.argv[3])
num_workers = int(sys.argv[4])
nginx_path = " ".join(sys.argv[5:])

client = zkConnect(hostport)
zkCreateJobDir(client, job_name)

done = False

while not done:
  try:
    actual_port = zkRegisterWorker(client, job_name, ni.ifaddresses('p1p1')[2][0]['addr'], 8888)
    done = True
  except NodeExistsError:
    pass

hosts = []
done = False
while not done:
  children = client.get_children("/napper/nginx/%s" % (job_name))
  if len(children) == num_workers:
    print "All tasks are here!"
    for c in children:
      data, stat = client.get("/napper/nginx/%s/%s" % (job_name, c))
      print "%s:%s" % (c, data)
    done = True
  time.sleep(1)

working_dir = createLocalScratchDir()
# get config file in place
os.mkdir("%s/html" % (working_dir))
shutil.copyfile("/home/srguser/firmament-experiments/workloads/nginx/nginx.conf", "%s/nginx.conf" % (working_dir))
shutil.copyfile("/home/srguser/firmament-experiments/workloads/nginx/index.html", "%s/html/index.html" % (working_dir))
shutil.copyfile("/home/srguser/firmament-experiments/workloads/nginx/muppet.jpg", "%s/html/muppet.jpg" % (working_dir))
sed_command = "sed -i %s/nginx.conf -e 's/8082/%d/'" % (working_dir, actual_port)
subprocess.call(shlex.split(sed_command))
# execute program
command = "%s -p %s -g 'error_log stderr;' -c %s/nginx.conf" % (nginx_path, working_dir, working_dir)
print "RUNNING: %s" % (command)
subprocess.call(shlex.split(command))

# this will implicitly clean up afterwards
client.stop()

sys.exit(0)
