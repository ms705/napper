import sys, socket, time, logging
import shlex, subprocess
import netifaces as ni
from hdfs import *
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
  while zk.exists("/napper/naiad/%s:%d" % (hostname, port)):
    port += 1
  print "Registering myself as %s:%d on %s:%d" % (job_name, worker_id, hostname, port)
  zk.create("/napper/naiad/%s:%d" % (hostname, port), "%d" % (port), ephemeral=True)
  zk.create("/napper/naiad/%s/%d" % (job_name, worker_id), "%s:%d" % (hostname, port), ephemeral=True)

def zkDeregisterWorker(zk, job_name, worker_id):
  print "Finished; unregistering myself from %s" % (job_name)
  zk.delete("/napper/naiad/%s/%d" % (job_name, worker_id))
  zk.delete("/napper/naiad/%s:%d" % (job_name, worker_id))

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

done = False

while not done:
  try:
    zkRegisterWorker(client, job_name, worker_id, ni.ifaddresses('p1p1')[2][0]['addr'], 2100 + worker_id)
    done = True
  except NodeExistsError:
    pass

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
      data, stat = client.get("/napper/naiad/%s/%s" % (job_name, c))
      print "%s @ %s" % (c, data)
      hosts.append("%s" % (data))
    done = True
  time.sleep(1)

if worker_id == 0:
  zkDeregisterWorker(client, job_name, worker_id)
client.stop()

if not os.path.exists(os.environ['FLAGS_task_data_dir']):
  os.makedirs(os.environ['FLAGS_task_data_dir'])

# fetch inputs from HDFS if necessary
if "tpch" in job_name:
  hdfs_fetch_file("/input/part_splits%d/part%d.in" % (num_workers, worker_id), os.environ['FLAGS_task_data_dir'])
  hdfs_fetch_file("/input/lineitem_splits%d/lineitem%d.in" % (num_workers, worker_id), os.environ['FLAGS_task_data_dir'])
  naiad_path += " %s" % (os.environ['FLAGS_task_data_dir'])
elif "netflix" in job_name:
  hdfs_fetch_file("/input/netflix_movies_splits%d/netflix_movies%d.in" % (num_workers, worker_id), os.environ['FLAGS_task_data_dir'])
  hdfs_fetch_file("/input/netflix_ratings_splits%d/netflix_ratings%d.in" % (num_workers, worker_id), os.environ['FLAGS_task_data_dir'])
  naiad_path += " netflix_ratings%d.in netflix_movies%d.in %s" % (worker_id, worker_id, os.environ['FLAGS_task_data_dir'])
elif "pagerank" in job_name:
  hdfs_fetch_file("/input/pagerank_livejournal_edges_splits%d/pagerank_livejournal_edges%d.in" % (num_workers, worker_id), os.environ['FLAGS_task_data_dir'])
  hdfs_fetch_file("/input/pagerank_livejournal_vertices_splits%d/pagerank_livejournal_vertices%d.in" % (num_workers, worker_id), os.environ['FLAGS_task_data_dir'])
  naiad_path += " livejournal %s" % (os.environ['FLAGS_task_data_dir'])
elif "sssp" in job_name:
  hdfs_fetch_file("/input/sssp_tw_edges_splits%d/sssp_tw_edges%d.in" % (num_workers, worker_id), os.environ['FLAGS_task_data_dir'])
  hdfs_fetch_file("/input/sssp_tw_vertices_splits%d/sssp_tw_vertices%d.in" % (num_workers, worker_id), os.environ['FLAGS_task_data_dir'])
  naiad_path += " tw %s" % (os.environ['FLAGS_task_data_dir'])
else:
  print "WARNING: unknown Naiad job type; won't fetch any input data from HDFS."

# execute program
command = "mono-sgen %s -p %d -n %d -t 1 -h %s --inlineserializer" % (naiad_path, worker_id, num_workers, " ".join(hosts))
print "RUNNING: %s" % (command)
ret = subprocess.call(shlex.split(command))

if ret != 0:
  print "ERROR: Naiad run failed!"
  print "Not cleaning up any state."
  sys.exit(ret)

hdfs_mkdir("/output/%s" % (job_name))
if "tpch" in job_name:
  push_ret = hdfs_push_file("%s/avg_yearly%d.out" % (os.environ['FLAGS_task_data_dir'], worker_id), "/output/%s/" % (job_name))
elif "netflix" in job_name:
  push_ret = hdfs_push_file("%s/prediction%d.out" % (os.environ['FLAGS_task_data_dir'], worker_id), "/output/%s/" % (job_name))
elif "pagerank" in job_name:
  push_ret = hdfs_push_file("%s/pagerank_livejournal%d.out" % (os.environ['FLAGS_task_data_dir'], worker_id), "/output/%s/" % (job_name))
elif "sssp" in job_name:
  push_ret = hdfs_push_file("%s/dij_vertices%d.out" % (os.environ['FLAGS_task_data_dir'], worker_id), "/output/%s/" % (job_name))
else:
  print "WARNING: unknown Naiad job type; won't fetch any input data from HDFS."

if push_ret != 0:
  print "ERROR: failed to push result to HDFS! Leaving state around for inspection."
else:
  print "Deleting scratch data..."
  del_command = "rm -rf %s" % (os.environ['FLAGS_task_data_dir'])
  ret = subprocess.call(shlex.split(del_command))

  if ret != 0:
    print "Failed to delete local scratch directory"

print "All done -- goodbye from Napper!"

sys.exit(0)
