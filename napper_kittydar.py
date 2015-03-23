import sys, socket, time, logging
import shlex, subprocess
from hdfs import *

logging.basicConfig()

if len(sys.argv) < 4:
  print "usage: napper_kittydar <job name> <worker ID> <executable>"
  sys.exit(1)

job_name = sys.argv[1]
worker_id = int(sys.argv[2])
kittydar_path = " ".join(sys.argv[3:])

# fetch inputs from HDFS if necessary
hdfs_fetch_file("/input/kittydar_splits30/CAT_%02d" % (worker_id), os.environ['FLAGS_task_data_dir'])

# execute program
command = "nodejs %s --dir %s/CAT_%02d/" % (kittydar_path, os.environ['FLAGS_task_data_dir'], worker_id)
print "RUNNING: %s" % (command)
subprocess.call(shlex.split(command))

print "Deleting scratch data..."
del_command = "rm -rf %s" % (os.environ['FLAGS_task_data_dir'])
subprocess.call(shlex.split(del_command))

print "All done -- goodbye from Napper!"

sys.exit(0)
