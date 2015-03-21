import sys, os
import shlex, subprocess
import time

def hdfs_mkdir(hdfs_path):
  print "Creating %s on HDFS..." % (hdfs_path)
  command = "hadoop fs -mkdir -p %s" % (hdfs_path)
  subprocess.call(shlex.split(command))

def hdfs_fetch_file(hdfs_path, local_path):
  print "Getting %s..." % (hdfs_path)
  start = time.time()
  command = "hadoop fs -get %s %s" % (hdfs_path, local_path)
  subprocess.call(shlex.split(command))
  end = time.time()
  print "Done getting %s, took %d seconds" % (hdfs_path, end - start)

def hdfs_push_file(local_path, hdfs_path):
  print "Putting %s..." % (local_path)
  start = time.time()
  command = "hadoop fs -put %s %s" % (local_path, hdfs_path)
  subprocess.call(shlex.split(command))
  end = time.time()
  print "Done putting %s, took %d seconds" % (local_path, end - start)
