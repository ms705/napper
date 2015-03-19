import sys, os
import shlex, subprocess

def hdfs_fetch_file(hdfs_path, local_path):
  print "Getting %s..."
  command = "hadoop fs -get %s %s" % (hdfs_path, local_path)
  subprocess.call(shlex.split(command))
  print "Done getting %s..."

def hdfs_push_file(local_path, hdfs_path):
  print "Putting %s..."
  command = "hadoop fs -put %s %s" % (local_path, hdfs_path)
  subprocess.call(shlex.split(command))
  print "Done putting %s..."
