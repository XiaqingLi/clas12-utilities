import re
import os
import sys
import glob
import json
import time
import math
import gzip
import stat
import shutil
import socket
import getpass
import argparse
import datetime
import subprocess
import collections

import config

def average(alist):
  if len(alist) > 0:
    return '%.2f' % (sum(alist) / len(alist))
  else:
    return config.null_field

def stddev(alist):
  if len(alist) > 0:
    m = average(alist)
    s = sum([ (x-float(m))*(x-float(m)) for x in alist ])
    return '%.2f' % math.sqrt(s / len(alist))
  else:
    return config.null_field

def sort_dict(dictionary, subkey):
  '''Sort a dictionary of sub-dictionaries by one of the keys
  in the sub-dictionaries'''
  ret = collections.OrderedDict()
  ordered_keys = []
  for k,v in dictionary.items():
    if len(ordered_keys) == 0:
      ordered_keys.append(k)
    else:
      inserted = False
      for i in range(len(ordered_keys)):
        if v[subkey] > dictionary[ordered_keys[i]][subkey]:
          ordered_keys.insert(i,k)
          inserted = True
          break
      if not inserted:
        ordered_keys.append(k)
  for x in ordered_keys:
    ret[x] = dictionary[x]
  return ret

def readlines(filename):
  if filename is not None:
    if os.path.isfile(filename):
      if filename.endswith('.gz'):
        f = gzip.open(filename, errors='replace')
      else:
        f = open(filename, errors='replace')
      for line in f.readlines():
        yield line.strip()
      f.close()

def readlines_reverse(filename, max_lines):
  '''Get the trailing lines from a file, stopping
  after max_lines unless max_lines is negative'''
  if filename is not None:
    if os.path.isfile(filename):
      if filename.endswith('.gz'):
        f = gzip.open(filename, errors='replace')
      else:
        f = open(filename, errors='replace')
      n_lines = 0
      f.seek(0, os.SEEK_END)
      position = f.tell()
      line = ''
      while position >= 0:
        if n_lines > max_lines and max_lines>0:
          break
        f.seek(position)
        next_char = f.read(1)
        if next_char == "\n":
           n_lines += 1
           yield line[::-1]
           line = ''
        else:
           line += next_char
        position -= 1
      yield line[::-1]

def check_cvmfs(job):
  '''Return wether a CVMFS error is detected'''
  for line in readlines_reverse(job.get('stdout'),20):
    for x in config.cvmfs_error_strings:
      if line.find(x) >= 0:
        return False
  return True

def check_xrootd(job):
  if job.get('ExitCode') is not None:
    if job.get('ExitCode') == 212:
      return False
  return True

def get_exit_code(job):
  '''Extract the exit code from the log file'''
  for line in readlines_reverse(job.get('stderr'),3):
    cols = line.strip().split()
    if len(cols) == 2 and cols[0] == 'exit':
      try:
        return int(cols[1])
      except:
        pass
  return None
  
def make_timeline_entry(args):
  data = {}
  summary = config.job_counts.copy()
  #condor = {}
  for cid,job in condor_cluster_summary(args).items():
    #try:
    #  condor[cid] = {'attempts':int(job['att'])}
    #except:
    #  pass
    for x in summary.keys():
      summary[x] += job[x]
  summary.pop('done')
  summary.pop('total')
  attempts = []
  for condor_id,job in condor_yield(args):
    try:
      n = int(job['NumJobStarts'])
      if n > 0:
        attempts.append(n)
    except:
      pass
  summary['attempts'] = 0
  if len(attempts) > 0:
    summary['attempts'] = round(sum(attempts) / len(attempts),2)
  sites = {}
  for site,val in condor_site_summary(args).items():
    if site is not None:
      sites[site] = val['run']
  data['global'] = summary
  data['sites'] = sites
  #data['condor'] = condor
  data['update_ts'] = int(datetime.datetime.now().timestamp())
  return data

def timeline(args):
  data = make_timeline_entry(args)
  basename = 'timeline.json'
  srcdir = os.getenv('HOME')
  webdir = '/u/group/clas/www/clasweb-2015/html/clas12offline/osg'
  srcpath = '%s/%s'%(srcdir,basename)
  webpath = '%s/%s'%(webdir,basename)
  cache = []
  perms = stat.S_IRWXU & (stat.S_IRUSR|stat.S_IWUSR)
  perms |= stat.S_IRWXG & (stat.S_IRGRP)
  perms |= stat.S_IRWXO & (stat.S_IROTH)
  try:
    os.chmod(srcpath, perms)
  except:
    pass
  if os.path.exists(srcpath) and os.access(srcpath, os.R_OK):
    with open(srcpath,'r') as f:
      cache = json.load(f)
  cache.append(data)
  if not os.path.exists(srcpath) or os.access(srcpath, os.W_OK):
    with open(srcpath,'w') as f:
      f.write(json.dumps(cache))
  else:
    print('Archive DNE or unwritable:  '+srcpath)
    print(json.dumps(cache,**config.json_format))
  if os.access(webpath, os.W_OK):
    shutil.copy(srcpath,webpath)
  try:
    os.chmod(srcpath,stat.S_IRWXU&(stat.S_IRUSR))
  except:
    pass
  #  now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
  #  archive = webdir+'/archive/timeline-%s.json'%now
  #  shutil.copy(srcpath,archive)
  #  cmd = ['find',webdir+'/archive/','-name','*.json','-ctime','+14','-delete']
  #  subprocess.check_output(cmd)

def tail_log(job, nlines):
  print(''.ljust(80,'#'))
  print(''.ljust(80,'#'))
  print(job_table.get_header())
  print(job_table.job_to_row(job))
  for x in (job['UserLog'],job['stdout'],job['stderr']):
    if x is not None and os.path.isfile(x):
      print(''.ljust(80,'>'))
      print(x)
      if args.tail > 0:
        print('\n'.join(reversed(list(readlines_reverse(x, args.tail)))))
      elif args.tail < 0:
        for x in readlines(x):
          print(x)




