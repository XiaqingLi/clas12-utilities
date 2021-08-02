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
