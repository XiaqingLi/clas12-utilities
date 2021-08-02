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
import tools

def condor_cluster_summary(args):
  '''Tally jobs by condor's ClusterId'''
  ret = collections.OrderedDict()
  for condor_id,job in condor_yield(args):
    cluster_id = condor_id.split('.').pop(0)
    if cluster_id not in ret:
      ret[cluster_id] = job.copy()
      ret[cluster_id].update(config.job_counts.copy())
      ret[cluster_id]['eff'] = []
      ret[cluster_id]['ceff'] = []
      ret[cluster_id]['att'] = []
    ret[cluster_id][get_status_key(job)] += 1
    ret[cluster_id]['done'] = ret[cluster_id]['TotalSubmitProcs']
    ret[cluster_id]['done'] -= ret[cluster_id]['held']
    ret[cluster_id]['done'] -= ret[cluster_id]['idle']
    ret[cluster_id]['done'] -= ret[cluster_id]['run']
    try:
      if job['NumJobStarts'] > 0:
        ret[cluster_id]['att'].append(job['NumJobStarts'])
      x = float(job['eff'])
      ret[cluster_id]['eff'].append(x)
      x = float(job['ceff'])
      ret[cluster_id]['ceff'].append(x)
    except:
      pass
  for v in ret.values():
    v['eff'] = tools.average(v['eff'])
    v['ceff'] = tools.average(v['ceff'])
    v['att'] = tools.average(v['att'])
  return ret

def condor_site_summary(args):
  '''Tally jobs by site.  Note, including completed jobs
  here is only possible if condor_history is included.'''
  sites = collections.OrderedDict()
  for condor_id,job in condor_yield(args):
    site = job.get('MATCH_GLIDEIN_Site')
    if site not in sites:
      sites[site] = job.copy()
      sites[site].update(config.job_counts.copy())
      sites[site]['wallhr'] = []
    sites[site]['total'] += 1
    sites[site][get_status_key(job)] += 1
    if args.running or config.job_states[job['JobStatus']] == 'C':
      try:
        x = float(job.get('wallhr'))
        sites[site]['wallhr'].append(x)
      except:
        pass
  for site in sites.keys():
    sites[site]['ewallhr'] = tools.stddev(sites[site]['wallhr'])
    sites[site]['wallhr'] = tools.average(sites[site]['wallhr'])
    if args.hours <= 0:
      sites[site]['done'] = config.null_field
  return tools.sort_dict(sites, 'total')

def condor_exit_code_summary(args):
  x = {}
  for cid,job in condor_yield(args):
    if job.get('ExitCode') is not None:
      if job.get('ExitCode') not in x:
        x[job.get('ExitCode')] = 0
      x[job.get('ExitCode')] += 1
  tot = sum(x.values())
  ret = '\nExit Code Summary:\n'
  ret += '------------------------------------------------\n'
  ret += '\n'.join(['%4s  %8d %6.2f%%  %s'%(k,v,v/tot*100,config.exit_codes.get(k)) for k,v in x.items()])
  return ret + '\n'

def condor_efficiency_summary():
  global condor_data_tallies
  x = condor_data_tallies
  ret = ''
  if len(x['attempts']) > 0:
    ret += '\nEfficiency Summary:\n'
    ret += '------------------------------------------------\n'
    ret += 'Number of Good Job Attempts:  %10d\n'%x['goodattempts']
    ret += 'Number of Bad Job Attempts:   %10d\n'%x['badattempts']
    ret += 'Average # of Job Attempts:    % 10.1f\n'%(sum(x['attempts'])/len(x['attempts']))
    ret += '------------------------------------------------\n'
    ret += 'Total Wall and Cpu Hours:   %.3e %.3e\n'%(x['totalwall'],x['totalcpu'])
    ret += 'Bad Wall and Cpu Hours:     %.3e %.3e\n'%(x['badwall'],x['badcpu'])
    ret += 'Good Wall and Cpu Hours:    %.3e %.3e\n'%(x['goodwall'],x['goodcpu'])
    ret += '------------------------------------------------\n'
    if x['goodwall'] > 0:
      ret += 'Cpu Utilization of Good Jobs:        %.1f%%\n'%(100*x['goodcpu']/x['goodwall'])
    if x['totalwall'] > 0:
      ret += 'Good Fraction of Wall Hours:         %.1f%%\n'%(100*x['goodwall']/x['totalwall'])
      ret += 'Total Efficiency:                    %.1f%%\n'%(100*x['goodcpu']/x['totalwall'])
    ret += '------------------------------------------------\n\n'
  return ret
