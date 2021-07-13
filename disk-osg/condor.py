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

condor_data_tallies = {'goodwall':0, 'badwall':0, 'goodcpu':0, 'badcpu':0, 'goodattempts':0, 'badattempts':0, 'attempts':[]}
condor_data = collections.OrderedDict()

def condor_query(args):
  '''Load data from condor_q and condor_history'''
  constraints = []
  for x in args.condor:
    if not str(x).startswith('-'):
      constraints.append(str(x))
  opts = []
  if args.held:
    opts.append('-hold')
  if args.running:
    opts.append('-run')
  if not args.completed or args.plot is not False:
    condor_q(constraints=constraints, opts=opts)
  if args.hours > 0:
    condor_history(args, constraints=constraints)
  condor_munge(args)

def condor_read(args):
  global condor_data
  data = json.load(open(args.input,'r'))
  if type(data) is list:
    for x in data:
      if 'ClusterId' in x and 'ProcId' in x:
        condor_data['%d.%d'%(x['ClusterId'],x['ProcId'])] = x
  elif type(data) is dict:
    condor_data = data
  else:
    raise TypeError()
  condor_munge(args)

def condor_write(path):
  with open(path,'w') as f:
    f.write(json.dumps(condor_data, **json_format))

def condor_add_json(cmd):
  '''Add JSON condor data to local dictionary'''
  global condor_data
  response = None
  try:
    response = subprocess.check_output(cmd).decode('UTF-8')
    if len(response) > 0:
      for x in json.loads(response):
        if 'ClusterId' in x and 'ProcId' in x:
          condor_data['%d.%d'%(x['ClusterId'],x['ProcId'])] = x
        else:
          pass
  except:
    print('Error running command:  '+' '.join(cmd)+':')
    print(response)
    sys.exit(1)

def condor_vacate_job(job):
  cmd = ['condor_vacate_job', '-fast', job.get('condorid')]
  response = None
  try:
    response = subprocess.check_output(cmd).decode('UTF-8').rstrip()
    if re.fullmatch('Job %s fast-vacated'%job.get('condorid'), response) is None:
      raise ValueError()
  except:
    print('ERROR running command "%s":\n%s'%(' '.join(cmd),response))
  print(str(job.get('MATCH_GLIDEIN_Site'))+' '+str(job.get('RemoteHost'))+' '+str(job.get('condorid')))

def condor_hold_job(job):
  cmd = ['condor_hold', job.get('condorid')]
  response = None
  try:
    response = subprocess.check_output(cmd).decode('UTF-8').rstrip()
    print(response)
  except:
    print('ERROR running command "%s":\n%s'%(' '.join(cmd),response))

def condor_q(constraints=[], opts=[]):
  '''Get the JSON from condor_q'''
  cmd = ['condor_q','gemc']
  cmd.extend(constraints)
  cmd.extend(opts)
  cmd.extend(['-nobatch','-json'])
  condor_add_json(cmd)

def condor_history(args, constraints=[]):
  '''Get the JSON from condor_history'''
  start = args.end + datetime.timedelta(hours = -args.hours)
  start = str(int(start.timestamp()))
  cmd = ['condor_history','gemc']
  cmd.extend(constraints)
  cmd.extend(['-json','-since',"CompletionDate!=0&&CompletionDate<%s"%start])
  condor_add_json(cmd)

def condor_munge(args):
  '''Assign custom parameters based on parsing some condor parameters'''
  for condor_id,job in condor_data.items():
    job['user'] = None
    job['gemc'] = None
    job['host'] = None
    job['condor'] = None
    job['stderr'] = None
    job['stdout'] = None
    job['eff'] = None
    job['ceff'] = None
    job['generator'] = get_generator(job)
    job['wallhr'] = condor_calc_wallhr(job)
    job['condorid'] = '%d.%d'%(job['ClusterId'],job['ProcId'])
    job['gemcjob'] = '.'.join(job.get('Args').split()[0:2])
    # setup clas12 job ids and usernames:
    if 'UserLog' in job:
      m = re.search(log_regex, job['UserLog'])
      if m is not None:
        job['user'] = m.group(1)
        job['gemc'] = m.group(2)
        job['condor'] = m.group(3)+'.'+m.group(4)
        job['stderr'] = job['UserLog'][0:-4]+'.err'
        job['stdout'] = job['UserLog'][0:-4]+'.out'
        if condor_id != job['condor']:
          raise ValueError('condor ids do not match.')
    # trim hostnames to the important bit:
    if job.get('RemoteHost') is not None:
      job['host'] = job.get('RemoteHost').split('@').pop()
    if job.get('LastRemoteHost') is not None:
      job['LastRemoteHost'] = job.get('LastRemoteHost').split('@').pop().split('.').pop(0)
    # calculate cpu utilization for good, completed jobs:
    if job_states[job['JobStatus']] == 'C' and  float(job.get('wallhr')) > 0:
        job['eff'] = '%.2f'%(float(job.get('RemoteUserCpu')) / float(job.get('wallhr'))/60/60)
    # calculate cumulative cpu efficiency for all jobs:
    if job.get('CumulativeSlotTime') > 0:
      if job_states[job['JobStatus']] == 'C' or job_states[job['JobStatus']] == 'R':
        job['ceff'] = '%.2f'%(float(job.get('RemoteUserCpu'))/job.get('CumulativeSlotTime'))
      else:
        job['ceff'] = 0
    # get exit code from log files (since it's not always available from condor):
    if args.parseexit and job_states[job['JobStatus']] == 'H':
      job['ExitCode'] = tools.get_exit_code(job)
    condor_tally(job)

def condor_tally(job):
  '''Increment total good/bad job counts and times'''
  global condor_data_tallies
  x = condor_data_tallies
  if job_states[job['JobStatus']] == 'C' or job_states[job['JobStatus']] == 'R':
    if job['NumJobStarts'] > 0:
      x['attempts'].append(job['NumJobStarts'])
    if job_states[job['JobStatus']] == 'C':
      x['goodattempts'] += 1
      x['goodwall'] += float(job['wallhr'])*60*60
      x['goodcpu'] += job['RemoteUserCpu']
    if job['NumJobStarts'] > 1:
      x['badattempts'] += job['NumJobStarts'] - 1
      x['badwall'] += job['CumulativeSlotTime'] - float(job['wallhr'])*60*60
      x['badcpu'] += job['CumulativeRemoteUserCpu'] - job['RemoteUserCpu']
  elif job['NumJobStarts'] > 0 and job_states[job['JobStatus']] != 'X':
      x['badattempts'] += job['NumJobStarts']
      x['badwall'] += job['CumulativeSlotTime']
      x['badcpu'] += job['CumulativeRemoteUserCpu']
  x['totalwall'] = x['badwall'] + x['goodwall']
  x['totalcpu'] = x['badcpu'] + x['goodcpu']

def condor_calc_wallhr(job):
  '''Calculate the wall hours of the final, completed instance of a job,
  because it does not seem to be directly available from condor.  This may
  may be an overestimate of the job itself, depending on how start date
  and end date are triggered, but that's ok.'''
  ret = None
  if job_states[job['JobStatus']] == 'C' or job_states[job['JobStatus']] == 'R':
    start = job.get('JobCurrentStartDate')
    end = job.get('CompletionDate')
    if start is not None and start > 0:
      start = datetime.datetime.fromtimestamp(int(start))
      if end is not None and end > 0:
        end = datetime.datetime.fromtimestamp(int(end))
      else:
        end = datetime.datetime.now()
      ret = '%.2f' % ((end - start).total_seconds()/60/60)
  return ret

###########################################################
###########################################################

def condor_yield(args):
  for condor_id,job in condor_data.items():
    if condor_match(job, args):
      yield (condor_id, job)

class Matcher():
  def __init__(self, values):
    self.values = []
    self.antivalues = []
    for v in [str(v) for v in values]:
      if v.startswith('-'):
        self.antivalues.append(v[1:])
      else:
        self.values.append(v)
  def matches(self, value):
    if len(self.values) > 0 and str(value) not in self.values:
      return False
    if len(self.antivalues) > 0 and str(value) in self.antivalues:
      return False
    return True
  def pattern_matches(self, value):
    for v in self.values:
      found = False
      if v.find(str(value)) >= 0:
        found = True
        break
      if not found:
        return False
    for v in self.antivalues:
      if v.find(str(value)) >= 0:
        return False
    return True

condor_matcher = None
site_matcher = None
gemc_matcher = None
user_matcher = None
exit_matcher = None
gen_matcher = None
host_matcher = None
def condor_match(job, args):
  ''' Apply job constraints, on top of those condor knows about'''
  global condor_matcher
  if condor_matcher is None:
    global site_matcher
    global gemc_matcher
    global user_matcher
    global exit_matcher
    global gen_matcher
    global host_matcher
    condor_matcher = Matcher(args.condor)
    site_matcher = Matcher(args.site)
    gemc_matcher = Matcher(args.gemc)
    user_matcher = Matcher(args.user)
    exit_matcher = Matcher(args.exit)
    gen_matcher = Matcher(args.generator)
    host_matcher = Matcher(args.host)
  if not condor_matcher.matches(job.get('condor').split('.').pop(0)):
    return False
  if not gemc_matcher.matches(job.get('gemc')):
    return False
  if not user_matcher.matches(job.get('user')):
    return False
  if not site_matcher.pattern_matches(job.get('MATCH_GLIDEIN_Site')):
    return False
  if not host_matcher.pattern_matches(job.get('LastRemoteHost')):
    return False
  if not gen_matcher.matches(job.get('generator')):
    return False
  if args.noexit:
    if job.get('ExitCode') is not None:
      return False
  elif not exit_matcher.matches(job.get('ExitCode')):
    return False
  if args.plot is False:
    if args.idle and job_states.get(job['JobStatus']) != 'I':
      return False
    if args.completed and job_states.get(job['JobStatus']) != 'C':
      return False
    if args.running and job_states.get(job['JobStatus']) != 'R':
      return False
    if args.held and job_states.get(job['JobStatus']) != 'H':
      return False
  try:
    if int(job['CompletionDate']) > int(args.end.timestamp()):
      return False
  except:
    pass
  return True

def get_status_key(job):
  if job_states[job['JobStatus']] == 'H':
    return 'held'
  elif job_states[job['JobStatus']] == 'I':
    return 'idle'
  elif job_states[job['JobStatus']] == 'R':
    return 'run'
  elif job_states[job['JobStatus']] == 'C':
    return 'done'
  else:
    return 'other'

def condor_cluster_summary(args):
  '''Tally jobs by condor's ClusterId'''
  ret = collections.OrderedDict()
  for condor_id,job in condor_yield(args):
    cluster_id = condor_id.split('.').pop(0)
    if cluster_id not in ret:
      ret[cluster_id] = job.copy()
      ret[cluster_id].update(job_counts.copy())
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
      sites[site].update(job_counts.copy())
      sites[site]['wallhr'] = []
    sites[site]['total'] += 1
    sites[site][get_status_key(job)] += 1
    if args.running or job_states[job['JobStatus']] == 'C':
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
  ret += '\n'.join(['%4s  %8d %6.2f%%  %s'%(k,v,v/tot*100,exit_codes.get(k)) for k,v in x.items()])
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

# cache generator names to only parse log once per cluster
generators = {}
def get_generator(job):
  if job.get('ClusterId') not in generators:
    generators['ClusterId'] = config.null_field
    if job.get('UserLog') is not None:
      job_script = os.path.dirname(os.path.dirname(job.get('UserLog')))+'/nodeScript.sh'
      for line in tools.readlines(job_script):
        line = line.lower()
        m = re.search('events with generator >(.*)< with options', line)
        if m is not None:
          if m.group(1).startswith('clas12-'):
            generators['ClusterId'] = m.group(1)[7:]
          else:
            generators['ClusterId'] = m.group(1)
          break
        if line.find('echo LUND Event File:') == 0:
          generators['ClusterId'] = 'lund'
          break
        if line.find('gemc') == 0 and line.find('INPUT') < 0:
          generators['ClusterId'] = 'gemc'
          break
  return generators.get('ClusterId')
