#!/usr/bin/env python3
#
# N. Baltzell, April 2021
#
# Wrap condor_q and condor_history commands into one, with convenenience
# options for common uses, e.g. query criteria specific to CLAS12 jobs,
# searching logs for CVMFS issues, and printing tails of logs.
#

import re
import os
import sys
import glob
import json
import time
import math
import socket
import getpass
import argparse
import datetime
import subprocess
import collections

dedicated_sites = [ 'CNAF', 'SGridGLA' ]
priority_sites = [ 'SU-ITS', 'UConn-HPS', 'UConn', 'MIT' , 'GRIF' ]

json_format =  {'indent':2, 'separators':(',',': '), 'sort_keys':True}
log_regex = '/([a-z]+)/job_([0-9]+)/log/job\.([0-9]+)\.([0-9]+)\.'
job_states = {0:'U', 1:'I', 2:'R', 3:'X', 4:'C', 5:'H', 6:'E'}
job_counts = {'done':0, 'run':0, 'idle':0, 'held':0, 'other':0, 'total':0}
null_field = '-'
cvmfs_error_strings = [
  'Loaded environment state is inconsistent',
  'Command not found',
  'Unable to access the Singularity image',
  'CVMFS ERROR'
#  'No such file or directory'
#  'Transport endpoint is not connected',
]

###########################################################
# Interactions with condor:
###########################################################

condor_data = collections.OrderedDict()

def condor_query(constraints=[], opts=[], hours=0, completed=False):
  '''Load data from condor_q and condor_history'''
  if not completed:
    condor_q(constraints=constraints, opts=opts)
  if hours > 0:
    condor_history(constraints=constraints, hours=hours)
  condor_munge()

def condor_read(path):
  '''Load condor data from a JSON file'''
  global condor_data
  condor_data = json.load(open(path,'r'))
  condor_munge()

def condor_write(path):
  '''Write condor data to a JSON file'''
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
  '''Vacate a job, e.g. send it from running to idle state'''
  cmd = ['condor_vacate_job', '-fast', job.get('condorid')]
  response = None
  try:
    response = subprocess.check_output(cmd).decode('UTF-8').rstrip()
    if re.fullmatch('Job %s fast-vacated'%job.get('condorid'), response) is None:
      raise ValueError()
  except:
    print('ERROR running command "%s":\n%s'%(' '.join(cmd),response))
  print(str(job.get('MATCH_GLIDEIN_Site'))+' '+str(job.get('RemoteHost'))+' '+str(job.get('condorid')))

def condor_q(constraints=[], opts=[]):
  '''Get the JSON from condor_q'''
  cmd = ['condor_q','gemc']
  cmd.extend(constraints)
  cmd.extend(opts)
  cmd.extend(['-nobatch','-json'])
  condor_add_json(cmd)

def condor_history(constraints=[], hours=1):
  '''Get the JSON from condor_history'''
  now = datetime.datetime.now()
  start = now + datetime.timedelta(hours = -hours)
  start = str(int(start.timestamp()))
  cmd = ['condor_history','gemc']
  cmd.extend(constraints)
  cmd.extend(opts)
  cmd.extend(['-json','-since',"CompletionDate!=0&&CompletionDate<%s"%start])
  condor_add_json(cmd)

def condor_munge():
  '''Assign custom parameters based on parsing some condor parameters'''
  for condor_id,job in condor_data.items():
    job['user'] = None
    job['gemc'] = None
    job['host'] = None
    job['condor'] = None
    job['stderr'] = None
    job['stdout'] = None
    job['generator'] = get_generator(job)
    job['wallhr'] = condor_calc_wallhr(job)
    job['condorid'] = '%d.%d'%(job['ClusterId'],job['ProcId'])
    job['gemcjob'] = '.'.join(job.get('Args').split()[0:2])
    if job.get('RemoteHost') is not None:
      job['host'] = job.get('RemoteHost').split('@').pop()
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

def condor_calc_wallhr(job):
  '''Use available info to calculate wall hours, since there does
  not seem to be a more reliable way'''
  ret = None
  if job_states[job['JobStatus']] == 'X':
    return ret
  if job_states[job['JobStatus']] == 'H':
    return ret
  if job_states[job['JobStatus']] == 'E':
    return ret
  start = job.get('JobCurrentStartDate')
  end = job.get('CompletionDate')
  if start is not None and start > 0:
    start = datetime.datetime.fromtimestamp(int(start))
    if end is not None and end > 0:
      end = datetime.datetime.fromtimestamp(int(end))
    else:
      end = datetime.datetime.now()
    ret = '%.1f' % ((end - start).total_seconds()/60/60)
  return ret

###########################################################
# Analysis of condor data:
###########################################################

def condor_yield(args):
  '''An iterator that applies matching criteria on condor_data'''
  for condor_id,job in condor_data.items():
    if condor_match(job, args):
      yield (condor_id, job)

def condor_match(job, args):
  '''Apply job constraints, on top of those condor knows about'''
  if len(args.condor)>0 and job['condor'] not in args.condor:
    if job['condor'].split('.').pop(0) not in args.condor:
      return False
  if len(args.gemc)>0 and job['gemc'] not in args.gemc:
    if job['gemcjob'] not in args.gemc:
      return False
  if len(args.user)>0 and job['user'] not in args.user:
    return False
  if len(args.site) > 0:
    if job.get('MATCH_GLIDEIN_Site') is None:
      return False
    matched = False
    for site in args.site:
      if job['MATCH_GLIDEIN_Site'].find(site) >= 0:
        matched = True
        break
    if not matched:
      return False
  if args.idle and job_states.get(job['JobStatus']) != 'I':
    return False
  if args.completed and job_states.get(job['JobStatus']) != 'C':
    return False
  if args.running and job_states.get(job['JobStatus']) != 'R':
    return False
  if args.held and job_states.get(job['JobStatus']) != 'H':
    return False
  return True

def get_status_key(job):
  '''Make a job state name for human consumption'''
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

def average(alist):
  '''Get the average of a list'''
  if len(alist) > 0:
    return '%.1f' % (sum(alist) / len(alist))
  else:
    return null_field

def stddev(alist):
  '''Get the standard deviation of a list'''
  if len(alist) > 0:
    m = average(alist)
    s = sum([ (x-float(m))*(x-float(m)) for x in alist ])
    return '%.1f' % math.sqrt(s / len(alist))
  else:
    return null_field

condor_cluster_data = None
def condor_cluster_summary(args):
  '''Tally jobs by condor's ClusterId'''
  global condor_cluster_data
  if condor_cluster_data is None:
    x = collections.OrderedDict()
    for condor_id,job in condor_yield(args):
      cluster_id = condor_id.split('.').pop(0)
      if cluster_id not in x:
        x[cluster_id] = job.copy()
        x[cluster_id].update(job_counts.copy())
      x[cluster_id][get_status_key(job)] += 1
      x[cluster_id]['done'] = x[cluster_id]['TotalSubmitProcs']
      x[cluster_id]['done'] -= x[cluster_id]['held']
      x[cluster_id]['done'] -= x[cluster_id]['idle']
      x[cluster_id]['done'] -= x[cluster_id]['run']
      condor_cluster_data = x
  return condor_cluster_data

condor_site_data = None
def condor_site_summary(args):
  '''Tally jobs by site.  Note, including completed jobs
  here is only possible if condor_history is included.'''
  global condor_site_data
  if condor_site_data is None:
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
      sites[site]['ewallhr'] = stddev(sites[site]['wallhr'])
      sites[site]['wallhr'] = average(sites[site]['wallhr'])
      if args.hours <= 0:
        sites[site]['done'] = null_field
    condor_site_data = sort_dict(sites, 'total')
  return condor_site_data

###########################################################
# General utility stuff:
###########################################################

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
  '''An iterator to read lines from a file'''
  if filename is not None:
    if os.path.isfile(filename):
      with open(filename, errors='replace') as f:
        for line in f.readlines():
          yield line.strip()

def readlines_reverse(filename, max_lines):
  '''Get the trailing lines from a file, stopping
  after max_lines unless max_lines is negative'''
  n_lines = 0
  with open(filename, errors='replace') as qfile:
    qfile.seek(0, os.SEEK_END)
    position = qfile.tell()
    line = ''
    while position >= 0:
      if n_lines > max_lines and max_lines>0:
        break
      qfile.seek(position)
      next_char = qfile.read(1)
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
  for line in readlines(job.get('stderr')):
    for x in cvmfs_error_strings:
      if line.find(x) >= 0:
        return False
  return True

def tail_log(job, nlines):
  '''Print the tails of a job's log files'''
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

###########################################################
# CLAS12-specific stuff:
###########################################################

generators = {}
def get_generator(job):
  '''Get the name of the event generator for a job.  Since this
  requires parsing logs currently, cache them by ClusterId'''
  if job.get('ClusterId') not in generators:
    generators['ClusterId'] = null_field
    if job.get('UserLog') is not None:
      job_script = os.path.dirname(os.path.dirname(job.get('UserLog')))+'/nodeScript.sh'
      for line in readlines(job_script):
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
  return generators.get('ClusterId')

def clas12mon(args):
  '''Publish current job counts to clas12mon for timelines'''
  data = job_counts.copy()
  data['sites'] = {}
  for site, job in condor_site_summary(args).items():
    for x in job_counts.keys():
      try:
        data[x] += job[x]
        if x == 'run':
          if site not in data['sites'] and site is not None:
            data['sites'][site] = 0
          data['sites'][site] += job[x]
      except:
        pass
  attempts = []
  for condor_id,job in condor_yield(args):
    if job.get('NumJobStarts') is not None:
      print(job.get('NumJobStarts'))
      if job.get('NumJobStarts') > 0:
        attempts.append(job.get('NumJobsStarts'))
  data['avg_attempts'] = 0
  if len(attempts) > 0:
    data['avg_attempts'] = sum(attempts) / len(attempts)
  data.pop('done')
  data.pop('total')
  data['update_ts'] = int(datetime.datetime.now().timestamp())
  return
  auth = os.getenv('HOME')+'/.clas12mon.auth'
  if not os.path.isfile(auth):
    print('ERROR:  Authorization file does not exist:  '+auth)
    sys.exit(1)
  auth = open(auth).read().strip()
  url = 'https://clas12mon.jlab.org/api/OSGEntries'
  return requests.post(url, data=data, headers={'Authorization':auth})

###########################################################
# Table classes for tallying and printing:
###########################################################

class Column():
  def __init__(self, name, width, tally=None):
    self.name = name
    self.width = width
    self.tally = tally
    self.fmt = '%%-%d.%ds' % (self.width, self.width)

class Table():
  max_width = 114
  def __init__(self):
    self.columns = []
    self.rows = []
    self.tallies = []
    self.width = 0
  def add_column(self, column, tally=None):
    if not isinstance(column, Column):
      raise TypeError()
    self.columns.append(column)
    self.tallies.append([])
    self.fmt = ' '.join([x.fmt for x in self.columns])
    self.width = sum([x.width for x in self.columns]) + len(self.columns) - 1
  def add_row(self, values):
    self.rows.append(self.values_to_row(values).rstrip())
    self.tally(values)
  def tally(self, values):
    for i in range(len(values)):
      if self.columns[i].tally is not None:
        try:
          x = float(values[i])
          self.tallies[i].append(x)
        except:
          pass
  def values_to_row(self, values):
    return self.fmt % tuple([str(x).strip() for x in values])
  def get_tallies(self):
    # assume it's never appropriate to tally the 1st column
    values = ['tally']
    for i in range(1,len(self.columns)):
      if self.columns[i].tally is not None and len(self.tallies[i]) > 0:
        values.append(sum(self.tallies[i]))
        if self.columns[i].tally is 'avg':
          if values[-1] > 0:
            values[-1] = '%.1f' % (values[-1]/len(self.tallies[i]))
        else:
          values[-1] = int(values[-1])
      else:
        values.append(null_field)
    return (self.fmt % tuple(values)).rstrip()
  def get_header(self):
    ret = ''.ljust(min(Table.max_width,self.width), null_field)
    ret += '\n' + (self.fmt % tuple([x.name for x in self.columns])).rstrip()
    ret += '\n' + ''.ljust(min(Table.max_width,self.width), null_field)
    return ret
  def __str__(self):
    rows = [self.get_header()]
    rows.extend(self.rows)
    rows.append(self.get_tallies())
    rows.append(self.get_header())
    return '\n'.join(rows)

class CondorColumn(Column):
  def __init__(self, name, varname, width, tally=None):
    super().__init__(name, width, tally)
    self.varname = varname

class CondorTable(Table):
  def add_column(self, name, varname, width, tally=None):
    super().add_column(CondorColumn(name, varname, width, tally))
  def job_to_values(self, job):
    return [self.munge(x.varname, job.get(x.varname)) for x in self.columns]
  def job_to_row(self, job):
    return self.values_to_row(self.job_to_values(job))
  def add_job(self, job):
    self.add_row(self.job_to_values(job))
    return self
  def add_jobs(self,jobs):
    for k,v in jobs.items():
      self.add_job(v)
    return self
  def munge(self, name, value):
    ret = value
    if value is None or value == 'undefined':
      ret = null_field
    elif name == 'Args':
      ret = ' '.join(value.split()[2:])
    elif name == 'ExitBySignal':
      ret = {True:'Y',False:'N'}[value]
    elif name == 'JobStatus':
      try:
        ret = job_states[value]
      except:
        pass
    elif name.endswith('Date'):
      if value == '0' or value == 0:
        ret = null_field
      else:
        try:
          x = datetime.datetime.fromtimestamp(int(value))
          ret = x.strftime('%m/%d %H:%M')
        except:
          pass
    return ret

###########################################################
###########################################################

summary_table = CondorTable()
summary_table.add_column('condor','ClusterId',9)
summary_table.add_column('gemc','gemc',6)
summary_table.add_column('submit','QDate',12)
summary_table.add_column('total','TotalSubmitProcs',8,tally='sum')
summary_table.add_column('done','done',8,tally='sum')
summary_table.add_column('run','run',8,tally='sum')
summary_table.add_column('idle','idle',8,tally='sum')
summary_table.add_column('held','held',8,tally='sum')
summary_table.add_column('user','user',10)
summary_table.add_column('gen','generator',9)

site_table = CondorTable()
site_table.add_column('site','MATCH_GLIDEIN_Site',26)
site_table.add_column('total','total',8,tally='sum')
site_table.add_column('done','done',8,tally='sum')
site_table.add_column('run','run',8,tally='sum')
site_table.add_column('idle','idle',8,tally='sum')
site_table.add_column('held','held',8,tally='sum')
site_table.add_column('wallhr','wallhr',6)
site_table.add_column('stddev','ewallhr',7)

job_table = CondorTable()
job_table.add_column('condor','condorid',13)
job_table.add_column('gemc','gemc',6)
job_table.add_column('site','MATCH_GLIDEIN_Site',10)
#job_table.add_column('host','host',20)
job_table.add_column('stat','JobStatus',4)
job_table.add_column('exit','ExitCode',4)
job_table.add_column('sig','ExitBySignal',4)
job_table.add_column('att','NumJobStarts',4,tally='avg')
job_table.add_column('wallhr','wallhr',6,tally='avg')
job_table.add_column('start','JobCurrentStartDate',12)
job_table.add_column('end','CompletionDate',12)
job_table.add_column('user','user',10)
job_table.add_column('gen','generator',9)
#job_table.add_column('args','Args',30)

###########################################################
###########################################################

if __name__ == '__main__':

  cli = argparse.ArgumentParser(description='Wrap condor_q and condor_history and add features for CLAS12.',
      epilog='''Per-site wall-hour tallies ignore running jobs, unless -running is specified.
                Repeatable "limit" options are first OR\'d independently, then all "limit" options are AND'd together.''')
  cli.add_argument('-condor', default=[], metavar='# or #.#', action='append', type=str, help='limit by condor id (repeatable)')
  cli.add_argument('-gemc', default=[], metavar='# or #.#', action='append', type=str, help='limit by gemc submission id (repeatable)')
  cli.add_argument('-user', default=[], action='append', type=str, help='limit by portal submitter\'s username (repeatable)')
  cli.add_argument('-site', default=[], action='append', type=str, help='limit by OSG site name, pattern matched (repeatable)')
  cli.add_argument('-held', default=False, action='store_true', help='limit to jobs currently in held state')
  cli.add_argument('-idle', default=False, action='store_true', help='limit to jobs currently in idle state')
  cli.add_argument('-running', default=False, action='store_true', help='limit to jobs currently in running state')
  cli.add_argument('-completed', default=False, action='store_true', help='limit to completed jobs')
  cli.add_argument('-summary', default=False, action='store_true', help='tabulate by cluster id instead of per-job')
  cli.add_argument('-sitesummary', default=False, action='store_true', help='tabulate by site instead of per-job')
  cli.add_argument('-hours', default=0, metavar='#', type=float, help='look back # hours for completed jobs (default=0)')
  cli.add_argument('-tail', default=None, metavar='#', type=int, help='print last # lines of logs (negative=all, 0=filenames)')
  cli.add_argument('-cvmfs', default=False, action='store_true', help='print hostnames from logs with CVMFS errors')
  cli.add_argument('-vacate', default=-1, metavar='#', type=float, help='vacate jobs with wall hours greater than #')
  cli.add_argument('-json', default=False, action='store_true', help='print full condor data in JSON format')
  cli.add_argument('-input', default=False, metavar='PATH', type=str, help='read condor data from a JSON file instead of querying')
  cli.add_argument('-clas12mon', default=False, action='store_true', help='publish results to clas12mon for timelines')

  args = cli.parse_args(sys.argv[1:])

  if args.held + args.idle + args.running + args.completed > 1:
    cli.error('Only one of -held/idle/running/completed is allowed.')

  if (bool(args.vacate>=0) + bool(args.tail is not None) + bool(args.cvmfs) + bool(args.json)) > 1:
    cli.error('Only one of -cvmfs/vacate/tail/json is allowed.')

  if args.completed and args.hours <= 0 and not args.input:
    cli.error('-completed requires -hours is greater than zero or -input.')

  if socket.gethostname() != 'scosg16.jlab.org' and not args.input:
    cli.error('You must be on scosg16 unless using the -input option.')

  if args.clas12mon and getpass.getuser() != 'gemc':
    cli.error('Only user=gemc can use -clas12mon.')

  opts, constraints = [], []

  if args.held:
    opts.append('-hold')

  if args.running:
    opts.append('-run')

  constraints.extend(args.condor)

  if args.input:
    condor_read(args.input)
  else:
    condor_query(constraints=constraints, opts=opts, hours=args.hours, completed=args.completed)

  if args.clas12mon:
    clas12mon(args)
    sys.exit(1)

  if args.json:
    print(json.dumps(condor_data, **json_format))
    sys.exit(0)

  for cid,job in condor_yield(args):

    if args.vacate > 0:
      if job.get('wallhr') is not None:
        if float(job.get('wallhr')) > args.vacate:
          if job_states.get(job['JobStatus']) == 'R':
            condor_vacate_job(job)

    elif args.cvmfs:
      if not check_cvmfs(job):
        if 'LastRemoteHost' in job:
          print(job.get('MATCH_GLIDEIN_Site')+' '+job['LastRemoteHost']+' '+cid)

    elif args.tail is not None:
      tail_log(job, args.tail)

    else:
      job_table.add_job(job)

  if len(job_table.rows) > 0:
    if args.summary or args.sitesummary:
      if args.summary:
        print(summary_table.add_jobs(condor_cluster_summary(args)))
      else:
        print(site_table.add_jobs(condor_site_summary(args)))
    else:
      print(job_table)

  sys.exit(0)

