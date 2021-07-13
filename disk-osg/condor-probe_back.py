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
import gzip
import stat
import shutil
import socket
import getpass
import argparse
import datetime
import subprocess
import collections

null_field = '-'
json_format =  {'indent':2, 'separators':(',',': '), 'sort_keys':True}
log_regex = '/([a-z]+)/job_([0-9]+)/log/job\.([0-9]+)\.([0-9]+)\.'
job_states = {0:'U', 1:'I', 2:'R', 3:'X', 4:'C', 5:'H', 6:'E'}
job_counts = {'done':0, 'run':0, 'idle':0, 'held':0, 'other':0, 'total':0}
exit_codes = { 202:'cvmfs', 203:'generator', 211:'ls', 204:'gemc', 0:'success/unknown',
               205:'evio2hipo', 207:'recon-util', 208:'hipo-utils', 212:'xrootd'}
cvmfs_error_strings = [ 'Loaded environment state is inconsistent',
  'Command not found','Unable to access the Singularity image','CVMFS ERROR']
#  'No such file or directory', 'Transport endpoint is not connected',

###########################################################
###########################################################

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
      job['ExitCode'] = get_exit_code(job)
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

def average(alist):
  if len(alist) > 0:
    return '%.2f' % (sum(alist) / len(alist))
  else:
    return null_field

def stddev(alist):
  if len(alist) > 0:
    m = average(alist)
    s = sum([ (x-float(m))*(x-float(m)) for x in alist ])
    return '%.2f' % math.sqrt(s / len(alist))
  else:
    return null_field

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
    v['eff'] = average(v['eff'])
    v['ceff'] = average(v['ceff'])
    v['att'] = average(v['att'])
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
    sites[site]['ewallhr'] = stddev(sites[site]['wallhr'])
    sites[site]['wallhr'] = average(sites[site]['wallhr'])
    if args.hours <= 0:
      sites[site]['done'] = null_field
  return sort_dict(sites, 'total')

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

root_store = []
def condor_plot(args, logscale=0):
  global root_store
  # pyROOT apparently looks at sys.argv and barfs if it finds an argument
  # it doesn't like, maybe ones starting with "h" (help).  Hopefully there
  # is a better way, but here we override sys.argv to avoid that:
  sys.argv = []
  #abort = True
  #for condor_id,job in condor_yield(args):
  #  if job.get('eff') is not None:
  #    abort = False
  #    break
  #if abort:
  #  print('Found no completed jobs to plot.')
  #  return None
  import ROOT
  for x in root_store:
    x.Delete()
  root_store = []
  ROOT.gStyle.SetCanvasColor(0)
  ROOT.gStyle.SetPadColor(0)
  ROOT.gStyle.SetTitleFillColor(0)
  ROOT.gStyle.SetTitleBorderSize(0)
  ROOT.gStyle.SetFrameBorderMode(0)
  ROOT.gStyle.SetPaintTextFormat(".0f")
  ROOT.gStyle.SetLegendBorderSize(1)
  ROOT.gStyle.SetLegendFillColor(ROOT.kWhite)
  ROOT.gStyle.SetTitleFontSize(0.04)
  ROOT.gStyle.SetPadTopMargin(0.05)
  ROOT.gStyle.SetPadLeftMargin(0.11)
  ROOT.gStyle.SetPadBottomMargin(0.12)
  ROOT.gStyle.SetTitleXSize(0.05)
  ROOT.gStyle.SetTitleYSize(0.05)
  ROOT.gStyle.SetTextFont(42)
  ROOT.gStyle.SetStatFont(42)
  ROOT.gStyle.SetLabelFont(42,"x")
  ROOT.gStyle.SetLabelFont(42,"y")
  ROOT.gStyle.SetLabelFont(42,"z")
  ROOT.gStyle.SetTitleFont(42,"x")
  ROOT.gStyle.SetTitleFont(42,"y")
  ROOT.gStyle.SetTitleFont(42,"z")
  ROOT.gStyle.SetHistLineWidth(2)
  ROOT.gStyle.SetGridColor(15)
  ROOT.gStyle.SetPadGridX(1)
  ROOT.gStyle.SetPadGridY(1)
  ROOT.gStyle.SetOptStat('emr')
  ROOT.gStyle.SetStatW(0.3)
  ROOT.gStyle.SetStatX(0.92)
  ROOT.gStyle.SetStatY(0.95)
  ROOT.gStyle.SetHistMinimumZero(ROOT.kTRUE)
  ROOT.gROOT.ForceStyle()
  can = ROOT.TCanvas('can','',1200,700)
  can.Divide(4,3)
  can.Draw()
  h1wall_site = {}
  h1eff_gen = {}
  h1eff_site = {}
  h1ceff_gen = {}
  h1ceff_site = {}
  h1att_gen = {}
  h1attq_gen = {}
  h1eff = ROOT.TH1D('h1eff',';CPU Utilization',100,0,1.5)
  h2eff = ROOT.TH2D('h2eff',';Wall Hours;CPU Utilization',100,0,20,100,0,1.6)
  h1ceff = ROOT.TH1D('h1ceff',';Cumulative Efficiency',100,0,1.6)
  h2ceff = ROOT.TH2D('h2ceff',';Cumulative Wall Hours;Cumulative Efficiency',200,0,40,100,0,1.5)
  h2att = ROOT.TH2D('h2att',';Job Attempts;Cumulative Efficiency',20,0.5,20.5,100,0,1.5)
  h1att = ROOT.TH1D('h1att',';Job Attempts',20,0.5,20.5)
  h1wall = ROOT.TH1D('h1wall',';Wall Hours',100,0,20)
  h1attq = h1att.Clone('h1attq')
  h1attq.GetXaxis().SetTitle('Queued Job Attempts')
  generators = set()

  # read condor data, fill histos:
  for condor_id,job in condor_yield(args):
    gen = job.get('generator')
    if job_states[job['JobStatus']] != 'C':
      try:
        n = int(job.get('NumJobStarts'))
        h1attq.Fill(n)
        if gen not in h1attq_gen:
          h1attq_gen[gen] = h1attq.Clone('h1attq_gen_%s'%gen)
          h1attq_gen[gen].Reset()
          generators.add(gen)
        h1attq_gen[gen].Fill(n)
      except:
        pass
    if job.get('eff') is not None:
      eff = float(job.get('eff'))
      ceff = float(job.get('ceff'))
      wall = float(job.get('wallhr'))
      cwall = float(job.get('CumulativeSlotTime'))/60/60
      site = job.get('MATCH_GLIDEIN_Site')
      if gen not in h1eff_gen:
        h1eff_gen[gen] = h1eff.Clone('h1eff_gen_%s'%gen)
        h1ceff_gen[gen] = h1ceff.Clone('h1ceff_gen_%s'%gen)
        h1att_gen[gen] = h1att.Clone('h1att_gen_%s'%gen)
        h1eff_gen[gen].Reset()
        h1ceff_gen[gen].Reset()
        h1att_gen[gen].Reset()
        generators.add(gen)
      if site not in h1eff_site:
        h1eff_site[site] = h1eff.Clone('h1eff_site_%s'%site)
        h1ceff_site[site] = h1ceff.Clone('h1ceff_site_%s'%site)
        h1wall_site[site] = h1wall.Clone('h1wall_site_%s'%site)
        h1eff_site[site].Reset()
        h1ceff_site[site].Reset()
        h1wall_site[site].Reset()
      try:
        h1eff.Fill(eff)
        h1ceff.Fill(ceff)
        h1wall.Fill(wall)
        h2eff.Fill(wall, eff)
        h2ceff.Fill(cwall, ceff)
        h2att.Fill(job.get('NumJobStarts'), ceff)
        h1att.Fill(job.get('NumJobStarts'))
        h1eff_gen[gen].Fill(eff)
        h1att_gen[gen].Fill(job.get('NumJobStarts'))
        h1eff_site[site].Fill(eff)
        h1ceff_gen[gen].Fill(ceff)
        h1ceff_site[site].Fill(ceff)
        h1wall_site[site].Fill(wall)
      except:
        pass

  # set y-limits on all histos so scale is good:
  set_histos_max([h1att,h1attq])
  set_histos_max(h1eff_gen.values())
  set_histos_max(h1ceff_gen.values())
  set_histos_max(h1eff_site.values())
  set_histos_max(h1ceff_site.values())
  set_histos_max(h1wall_site.values())

  # sort sites by entries, to only plot the first N:
  max_sites = []
  for site in h1eff_site.keys():
    if site not in max_sites:
      inserted = False
      for ii,ss in enumerate(max_sites):
        if h1eff_site[site].GetEntries() > h1eff_site[ss].GetEntries():
          inserted = True
          max_sites.insert(ii, site)
          break
      if not inserted:
        max_sites.append(site)

  # sort generators, ensuring all get the correct color:
  # (because all groups are not guaranteed to have the same set of generators)
  gens = sorted(list(generators))
  generators = collections.OrderedDict()
  for gen in gens:
    if gen not in generators:
      generators[gen] = []
    if gen in h1eff_gen:
      generators[gen].append(h1eff_gen[gen])
    if gen in h1ceff_gen:
      generators[gen].append(h1ceff_gen[gen])
    if gen in h1att_gen:
      generators[gen].append(h1att_gen[gen])
    if gen in h1attq_gen:
      generators[gen].append(h1attq_gen[gen])
  leg_gen = ROOT.TLegend(0.72,0.95-len(generators)*0.08,0.92,0.95)
  leg_site = ROOT.TLegend(0.11,0.12,0.92,0.95)
  ii=1
  for gen,histos in generators.items():
    for jj,h in enumerate(histos):
      h.SetLineColor(ii)
      if jj==0:
        leg_gen.AddEntry(h, gen, "l")
    ii += 1

  # cache them globally to keep in scope:
  root_store = [h1eff, h2eff, h1ceff, h2ceff, h2att, h1att, h1attq, h1wall, leg_gen, leg_site, can]
  root_store.extend(h1att_gen.values())
  root_store.extend(h1attq_gen.values())
  root_store.extend(h1eff_gen.values())
  root_store.extend(h1eff_site.values())
  root_store.extend(h1ceff_gen.values())
  root_store.extend(h1ceff_site.values())
  root_store.extend(h1wall_site.values())

  # there's only one we want stats on, this may be the easiest way:
  for x in root_store:
    try:
      x.SetStats(ROOT.kFALSE)
    except:
      pass
  h1att.SetStats(ROOT.kTRUE)

  can.cd(1) #####################################
  ROOT.gPad.SetLogy(logscale)
  h1attq.Draw()
  can.cd(2) #####################################
  ROOT.gPad.SetLogy(logscale)
  h1att.Draw()
  can.cd(3) #####################################
  ROOT.gPad.SetLogz(logscale)
  h2att.Draw('COLZ')
  can.cd(4) #####################################
  ROOT.gPad.SetLogz(logscale)
  h2ceff.Draw('COLZ')
  can.cd(5) #####################################
  ROOT.gPad.SetLogy(logscale)
  opt = ''
  for ii, gen in enumerate(sorted(h1attq_gen.keys())):
    h1attq_gen[gen].Draw(opt)
    opt = 'SAME'
  leg_gen.Draw()
  can.cd(6) #####################################
  ROOT.gPad.SetLogy(logscale)
  opt = ''
  for ii, gen in enumerate(sorted(h1att_gen.keys())):
    h1att_gen[gen].Draw(opt)
    opt = 'SAME'
  leg_gen.Draw()
  can.cd(7)
  leg_site.Draw()
  can.cd(8) #####################################
  ROOT.gPad.SetLogz(logscale)
  h2eff.Draw('COLZ')
  can.cd(9) #####################################
  ROOT.gPad.SetLogy(logscale)
  opt = ''
  for ii,gen in enumerate(sorted(h1eff_gen.keys())):
    h1eff_gen[gen].Draw(opt)
    opt = 'SAME'
  leg_gen.Draw()
  can.cd(10) #####################################
  ROOT.gPad.SetLogy(logscale)
  opt = ''
  for ii,gen in enumerate(sorted(h1ceff_gen.keys())):
    h1ceff_gen[gen].Draw(opt)
    opt = 'SAME'
  leg_gen.Draw()
  opt = ''
  for ii,site in enumerate(max_sites):
    if ii > 10:
      break
    leg_site.AddEntry(h1eff_site[site], '%s %d'%(site,h1eff_site[site].GetEntries()), "l")
    h1eff_site[site].SetLineColor(ii+1)
    h1wall_site[site].SetLineColor(ii+1)
    can.cd(11) #####################################
    ROOT.gPad.SetLogy(logscale)
    h1eff_site[site].Draw(opt)
    can.cd(12) #####################################
    ROOT.gPad.SetLogy(logscale)
    h1wall_site[site].Draw(opt)
    opt = 'SAME'

  can.Update()

  return can

def set_histos_max(histos):
  hmax = -999
  for h in histos:
    if h.GetMaximum() > hmax:
      hmax = h.GetMaximum()
  for h in histos:
    h.SetMaximum(hmax*1.1)

###########################################################
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

###########################################################
###########################################################

def check_cvmfs(job):
  '''Return wether a CVMFS error is detected'''
  for line in readlines_reverse(job.get('stdout'),20):
    for x in cvmfs_error_strings:
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

# cache generator names to only parse log once per cluster
generators = {}
def get_generator(job):
  if job.get('ClusterId') not in generators:
    generators['ClusterId'] = null_field
    if job.get('UserLog') is not None:
      job_script = os.path.dirname(os.path.dirname(job.get('UserLog')))+'/nodeScript.sh'
      for line in readlines(job_script):
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

def make_timeline_entry(args):
  data = {}
  summary = job_counts.copy()
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
    print(json.dumps(cache,**json_format))
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

###########################################################
###########################################################

class Column():
  def __init__(self, name, width, tally=None):
    self.name = name
    self.width = width
    self.tally = tally
    self.fmt = '%%-%d.%ds' % (self.width, self.width)

class Table():
  max_width = 131
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
    # left-truncate and prefix with a '*' if a column is too long
    x = []
    for i,v in enumerate([str(v).strip() for v in values]):
      if len(v) > self.columns[i].width:
        v = '*'+v[len(v)-self.columns[i].width+1:]
      x.append(v)
    return self.fmt % tuple(x)
#    return self.fmt % tuple([str(x).strip() for x in values])
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
    elif name == 'NumJobStarts':
      if value == 0:
        ret = null_field
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
summary_table.add_column('util','eff',4)
summary_table.add_column('ceff','ceff',4)
summary_table.add_column('att','att',4)

site_table = CondorTable()
site_table.add_column('site','MATCH_GLIDEIN_Site',26)
site_table.add_column('total','total',8,tally='sum')
site_table.add_column('done','done',8,tally='sum')
site_table.add_column('run','run',8,tally='sum')
site_table.add_column('idle','idle',8,tally='sum')
site_table.add_column('held','held',8,tally='sum')
site_table.add_column('wallhr','wallhr',6)
site_table.add_column('stddev','ewallhr',7)
site_table.add_column('util','eff',4,tally='avg')

job_table = CondorTable()
job_table.add_column('condor','condorid',13)
job_table.add_column('gemc','gemc',6)
job_table.add_column('site','MATCH_GLIDEIN_Site',15)
job_table.add_column('host','LastRemoteHost',16)
job_table.add_column('stat','JobStatus',4)
job_table.add_column('exit','ExitCode',4)
job_table.add_column('sig','ExitBySignal',4)
job_table.add_column('att','NumJobStarts',4,tally='avg')
job_table.add_column('wallhr','wallhr',6,tally='avg')
job_table.add_column('util','eff',4,tally='avg')
job_table.add_column('ceff','ceff',4)
job_table.add_column('start','JobCurrentStartDate',12)
job_table.add_column('end','CompletionDate',12)
job_table.add_column('user','user',10)
job_table.add_column('gen','generator',9)

###########################################################
###########################################################

if __name__ == '__main__':

  cli = argparse.ArgumentParser(description='Wrap condor_q and condor_history and add features for CLAS12.',
      epilog='''Repeatable "limit" options are first OR\'d independently, then AND'd together, and if their
      argument is prefixed with a dash ("-"), it is a veto (overriding the \'OR\').  For non-numeric arguments
      starting with a dash, use the "-opt=arg" format.  Per-site wall-hour tallies ignore running jobs, unless
      -running is specified.  Efficiencies are only calculated for completed jobs.''')
  cli.add_argument('-condor', default=[], metavar='#', action='append', type=int, help='limit by condor cluster id (repeatable)')
  cli.add_argument('-gemc', default=[], metavar='#', action='append', type=int, help='limit by gemc submission id (repeatable)')
  cli.add_argument('-user', default=[], action='append', type=str, help='limit by portal submitter\'s username (repeatable)')
  cli.add_argument('-site', default=[], action='append', type=str, help='limit by site name, pattern matched (repeatable)')
  cli.add_argument('-host', default=[], action='append', type=str, help='limit by host name, pattern matched (repeatable)')
  cli.add_argument('-exit', default=[], metavar='#', action='append', type=int, help='limit by exit code (repeatable)')
  cli.add_argument('-noexit', default=False, action='store_true', help='limit to jobs with no exit code')
  cli.add_argument('-generator', default=[], action='append', type=str, help='limit by generator name (repeatable)')
  cli.add_argument('-held', default=False, action='store_true', help='limit to jobs currently in held state')
  cli.add_argument('-idle', default=False, action='store_true', help='limit to jobs currently in idle state')
  cli.add_argument('-running', default=False, action='store_true', help='limit to jobs currently in running state')
  cli.add_argument('-completed', default=False, action='store_true', help='limit to completed jobs')
  cli.add_argument('-summary', default=False, action='store_true', help='tabulate by cluster id instead of per-job')
  cli.add_argument('-sitesummary', default=False, action='store_true', help='tabulate by site instead of per-job')
  cli.add_argument('-hours', default=0, metavar='#', type=float, help='look back # hours for completed jobs, reative to -end (default=0)')
  cli.add_argument('-end', default=None, metavar='YYYY/MM/DD[_HH:MM:SS]', type=str, help='end date for look back for completed jobs (default=now)')
  cli.add_argument('-tail', default=None, metavar='#', type=int, help='print last # lines of logs (negative=all, 0=filenames)')
  cli.add_argument('-cvmfs', default=False, action='store_true', help='print hostnames from logs with CVMFS errors')
  cli.add_argument('-xrootd', default=False, action='store_true', help='print hostnames from logs with XRootD errors')
  cli.add_argument('-vacate', default=-1, metavar='#', type=float, help='vacate jobs with wall hours greater than #')
  cli.add_argument('-hold', default=False, action='store_true', help='send matching jobs to hold state (be careful!!!)')
  cli.add_argument('-json', default=False, action='store_true', help='print full condor data in JSON format')
  cli.add_argument('-input', default=False, metavar='FILEPATH', type=str, help='read condor data from a JSON file instead of querying')
  cli.add_argument('-timeline', default=False, action='store_true', help='publish results for timeline generation')
  cli.add_argument('-parseexit', default=False, action='store_true', help='parse log files for exit codes')
  cli.add_argument('-printexit', default=False, action='store_true', help='just print the exit code definitions')
  cli.add_argument('-plot', default=False, metavar='FILEPATH', const=True, nargs='?', help='generate plots (requires ROOT)')

  args = cli.parse_args(sys.argv[1:])

  if args.printexit:
    for k,v in sorted(exit_codes.items()):
      print('%5d %s'%(k,v))
    sys.exit(0)

  if args.held + args.idle + args.running + args.completed > 1:
    cli.error('Only one of -held/idle/running/completed is allowed.')

  if (bool(args.vacate>=0) + bool(args.tail is not None) + bool(args.cvmfs) + bool(args.json)) > 1:
    cli.error('Only one of -cvmfs/vacate/tail/json is allowed.')

  if args.completed and args.hours <= 0 and not args.input:
    cli.error('-completed requires -hours is greater than zero or -input.')

  if socket.gethostname() != 'scosg16.jlab.org' and not args.input:
    cli.error('You must be on scosg16 unless using the -input option.')

  if len(args.exit) > 0 and not args.parseexit:
    print('Enabling -parseexit to accommodate -exit.  This may be slow ....')
    args.parseexit = True

  if args.plot and os.environ.get('DISPLAY') is None:
    cli.error('-plot requires graphics, but $DISPLAY is not set.')

  if args.end is None:
    args.end = datetime.datetime.now()
  else:
    try:
      args.end = datetime.datetime.strptime(args.end,'%Y/%m/%d_%H:%M:%S')
    except:
      try:
        args.end = datetime.datetime.strptime(args.end,'%Y/%m/%d')
      except:
        cli.error('Invalid date format for -end:  '+args.end)

  if args.plot is not False:
    import ROOT

  if args.input:
    condor_read(args)
  else:
    condor_query(args)

  if args.timeline:
    timeline(args)
    sys.exit(0)

  if args.json:
    print(json.dumps(condor_data, **json_format))
    sys.exit(0)

  if args.plot is not False:
    c = condor_plot(args)
    if c is not None and args.plot is not True:
      c.SaveAs(args.plot)
      c = condor_plot(args, 1)
      suffix = args.plot.split('.').pop()
      logscalename = ''.join(args.plot.split('.')[0:-1])+'-logscale.'+suffix
      c.SaveAs(logscalename)
    else:
      print('Done Plotting.  Press Return to close.')
      input()
    sys.exit(0)

  for cid,job in condor_yield(args):

    if args.hold:
      condor_hold_job(job)

    if args.vacate>0:
      if job.get('wallhr') is not None:
        if float(job.get('wallhr')) > args.vacate:
          if job_states.get(job['JobStatus']) == 'R':
            condor_vacate_job(job)

    elif args.cvmfs:
      if not check_cvmfs(job):
        if 'LastRemoteHost' in job:
          print(job.get('MATCH_GLIDEIN_Site')+' '+job['LastRemoteHost']+' '+cid)

    elif args.xrootd:
      if not check_xrootd(job):
        if 'LastRemoteHost' in job:
          print(job.get('MATCH_GLIDEIN_Site')+' '+job['LastRemoteHost']+' '+cid)

    elif args.tail is not None:
      tail_log(job, args.tail)

    else:
      job_table.add_job(job)

  if args.tail is None and not args.cvmfs:
    if len(job_table.rows) > 0:
      if args.summary or args.sitesummary:
        if args.summary:
          print(summary_table.add_jobs(condor_cluster_summary(args)))
        else:
          print(site_table.add_jobs(condor_site_summary(args)))
      else:
        print(job_table)
      if (args.held or args.idle) and args.parseexit:
        print(condor_exit_code_summary(args))
      print(condor_efficiency_summary())

  sys.exit(0)

