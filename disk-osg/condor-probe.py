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

import config
import condor
import tools
import summary
from condor_plot import condor_plot
from lib.timeline import timeline
from lib.table import CondorTable

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
    for k,v in sorted(config.exit_codes.items()):
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
    condor.condor_read(args)
  else:
    condor.condor_query(args)

  if args.timeline:
    timeline(args)
    sys.exit(0)

  if args.json:
    print(json.dumps(condor.condor_data, **config.json_format))
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

  for cid,job in condor.condor_yield(args):

    if args.hold:
      condor.condor_hold_job(job)

    if args.vacate>0:
      if job.get('wallhr') is not None:
        if float(job.get('wallhr')) > args.vacate:
          if config.job_states.get(job['JobStatus']) == 'R':
            condor.condor_vacate_job(job)

    elif args.cvmfs:
      if not tools.check_cvmfs(job):
        if 'LastRemoteHost' in job:
          print(job.get('MATCH_GLIDEIN_Site')+' '+job['LastRemoteHost']+' '+cid)

    elif args.xrootd:
      if not tools.check_xrootd(job):
        if 'LastRemoteHost' in job:
          print(job.get('MATCH_GLIDEIN_Site')+' '+job['LastRemoteHost']+' '+cid)

    elif args.tail is not None:
      tools.tail_log(job, args.tail)

    else:
      job_table.add_job(job)

  if args.tail is None and not args.cvmfs:
    if len(job_table.rows) > 0:
      if args.summary or args.sitesummary:
        if args.summary:
          print(summary_table.add_jobs(summary.condor_cluster_summary(args)))
        else:
          print(site_table.add_jobs(summary.condor_site_summary(args)))
      else:
        print(job_table)
      if (args.held or args.idle) and args.parseexit:
        print(summary.condor_exit_code_summary(args))
      print(summary.condor_efficiency_summary())

  sys.exit(0)

