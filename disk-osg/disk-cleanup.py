#!/usr/bin/env python3

import re
import os
import sys
import time
import math
import argparse
import subprocess

protected_dirs = [ '/osgpool/hallb/clas12/gemc' , '/volatile/clas12/osg2' ]
default_ignores = [ '^.*\.hipo$', '^.*/job_[0-9]+/[0-9a-zA-Z\.\-_]+$' ]
default_trashes = [ '.*\.root$', '.*\.evio$', '^core\.*' ]

cli = argparse.ArgumentParser(description='''Utility for filesystem cleanup.
  Defaults values of the -ignores and -trashes options are for CLAS12 OSG cleanup.
  Note, this preserves directory modification times, unlike a `find -delete.`
  ''')
cli.add_argument('-path', required=True, type=str, help='path to search recursively for deletions')
cli.add_argument('-delete', default=-1, metavar='#', type=int, help='age threshold in days for file deletion (default=infinity)')
cli.add_argument('-empty', default=-1, metavar='#', type=int, help='age threshold in days for empty directory deletion (default=infinity)')
cli.add_argument('-trash', default=-1, metavar='#', type=int, help='age threshold in days for trash deletion (default=infinity)')
cli.add_argument('-gzip', default=False, action='store_true', help='gzip files instead of deleting')
cli.add_argument('-ignores', default=[], type=str, action='append', help='regular expressions of paths to ignore, repeatable (default=%s)'%default_ignores)
cli.add_argument('-trashes', default=[], type=str, action='append', help='regular expressions of file basenames to always delete, repeatable (default=%s)'%default_trashes)
cli.add_argument('-noignores', default=False, action='store_true', help='disable the -ignores option')
cli.add_argument('-notrashes', default=False, action='store_true', help='disable the -trashes option')
cli.add_argument('-dryrun', default=False, action='store_true', help='do not delete/gzip anything, just print')

args = cli.parse_args(sys.argv[1:])

now = time.time()

########################################################################
########################################################################

if args.delete < 0:
  if args.empty < 0:
    if args.trash < 0:
      cli.error('At least one of -delete/empty/trash must be set.')
      sys.exit(1)

if len(args.ignores)>0 and args.noignores:
  cli.error('You cannot set both -noignores and -ignores.')

if len(args.trashes)==0:
  args.trashes = default_trashes

if len(args.ignores)==0:
  args.ignores = default_ignores

if args.noignores:
  args.ignores = []

########################################################################
########################################################################

def is_old(path, days):
  '''Check whether modification time is older than than a number of days'''
  age_days = float(now - os.path.getmtime(path))/60/60/24
  return age_days > days

def is_trash(path):
  '''Test whether it qualifies as trash'''
  ret = False
  for x in args.trashes:
    if re.fullmatch(x, os.path.basename(path)) is not None:
      ret = True
      break
  return ret

def is_ignored(path):
  '''Test whether it qualifies for being ignored'''
  ret = False
  for x in args.ignores:
    if re.fullmatch(x, path) is not None:
      ret = True
      break
  return ret

########################################################################
########################################################################

def should_delete_file(path):
  '''Test whether the file should be deleted'''
  if args.trash>0 and is_old(path, args.trash) and is_trash(path):
    return True
  if args.delete>0 and is_old(path, args.delete) and not is_ignored(path):
    return True
  return False

def should_delete_dir(path):
  '''Test whether the directory should be deleted'''
  return args.empty>0 and is_old(path, args.empty) and len(os.listdir(path))==0

########################################################################
########################################################################

# Finally, crawl the filesystem and do stuff:
# some directories may become empty along the way,
# so we iterate until nothing gets deleted:

deletes = []
cycles = 0

while True:

  delete_happened = False

  for dirpath,dirnames,filenames in os.walk(args.path):

    dir_got_modified = False

    # don't delete anything at the top level within protected_dir:
    # (this is to avoid race conditions from the OSG submit portal)
    if dirpath in protected_dirs:
      continue

    # store the top directory's modification time:
    dirpath_mtime = os.path.getmtime(dirpath)

    # deal with files:
    for filename in filenames:
      fullfilepath = dirpath+'/'+filename
      if should_delete_file(fullfilepath):
        print(fullfilepath)
        deletes.append(fullfilepath)
        if not args.dryrun:
          if args.gzip:
            if not path.endswith('.gz'):
              subprocess.run(['gzip',path])
          else:
            os.remove(fullfilepath)
          delete_happened = True
          dir_got_modified = True

    # deal with directories:
    for dirname in dirnames:
      fulldirpath = dirpath+'/'+dirname
      if should_delete_dir(fulldirpath):
        print(fulldirpath)
        deletes.append(fulldirpath)
        if not args.dryrun:
          os.rmdir(fulldirpath)
          delete_happened = True
          dir_got_modified = True

    # restore the directory's modification time if we modified it:
    if dir_got_modified:
      os.utime(dirpath, (dirpath_mtime, dirpath_mtime))

  # if we didn't delete anything on this iteration, stop:
  # (for a dryrun, we can't iterate to find empties, so also stop)
  if not delete_happened or args.dryrun:
    break

  else:
    cycles += 1
    print('Cycle #%d to find empty directories ...'%(cycles+1))

if args.dryrun:
  print('Would have deleted %d things.'%len(deletes))
else:
  print('Deleted %d things.'%len(deletes))

