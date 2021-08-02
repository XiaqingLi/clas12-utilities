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
