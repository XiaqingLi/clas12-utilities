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
    if config.job_states[job['JobStatus']] != 'C':
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
