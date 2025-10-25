#!/usr/bin/env python3
"""
DirTreeMirror_PrdPrjSw:
  cmm/rep/reportYdidsRepeats.py
Description below.


This script reports repeated ytids reading data from db.
  Obs: It's does not "walk" the "live" treedir, rather it uses the db itself.
       Because of that, the sqlite-repo should be up to date.

Usage:
  reportYdidsRepeats.py <directory-path-where-the-sqlite-repo-resides>

Example:
  reportYdidsRepeats.py "/media/user/Sci HD 2T"
"""
import datetime
import os
import sys

import llib.dirfilefs.ytids_functions as ytfs
import models.entries.dirtree_mod as dt
import default_settings as defaults


class ReportRepeatedYtids:

  def __init__(self, src_mountpath):
    self.start_time = datetime.datetime.now()
    self.ori_dt = dt.DirTree('ori', src_mountpath)
    self.ytids = []
    self.ytid_n_dbid_dict = {}

  def extract_ytids_n_dbids(self):
    sql = 'SELECT id, name FROM %(tablename)s;'
    fetched_list = self.ori_dt.dbtree.do_select_with_sql_without_tuplevalues(sql)
    self.ytid_n_dbid_dict = {}
    for row in fetched_list:
      if len(row) < 2:
        continue
      dbid, filename = row[0], row[1]
      ytid = ytfs.extract_ytid_from_filename(filename)
      if ytid is None:
        continue
      if ytid in self.ytid_n_dbid_dict:
        self.ytid_n_dbid_dict[ytid].append(dbid)
      else:
        self.ytid_n_dbid_dict[ytid] = [dbid]

  def filter_only_those_repeated(self):
    tmp_ytid_n_dbid_dict = {}
    for ytid in self.ytid_n_dbid_dict:
      dbid_list = self.ytid_n_dbid_dict[ytid]
      if len(dbid_list) > 1:
        tmp_ytid_n_dbid_dict[ytid] = dbid_list
    self.ytid_n_dbid_dict = tmp_ytid_n_dbid_dict

  def process(self):
    self.extract_ytids_n_dbids()
    print('ytid_n_dbid_dict size =', len(self.ytid_n_dbid_dict))
    # print(self.ytid_n_dbid_dict)
    self.filter_only_those_repeated()
    print('after filtering, ytid_n_dbid_dict with repeats has', len(self.ytid_n_dbid_dict), 'ytids')
    self.report()

  def report(self):
    print(' === Reporting ===')
    for i, ytid in enumerate(self.ytid_n_dbid_dict):
      seq = i + 1
      dbid_list = self.ytid_n_dbid_dict[ytid]
      n_ids = len(dbid_list)
      print(seq, ytid, n_ids, dbid_list)
    end_time = datetime.datetime.now()
    run_duration = end_time - self.start_time
    if len(self.ytid_n_dbid_dict) == 0:
      print(' => no ytid repeats found')
    print('End time =', end_time)
    print('Run duration =', run_duration)


def show_help_from_cli_n_exit_if_asked():
  for arg in sys.argv:
    if arg.startswith('-h') or arg.startswith('--help'):
      print(__doc__)
      sys.exit(0)


def process():
  """
  """
  show_help_from_cli_n_exit_if_asked()
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  reporter = ReportRepeatedYtids(src_mountpath)
  reporter.process()


if __name__ == '__main__':
  process()
