#!/usr/bin/env python3
"""
commands/report_relativepaths_not_existing_in_centralrepo.py
  Shows relative paths in a (partial) dirtree that do not exist in central repo
    (a comparative reference [supposedly more complete] dirtree)

Example parameters:
  --dirtree "/home/dados/Basefolder" --centralrepo "/media/user/Drive/Basefolder"
"""
import argparse
import datetime
import os.path
import fs.hashfunctions.hash_mod as hm
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import fs.strnlistfs.strfunctions_mod as strf
import default_settings as defaults
parser = argparse.ArgumentParser(description="Report missing relativepath between 'dirtree' and 'centralrepo'.")
parser.add_argument("--dirtree", type=str,
                    help="Directory tree that will be compared to 'centralrepo'")
parser.add_argument("--centralrepo", type=str,
                    help="Directory base to be compared with")
args = parser.parse_args()


class MissingRelativePathReporter:

  def __init__(self, partialdirtree_abspath, centralrepo_abspath):
    """
    """
    self.partialdirtree_abspath, self.centralrepo_abspath = partialdirtree_abspath, centralrepo_abspath
    self.n_processed_dirs = 0
    self.ongo_partialtree_folderabspath = None
    self.begin_time = datetime.datetime.now()
    self.end_time = None

  @property
  def run_duration(self):
    if self.end_time is None:
      return datetime.datetime.now() - self.begin_time
    return self.end_time - self.begin_time

  def does_folder_in_partialrepo_exist_in_centralrepo(self):
    if os.path.isdir(self.ongo_centralrepo_abspath):
      return True
    return False

  def report_ongoing_relativepartialfolder_existence_on_centralpath(self):
    bool_existence = self.does_folder_in_partialrepo_exist_in_centralrepo()
    if bool_existence:
      str_existence = 'EXISTS'
      return
    else:
      str_existence = 'DOES NOT EXIST'
    fopath = self.ongo_partialtree_folderabspath
    answer = "{str_existence} {fopath}"
    scrmsg = answer.format(str_existence=str_existence, fopath=fopath)
    print(scrmsg)

  @property
  def ongo_centralrepo_abspath(self):
    crpath = os.path.join(self.centralrepo_abspath, self.ongo_partialtree_relativepath)
    return crpath

  @property
  def ongo_partialtree_relativepath(self):
    """

    """
    relpath = self.ongo_partialtree_folderabspath[len(self.partialdirtree_abspath):]
    relpath = relpath.strip('/')
    return relpath

  def process(self):
    for self.ongo_partialtree_folderabspath, _, _ in os.walk(self.partialdirtree_abspath):
      self.report_ongoing_relativepartialfolder_existence_on_centralpath()


def get_args():
  partialdirtree_abspath = args.dirtree
  centralrepo_abspath = args.centralrepo
  return partialdirtree_abspath, centralrepo_abspath


def process():
  partialdirtree_abspath, centralrepo_abspath = get_args()
  reporter = MissingRelativePathReporter(partialdirtree_abspath, centralrepo_abspath)
  reporter.process()


if __name__ == '__main__':
  process()
