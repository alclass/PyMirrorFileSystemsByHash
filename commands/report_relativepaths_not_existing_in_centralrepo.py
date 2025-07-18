#!/usr/bin/env python3
"""
commands/report_relativepaths_not_existing_in_centralrepo.py
  Prints (to stdout) relative paths in a dirtree *
    that do not exist in another dirtree (here called the "centralrepo" *)


  * a "dirtree" is the directory or subdirectory tree starting from a certain (root) folderpath

  ** The two terms source and destination (or target) are avoided here.
  The 'source' here is simply called a dirtree.
  The 'destination' is called the centralrepo (or simply "repo" for repository).

  *+* This script does not copy, move, rename or delete files or folders.

  This script initially was intended as a helper to back up files
    existing in a "partial dirtree" to its "complete dirtree". This can be done
    in 2 steps, they are:
      a) first run the program to see differences and equalize (manually) all paths
      b) run the 'legacy script' * (in this system) that will copy over
        files from dirtree to centralrepo based on their sha1hex's
    * the main 'legacy script' mold_mold_trg_by_src_sha1s_mod.py

    The equalization of paths is not automatic -- it's to be done manually --,
      but its automation could be thought about in a future TODO.

  + note that the main back-up script (*) in this system does the path equalization automatically,
    but this equalization equalizes source to destination,
      not destination to source as it's intended here.

    (*) mold_mold_trg_by_src_sha1s_mod.py

    This is an implicitly -- or naturally -- logical action when backing up
      source to destination, but this same action is to be avoided
      in the case here where a 'partial' dirtree acts as the source dirtree
        and may not have its relative paths equal to their counterpars
        in destination (which should not have its paths modified because of an inverse mirroring).

Usage:
======

  $this_script --dirtree <abspath_to_dirtree> --centralrepo <abspath_to_centralrepo> [--onlynotexists]

  Where:
    --dirtree: the absolute path to the rootfolder of "dirtree"
    --dirtree: the absolute path to the rootfolder of "centralrepo"
    --onlynotexists: optional flag that makes this script only output the missing relative paths

Example:
======

  $this_script --dirtree "/home/dados/Basefolder" --centralrepo "/media/user/Drive/Basefolder"
    which means:
      reports all relative paths from "/home/dados/Basefolder"
        each one showing whether it exists or not in "/media/user/Drive/Basefolder"

"""
import argparse
import datetime
import os.path
parser = argparse.ArgumentParser(description="Report missing relativepath between 'dirtree' and 'centralrepo'.")
parser.add_argument("--dirtree", type=str,
                    help="Directory tree that will be compared to 'centralrepo'")
parser.add_argument("--centralrepo", type=str,
                    help="Directory base to be compared with")
parser.add_argument("--onlynotexists", action="store_true",
                    help="Directory base to be compared with")
args = parser.parse_args()


class MissingRelativePathReporter:

  def __init__(self, yet_to_equalize_abspath, dst_dtrepo_rootpath, onlynotexists=False):
    """
    """
    self.yet_to_equalize_dtrootpath, self.dst_dtrepo_rootpath = yet_to_equalize_abspath, dst_dtrepo_rootpath
    self.n_processed_dirs = 0
    self.n_fopath_exists = 0
    # self.n_fopath_notexists is a 'dynamic' @property
    self.ongoing_yet_to_equalize_folderpath = None
    self.onlynotexists = bool(onlynotexists)
    self.start_time = datetime.datetime.now()
    self.end_time = None

  @property
  def run_duration(self):
    if self.end_time is None:
      return datetime.datetime.now() - self.start_time
    return self.end_time - self.start_time

  @property
  def n_fopath_notexists(self):
    """
    This is not 'final total', it's the ongoing (looping) variable
    """
    return self.n_processed_dirs - self.n_fopath_exists

  def does_relativefolder_exist_in_repo(self):
    if os.path.isdir(self.ongoing_repo_abspath):
      self.n_fopath_exists += 1
      return True
    return False

  def report_relativepath_existence_in_repo(self):
    bool_existence = self.does_relativefolder_exist_in_repo()
    if bool_existence:
      if self.onlynotexists:
        return
      str_existence = f'{self.n_fopath_exists}/{self.n_processed_dirs} EXISTS'
    else:
      str_existence = f'{self.n_fopath_notexists}/{self.n_processed_dirs} DOES NOT EXIST'
    fopath = self.ongoing_yet_to_equalize_folderpath
    answer = "{str_existence} {fopath}"
    scrmsg = answer.format(str_existence=str_existence, fopath=fopath)
    print(scrmsg)

  @property
  def ongoing_repo_abspath(self):
    crpath = os.path.join(self.dst_dtrepo_rootpath, self.ongo_partialtree_relativepath)
    return crpath

  @property
  def ongo_partialtree_relativepath(self):
    """

    """
    relpath = self.ongoing_yet_to_equalize_folderpath[len(self.yet_to_equalize_dtrootpath):]
    relpath = relpath.strip('/')  # relative paths cannot start with '/'
    return relpath

  def process(self):
    for self.ongoing_yet_to_equalize_folderpath, _, _ in os.walk(self.yet_to_equalize_dtrootpath):
      self.n_processed_dirs += 1
      self.report_relativepath_existence_in_repo()
    self.end_time = datetime.datetime.now()
    print(self)

  def __str__(self):
    fo_not_existing = self.n_processed_dirs - self.n_fopath_exists
    outstr = f"""{self.__class__.__name__}
    nº of processed dirs = {self.n_processed_dirs}
    paths existing = {self.n_fopath_exists}
    paths not-existing = {fo_not_existing}
    start time = {self.start_time}
    end time = {self.end_time}
    elapsed run duration = {self.run_duration}
    """
    return outstr


def get_args():
  partialdirtree_abspath = args.dirtree
  centralrepo_abspath = args.centralrepo
  onlynotexists = args.onlynotexists
  return partialdirtree_abspath, centralrepo_abspath, onlynotexists


def process():
  partialdirtree_abspath, centralrepo_abspath, onlynotexists = get_args()
  reporter = MissingRelativePathReporter(
    partialdirtree_abspath,
    centralrepo_abspath,
    onlynotexists
  )
  reporter.process()


if __name__ == '__main__':
  process()
