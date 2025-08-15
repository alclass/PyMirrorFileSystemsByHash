#!/usr/bin/env python3
"""
commands/reportDifferentYdidsFromToDirTree.py
  Reports, between two DirTrees, how many ytds are:
    a) equal in the two dirtrees
    b) extra/missing ytds in 'source' dirtree
    c) extra/missing ytds in 'destination' dirtree

Usage:
  $<this_script>
    <source-directory-path-where-its-sqlite-repo-resides>
    <destination-directory-path-where-the-sqlite-repo-resides>

Example:
  reportDifferentYdidsFromToDirTree.py
    "/media/user/Sci HD 2T/quantum physics"
    "/media/user/Sci HD 2T backup/quantum physics"

Notice that the ending folders in the command above
  must have each its sqlite-repo-file
    (at the time of writing, it has a conventioned name ".updirfileentries.sqlite")
  these ending folders are also the root folders of each dirtree
"""
import argparse
import datetime
import os
import sqlite3
import sys
# import fs.dirfilefs.ytids_functions as ytfs
# import models.entries.dirtree_mod as dt
# import default_settings as defaults
import lib.dirfilefs.ytids_functions as ytfs  # .extract_ytid_from_filename
default_sqlitefilename = ".updirfileentries.sqlite"
default_dbtablename = "ytids"
default_dbcolumn_name = "ytid"
default_filepaths_tablename = 'files_in_tree'
parser = argparse.ArgumentParser(description="Compare and report ytid differences in-between dirtrees.")
parser.add_argument("--docstr", action="store_true",
                    help="show docstr help and exit")
parser.add_argument("--src", type=str,
                    help="source dirtree absolute path")
parser.add_argument("--dst", type=str,
                    help="destination dirtree absolute path")
args = parser.parse_args()


def get_elems_in_1_not_in_2(list1, list2):
  return list(set(filter(lambda e: e not in list2, list1)))


def get_elems_in_both_1_n_2(list1, list2):
  return list(set(filter(lambda e: e in list2, list1)))


def fetch_ytids_in_sqlitefile_wo_params(sql, sqlitefilepath):
  """
  Fetchs all 1-column rows in sqlitefile's ytid-table
  """
  conn = sqlite3.connect(sqlitefilepath)
  cursor = conn.cursor()
  retcursor = cursor.execute(sql)
  ytids = []
  if retcursor:
    for row in retcursor.fetchall():
      ytid = row[0]
      ytids.append(ytid)
  conn.close()
  return ytids


def mount_n_get_dict_ytid_n_filepath(sqlitefilepath):
  """
  In a Python script we were developing, it creates a very large dict that we're afraid
  may run into trouble with RAM-memory.
  So we ask: what is a good way to make a data structure that, if larger than a config amount,
  writes itself on disk, maybe a sqlite-file or anything that could be managed as a dict?
  """
  conn = sqlite3.connect(sqlitefilepath)
  conn.row_factory = sqlite3.Row
  cursor = conn.cursor()
  sql = f"SELECT name, parentpath FROM {default_filepaths_tablename};"
  retcursor = cursor.execute(sql)
  odict = {}
  if retcursor:
    for row in retcursor.fetchall():
      name = row['name']
      ytid = ytfs.extract_ytid_from_filename(name)
      if ytid is None:
        continue
      parentpath = row['parentpath']
      filepath = os.path.join(parentpath, name)
      odict[ytid] = filepath
  conn.close()
  return odict


def get_tuplelist_ytids_filepaths_fr_ytids(ytids, sqlitefilepath):
  indict = mount_n_get_dict_ytid_n_filepath(sqlitefilepath)
  # outdict = dict(filter(lambda it: it[0] in ytids, indict.items()))
  tuplelist = tuple(filter(lambda it: it[0] in ytids, indict.items()))
  return tuplelist


class YtidsComparatorReporter:

  def __init__(self, src_mountpath, dst_mountpath):
    self.start_time = datetime.datetime.now()
    self.end_time = None  # set when @property process_time is invoked
    self.src_mountpath = src_mountpath
    self.dst_mountpath = dst_mountpath
    self.treat_attrs()
    self.src_ytids = []
    self.dst_ytids = []
    self._ytids_in_both = None
    self._ytids_existing_in_src_not_in_dst = None
    self._ytids_existing_in_dst_not_in_src = None

  def treat_attrs(self):
    if self.src_mountpath and not os.path.isdir(self.src_mountpath):
      errmsg = f"Error: src dir {self.src_mountpath} does not exist. Please retry with a valid one."
      raise OSError(errmsg)
    if self.dst_mountpath and not os.path.isdir(self.dst_mountpath):
      errmsg = f"Error: dst dir {self.dst_mountpath} does not exist. Please retry with a valid one."
      raise OSError(errmsg)

  @property
  def process_time(self):
    """
    Notice that process_time sets, upon first access, end_time
      (so its invocation must happen at the end of processing)
    """
    if self.end_time is None:
      self.end_time = datetime.datetime.now()
    return self.end_time - self.start_time

  @property
  def ytids_existing_in_src_not_in_dst(self):
    if self._ytids_existing_in_src_not_in_dst is None:
      self._ytids_existing_in_src_not_in_dst = get_elems_in_1_not_in_2(self.src_ytids, self.dst_ytids)
    return self._ytids_existing_in_src_not_in_dst

  @property
  def src_sqlitefilepath(self):
    return os.path.join(self.src_mountpath, default_sqlitefilename)

  @property
  def dst_sqlitefilepath(self):
    return os.path.join(self.dst_mountpath, default_sqlitefilename)

  @property
  def ytids_existing_in_dst_not_in_src(self):
    if self._ytids_existing_in_dst_not_in_src is None:
      self._ytids_existing_in_dst_not_in_src = get_elems_in_1_not_in_2(self.dst_ytids, self.src_ytids)
    return self._ytids_existing_in_dst_not_in_src

  def fetch_ytids(self, destination=False):
    sql = f'SELECT {default_dbcolumn_name} FROM {default_dbtablename};'
    scrmsg = f""" =============================
    Transferring ytids with: {sql}
            *** Please, wait. *** 
    """
    print(scrmsg)
    if not destination:
      self.src_ytids = fetch_ytids_in_sqlitefile_wo_params(sql, self.src_sqlitefilepath)
      fetched = self.src_ytids
    else:
      self.dst_ytids = fetch_ytids_in_sqlitefile_wo_params(sql, self.dst_sqlitefilepath)
      fetched = self.dst_ytids
    dirtree_name = 'source dirtree' if not destination else 'destination dirtree'
    print('\tdone: fetched', len(fetched), 'records from', dirtree_name)

  @property
  def ytids_in_both(self):
    """
    The union set of src_ytids with dst_ytids
    """
    if self._ytids_in_both is None:
      self._ytids_in_both = get_elems_in_both_1_n_2(self.src_ytids, self.dst_ytids)
    return self._ytids_in_both

  def list_filepaths_src_ytds_not_in_dst(self):
    scrmsg = 'LIST filepaths_src_ytds_not_in_dst'
    print(scrmsg)
    if len(self.ytids_existing_in_src_not_in_dst) == 0:
      scrmsg = '\tno filepaths_src_ytds_not_in_dst'
      print(scrmsg)
      return
    ytids = self.ytids_existing_in_src_not_in_dst
    tuplelist = get_tuplelist_ytids_filepaths_fr_ytids(ytids, self.src_sqlitefilepath)
    for i, tupl in enumerate(tuplelist):
      seq = i + 1
      ytid, filepath = tupl
      line = f"{seq} | {ytid} | [{filepath}]"
      print(line)

  def list_filepaths_dst_ytds_not_in_src(self):
    scrmsg = 'LIST filepaths_dst_ytds_not_in_src'
    print(scrmsg)
    if len(self.ytids_existing_in_dst_not_in_src) == 0:
      scrmsg = '\tno filepaths_dst_ytds_not_in_src'
      print(scrmsg)
      return
    ytids = self.ytids_existing_in_dst_not_in_src
    tuplelist = get_tuplelist_ytids_filepaths_fr_ytids(ytids, self.src_sqlitefilepath)
    for i, tupl in enumerate(tuplelist):
      seq = i + 1
      ytid, filepath = tupl
      line = f"{seq} | {ytid} | [{filepath}]"
      print(line)

  def process(self):
    self.fetch_ytids()  # fetch source dirtree first
    self.fetch_ytids(destination=True)  # then fetch destination dirtree
    self.report()

  def report(self):
    print(self)

  def __str__(self):
    _ = self.process_time  # this sets end_time
    outstr = f"""{self.__class__.__name__}
    source dirtree = {self.src_mountpath} 
    destination dirtree = {self.dst_mountpath} 
    start time = {self.start_time}
    end time = {self.end_time}
    process time = {self.process_time}
    number of src ytids = {len(self.src_ytids)}
    number of dst ytids = {len(self.dst_ytids)}
    number of src ytids not in det = {len(self.ytids_existing_in_src_not_in_dst)}
    -------------------------------
      src ytids not in dst => {self.ytids_existing_in_src_not_in_dst}
    -------------------------------
    number of dst ytids not in src = {len(self.ytids_existing_in_dst_not_in_src)}
    -------------------------------
      dst ytids not in src => {self.ytids_existing_in_dst_not_in_src}
    -------------------------------
    number of ytids in both src & dst = {len(self.ytids_in_both)}
    -------------------------------
      union set (ytids in both) => {self.ytids_in_both}
    """
    return outstr


def get_args():
  src_abspath, dst_abspath = None, None
  try:
    if args.docstr:
      print(__doc__)
      sys.exit(0)
    src_abspath = args.src
    dst_abspath = args.dst
  except AttributeError:
    pass
  return src_abspath, dst_abspath


def process():
  """
  """
  src_abspath, dst_abspath = get_args()
  reporter = YtidsComparatorReporter(src_abspath, dst_abspath)
  reporter.process()
  reporter.list_filepaths_src_ytds_not_in_dst()
  reporter.list_filepaths_dst_ytds_not_in_src()


if __name__ == '__main__':
  process()
