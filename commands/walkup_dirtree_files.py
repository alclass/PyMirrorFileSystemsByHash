#!/usr/bin/env python3
"""
This script intends to "walk up" a dirtree and db-inserts file dirnodes.
After this db-inserting, one application arises which is back-up between two disks
(or in fact any two dirtrees - a dirtree is a directory with its descendants: subfolders and files).

A bit of explanation of the capabilities of mirroring one dirtree to another with file-hashes
=============================================================================================

The back-up (or mirroring) operation is possible by comparisons file to file.
Excepting where hash collisions are concerned (this subject is not treated here),
  it's postulated that equal sha1's means equal files if even they have different names and locations.
  (Its size attribute must be the same for the two files in case are equal.)

Let's visualize an instance of the above mentioned postulate:
  - suppose a file "abc" has its hash as "123" and is located in the source dirtree t1;
  - if the compared target dirtree t2 does not have it, this means that file "abc"
    is missing in the target dirtree t2.
  - Based on this sqlite data, the system should copy over file "123" from the source dirtree t1
    to the target dirtree t2 and "mirror", so to say, this file there.

Various operations for realizing mirroring (or back-up)
=======================================================

Operations like copy (in-between dirtrees), move/rename (within the same dirtree)
   and delete (for cleaning up excess or files in target non-existent in source)
   are the main back-up operations.

The data structure of dirnode:
-----------------------------
DirNode (at the time of writing) is an 8-tuple ie:
  0 _id | 1 name | 2 parentpath | 3 sha1 | 4 bytesize | 5 mdatetime
    OBS: dbtree has a property called fieldnames which must be used to avoid hardcoding the indices
    (though it's still necessary to hardcode the field names themselves).

Transient operations:
====================
At the time of this writing, one transient operation is to verify
  the presence of files recorded in db in the actual dirtree.
It's called transient, because this system is not integrated with the OS's file system.
  Because of that, the user must use this system in a non-multiuser environment
  and also abstain from changing the dirtrees during operations
  though the scripts here will always check for existence or non-existence
  of files in disk before any copying, moving/renaming or deleting operations.
"""
import datetime
import hashlib
import os.path
import sys
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
import fs.hashfunctions.hash_mod as hm
import commands.dbentry_updater_by_filemove_based_on_size_n_mdt_mod as dbentry_upd
import commands.dbentry_deleter_those_without_corresponding_osentry_mod as dbentry_del
import default_settings as defaults
BUF_SIZE = 65536
ori_mount_abspath = '/media/friend/CompSci 2T Orig'


def calc_sha1_from_file(filepath):
  sha1 = hashlib.sha1()
  with open(filepath, 'rb') as f:
    while True:
      try:
        data = f.read(BUF_SIZE)
        if not data:
          break
        sha1.update(data)
      except OSError:
        return None
    return sha1.digest()


def convert_to_size_w_unit(bytesize):
  if bytesize is None:
    return "0KMG"
  kilo = 1024
  if bytesize < kilo:
    return str(bytesize) + 'b'
  mega = 1024*1024
  if bytesize < mega:
    bytesize = round(bytesize / kilo, 1)
    return str(bytesize) + 'K'
  giga = 1024*1024*1024
  if bytesize < giga:
    bytesize = round(bytesize / mega, 1)
    return str(bytesize) + 'M'
  bytesize = round(bytesize / giga, 1)
  tera = 1024*1024*1024*1024
  if bytesize < tera:
    return str(bytesize) + 'G'
  bytesize = round(bytesize / tera, 3)
  return str(bytesize) + 'T'


class FileSweeper:

  def __init__(self, mountpath=None, treename='ori', restart_at_walkloopseq=None):
    # treename is generally 'ori' (source) or 'bak' (back-up)
    self.n_dirs = 0
    self.n_files = 0
    self.restart_at_walkloopseq = restart_at_walkloopseq
    self.treat_restart_at_walkloopseq()
    self.n_all_files_in_dirtree = 0
    self.n_files_empty_sha1 = 0
    self.n_dbentries_ins_upd = 0
    self.n_dbentries_failed_ins_upd = 0
    self.all_nodes_with_osread_problem = []
    self.mountpath = mountpath
    if self.mountpath is None:
      self.mountpath = ori_mount_abspath
    if not os.path.isdir(self.mountpath):
      error_msg = 'Missing file errror mount_abspath (%s) does not exist.'
      raise OSError(error_msg)
    self.dirtree = dt.DirTree(treename, self.mountpath)
    self.dbtree = self.dirtree.dbtree  # dbu.DBDirTree(mount_abspath)

  def treat_restart_at_walkloopseq(self):
    """
    attribute restart_at_walkloopseq is an input parameter that tells the position in the walk loop
      to restart in case the processing was previously interrupted.
    """
    if self.restart_at_walkloopseq is None:
      self.restart_at_walkloopseq = 0

  def exists_in_db_name_parent_n_size(self, name, parentpath, bytesize):
    sql = 'SELECT * from %(tablename)s WHERE name=? and parentpath=? and bytesize=?;'
    tuplevalues = (name, parentpath, bytesize)
    reslist = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(reslist) > 0:
      print(self.n_files, ' => file', name, 'EXISTS. Continuing.')
      return True
    return False

  def dbinsert(self, current_abspath, files, middlepath):
    for filename in files:
      self.n_files += 1
      if self.n_files < self.restart_at_walkloopseq:
        print('Jumping', self.n_files, 'until', self.restart_at_walkloopseq)
        continue
      filepath = os.path.join(current_abspath, filename)
      name = filename
      parentpath = middlepath
      if not parentpath.startswith('/'):
        parentpath = '/' + parentpath
      filestat = os.stat(filepath)
      bytesize = filestat.st_size
      mdatetime = filestat.st_mtime  # file_attr_tuple[9]
      pydt = datetime.datetime.fromtimestamp(mdatetime)
      print('bytesize =', bytesize, convert_to_size_w_unit(bytesize), ':: mdatetime =', mdatetime, pydt)
      if self.exists_in_db_name_parent_n_size(name, parentpath, bytesize):
        print(self.n_files, 'of', self.n_all_files_in_dirtree,
              'DirNode already exists in db. Continuing.')
        continue
      print(self.n_files, 'of', self.n_all_files_in_dirtree,
            ' => calculating sha1 for', name, parentpath)
      sha1 = calc_sha1_from_file(filepath)
      print('sha1 =', sha1)
      dirnode = dn.DirNode(name, parentpath, sha1, bytesize, mdatetime)
      if sha1 == hm.EMPTY_SHA1_AS_BIN:
        self.n_files_empty_sha1 += 1
        print('DirNodoe has the empty (zero) sha1. Continuing', dirnode.name, dirnode.parentpath)
      if sha1 is not None:
        insert_update_result = self.dirtree.dbinsert_dirnode(dirnode)
        if insert_update_result:
          self.n_dbentries_ins_upd += 1
          print('n_dbentries_ins_upd', self.n_dbentries_ins_upd, dirnode)
        else:
          self.n_dbentries_failed_ins_upd += 1
          print('n_dbentries_failed_ins_upd', self.n_dbentries_failed_ins_upd, dirnode)
      else:
        # sha1 is None here (file is probably unreadable from disk)
        self.report_unreadable_file(dirnode)

  def report_unreadable_file(self, dirnode):
    self.all_nodes_with_osread_problem.append(dirnode)

  def count_files_in_dirtree(self):
    print('Counting all files in dirtree ' + self.mountpath)
    n_all_files_in_dirtree = 0
    for ongoingfolder_abspath, dirs, files in os.walk(self.mountpath):
      n_all_files_in_dirtree += len(files)
    self.n_all_files_in_dirtree = n_all_files_in_dirtree
    print('-' * 50)
    print('Number of files in dirtree %d' % self.n_all_files_in_dirtree)
    print('-'*50)

  def walkup_dirtree_files(self):
    """

    """
    for ongoingfolder_abspath, dirs, files in os.walk(self.mountpath):
      middlepath = ongoingfolder_abspath[len(self.mountpath):]
      middlepath = middlepath.lstrip('./')  # parentpath is '/' + middlepath (in some cases they are the same)
      if ongoingfolder_abspath != self.mountpath:  # this means not to process the mount_abspath folder itself
        self.n_dirs += 1
        self.dbinsert(ongoingfolder_abspath, files, middlepath)

  def report_all_nodes_with_osread_problem(self):
    print('-='*20)
    print('report_all_nodes_with_osread_problem:')
    print('-='*20)
    for i, dirnode in enumerate(self.all_nodes_with_osread_problem):
      print(i+1, dirnode)
    n_unreadable = len(self.all_nodes_with_osread_problem)
    print('Total', n_unreadable, ':: report_all_nodes_with_osread_problem')

  def report(self):
    print('n_all_files_in_dirtree', self.n_all_files_in_dirtree)
    print('n_dirs', self.n_dirs)
    print('n_files', self.n_files)
    print('n_files_empty_sha1', self.n_files_empty_sha1)
    print('n_dbentries_ins_upd', self.n_dbentries_ins_upd)
    print('n_dbentries_failed_ins_upd', self.n_dbentries_failed_ins_upd)

  def process(self):
    self.count_files_in_dirtree()
    self.walkup_dirtree_files()
    dbupdater = dbentry_upd.DBEntryUpdater(self.mountpath)
    dbupdater.process()
    dbdeleter = dbentry_del.DBEntryWithoutCorrespondingOsEntryDeleter(self.mountpath)
    dbdeleter.process()
    self.report_all_nodes_with_osread_problem()
    self.report()


def get_arg_restart_at_position_or_zero():
  """
  This cli arg is used when the user wants to continue processing after interrupting it at some position
  """
  for arg in sys.argv:
    if arg.startswith('-r='):
      try:
        arg = int(arg[len('-r='):])
        return arg
      except ValueError:
        pass
  return 0  # the default


def process():
  start_time = datetime.datetime.now()
  print('Start Time', start_time)
  # ------------------
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  restart_at_position = get_arg_restart_at_position_or_zero()
  treename = 'ori'  # ori stands for origin instead of target
  sweeper = FileSweeper(src_mountpath, treename, restart_at_position)
  sweeper.process()
  finish_time = datetime.datetime.now()
  elapsed_time = finish_time - start_time
  # ------------------
  print('-'*50)
  print('Finish Time:', finish_time)
  print('Run Time:', elapsed_time)


if __name__ == '__main__':
  process()
