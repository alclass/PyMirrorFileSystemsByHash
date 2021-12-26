#!/usr/bin/env python3
"""
force_delete_repeatfiles_lookingup_targetdirtree_mod.py

This script takes two folder paths (source and target) and deletes file copies (ie file repeats (*))
  in the target dirtree.

(*) repeats are based on the sha1-hash of its content

Notice the main class here needs 4 parameters (ori_mountpath, bak_mountpath, src_fulldirpath, trg_fulldirpath)
  to do its job. This is an important difference from the script:
    => force_delete_every_equal_sha1_in_targetdirtree_mod
  which only needs 2 parameters, ie src_dirtree (or src_mountpath) and trg_dirtree (or trg_mountpath)

IMPORTANT:
  1) all file deletions are always somehow dangerous but this script only deletes "excess" copies
     (ie the original file is not to be deleted in this script);
  2) once a file is considered "source" (in the target dirtree), its db-id prevents it from being deleted itself;
"""
import os
import sys
import fs.db.dbdirtree_mod as dbdt
import models.entries.dirnode_mod as dn
import default_settings as defaults
import fs.strfs.strfunctions_mod as strf
# import shutil


class ForceDeleterLookingDirUp:

  def __init__(self, ori_mountpath, bak_mountpath, src_fulldirpath, trg_fulldirpath):
    self.ori_dbtree = dbdt.DBDirTree(ori_mountpath)
    self.bak_dbtree = dbdt.DBDirTree(bak_mountpath)
    self.src_fulldirpath = src_fulldirpath
    self.trg_fulldirpath = trg_fulldirpath
    self.verify_paths_source_inside_target_n_raise_if_error()
    self.ids_n_sha1s = []
    self.delete_ids = []
    self.n_processed_files = 0
    self.n_deletes = 0

  def verify_paths_source_inside_target_n_raise_if_error(self):
    """
    The method does THREE checkings, ie:
    1) it checks existence of source folder
    2) it checks existence of target folder
    3) it prevents the source folder from being inside the target folder
       (though the contrary [the target inside the source] is not prevented).

    Obs:
      when equal files with different names exist in the same folder,
      the repeats must be moved out to another directory
        (because of the ambiguity of names)
    """
    error_msgs = []
    # 1) it checks existence of source folder
    if not os.path.isdir(self.src_fulldirpath):
      error_msg = 'OSError: src_fulldirpath (%s) does not exist.' % self.src_fulldirpath
      error_msgs.append(error_msg)
    # 2) it checks existence of target folder
    if not os.path.isdir(self.trg_fulldirpath):
      error_msg = 'OSError: trg_fulldirpath (%s) does not exist.' % self.trg_fulldirpath
      error_msgs.append(error_msg)
    # 3) it prevents the source folder from being inside the target folder
    if self.src_fulldirpath.startswith(self.trg_fulldirpath):
      error_msg = 'Context Error: src_fulldirpath (%s) is within trg (%s).' \
                  % (self.src_fulldirpath, self.trg_fulldirpath)
      error_msgs.append(error_msg)
    if len(error_msgs) > 0:
      error_msg = '\n'.join(error_msgs)
      raise OSError(error_msg)

  @property
  def src_dirpath(self):
    middlepath = self.src_fulldirpath[len(self.ori_dbtree.mountpath):]
    return strf.prepend_slash_if_needed(middlepath)

  @property
  def trg_dirpath(self):
    middlepath = self.trg_fulldirpath[len(self.bak_dbtree.mountpath):]
    return strf.prepend_slash_if_needed(middlepath)

  def gather_sha1s_in_srcdir(self):
    charsize = len(self.src_dirpath)
    sql = 'select * from %(tablename)s where substr(parentpath, ' + str(charsize) + ')=?;'
    tuplevalues = (self.src_dirpath, )
    fetched_list = self.ori_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    self.n_processed_files += 1
    for row in fetched_list:
      dirnode = dn.DirNode.create_with_tuplerow(row, self.ori_dbtree.fieldnames)
      if not dirnode.does_dirnode_exist_in_disk(self.ori_dbtree.mountpath):
        continue
      id_n_sha1 = (dirnode.get_db_id(), dirnode.sha1)
      self.ids_n_sha1s.append(id_n_sha1)

  def look_up_trg_dirup(self):
    for id_n_sha1 in self.ids_n_sha1s:
      charsize = len(self.trg_dirpath)
      sql = 'select * from %(tablename)s where substr(parentpath, ' + str(charsize) + ')=? and sha1=?;'
      _id, sha1 = id_n_sha1
      tuplevalues = (self.trg_dirpath, sha1)
      fetched_list = self.bak_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
      for row in fetched_list:
        self.n_deletes += 1
        dirnode = dn.DirNode.create_with_tuplerow(row, self.bak_dbtree.fieldnames)
        if not dirnode.does_dirnode_exist_in_disk(self.ori_dbtree.mountpath):
          continue
        if _id == dirnode.get_db_id():
          # deletion cannot happen on the supposed source
          continue
        self.n_deletes += 1
        _id = row[0]  # id is always index 0
        self.delete_ids.append(_id)
        print(self.n_deletes, 'trg dirnode', dirnode)

  def confirm_deletion(self):
    print('Confirm deletion: ids:')
    n_to_delete = 0
    for i, _id in enumerate(self.delete_ids):
      n_to_delete += 1
      print(n_to_delete, _id)

  def report(self):
    print('Report:')
    print('=======')
    print('dirtrees:', self.ori_dbtree.mountpath, self.bak_dbtree.mountpath)
    print('n_deletes', self.n_deletes)

  def process(self):
    self.gather_sha1s_in_srcdir()
    self.look_up_trg_dirup()
    self.confirm_deletion()


def get_src_n_trg_full_inner_paths():
  """
  This main class in this script needs 4 arguments (parameters) from the command line, ie:
    => src_mountpath, trg_mountpath, src_fpath, trg_fpath
  This function looks for the last 2 above, ie:
    => src_fpath & trg_fpath

  An example with the 4 paths above:
  src_mountpath = /disk1/mountpath1
  trg_mountpath = /disk1/mountpath1 (the example here is a search in the same dirtree)
  src_fpath = /sciences/physics/quantum/lectures (suppose a search for lecture1.mp4)
  trg_fpath = /sciences/quantum/lectures (suppose a search for lectureA.mp4)
  In the example above if lectureA.mp4 is a "repeat" of lecture1.mp4, the former will be deleted.
  """
  src_fullpath = None
  trg_fullpath = None
  for arg in sys.argv:
    if arg.startswith('-sp='):
      src_fullpath = arg[len('-sp=')]
    elif arg.startswith('-tp='):
      trg_fullpath = arg[len('-tp=')]
  if not os.path.isdir(src_fullpath):
    error_msg = 'Error: src dirpath ' + src_fullpath + ' does not exist.'
    raise OSError(error_msg)
  if not os.path.isdir(trg_fullpath):
    error_msg = 'Error: trg dirpath ' + trg_fullpath + ' does not exist.'
    raise OSError(error_msg)
  return src_fullpath, trg_fullpath


def process():
  """
  """
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  src_fpath, trg_fpath = get_src_n_trg_full_inner_paths()
  forcedeleter = ForceDeleterLookingDirUp(src_mountpath, trg_mountpath, src_fpath, trg_fpath)
  forcedeleter.process()


if __name__ == '__main__':
  process()
