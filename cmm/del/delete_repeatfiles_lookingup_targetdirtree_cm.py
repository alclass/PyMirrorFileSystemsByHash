#!/usr/bin/env python3
"""
delete_repeatfiles_lookingup_targetdirtree_cm.py

This script does the following:
  1) it considers one only dirtree (not two as ori [origin] and bak [back-up], the whole dirtree is ori);
  2) takes two folder paths (source and target), target can be inside source but not the other way around;
     (if source were inside target, the logical consequence would be deletion of items
      in source itself, if any, not desirable);
  3) deletes file copies (ie file repeats (*)) inside the target dirtree, keeping the original in the source dirtree
     (or subset source minus target if target is inside source).

(*) repeats are based on the sha1-hash of its content

Obs:
  => this script DOES NOT DELETE repeats in the same folder as source's;
     example:
       if A.txt and B.txt are equals and exists in one source folder (or source minus target),
       both will be kept. There is another script in this system for that (*).

       (*) deleteRepeatsIntradirDirByDir.py does that, ie it deletes repeats inside same folders,
       the name choice for the one that will remain is based on "largest" filename.
         Eg: if A.txt and AA.txt are equals and in the same folder,
             AA.txt (largest filename) will remain and A.txt will be deleted.

Notice the main class here needs 3 parameters (ori_mountpath, src_branchdir_abspath, trg_branchdir_abspath)
  to do its job. This is an important difference from the script:
    => clean_delete_sha1s_in_trg_that_exist_in_src_mod.py
  which only needs 2 parameters, ie src_dirtree (or src_mountpath) and trg_dirtree (or trg_mountpath)

Also IMPORTANT:
  1) all file deletions are always somehow dangerous but this script only deletes "excess" copies
     (ie the original file is not to be deleted in this script);
  2) once a file is considered "source" (in the target dirtree),
     its db-id prevents it from being deleted itself, a kind of an aditional protection mechanism;
"""
import os
import sys
import lib.db.dbdirtree_mod as dbdt
import models.entries.dirnode_mod as dn
import default_settings as defaults
import lib.strnlistfs.strfunctions_mod as strf
import lib.dirfilefs.dir_n_file_fs_mod as dirf


class ForceDeleterLookingDirUp:

  def __init__(self, ori_mountpath, src_branchdir_abspath, trg_branchdir_abspath):
    self.ori_dbtree = dbdt.DBDirTree(ori_mountpath)
    self.src_branchdir_abspath = src_branchdir_abspath
    self.trg_branchdir_abspath = trg_branchdir_abspath
    self.verify_paths_source_inside_target_n_raise_if_error()
    self.src_sha1s = []
    self.trg_ids_to_delete_upon_confirm = []
    self.processed_sha1 = []
    self.total_src_files_in_db = 0
    self.total_src_files_in_os = 0
    self.total_src_dirs_in_os = 0
    self.total_sha1s_src_files_in_db = 0
    self.n_src_processed_files = 0
    self.n_trg_processed_files = 0
    self.n_src_exists = 0
    self.n_trg_exists = 0
    self.n_processed_deletes = 0
    self.n_trg_os_phys_files_deleted = 0
    self.bool_del_confirmed = False
    self.calc_totals()

  def calc_totals(self):
    t_src_osfiles, t_src_osdirs = dirf.count_total_files_n_folders_with_restriction(self.ori_dbtree.mountpath)
    self.total_src_files_in_os, self.total_src_dirs_in_os = t_src_osfiles, t_src_osdirs
    self.total_src_files_in_db = self.ori_dbtree.count_rows_as_int()
    self.total_sha1s_src_files_in_db = self.ori_dbtree.count_unique_sha1s_as_int()

  @property
  def total_src_sha1s(self):
    return len(self.src_sha1s)

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
    if not os.path.isdir(self.src_branchdir_abspath):
      error_msg = 'OSError: src_fulldirpath (%s) does not exist.' % self.src_branchdir_abspath
      error_msgs.append(error_msg)
    # 2) it checks existence of target folder
    if not os.path.isdir(self.trg_branchdir_abspath):
      error_msg = 'OSError: trg_fulldirpath (%s) does not exist.' % self.trg_branchdir_abspath
      error_msgs.append(error_msg)
    # 3) it prevents the source folder from being inside the target folder
    if self.src_branchdir_abspath.startswith(self.trg_branchdir_abspath):
      error_msg = 'Context Error: src_branchdir_abspath (%s) cannot be within (inside) trg_branchdir_abspath (%s).' \
                  % (self.src_branchdir_abspath, self.trg_branchdir_abspath)
      error_msgs.append(error_msg)
    if len(error_msgs) > 0:
      error_msg = '\n'.join(error_msgs)
      raise OSError(error_msg)

  @property
  def src_branchdir_parentpath(self):
    middlepath = self.src_branchdir_abspath[len(self.ori_dbtree.mountpath):]
    return strf.prepend_slash_if_needed(middlepath)

  @property
  def trg_branchdir_parentpath(self):
    middlepath = self.trg_branchdir_abspath[len(self.ori_dbtree.mountpath):]
    return strf.prepend_slash_if_needed(middlepath)

  def gather_sha1s_from_srcdir_minus_trgdir(self):
    src_charsize = 1 + len(self.src_branchdir_parentpath)
    trg_charsize = 1 + len(self.trg_branchdir_parentpath)
    sql = 'SELECT * FROM %(tablename)s'
    sql += ' WHERE substr(parentpath, 0, ' + str(src_charsize) + ')=?'
    sql += ' AND NOT substr(parentpath, 0, ' + str(trg_charsize) + ')=?;'
    tuplevalues = (self.src_branchdir_parentpath, self.trg_branchdir_parentpath)
    fetched_list = self.ori_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    sha1s_in_src_set = set()
    for row in fetched_list:
      self.n_src_processed_files += 1
      dirnode = dn.DirNode.create_with_tuplerow(row, self.ori_dbtree.fieldnames)
      if dirnode.does_dirnode_exist_in_disk(self.ori_dbtree.mountpath):
        self.n_src_exists += 1
        print(
          'src fil exi', self.n_src_exists, 'proc', self.n_src_processed_files,
          'tot', self.total_src_files_in_os,
          '[', dirnode.name, '] @', '[', strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50), ']'
          ' sha1 trg comp:', dirnode.sha1.hex()[:10] + '...'
        )
        sha1s_in_src_set.add(dirnode.sha1)
    self.src_sha1s = list(sha1s_in_src_set)

  def gather_trg_ids_to_delete_upon_confirm(self):
    trg_charsize = 1 + len(self.trg_branchdir_parentpath)
    sql = 'SELECT * FROM %(tablename)s WHERE substr(parentpath, 0, ' + str(trg_charsize) + ')=? and sha1=?;'
    for i, sha1 in enumerate(self.src_sha1s):
      print(i+1, 'processing', sha1.hex())
      tuplevalues = (self.trg_branchdir_parentpath, sha1)
      fetched_list = self.ori_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
      for row in fetched_list:
        self.n_trg_processed_files += 1
        dirnode = dn.DirNode.create_with_tuplerow(row, self.ori_dbtree.fieldnames)
        if dirnode.does_dirnode_exist_in_disk(self.ori_dbtree.mountpath):
          self.n_trg_exists += 1
          print(
            'trg fil exi', self.n_trg_exists, 'proc', self.n_trg_processed_files, 'tot', self.total_src_files_in_os,
            'trg id=%d for later deletion confirm:' % dirnode.get_db_id(),
            '[', dirnode.name, ']',
            '@', '[', strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50), ']'
          )
          self.trg_ids_to_delete_upon_confirm.append(dirnode.get_db_id())

  def confirm_deletion(self):
    print('Confirm deletion: ids:')
    if len(self.trg_ids_to_delete_upon_confirm) == 0:
      print(' >>>>>>>>>>>> No ids to delete.')
      return False
    total_trg_files_to_delete = len(self.trg_ids_to_delete_upon_confirm)
    for i, _id in enumerate(self.trg_ids_to_delete_upon_confirm):
      self.n_processed_deletes += 1
      dirnode = self.ori_dbtree.fetch_dirnode_by_id(_id)
      print(
        self.n_processed_deletes, 'of', total_trg_files_to_delete, 'to delete and',
        self.total_src_files_in_os, 'in dirtree :',
        'File to delete in target: id=%d' % dirnode.get_db_id(), '::', dirnode.sha1.hex()[:10] + '...'
      )
      print(
        '[', dirnode.name, '] @ [',
        strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50), ']'
      )
    screen_msg = 'Do you want to delete the %d target files above? (*Y/n) [ENTER means yes] ' \
                 % total_trg_files_to_delete
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def do_confirmed_deletes(self):
    # double-check
    if not self.bool_del_confirmed:
      return False
    self.n_processed_deletes = 0
    for _id in self.trg_ids_to_delete_upon_confirm:
      print(self.n_processed_deletes + 1, 'Deleting id', _id, 'in db and in os.')
      dirnode = self.ori_dbtree.fetch_dirnode_by_id(_id)
      file_abspath = dirnode.get_abspath_with_mountpath(self.ori_dbtree.mountpath)
      if os.path.isfile(file_abspath):
        print(self.n_trg_os_phys_files_deleted + 1, 'Deleting', file_abspath)
        os.remove(file_abspath)
        self.n_trg_os_phys_files_deleted += 1
      print('Deleting in db', _id, dirnode.name, '@', strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50))
      _ = self.ori_dbtree.delete_row_by_id(_id)
      self.n_processed_deletes += 1
    print('-'*50)
    print('Deleted altogether', self.n_processed_deletes, 'ids')
    print('-'*50)

  def process(self):
    self.gather_sha1s_from_srcdir_minus_trgdir()
    self.gather_trg_ids_to_delete_upon_confirm()
    self.bool_del_confirmed = self.confirm_deletion()
    if self.bool_del_confirmed:
      self.do_confirmed_deletes()
    self.report()

  def report(self):
    print('Report:')
    print('=======')
    print('dirtree:', self.ori_dbtree.mountpath)
    print('src_branchdir_parentpath:', self.src_branchdir_parentpath)
    print('trg_branchdir_parentpath:', self.trg_branchdir_parentpath)
    print('total_src_sha1s', self.total_src_sha1s)
    print('total_src_files_in_db', self.total_src_files_in_db)
    print('total_src_files_in_os', self.total_src_files_in_os)
    print('total_src_dirs_in_os', self.total_src_dirs_in_os)
    print('total_sha1s_src_files_in_db', self.total_sha1s_src_files_in_db)
    print('n_src_processed_files', self.n_src_processed_files)
    print('n_trg_processed_files', self.n_trg_processed_files)
    print('n_src_exists', self.n_src_exists)
    print('n_trg_exists', self.n_trg_exists)
    print('n_processed_deletes', self.n_processed_deletes)
    print('n_trg_os_phys_files_deleted', self.n_trg_os_phys_files_deleted)


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
  src_branchdir_abspath = None
  trg_branchdir_abspath = None
  for arg in sys.argv:
    if arg.startswith('-sp='):
      src_branchdir_abspath = arg[len('-sp='):]
    elif arg.startswith('-tp='):
      trg_branchdir_abspath = arg[len('-tp='):]
  if not os.path.isdir(src_branchdir_abspath):
    error_msg = 'Error: src dirpath ' + src_branchdir_abspath + ' does not exist.'
    raise OSError(error_msg)
  if not os.path.isdir(trg_branchdir_abspath):
    error_msg = 'Error: trg dirpath ' + trg_branchdir_abspath + ' does not exist.'
    raise OSError(error_msg)
  return src_branchdir_abspath, trg_branchdir_abspath


def show_help_cli_msg_if_asked():
  for arg in sys.argv:
    if arg in ['-h', '--help']:
      print(__doc__)
      sys.exit(0)


def process():
  """
  """
  show_help_cli_msg_if_asked()
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  src_branchdir_abspath, trg_branchdir_abspath = get_src_n_trg_full_inner_paths()
  forcedeleter = ForceDeleterLookingDirUp(src_mountpath, src_branchdir_abspath, trg_branchdir_abspath)
  forcedeleter.process()


if __name__ == '__main__':
  process()
