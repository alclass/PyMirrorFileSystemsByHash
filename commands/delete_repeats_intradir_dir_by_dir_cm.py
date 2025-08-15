#!/usr/bin/env python3
"""
delete_repeats_intradir_dir_by_dir_cm.py

This script does the following:
  1) it loops with os.walk() all directories;
  2) per directory, it looks up repeats via db;
  3) repeats inside directories (compared to files with the same directory not elsewhere)
     are marked for deletion if any;
  4) the script will ask confirmation for the deletions, if any;
  5) if confirmed, the script will delete the repeats.
"""
import os
import sys
import lib.db.dbdirtree_mod as dbdt
import models.entries.dirnode_mod as dn
import default_settings as defaults
import lib.strnlistfs.strfunctions_mod as strf
import lib.strnlistfs.listfunctions_mod as listf
import lib.dirfilefs.dir_n_file_fs_mod as dirf


class IntraDirRepeatsDeleter:

  def __init__(self, ori_mountpath):
    self.ori_dbtree = dbdt.DBDirTree(ori_mountpath)
    self.current_folder_abspath = None
    self.trg_ids_to_delete_upon_confirm = []
    self.processed_sha1 = []
    self.n_processed_files = 0
    self.n_processed_dirs = 0
    self.total_files_in_db = 0
    self.total_files_in_os = 0
    self.total_dirs_in_os = 0
    self.total_sha1s_files_in_db = 0
    self.n_processed_deletes = 0
    self.n_os_phys_files_deleted = 0
    self.bool_del_confirmed = False
    self.calc_totals()

  def calc_totals(self):
    tot_osfiles, tot_osdirs = dirf.count_total_files_n_folders_with_restriction(self.ori_dbtree.mountpath)
    self.total_files_in_os, self.total_dirs_in_os = tot_osfiles, tot_osdirs
    self.total_files_in_db = self.ori_dbtree.count_rows_as_int()
    self.total_sha1s_files_in_db = self.ori_dbtree.count_unique_sha1s_as_int()

  @property
  def dyn_parentpath(self):
    middlepath = self.current_folder_abspath[len(self.ori_dbtree.mountpath):]
    return strf.prepend_slash_if_needed(middlepath)

  def process_sha1s_in_folder(self, sha1_dict):
    dirnode_id_dict = {}  # this dict is a print-screen-helper, it doesn't play a role for the id-finding process
    ids = []  # this list will keep the ids found for deletion in the corresponding folder (self.current_folder_abspath)
    for sha1 in sha1_dict:
      inner_del_id_n_namecharsize_tuplelist = []
      sql = 'select * from %(tablename)s where sha1=? and parentpath=?;'
      tuplevalues = (sha1, self.dyn_parentpath)
      inner_fetched_list = self.ori_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
      for inner_row in inner_fetched_list:
        dirnode = dn.DirNode.create_with_tuplerow(inner_row, self.ori_dbtree.fieldnames)
        if not dirnode.does_dirnode_exist_in_disk(self.ori_dbtree.mountpath):
          continue
        dirnode_id_dict[inner_row[0]] = dirnode
        id_n_namecharsize = (inner_row[0], len(dirnode.name))
        inner_del_id_n_namecharsize_tuplelist.append(id_n_namecharsize)
      if len(inner_del_id_n_namecharsize_tuplelist) < 2:  # if this happens, some files in db may not exist in os
        continue
      ids += listf.remove_larger_number_n_return_the_ids(inner_del_id_n_namecharsize_tuplelist)
    for _id in ids:
      dirnode = dirnode_id_dict[_id]
      self.n_processed_deletes += 1
      print(
        self.n_processed_deletes, 'storing to delete list: [', dirnode.name, '] @ [',
        strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50), ']'
      )
    self.trg_ids_to_delete_upon_confirm += ids
    print(self.n_processed_dirs, '/', self.total_dirs_in_os, 'Directory [', self.current_folder_abspath, ']')
    print('Total accumulated delete items:', len(self.trg_ids_to_delete_upon_confirm))
    return

  def process_current_folder_lookingup_inner_repeats(self):
    sha1_dict = {}
    sql = 'select DISTINCT sha1, count(sha1) as c from %(tablename)s where parentpath=? group by sha1 having c > 1;'
    tuplevalues = (self.dyn_parentpath,)
    fetched_list = self.ori_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    for row in fetched_list:
      self.n_processed_files += 1
      sha1_count = row[1]
      sha1 = row[0]
      sha1_dict[sha1] = sha1_count
    return self.process_sha1s_in_folder(sha1_dict)

  def walk_dir_by_by_to_find_repeats(self):
    for self.current_folder_abspath, _, _ in os.walk(self.ori_dbtree.mountpath):
      if self.current_folder_abspath == self.ori_dbtree.mountpath:
        continue
      if dirf.is_forbidden_dirpass(self.current_folder_abspath):
        continue
      self.n_processed_dirs += 1
      self.process_current_folder_lookingup_inner_repeats()

  def confirm_deletion(self):
    print('Confirm deletion: ids:')
    if len(self.trg_ids_to_delete_upon_confirm) == 0:
      print(' >>>>>>>>>>>> No ids to delete.')
      return False
    total_trg_files_to_delete = len(self.trg_ids_to_delete_upon_confirm)
    self.n_processed_deletes = 0
    for i, _id in enumerate(self.trg_ids_to_delete_upon_confirm):
      self.n_processed_deletes += 1
      dirnode = self.ori_dbtree.fetch_dirnode_by_id(_id)
      print(
        self.n_processed_deletes, 'of', total_trg_files_to_delete, 'to delete and',
        self.total_files_in_os, 'in dirtree :',
        'File to delete in target: id=%d' % dirnode.get_db_id(), '::', dirnode.sha1.hex()[:10] + '...'
      )
      print(
        '[', dirnode.name, '] @ [',
        strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50), ']'
      )
    screen_msg = 'Do you want to delete the %d target files above? (*Y/n) [ENTER means yes] '\
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
        print(self.n_os_phys_files_deleted + 1, 'Deleting', file_abspath)
        os.remove(file_abspath)
        self.n_os_phys_files_deleted += 1
      print('Deleting in db', _id, dirnode.name, '@', strf.put_ellipsis_in_str_middle(dirnode.parentpath, 50))
      _ = self.ori_dbtree.delete_row_by_id(_id)
      self.n_processed_deletes += 1
    print('-'*50)
    print('Deleted altogether', self.n_processed_deletes, 'ids')
    print('-'*50)

  def process(self):
    self.walk_dir_by_by_to_find_repeats()
    self.bool_del_confirmed = self.confirm_deletion()
    if self.bool_del_confirmed:
      self.do_confirmed_deletes()
    self.report()

  def report(self):
    print('Report:')
    print('=======')
    print('dirtree:', self.ori_dbtree.mountpath)
    print('total_files_in_db', self.total_files_in_db)
    print('total_files_in_os', self.total_files_in_os)
    print('total_dirs_in_os', self.total_dirs_in_os)
    print('total_sha1s_files_in_db', self.total_sha1s_files_in_db)
    print('n_processed_files_in_trg', self.n_processed_files)
    print('n_processed_dirs', self.n_processed_dirs)
    print('n_processed_deletes', self.n_processed_deletes)
    print('n_trg_os_phys_files_deleted', self.n_os_phys_files_deleted)


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
  forcedeleter = IntraDirRepeatsDeleter(src_mountpath)
  forcedeleter.process()


if __name__ == '__main__':
  process()
