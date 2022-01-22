#!/usr/bin/env python3
"""
clean_restricted_path_rows_in_db_mod.py

This script removes from db the rows that have restricted paths.
These rows are probably derived from the action of moving files (*) into the restricted directores [eg z-del z-tri etc]

(*) script dbentry_updater_by_filemove_based_on_size_n_mdt_cm.py does that (at the moment).
"""
import fs.db.dbdirtree_mod as dbdt
import default_settings as defaults
import fs.dirfilefs.dir_n_file_fs_mod as dirfil


class RowsWithRestrictedPathsCleaner:
  """
  """

  def __init__(self, mountpath):
    self.deletion_confirmed = False
    self.n_processed_files = 0
    self.n_processed_deletes = 0
    self.rows_with_restricted_paths_ids = []
    self.n_deleted = 0
    self.n_failed_deletes = 0
    self.total_files_in_db = 0
    self.total_files_os = 0
    self.total_dirs_os = 0
    self.n_rmdirs_visited = 0
    self.n_dirs_deleted = 0
    self.n_rmdirs_failed = 0
    self.dbtree = dbdt.DBDirTree(mountpath)
    self.curr_cleandel_dirpath = None

  def confirm_rows_deletion(self):
    total_to_del = len(self.rows_with_restricted_paths_ids)
    if total_to_del == 0:
      return False
    screen_msg = 'Confirm deletion of the %d rows above? (*Y/n) ' % total_to_del
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def do_delete_rows(self):
    self.n_processed_deletes = 0
    for _id in self.rows_with_restricted_paths_ids:
      self.n_processed_deletes += 1
      print(self.n_processed_deletes, 'deleting', _id)
      self.dbtree.delete_row_by_id(_id)

  def show_delete_ids(self):
    print('show_delete_ids')
    print('='*40)
    if len(self.rows_with_restricted_paths_ids) == 0:
      print('\tThere have not been found any delete ids.')
    for _id in self.rows_with_restricted_paths_ids:
      dirnode = self.dbtree.fetch_dirnode_by_id(_id)
      print(dirnode)

  def verify_if_row_has_a_restricted_dir_prefix_n_append_if_so(self, row):
    idx = self.dbtree.fieldnames.index('parentpath')
    parentpath = row[idx]
    if dirfil.is_any_name_in_path_startingwith_any_prefix_in_list(parentpath):
      _id = row[0]
      self.rows_with_restricted_paths_ids.append(_id)

  def loop_thru_rows_that_have_paths_in_restricted_dirs(self):
    if len(defaults.RESTRICTED_DIRNAMES_FOR_WALK) == 0:
      print('No processing - defaults.RESTRICTED_DIRNAMES_FOR_WALK is empty.')
      return
    listvalues = []
    sql = 'SELECT * FROM %(tablename)s WHERE '
    for restricted_dir_prefix in defaults.RESTRICTED_DIRNAMES_FOR_WALK:
      restricted_sql_value = '%' + restricted_dir_prefix + '%'
      listvalues.append(restricted_sql_value)
      sql += 'parentpath LIKE ? OR '
    sql = sql.rstrip(' OR ') + ';'
    tuplevalues = tuple(listvalues)
    generated_rows = self.dbtree.do_select_sql_n_tuplevalues_w_limit_n_offset(sql, tuplevalues)
    for rows in generated_rows:
      for row in rows:
        self.verify_if_row_has_a_restricted_dir_prefix_n_append_if_so(row)

  def process(self):
    self.loop_thru_rows_that_have_paths_in_restricted_dirs()
    self.show_delete_ids()
    if self.confirm_rows_deletion():
      self.do_delete_rows()
    self.report()

  def report(self):
    print('Report:')
    print('=======')
    print('dirtrees:', self.dbtree.mountpath, self.dbtree.mountpath)
    print('len(delete_ids)', len(self.rows_with_restricted_paths_ids))
    print('total_files_in_db', self.total_files_in_db)
    print('total_files_os', self.total_files_os)
    print('total_dirs_os', self.total_dirs_os)
    print('After process:')
    print('='*40)
    print('total_files_in_db', self.total_files_in_db)
    print('total_files_os', self.total_files_os)
    print('total_dirs_os', self.total_dirs_os)
    print('n_rmdirs_visited', self.n_rmdirs_visited)
    print('n_dirs_deleted', self.n_dirs_deleted)
    print('n_rmdirs_failed', self.n_rmdirs_failed)


def process():
  src_mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  cleandeleter = RowsWithRestrictedPathsCleaner(src_mountpath)
  cleandeleter.process()


if __name__ == '__main__':
  process()
