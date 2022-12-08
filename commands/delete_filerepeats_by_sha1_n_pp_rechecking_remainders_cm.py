#!/usr/bin/env python3
"""
Info-Help on delete_filerepeats_by_sha1_n_pp_rechecking_remainders_cm.py

This script groups all repeat files (*) that exist, if any, under the given dirtree
  and chooses one of them to be saved (ie to not be deleted), deleting all others.
(*) repeat files are those that have the same sha1 hash

Thus, this file-to-be-saved so to say mentioned above is rehashed to be rechecked
  that its sha1 is the same as before and it is fully readable.

All remaining files in the grouped sha1 set are queued up to be deleted.
  Deletion will happen at the end against a user confirmation prompt response.
--------------------
In a nutshell, the process run with this script deletes excess files under a dirtree
  resulting a dirtree that does not contain repeats.
--------------------
Usage:
  $delete_filerepeats_by_sha1_n_pp_rechecking_remainders_cm.py <given-dirtree>
Example:
  $delete_filerepeats_by_sha1_n_pp_rechecking_remainders_cm.py "/Science/Physics/Einsteian Relativity"
"""
# import fs.db.dbrepeats_mod as dbr
import os.path
import fs.db.dbdirtree_mod as dbt
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import models.entries.dirnode_mod as dn
import default_settings as defaults
import fs.hashfunctions.hash_mod as hm


class FileRepeatsDeleter:

  def __init__(self, mountpath=None):
    self.sha1s = set()
    self.sha1_n_dirnodes_dict = {}
    self.total_deleted_in_db = 0
    self.dirnode_to_save_passed = 0
    self.n_saved_file_sha1_passed = 0
    self.n_failed_deletes = 0
    self.n_processed_deletes = 0
    self.dirnodes_to_del = []
    self.dirnodes_to_save = []
    self.total_files_in_db = 0
    self.total_unique_files_in_db = 0
    self.total_files_in_os = 0
    self.total_dirs_in_os = 0
    self.total_counted_items = 0
    self.histogram_sha1_quant = {}
    self.n_processed_files = 0
    self.n_processed_sha1s = 0
    self.n_found_files_name_n_parent_in_db = 0
    self.n_found_files_size_n_date_in_db = 0
    self.n_updated_dbentries = 0
    self.n_files_empty_sha1 = 0
    # self.dbrepeat = dbr.DBRepeat(mountpath)
    self.dbtree = dbt.DBDirTree(mountpath)

  def calc_totals(self):
    """
    count_total_files_n_folders_with_restriction(mountpath, restricted_dirnames, forbidden_first_level_dirs)
    """
    print('Counting files and dirs in db and os. Please wait.')
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    self.total_unique_files_in_db = self.dbtree.count_unique_sha1s_as_int()
    total_files, total_dirs = dirf.count_total_files_n_folders_with_restriction(self.dbtree.mountpath)
    self.total_files_in_os = total_files
    self.total_dirs_in_os = total_dirs

  def fetch_sha1_with_quantities(self):
    sql = 'SELECT DISTINCT(sha1), count(id) from %(tablename)s GROUP BY sha1;'
    rowlist = self.dbtree.do_select_with_sql_without_tuplevalues(sql)
    self.total_counted_items = 0
    for i, row in enumerate(rowlist):
      sha1 = row[0]
      sha1hex = sha1.hex()
      quant = row[1]
      self.histogram_sha1_quant[sha1] = quant
      self.total_counted_items += quant
      print(i+1, 'q =', quant, sha1hex)
    print('total sha1s', len(rowlist), 'items', self.total_counted_items)

  @property
  def total_dirnodes_in_sha1_dict(self):
    total = [len(self.sha1_n_dirnodes_dict[s]) for s in self.sha1_n_dirnodes_dict]
    return sum(total)

  def transpose_sha1s_n_ids_to_sha1_n_dirnodes(self):
    for sha1 in self.sha1s:
      sql = 'SELECT * from %(tablename)s where sha1=?;'
      tuplevalues = (sha1,)
      rowlist = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
      for row in rowlist:
        dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
        self.n_processed_files += 1
        print(self.n_processed_files, self.total_files_in_db, 'transposing', dirnode)
        try:
          self.sha1_n_dirnodes_dict[sha1].append(dirnode)
        except KeyError:
          self.sha1_n_dirnodes_dict[sha1] = [dirnode]
    for sha1 in self.sha1s:
      dirnodes = self.sha1_n_dirnodes_dict[sha1]
      print(len(dirnodes), sha1.hex())
    pass

  def check_if_dirnode_is_sha1able(self, dirnode):
    filepath = dirnode.get_abspath_with_mountpath(self.dbtree.mountpath)
    if not os.path.isfile(filepath):
      # error_msg = 'file to save does not exist ' + filepath
      return False
    filestat = os.stat(filepath)
    if dirnode.bytesize != filestat.st_size:
      # error_msg = 'file size in db %d is diff than in os %d %s' % (dirnode.bytesize, filestat.st_size, filepath)
      return False
    print('Recalculating sha1')
    sha1 = hm.calc_sha1_from_file(filepath)
    if dirnode.sha1 != sha1:
      # error_msg = 'sha1 recalculated %s is diff than in db %s %s' % (dirnode.sha1.hex(), sha1.hex(), filepath)
      return False
    self.dirnode_to_save_passed += 1
    print(self.dirnode_to_save_passed, '/', len(self.dirnodes_to_save), '/', self.n_processed_files, 'PASSED')
    print(dirnode)
    return True

  def do_delete_files_in_os_n_db(self):
    bulk_ids_to_delete_in_db = []
    for i, dirnode in enumerate(self.dirnodes_to_del):
      filepath = dirnode.get_abspath_with_mountpath(self.dbtree.mountpath)
      try:
        if os.path.isfile(filepath):
          print(self.n_processed_deletes + 1, '/', len(self.dirnodes_to_del), 'DELETING', filepath)
          os.remove(filepath)
          self.n_processed_deletes += 1
          bulk_ids_to_delete_in_db.append(dirnode.get_db_id())
      except (IOError, OSError):
        self.n_failed_deletes += 1
        print(
          self.n_failed_deletes, self.n_processed_deletes+1, '/', len(self.dirnodes_to_del),
          'FAILED delete', filepath
        )
    self.dirnodes_to_del = bulk_ids_to_delete_in_db
    self.total_deleted_in_db = self.dbtree.delete_ids(self.dirnodes_to_del)
    print('Total rows deleted ************** ', self.total_deleted_in_db)

  def show_deletes_for_confirmation(self):
    print(' ======= show_deletes_for_confirmation =======')
    self.show_totals_to_del()
    for i, dirnode in enumerate(self.dirnodes_to_del):
      print(i, dirnode.fpath)

  def adjust_sha1s_n_dirnodes(self):
    total_sha1s = len(self.sha1_n_dirnodes_dict)
    remove_sha1s_later = set()
    acc_to_del = 0
    for i, sha1 in enumerate(self.sha1_n_dirnodes_dict):
      dirnodes = self.sha1_n_dirnodes_dict[sha1]
      tuplelist_dirnode_n_ppcharsize = []
      for dirnode in dirnodes:
        # fpath is os.path.join(parentpath, name)
        # if the repeats are in the same folder, the one with largest name (filename) will be the one to remain
        dirnode_n_ppcharsize_tupl = (dirnode, len(dirnode.fpath))
        tuplelist_dirnode_n_ppcharsize.append(dirnode_n_ppcharsize_tupl)
      sorted(tuplelist_dirnode_n_ppcharsize, key=lambda e: e[1])
      if len(tuplelist_dirnode_n_ppcharsize) < 2:
        remove_sha1s_later.add(sha1)
      inner_ids_to_del = []
      while len(tuplelist_dirnode_n_ppcharsize) > 1:
        # the last one is not to be deleted
        inner_ids_to_del = [tupl[0] for tupl in tuplelist_dirnode_n_ppcharsize[:-1]]
        dirnode, _ = tuplelist_dirnode_n_ppcharsize[-1]
        dirnode_passed = self.check_if_dirnode_is_sha1able(dirnode)
        if dirnode_passed:
          self.n_saved_file_sha1_passed += 1
          break
        del tuplelist_dirnode_n_ppcharsize[-1]
      if len(inner_ids_to_del) > 0:
        self.dirnodes_to_del += inner_ids_to_del
      else:
        remove_sha1s_later.add(sha1)
      n_loop = i + 1  # n_loop is also the number of "files to save" (or files to remain)
      len_tuplelist = len(tuplelist_dirnode_n_ppcharsize)
      acc_to_del += len_tuplelist - 1
      print(
        sha1.hex(),
        'len dirnodes_to_del', len(self.dirnodes_to_del),
        'prep', n_loop, 'tot sha1s', total_sha1s,
        'len ids_n_ppcharsizes', len_tuplelist, 'remains', n_loop, 'to confirm del', acc_to_del
      )
    # lastly remove the sha1s that do not have at least two elements anymore
    for sha1 in remove_sha1s_later:
      del self.sha1_n_dirnodes_dict[sha1]

  def show_totals_to_del(self):
    print('total_dirnodes_in_sha1_dict', self.total_dirnodes_in_sha1_dict)
    print('n_processed_files_in_trg', self.n_processed_files)
    print('len dirnodes_to_del', len(self.dirnodes_to_del))
    print('len dirnodes_to_save', len(self.dirnodes_to_save))

  def confirm_deletion_of_files_that_have_one_sha1_rechecked(self):
    if len(self.dirnodes_to_del) == 0:
      print('No files to delete.')
      return False
    screen_msg = 'Confirm deletion of %d repeat files that have each sha1 ' \
                 'rechecked and maintained (altogether %d)?' \
                 ' (*Y/n) [ENTER] means Yes ' % (len(self.dirnodes_to_del), len(self.dirnodes_to_save))
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def fetch_repeat_sha1s(self):
    """
    First SELECT in here generates a list that shows records starting by sha1s, ordered by sha1.
    It goes like the following example:
    sha1 s1:
      - s1 row1
      - s1 row2 (if more)
      - s1 row etc  (if more)
    sha1 s2:
      - s2 row1
      - s2 row2 (if more)
      - s2 row etc (if more)
    etc.

    '''
      SELECT DISTINCT t1.sha1, t2.* FROM %(tablename)s as t1
        INNER JOIN %(tablename)s as t2
        WHERE t1.sha1 = t2.sha1
        ORDER BY t1.sha1
    '''
    """
    sql = '''
    SELECT DISTINCT sha1, count(sha1) as c FROM files_in_tree
      GROUP BY sha1
      HAVING count(sha1) > 1;
    '''
    rowlist = self.dbtree.do_select_with_sql_without_tuplevalues(sql)
    print('-=+|+=-'*10)
    n_sha1s = 0
    for row in rowlist:
      sha1 = row[0]
      if sha1 == hm.EMPTY_SHA1_AS_BIN:
        continue
      counted = row[1]
      n_sha1s += 1
      print(n_sha1s, 'qtd', counted, '|', sha1.hex())
      self.sha1s.add(sha1)
    pass

  def process(self):
    self.calc_totals()
    # self.fetch_sha1_with_quantities()
    self.fetch_repeat_sha1s()
    self.transpose_sha1s_n_ids_to_sha1_n_dirnodes()
    self.adjust_sha1s_n_dirnodes()
    self.show_deletes_for_confirmation()
    if self.confirm_deletion_of_files_that_have_one_sha1_rechecked():
      self.do_delete_files_in_os_n_db()
    self.report()
    print('='*50)
    self.show_totals_to_del()

  def report(self):
    self.calc_totals()
    print('total_files_in_db', self.total_files_in_db)
    print('total_unique_files_in_db', self.total_unique_files_in_db)
    print('total_files_in_os', self.total_files_in_os)
    print('total_dirs_in_os', self.total_dirs_in_os)
    print('n_processed_files_in_trg', self.n_processed_files)
    print('n_saved_file_sha1_passed', self.n_saved_file_sha1_passed)
    print('n_processed_deletes', self.n_processed_deletes)
    print('total_deleted_in_db', self.total_deleted_in_db)
    print('n_processed_sha1s', self.n_processed_sha1s)
    print('n_failed_deletes', self.n_failed_deletes)
    print('n_found_files_name_n_parent_in_db', self.n_found_files_name_n_parent_in_db)
    print('n_found_files_size_n_date_in_db', self.n_found_files_size_n_date_in_db)
    print('n_updated_dbentries', self.n_updated_dbentries)
    print('total_dirnodes_in_sha1_dict', self.total_dirnodes_in_sha1_dict)
    print('n_files_empty_sha1', self.n_files_empty_sha1)
    for sha1 in self.histogram_sha1_quant:
      quant = self.histogram_sha1_quant[sha1]
      print('q =', quant, sha1.hex())


def process():
  mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  deleter = FileRepeatsDeleter(mountpath)
  deleter.process()


if __name__ == '__main__':
  process()
