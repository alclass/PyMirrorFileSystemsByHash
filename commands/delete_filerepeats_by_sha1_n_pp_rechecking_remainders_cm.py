#!/usr/bin/env python3
# import fs.db.dbrepeats_mod as dbr
import os.path

import fs.db.dbdirtree_mod as dbt
import fs.dirfilefs.dir_n_file_fs_mod as dirf
import models.entries.dirnode_mod as dn
import default_settings as defaults
import fs.hashfunctions.hash_mod as hm


class ReportFileRepeat:

  def __init__(self, mountpath=None):
    self.sha1_n_ids_dict = {}
    self.n_processing_ids = 0
    self.total_deleted_in_db = 0
    self.n_failed_deletes = 0
    self.n_processed_deletes = 0
    self.ids_to_del = []
    self.ids_to_save = []
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

  def add_to_sha1_n_ids_dict(self, dirnode):
    sha1 = dirnode.sha1
    _id = dirnode.get_db_id()
    if sha1 in self.sha1_n_ids_dict:
      self.sha1_n_ids_dict[sha1].append(_id)
    else:
      self.sha1_n_ids_dict[sha1] = [_id]

  @property
  def total_ids_in_sha1_dict(self):
    total = [len(self.sha1_n_ids_dict[s]) for s in self.sha1_n_ids_dict]
    return sum(total)

  def prepare_ids_to_del(self):
    for sha1 in self.sha1_n_ids_dict:
      tuplelist_id_n_ppcharsize = []
      for _id in self.sha1_n_ids_dict[sha1]:
        self.n_processing_ids += 1
        dirnode = self.dbtree.fetch_dirnode_by_id(_id)
        id_n_ppcharsize_tupl = (_id, len(dirnode.parentpath))
        tuplelist_id_n_ppcharsize.append(id_n_ppcharsize_tupl)
      sorted(tuplelist_id_n_ppcharsize, key=lambda e: e[1])
      if len(tuplelist_id_n_ppcharsize) > 1:
        # the last one is not to be deleted
        inner_ids_to_del = [tupl[0] for tupl in tuplelist_id_n_ppcharsize[:-1]]
        self.ids_to_del += inner_ids_to_del
        _id, _ = tuplelist_id_n_ppcharsize[-1]
        self.ids_to_save.append(_id)

  def show_ids_to_del(self):
    print('total sha1s with repeats', len(self.sha1_n_ids_dict))
    print('total ids_to_del', len(self.ids_to_del))
    print('total_ids_in_sha1_dict', self.total_ids_in_sha1_dict)
    print('n_processing_ids', self.n_processing_ids)
    print('total ids_to_save', len(self.ids_to_save))

  def recheck_ids_to_save(self):
    for i, _id in enumerate(self.ids_to_save):
      dirnode = self.dbtree.fetch_dirnode_by_id(_id)
      filepath = dirnode.get_abspath_with_mountpath(self.dbtree.mountpath)
      if not os.path.isfile(filepath):
        error_msg = 'file to save does not exist ' + filepath
        raise OSError(error_msg)
      filestat = os.stat(filepath)
      if dirnode.bytesize != filestat.st_size:
        error_msg = 'file size in db %d is diff than in os %d %s' % (dirnode.bytesize, filestat.st_size, filepath)
        raise OSError(error_msg)
      print('Recalculating sha1')
      sha1 = hm.calc_sha1_from_file(filepath)
      if dirnode.sha1 != sha1:
        error_msg = 'sha1 recalculated %s is diff than in db %s %s' % (dirnode.sha1.hex(), sha1.hex(), filepath)
        raise OSError(error_msg)
      print(i+1, '/', len(self.ids_to_save), '/', self.n_processing_ids, 'PASSED')
      print(dirnode)

  def confirm_deletion_of_files_that_have_one_sha1_rechecked(self):
    if len(self.ids_to_del) == 0:
      print('No files to delete.')
      return False
    screen_msg = 'Confirm deletion of %d repeat files that have each sha1 ' \
                 'rechecked and maintained (altogether %d)?' \
                 ' (*Y/n) [ENTER] means Yes ' % (len(self.ids_to_del), len(self.ids_to_save))
    ans = input(screen_msg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def do_delete_ids_in_os_n_db(self):
    bulk_ids_to_delete_in_db = []
    for i, _id in enumerate(self.ids_to_del):
      dirnode = self.dbtree.fetch_dirnode_by_id(_id)
      filepath = dirnode.get_abspath_with_mountpath(self.dbtree.mountpath)
      try:
        if os.path.isfile(filepath):
          print(self.n_processed_deletes+1, '/', len(self.ids_to_del), 'DELETING', filepath)
          os.remove(filepath)
          self.n_processed_deletes += 1
          bulk_ids_to_delete_in_db.append(_id)
      except (IOError, OSError):
        self.n_failed_deletes += 1
        print(
          self.n_failed_deletes, self.n_processed_deletes+1, '/', len(self.ids_to_del),
          'FAILED delete', filepath
        )
    self.ids_to_del = bulk_ids_to_delete_in_db
    self.total_deleted_in_db = self.dbtree.delete_ids(self.ids_to_del)
    print('Total rows deleted ************** ', self.total_deleted_in_db)

  def show_sha1s_followed_by_records(self):
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
      SELECT * FROM files_in_tree
      WHERE sha1 IN (
        SELECT DISTINCT sha1 AS c FROM files_in_tree
        GROUP BY sha1
        HAVING count(sha1) > 1
      )
      ORDER BY 
        sha1,
        parentpath,
        name;
    '''
    rowlist = self.dbtree.do_select_with_sql_without_tuplevalues(sql)
    former_sha1 = None
    former_pp = None
    print('-=+|+=-'*10)
    ids_delete_buffer = []
    for i, row in enumerate(rowlist):
      dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
      self.add_to_sha1_n_ids_dict(dirnode)
      if former_sha1 != dirnode.sha1:
        ids_delete_buffer = []
        self.n_processed_sha1s += 1
        former_sha1 = dirnode.sha1
        print('-'*80)
        print(self.n_processed_sha1s, dirnode.sha1.hex())
        print('-'*45)
      ids_delete_buffer.append(dirnode.get_db_id())
      if former_pp != dirnode.parentpath:
        former_pp = dirnode.parentpath
        print('\t @', dirnode.parentpath, dirnode.get_db_id())
      self.n_processed_files += 1
      print(self.n_processed_files, '\t\t [', dirnode.name, ']', dirnode.bytesize, dirnode.mdatetime)
    print('-=+|+=-'*10)

  def process(self):
    self.calc_totals()
    # self.fetch_sha1_with_quantities()
    self.show_sha1s_followed_by_records()
    self.prepare_ids_to_del()
    self.recheck_ids_to_save()
    if self.confirm_deletion_of_files_that_have_one_sha1_rechecked():
      self.do_delete_ids_in_os_n_db()
    self.report()
    print('='*50)
    self.show_ids_to_del()

  def report(self):
    self.calc_totals()
    print('total_files_in_db', self.total_files_in_db)
    print('total_unique_files_in_db', self.total_unique_files_in_db)
    print('total_files_in_os', self.total_files_in_os)
    print('total_dirs_in_os', self.total_dirs_in_os)
    print('n_processed_files', self.n_processed_files)
    print('n_processed_deletes', self.n_processed_deletes)
    print('total_deleted_in_db', self.total_deleted_in_db)
    print('n_processed_sha1s', self.n_processed_sha1s)
    print('n_failed_deletes', self.n_failed_deletes)
    print('n_found_files_name_n_parent_in_db', self.n_found_files_name_n_parent_in_db)
    print('n_found_files_size_n_date_in_db', self.n_found_files_size_n_date_in_db)
    print('n_updated_dbentries', self.n_updated_dbentries)
    print('n_files_empty_sha1', self.n_files_empty_sha1)
    for sha1 in self.histogram_sha1_quant:
      quant = self.histogram_sha1_quant[sha1]
      print('q =', quant, sha1.hex())


def process():
  mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  reporter = ReportFileRepeat(mountpath)
  reporter.process()


if __name__ == '__main__':
  process()
