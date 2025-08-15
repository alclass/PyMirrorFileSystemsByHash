#!/usr/bin/env python3
# import fs.db.dbrepeats_mod as dbr
import lib.db.dbdirtree_mod as dbt
import lib.dirfilefs.dir_n_file_fs_mod as dirf
import models.entries.dirnode_mod as dn
import default_settings as defaults


class ReportFileRepeat:

  def __init__(self, mountpath=None):
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
    self.total_unique_files_in_db = self.dbtree.count_unique_sha1s_as_int()
    self.total_files_in_db = self.dbtree.count_rows_as_int()
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

    for i, row in enumerate(rowlist):
      dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
      if former_sha1 != dirnode.sha1:
        self.n_processed_sha1s += 1
        former_sha1 = dirnode.sha1
        print('-'*80)
        print(self.n_processed_sha1s, dirnode.sha1.hex())
        print('-'*45)
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
    self.report()

  def report(self):
    self.calc_totals()
    print('total_files_in_db', self.total_files_in_db)
    print('total_unique_files_in_db', self.total_unique_files_in_db)
    print('total_files_in_os', self.total_files_in_os)
    print('total_dirs_in_os', self.total_dirs_in_os)
    print('n_processed_files_in_trg', self.n_processed_files)
    print('n_processed_sha1s', self.n_processed_sha1s)
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
