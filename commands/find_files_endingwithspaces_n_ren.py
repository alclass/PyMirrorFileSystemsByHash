#!/usr/bin/env python3
"""
find_files_endingwithspaces_n_ren.py
This script aims to finding files that have names ending with whitespace (basically the 4 characters ' \t\r\n')
If so, it will rename them in folder and update corresponding entries in db.
"""
import datetime
import os
import models.entries.dirtree_mod as dt
import models.entries.dirnode_mod as dn
import default_settings as ds


class FileFinderTreeTraversor:

  def __init__(self, mountpath):
    self.dirtree = dt.DirTree('ori', mountpath)  # dbtree is taken from dirtree.dbtree
    self.n_files_in_db = 0
    self.n_renamed_in_dir = 0
    self.n_updated_in_db = 0
    self.n_unique_sha1_in_db = 0
    self.n_processed_files_in_db = 0
    self.n_files_in_dirtree = 0
    self.n_dirs_in_dirtree = 0
    self.n_files_endingwithspaces = 0
    self.read_totals_from_db()

  @property
  def mountpath(self):
    return self.dirtree.mountpath

  @property
  def dbtree(self):
    return self.dirtree.dbtree

  def read_totals_from_db(self):
    self.n_files_in_db = self.dbtree.count_rows_as_int()
    self.n_unique_sha1_in_db = self.dbtree.count_unique_sha1s_as_int()

  def treat_file_with_endingspaces(self, row):
    dirnode = dn.DirNode.create_with_tuplerow(row, self.dbtree.fieldnames)
    # strip extension
    extensionless_name, ext = os.path.splitext(dirnode.name)
    new_extensionless_name = extensionless_name.rstrip(' \t\r\n')
    newname = new_extensionless_name + ext
    idx = self.dirtree.dbtree.fieldnames.index('parentpath')
    parentpath = row[idx]
    self.n_files_endingwithspaces += 1
    print('-' * 40)
    print(
      self.n_files_endingwithspaces, '[caught file with ending spaces]',
      self.n_files_endingwithspaces, '[' + extensionless_name + '] in', parentpath
    )
    print('-' * 40)
    oldfilepath = dirnode.get_abspath_with_mountpath(self.mountpath)
    folderpath, _ = os.path.split(oldfilepath)
    newfilepath = os.path.join(folderpath, newname)
    if os.path.isfile(oldfilepath) and not os.path.isfile(newfilepath):
      print(self.n_processed_files_in_db, '/', self.n_files_in_db, 'Rename:')
      print('oldfilepath', oldfilepath)
      print('newfilepath', newfilepath)
      os.rename(oldfilepath, newfilepath)
      self.n_renamed_in_dir += 1
      sql = 'UPDATE %(tablename)s SET name=? WHERE name=? and parentpath=?;'
      tuplevalues = (newname, dirnode.name, dirnode.parentpath)
      self.dbtree.do_update_with_sql_n_tuplevalues(sql, tuplevalues)
      self.n_updated_in_db += 1
      print('Updated:', self.n_updated_in_db, '/', self.n_processed_files_in_db, '/', self.n_files_in_db)

  def verify_endingspaces_in_names(self, rowlist):
    for row in rowlist:
      self.n_processed_files_in_db += 1
      idx = self.dirtree.dbtree.fieldnames.index('name')
      current_name = row[idx]
      # strip extension and check namepart
      extensionless_name, _ = os.path.splitext(current_name)
      new_extensionless_name = extensionless_name.rstrip(' \t\r\n')
      if len(extensionless_name) > len(new_extensionless_name):
        self.treat_file_with_endingspaces(row)

  def os_traverse_to_count_files(self):
    self.n_files_in_dirtree = 0
    is_root_dir = True
    for _, _, files in os.walk(self.mountpath):
      if is_root_dir:
        # ie files are not counted in rootdir
        is_root_dir = False
        continue
      self.n_files_in_dirtree += len(files)
      self.n_dirs_in_dirtree += len(files)

  def db_traverse_to_find_files_endingwithspaces(self):
    for rowlist in self.dirtree.dbtree.do_select_all_w_limit_n_offset():
      self.verify_endingspaces_in_names(rowlist)

  def process(self):
    self.os_traverse_to_count_files()
    self.db_traverse_to_find_files_endingwithspaces()
    self.report()

  def as_dict(self):
    outdict = {
      'mountpath': self.mountpath,
      'n_files_in_db': self.n_files_in_db,
      'n_unique_sha1_in_db': self.n_unique_sha1_in_db,
      'n_files_in_dirtree': self.n_files_in_dirtree,
      'n_dirs_in_dirtree': self.n_dirs_in_dirtree,
      'n_processed_files_in_db': self.n_processed_files_in_db,
      'n_renamed_in_dir': self.n_renamed_in_dir,
      'n_updated_in_db': self.n_updated_in_db,
      'n_files_endingwithspaces': self.n_files_endingwithspaces,
    }
    return outdict

  def mount_reportstr(self):
    outstr = '''    mountpath = %(mountpath)s
    n_files_in_db = %(n_files_in_db)d
    n_unique_sha1_in_db = %(n_unique_sha1_in_db)d
    n_files_in_dirtree = %(n_files_in_dirtree)d
    n_dirs_in_dirtree = %(n_dirs_in_dirtree)d
    n_processed_files_in_db = %(n_processed_files_in_db)d
    n_renamed_in_dir = %(n_renamed_in_dir)d
    n_updated_in_db = %(n_updated_in_db)d
    n_files_endingwithspaces = %(n_files_endingwithspaces)d
    ''' % self.as_dict()
    return outstr

  def report(self):
    print('Report:')
    print(self.mount_reportstr())

  def __str__(self):
    return self.mount_reportstr()


def process():
  mountpath, _ = ds.get_src_n_trg_mountpath_args_or_default()
  start_time = datetime.datetime.now()
  traversor = FileFinderTreeTraversor(mountpath)
  traversor.process()
  end_time = datetime.datetime.now()
  runtime = end_time - start_time
  print('runtime', runtime)


if __name__ == '__main__':
  process()
