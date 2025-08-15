#!/usr/bin/env python3
"""
This module (dbdirtree_mod.py) contains:
 class DBDirTree(dbb.DBBase):

This class models db-table files_in_tree and inherits from class DBBase
  in module dbbase_mod. The parent class contains most of the functionalities available.
"""
import datetime
import hashlib
import os
import lib.hashfunctions.hash_mod as hm
import lib.db.dbbase_mod as dbb
import lib.db.dbutil as dbu
import models.entries.dirnode_mod as dn


class DBDirTree(dbb.DBBase):

  default_tablename = 'files_in_tree'

  def __init__(self, mountpath=None, inlocus_sqlite_filename=None, tablename=None):
    self.mountpath = mountpath
    self._fieldnames = ['id', 'name', 'parentpath', 'sha1', 'bytesize', 'mdatetime']
    if tablename is None:
      self.tablename = self.default_tablename
    super().__init__(mountpath, inlocus_sqlite_filename)

  # def get_connection(self):
  #   return sqlite3.Connection(self.sqlitefile_abspath)

  def form_fields_line_for_createtable(self):
    """
    This method is to be implemented in child-inherited classes
    """
    middle_sql = """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      parentpath TEXT NOT NULL,
      sha1 BLOB NOT NULL,
      bytesize INTEGER NOT NULL,
      mdatetime TEXT,
      UNIQUE(name, parentpath)
    """
    return middle_sql

  @property
  def fieldnames(self):
    return self._fieldnames

  def sqlite_createtable_if_not_exists(self):
    conn = self.get_connection()
    cursor = conn.cursor()
    sql = self.interpolate_create_table_sql()
    cursor.execute(sql)
    # print(sql)
    # print('Created table', tablename)
    cursor.close()
    conn.close()

  def total_files(self):
    """
    total_files = total number of entries
    """
    _total_files = 0
    result_as_tuplelist = self.count_rows()
    try:
      _total_files = result_as_tuplelist[0][0]
    except IndexError:
      pass
    return _total_files

  def fetch_row_by_id(self, _id):
    sql = 'SELECT * FROM %(tablename)s WHERE id=?;' % {'tablename': self.tablename}
    tuplevalues = (_id, )
    conn = self.get_connection()
    cursor = conn.cursor()
    fetch_result = cursor.execute(sql, tuplevalues)
    result_tuple_list = fetch_result.fetchall()
    cursor.close()
    conn.close()
    return result_tuple_list

  def transform_row_to_dirnode(self, row):
    try:
      _id = row[0]  # id is always index 0
      idx = self.fieldnames.index('name')
      name = row[idx]
      idx = self.fieldnames.index('parentpath')
      parentpath = row[idx]
      idx = self.fieldnames.index('sha1')
      sha1 = row[idx]
      idx = self.fieldnames.index('bytesize')
      bytesize = row[idx]
      idx = self.fieldnames.index('mdatetime')
      mdatetime = row[idx]
      dirnode = dn.DirNode(name, parentpath, sha1, bytesize, mdatetime)
      dirnode.set_db_id(_id)
      return dirnode
    except (AttributeError, IndexError):
      pass
    return None

  def fetch_dirnode_by_id(self, _id):
    rowlist = self.fetch_rowlist_by_id(_id)
    if rowlist and len(rowlist) == 1:
      row = rowlist[0]
      return self.transform_row_to_dirnode(row)
    return None

  def fetch_dirnode_with_name_n_parent(self, p_filename, p_parentpath):
    dirnode = None
    sql = 'SELECT * FROM %(tablename)s WHERE name=? and parentpath=?;' % {'tablename': self.tablename}
    tuplevalues = (p_filename, p_parentpath)
    conn = self.get_connection()
    cursor = conn.cursor()
    fetch_result = cursor.execute(sql, tuplevalues)
    result_tuple_list = fetch_result.fetchall()
    if result_tuple_list and len(result_tuple_list) == 1:
      row = result_tuple_list[0]
      return self.transform_row_to_dirnode(row)
    cursor.close()
    conn.close()
    return dirnode

  def count_unique_sha1s_as_int(self):
    """
    This count method uses the direct SELECT as following:
      SELECT count(distinct sha1) FROM files_in_tree ORDER BY sha1;

    Another option with a SELECT and an inner SELECT is:
      SELECT count(T.sha1) FROM (SELECT distinct sha1 FROM files_in_tree) as T;
      Notice in the SELECT above that the count() function uses a table/field from an inner SELECT:
    # sql = 'SELECT count(T.sha1) FROM (SELECT distinct sha1 FROM %(tablename)s) as T;' % {'tablename': self.tablename}
    """
    sql = 'SELECT count(distinct sha1) FROM %(tablename)s ORDER BY sha1;' % {'tablename': self.tablename}
    fetched_list = self.do_select_with_sql_without_tuplevalues(sql)
    if fetched_list or len(fetched_list) == 1:
      return fetched_list[0][0]
    return 0

  def do_insert_with_dict(self, pdict):
    """
    This method does not check for keys existence in db, so caller must be sure the new row does not yet exist in db.
      As a general rule, if the table keys exist, instead of an insert, an update should be tried.
    """
    sql, tuplevalues = dbu.prep_insert_sql_from_dict_n_return_sql_n_tuplevalues(pdict, self.fieldnames)
    if (sql, tuplevalues) == (None, None):
      return False
    return self.do_insert_with_sql_n_tuplevalues(sql, tuplevalues)

  def do_update_with_dict_n_fetchedrow(self, pdict, fetched_row):
    sql, tuplevalues = dbu.prep_updatesql_with_dict_n_frow_return_sql_n_tuplevalues(pdict, self.fieldnames, fetched_row)
    return self.do_update_with_sql_n_tuplevalues(sql, tuplevalues)

  def form_update_with_all_fields_sql(self):
    """
    Notice that the interpolation %(tablename)s is not done here, it'll be done later on.
    """
    sql_before_interpol = '''
    UPDATE %(tablename)s 
      SET
        name=?, 
        parentpath=?, 
        sha1=?,
        bytesize=?, 
        mdatetime=? 
      WHERE
        id=?;
      '''
    return sql_before_interpol

  def delete_rows_not_existing_on_dirtree(self, mountpath):
    """
    This method is not implemented in the super class
    """
    pass
    plimit = 50
    offset = 0
    ids = []
    rowsgenerator = self.do_select_all_w_limit_n_offset(plimit, offset)
    for rows in rowsgenerator:
      for row in rows:
        idx = self.fieldnames.index('name')
        name = row[idx]
        idx = self.fieldnames.index('parentpath')
        parentpath = row[idx]
        middlepath = os.path.join(parentpath, name)
        if middlepath.startswith('/'):
          middlepath = middlepath.lstrip('/')
        fpath = os.path.join(mountpath, middlepath)
        if not os.path.isfile(fpath):
          ids.append(row[0])
    print('Deleting', ids)
    conn = self.get_connection()
    cursor = conn.cursor()
    for _id in ids:
      sql = 'delete from %(tablename)s where id=?;' % {'tablename': self.tablename}
      tuplevalues = (_id, )
      cursor.execute(sql, tuplevalues)
    conn.commit()
    cursor.close()
    conn.close()
    print('Deleted/Committed', len(ids), 'records')

  def does_sha1_exist_in_thisdirtree(self, scr_dirnode):
    try:
      sha1 = scr_dirnode.sha1
    except AttributeError:
      return False
    sql = 'SELECT sha1 FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (sha1, )
    fetched_list = self.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list and len(fetched_list) > 0:
      return True
    return False


def adhoc_select():
  db = DBDirTree()
  tuplelist = db.do_select_all()
  print(tuplelist)
  for row in tuplelist:
    _id = row[0]
    print('_id', _id)
    hkey = row[1]
    print('hkey', hkey)
    name = row[2]
    print('name', name)
    parentpath = row[3]
    print('parentpath', parentpath)
    is_file = row[4]
    print('is_file', bool(is_file))
    sha1 = row[5]
    print('sha1', sha1.hex())
    bytesize = row[6]
    print('bytesize', bytesize)
    mdatetime = row[7]
    print('mdatetime', mdatetime)


def adhoc_select_all():
  db = DBDirTree()
  result_tuple_list = db.do_select_all()
  for tuplerow in result_tuple_list:
    print(tuplerow)
  return result_tuple_list


def adhoc_delete_all_rows():
  db = DBDirTree()
  n_rows_before = db.count_rows()
  print('n_rows_before', n_rows_before)
  result_tuple_list = db.delete_all_rows()
  n_rows_after = db.count_rows()
  print('result_tuple_list', result_tuple_list)
  print('n_rows_after', n_rows_after)


def adhoc_insert_some():
  name = 'file1'
  parentpath = '/folder1/secondç'
  fpath = os.path.join(parentpath, name)
  hkey = hm.HashSimple(fpath).num
  is_file = int(True)
  sha_obj = hashlib.sha1()
  strdata = 'dafbn bnç~pafsdkç'.encode('utf8')
  sha_obj.update(strdata)
  sha1 = sha_obj.digest()
  bytesize = 1000
  mdatetime = datetime.datetime.now()
  tuple_values = (None, hkey, name, parentpath, is_file, sha1, bytesize, mdatetime)
  question_marks = '?, ' * len(tuple_values)
  question_marks = question_marks.rstrip(', ')
  insert_sql = "INSERT into %(tablename)s VALUES (" + question_marks + ");"
  print('Exec adhoc_insert_some()')
  db = DBDirTree()
  return db.do_insert_with_sql_n_tuplevalues(insert_sql, tuple_values)


def process():
  """
  adhoc_select()
  adhoc_delete_all_rows()
  adhoc_select_all()
  bool_res = adhoc_insert_some()
  print('was_inserted', bool_res)
  """
  adhoc_select_all()


if __name__ == '__main__':
  process()
