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
import fs.hashfunctions.hash_mod as hm
import fs.db.dbbase_mod as dbb
import fs.db.dbutil as dbu


class DBDirTree(dbb.DBBase):

  default_tablename = 'files_in_tree'

  def __init__(self, mount_abspath=None, inlocus_sqlite_filename=None, tablename=None):
    self.mountpath = mount_abspath
    self._fieldnames = ['id', 'name', 'parentpath', 'sha1', 'bytesize', 'mdatetime']
    if tablename is None:
      self.tablename = self.default_tablename
    super().__init__(mount_abspath, inlocus_sqlite_filename)

  # def get_connection(self):
  #   return sqlite3.Connection(self.sqlitefile_abspath)

  def form_fields_line_for_createtable(self):
    """
    This method is to be implemented in child-inherited classes
    """
    middle_sql = """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      parentpath TEXT,
      sha1 BLOB,
      bytesize INTEGER,
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

  def fetch_rec_if_hkey_exists_in_db(self, hkey):
    sql = 'SELECT * FROM %(tablename)s WHERE hkey=?;' % {'tablename': self.tablename}
    tuplevalues = (hkey, )
    conn = self.get_connection()
    cursor = conn.cursor()
    fetch_result = cursor.execute(sql, tuplevalues)
    result_tuple_list = fetch_result.fetchall()
    cursor.close()
    conn.close()
    return result_tuple_list

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
        is_present=?,
        sha1=?,
        bytesize=?, 
        mdatetime=? 
      WHERE
        hkey=?;
      '''
    return sql_before_interpol


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
