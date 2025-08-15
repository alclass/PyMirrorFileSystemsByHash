#!/usr/bin/env python3
import datetime
import hashlib
import os
import lib.hashfunctions.hash_mod as hm
import lib.db.dbbase_mod as dbb


class DBRepeat(dbb.DBBase):

  default_tablename = 'file_repeats'

  def __init__(self, mount_abspath=None, inlocus_sqlite_filename=None, tablename=None):
    if tablename is None:
      self.tablename = self.default_tablename
    super().__init__(mount_abspath, inlocus_sqlite_filename)

  def form_fields_line_for_createtable(self):
    """
    This method is to be implemented in child-inherited classes
    """
    middle_sql = """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      hkey INTEGER UNIQUE,
      sha1 BLOB,
      is_to_delete INTEGER
    """
    return middle_sql

  def form_update_with_all_fields_sql(self):
    """
    Notice that the interpolation %(tablename)s is not done here, it'll be done later on.
    """
    sql_before_interpol = '''
    UPDATE %(tablename)s
      SET
        sha1=?
      WHERE 
        hkey=?
    '''
    return sql_before_interpol


def adhoc_select():
  db = DBRepeat()
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
  db = DBRepeat()
  result_tuple_list = db.do_select_all()
  for tuplerow in result_tuple_list:
    print(tuplerow)
  return result_tuple_list


def adhoc_delete_all_rows():
  db = DBRepeat()
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
  db = DBRepeat()
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
