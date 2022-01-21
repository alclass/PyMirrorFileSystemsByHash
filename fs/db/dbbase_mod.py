#!/usr/bin/env python3
import os
import sqlite3
import default_settings as ls


class DBBase:
  """
  This base class models general functionalities for individual db-table classes.
  For the time being, there TWO inherited classes ie:
    1) DBDirTree
    2) DBRepeat

  Important to notice in terms of the relationship from base class to inherited classes:
    1) each table in the database uses cojointly -- as the two first fields -- id and hkey
    2) this detail is important because the base class (this one) will use them
    3) the base class must not use inherited fields
       (examples are bytesize in table files_in_tree and is_to_delete in table files_repeat)

  SQLITE_INLOCUS_FILENAME = '.updirfileentries.sqlite'
  SQLITE_TREEFILES_TABLENAME = 'files_in_tree_db'
  """

  _mount_abspath = None
  default_inlocus_sqlite_filename = '.updirfileentries.sqlite'
  default_tablename = None
  default_limit = 50
  default_offset = 0
  tablename = None

  def __init__(self, mount_abspath=None, inlocus_sqlite_filename=None):
    self.mount_abspath = mount_abspath
    self.sqlitedir_abspath = self.mount_abspath  # it's the same as mount abspath, treat_attributes() will check it
    self.inlocus_sqlite_filename = inlocus_sqlite_filename
    self.sqlitefile_abspath = None
    self.treat_attributes()

  def treat_attributes(self):
    if self.mount_abspath is None or not os.path.isdir(self.mount_abspath):
      self.mount_abspath = ls.Paths.get_datafolder_abspath()
      self.sqlitedir_abspath = self.mount_abspath
    if not os.path.isdir(self.mount_abspath):
      error_msg = 'Directory for sqlitefile (%s) does not exist.' % self.mount_abspath
      raise OSError(error_msg)
    if self.inlocus_sqlite_filename is None:
      self.inlocus_sqlite_filename = self.default_inlocus_sqlite_filename
    self.sqlitefile_abspath = os.path.join(self.sqlitedir_abspath, self.inlocus_sqlite_filename)
    if not os.path.isfile(self.sqlitefile_abspath):
      # create filepath with a connection call
      _ = sqlite3.Connection(self.sqlitefile_abspath)
      # try again
      if not os.path.isfile(self.sqlitefile_abspath):
        error_msg = 'Sqlitefile (%s) does not exist.' % self.sqlitefile_abspath
        raise OSError(error_msg)
    self.sqlite_createtable_if_not_exists()
    return

  def get_connection(self):
    return sqlite3.Connection(self.sqlitefile_abspath)

  def form_fields_line_for_createtable(self):
    """
    This method is to be implemented in child-inherited classes
    """
    return " tablename " + self.tablename

  def interpolate_create_table_sql(self):
    sql = 'CREATE TABLE IF NOT EXISTS "%(tablename)s" (' % {'tablename': self.tablename}
    sql += self.form_fields_line_for_createtable()
    sql += ')'
    return sql

  def get_n_fields(self):
    """
    This method depends on the interpolate_create_table_sql() method
    to be implemented in child-inherited classes because from it this method will derive n_fields
    """
    middle_sql = self.form_fields_line_for_createtable()
    middle_sql = middle_sql.lstrip('\n').rstrip(' \n')
    return len(middle_sql.split('\n'))

  def sqlite_createtable_if_not_exists(self):
    sql = self.interpolate_create_table_sql()
    conn = self.get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    # print(sql)
    # print('Created table', tablename)
    cursor.close()
    conn.close()

  @property
  def fieldnames(self):
    """
    This method should be implemented in inherited classes
    """
    return []

  def delete_rows_not_existing_on_dirtree(self, mountpath):
    """
    This method is implemented in inherited classes
    """
    pass

  def delete_all_rows(self):
    sql = 'DELETE FROM %(tablename)s;' % {'tablename': self.tablename}
    conn = self.get_connection()
    cursor = conn.cursor()
    delete_result = cursor.execute(sql)
    n_rows_deleted = delete_result.rowcount  # debug at this point, another option conn.total_changes
    conn.commit()
    cursor.close()
    conn.close()
    return n_rows_deleted

  def delete_ids(self, delete_ids):
    if delete_ids is None or len(delete_ids) == 0:
      return 0
    total_rows_deleted = 0
    sql = 'delete from %(tablename)s where id=?;' % {'tablename': self.tablename}
    conn = self.get_connection()
    cursor = conn.cursor()
    for _id in delete_ids:
      tuplevalues = (_id,)
      delete_result = cursor.execute(sql, tuplevalues)
      n_rows_deleted = delete_result.rowcount  # debug at this point, another option conn.total_changes
      total_rows_deleted += n_rows_deleted
    conn.commit()
    cursor.close()
    conn.close()
    return total_rows_deleted

  def delete_with_sql_n_tuplevalues(self, sql, tuplevalues):
    sql = sql % {'tablename': self.tablename}
    conn = self.get_connection()
    cursor = conn.cursor()
    delete_result = cursor.execute(sql, tuplevalues)
    conn.commit()
    n_rows_deleted = delete_result.rowcount  # debug at this point, another option conn.total_changes
    cursor.close()
    conn.close()
    return n_rows_deleted

  def delete_row_by_id(self, _id):
    if id is None or not isinstance(_id, type(3)):
      return 0
    sql = 'DELETE FROM %(tablename)s WHERE id=?;' % {'tablename': self.tablename}
    tuplevalues = (_id, )
    conn = self.get_connection()
    cursor = conn.cursor()
    delete_result = cursor.execute(sql, tuplevalues)
    n_rows_deleted = delete_result.rowcount  # debug at this point, another option conn.total_changes
    conn.commit()
    cursor.close()
    conn.close()
    return n_rows_deleted

  def delete_row_with_params(self, sql, tuplevalues):
    sql = sql % {'tablename': self.tablename}
    conn = self.get_connection()
    cursor = conn.cursor()
    delete_result = cursor.execute(sql, tuplevalues)
    n_rows_deleted = delete_result.rowcount  # debug at this point, another option conn.total_changes
    conn.commit()
    cursor.close()
    conn.close()
    return n_rows_deleted

  def fetch_row_by_id(self, _id):
    sql = 'select * from %(tablename)s WHERE id=?;' % {'tablename': self.tablename}
    tuplevalues = (_id, )
    conn = self.get_connection()
    cursor = conn.cursor()
    fetch_result = cursor.execute(sql, tuplevalues)
    result_tuple_list = fetch_result.fetchall()
    cursor.close()
    conn.close()
    return result_tuple_list

  def count_rows(self):
    sql = 'select count(*) from %(tablename)s;' % {'tablename': self.tablename}
    conn = self.get_connection()
    cursor = conn.cursor()
    fetch_result = cursor.execute(sql)
    result_tuple_list = fetch_result.fetchall()
    cursor.close()
    conn.close()
    return result_tuple_list

  def count_rows_as_int(self):
    rows = self.count_rows()
    if len(rows) > 0:
      row = rows[0]
      try:
        n_rows = int(row[0])
        return n_rows
      except ValueError:
        pass
    return 0

  def do_select_sql_n_tuplevalues_w_limit_n_offset(self, sql, tuplevalues=None, plimit=None, poffset=None):
    limit = plimit
    if limit is None:
      limit = self.default_limit
    else:
      limit = int(limit)
    offset = poffset
    if offset is None:
      offset = self.default_offset
    else:
      offset = int(offset)
    conn = self.get_connection()
    cursor = conn.cursor()
    sql = sql % {'tablename': self.tablename, 'limit': limit, 'offset': offset}
    while 1:  # up until n_fetched < limit
      if tuplevalues and type(tuplevalues) == tuple:
        fetch_result = cursor.execute(sql, tuplevalues)
      else:
        fetch_result = cursor.execute(sql)
      result_tuple_list = fetch_result.fetchall()
      n_fetched = len(result_tuple_list)
      yield result_tuple_list  # this method fetches chunks of "limit" records each time
      if n_fetched < limit:
        # break out of "infinite" loop
        break
      offset += limit
      sql = sql % {'tablename': self.tablename, 'limit': limit, 'offset': offset}
    cursor.close()
    conn.close()
    return None  # the statement "yield" above returns each chunk of data limit/offset by limit/offset

  def do_select_with_sql_wo_tuplevalues_w_limit_n_offset(self, sql, plimit=None, poffset=None):
    return self.do_select_sql_n_tuplevalues_w_limit_n_offset(sql, None, plimit, poffset)

  def do_select_all_w_limit_n_offset(self, plimit=None, poffset=None):
    """
    This method fetches chunks of "limit" records each time.
    This saves memory and avoids a situation when a large db might take hold of the whole
      and eventually crash the script.

    IMPORTANT: this method cannot be used when record-deletions will occur along the way,
       because the limit/offset will skip ahead the same amount of deleted records,
       those not entering the underlying verifying in code
    """
    limit = plimit
    if limit is None:
      limit = self.default_limit
    else:
      limit = int(limit)
    offset = poffset
    if offset is None:
      offset = self.default_offset
    else:
      offset = int(offset)
    sql = 'SELECT * FROM %(tablename)s LIMIT %(limit)d OFFSET %(offset)d ;' \
          % {'tablename': self.tablename, 'limit': limit, 'offset': offset}
    conn = self.get_connection()
    cursor = conn.cursor()
    while 1:  # up until n_fetched < limit
      fetch_result = cursor.execute(sql)
      result_tuple_list = fetch_result.fetchall()
      n_fetched = len(result_tuple_list)
      yield result_tuple_list  # this method fetches chunks of "limit" records each time
      if n_fetched < limit:
        # break out of "infinite" loop
        break
      offset += limit
      sql = 'select * from %(tablename)s LIMIT %(limit)d OFFSET %(offset)d ;' \
            % {'tablename': self.tablename, 'limit': limit, 'offset': offset}
    cursor.close()
    conn.close()
    return None  # the statement "yield" above returns each chunk of data limit/offset by limit/offset

  def do_select_all(self):
    sql = 'select * from %(tablename)s;' % {'tablename': self.tablename}
    conn = self.get_connection()
    cursor = conn.cursor()
    fetch_result = cursor.execute(sql)
    result_tuple_list = fetch_result.fetchall()
    cursor.close()
    conn.close()
    return result_tuple_list

  def do_select_with_sql_without_tuplevalues(self, sql):
    self.sqlite_createtable_if_not_exists()
    sql = sql % {'tablename': self.tablename}
    conn = self.get_connection()
    cursor = conn.cursor()
    fetch_result = cursor.execute(sql)
    result_tuple_list = fetch_result.fetchall()
    cursor.close()
    conn.close()
    return result_tuple_list

  def do_select_with_sql_n_tuplevalues(self, sql, tuplevalues):
    self.sqlite_createtable_if_not_exists()
    sql = sql % {'tablename': self.tablename}
    conn = self.get_connection()
    cursor = conn.cursor()
    fetch_result = cursor.execute(sql, tuplevalues)
    result_tuple_list = fetch_result.fetchall()
    cursor.close()
    conn.close()
    return result_tuple_list

  def fetch_rowlist_by_id(self, _id):
    sql = 'select * from %(tablename)s where id=?;' % {'tablename': self.tablename}
    tuplevalues = (_id,)
    return self.do_select_with_sql_n_tuplevalues(sql, tuplevalues)

  def fetch_node_by_name_n_parentpath(self, name, parentpath):
    sql = 'SELECT * FROM %(tablename)s WHERE name=? AND parentpath=?;'
    tuplevalues = (name, parentpath)
    return self.do_select_with_sql_n_tuplevalues(sql, tuplevalues)

  def fetch_all(self):
    sql = 'select * from %(tablename)s;' % {'tablename': self.tablename}
    return self.do_select_with_sql_without_tuplevalues(sql)

  def do_update_with_all_fields_with_tuplevalues_if_needed(self, found_row, tuplevalues):
    if len(found_row) > 0 and len(found_row) == len(tuplevalues):
      for i in range(1, len(found_row)):
        if found_row[i] != tuplevalues[i]:
          return self.do_update_with_all_fields_with_tuplevalues(tuplevalues)
    return False  # ie record was not updated for contents are the same

  def do_insert_or_update_with_tuplevalues(self, tuplevalues):
    """
    returns a boolean ie True if inserted/updated False otherwise ie no inserts or updates happened
    """
    idx = self.fieldnames.index('name')
    name = tuplevalues[idx]
    idx = self.fieldnames.index('parentpath')
    parentpath = tuplevalues[idx]
    rowlist = self.fetch_node_by_name_n_parentpath(name, parentpath)
    if len(rowlist) == 0:
      return self.do_insert_with_all_fields_with_tuplevalues(tuplevalues)
    elif len(rowlist) == 1:
      found_row = rowlist[0]
      return self.do_update_with_all_fields_with_tuplevalues_if_needed(found_row, tuplevalues)
    # hkey is UNIQUE in db, ie it could not have more than one row in db
    error_msg = 'Inconsistency Error: the entry (name, parentpath) (%s, %s) has more than one record in db.' \
                % (name, parentpath)
    raise ValueError(error_msg)

  def do_insert_with_all_fields_with_tuplevalues(self, tuplevalues):
    question_marks = '?, ' * len(tuplevalues)
    question_marks = question_marks.rstrip(', ')
    sql_before_interpol = 'insert into %(tablename)s VALUES (' + question_marks + ')'
    return self.do_insert_with_sql_n_tuplevalues(sql_before_interpol, tuplevalues)

  def form_update_with_all_fields_sql(self):
    sql_before_interpol = '''To be implemented in child class
     %(tablename)s ''' % {'tablename': self.tablename}
    return sql_before_interpol

  def do_update_with_all_fields_with_tuplevalues(self, tuplevalues):
    # n_fields = self.get_n_fields()
    n_fields = len(self.fieldnames)
    if len(tuplevalues) != n_fields:
      error_msg = 'len(tuplevalues)=%d != len(fieldnames)=%d => ' % (len(tuplevalues), n_fields)
      error_msg += str(tuplevalues)
      raise ValueError(error_msg)
    # change the order of _id because it's going to the WHERE clause at the end
    _id = tuplevalues[0]
    tuplevalues = tuplevalues[1:] + (_id, )
    sql_before_interpol = self.form_update_with_all_fields_sql()
    return self.do_update_with_sql_n_tuplevalues(sql_before_interpol, tuplevalues)

  def do_update_with_sql_n_tuplevalues(self, sql, tuplevalues):
    conn = self.get_connection()
    cursor = conn.cursor()
    sql = sql % {'tablename': self.tablename}
    print('do_update =>', tuplevalues)
    try:
      _ = cursor.execute(sql, tuplevalues)
      was_updated = True
    except sqlite3.IntegrityError:
      was_updated = False
    cursor.close()
    conn.commit()
    conn.close()
    return was_updated

  def do_insert_with_sql_n_tuplevalues(self, sql, tuplevalues):
    conn = self.get_connection()
    cursor = conn.cursor()
    sql = sql % {'tablename': self.tablename}
    try:
      _ = cursor.execute(sql, tuplevalues)
      was_inserted = True
    except sqlite3.IntegrityError:
      was_inserted = False
    cursor.close()
    conn.commit()
    conn.close()
    return was_inserted


def process():
  """
  adhoc_select()
  adhoc_delete_all_rows()
  adhoc_select_all()
  bool_res = adhoc_insert_some()
  print('was_inserted', bool_res)
  """
  pass


if __name__ == '__main__':
  process()
