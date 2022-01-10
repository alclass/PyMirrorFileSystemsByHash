#!/usr/bin/env python3
"""
This script reads db-table files_in_tree (or its current name) and looks for sha1 repeats.
If there are repeats, it will record them into db-table file_repeats (or its current name).

IMPORTANT: the solution above was discontinued. It now opens caja-windows with the files that are repeated.

Other scripts will take on from here and do things such as:
- clean up repeats within the same dirtree (excepting the empty file as it still has some usages) (*);
- mirror files between different dirtrees ressembling the back-up operation.

TO-DO: one thing that may be implemented in the future is a window-manager GUI integrated with this system,
       so that copying, moving and deleting may be done inside this planned GUI.
"""
import datetime
import os.path
import fs.db.dbdirtree_mod as dbt
import fs.db.dbrepeats_mod as dbr
import default_settings as defaults
import models.entries.dirnode_mod as dn
import fs.hashfunctions.hash_mod as hm


class RepeatVerifier:

  sha1_repeat_dict = {}

  def __init__(self, mountpath, last_index=0, sql_select_limit=None):
    self.last_index = last_index
    self.sql_select_limit = sql_select_limit
    if sql_select_limit is None:
      self.sql_select_limit = defaults.SQL_SELECT_LIMIT_DEFAULT
    self.offset = 0
    self.chunk_rounds = 0
    self.n_item = 0
    self.db_fetch_ended = False  # this boolean stops the main while-loop in process()
    self.n_sha1_repeats = 0
    self.total_files_in_db = 0
    self.total_sha1s_in_db = 0
    self.n_rows_deleted = 0
    self.mountpath = mountpath
    self.dbtree = dbt.DBDirTree(self.mountpath)
    self.dbrepeat = dbr.DBRepeat(self.mountpath)
    self.count_stats()

  def count_stats(self):
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    self.total_sha1s_in_db = self.dbtree.count_unique_sha1s_as_int()

  @property
  def total_repeats_in_db(self):
    return self.total_files_in_db - self.total_sha1s_in_db

  def verify_sha1_repeats(self, sha1):
    """
    The method intends to insert/update table dbrepeats
      Obs:
        1) for the time being, table dbrepeats is discontinued
        2) TO-DO refactor here when a decision to treat the repeats on-the-fly is taken
    """
    if sha1 in self.sha1_repeat_dict:
      return
    sql = 'SELECT * FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (sha1, )
    rowlist = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(rowlist) > 1:
      self.sha1_repeat_dict[sha1] = len(rowlist)
      self.n_sha1_repeats += 1
      print('round', self.chunk_rounds, 'n of repeat =', len(rowlist)-1)
      for rowtuple in rowlist:
        _id = rowtuple[0]
        tuplevalues = (_id, sha1, 0)
        bool_ins_upd = 1
        # for the time being, table dbrepeats is discontinued
        # bool_ins_upd = self.dbrepeat.do_insert_or_update_with_tuplevalues(tuplevalues)
        print('bool_ins_upd', bool_ins_upd, tuplevalues)
    else:
      print(' ********  NO REPEATS ******** acumulated repeats :', self.n_sha1_repeats)

  def fetch_rows_with_offset(self):
    self.chunk_rounds += 1
    sql_limit_n_offset_paramdict = {'limit': self.sql_select_limit, 'offset': self.offset}
    sql_limit_n_offset = 'LIMIT %(limit)d OFFSET %(offset)d;' % sql_limit_n_offset_paramdict
    sql = 'SELECT * from %(tablename)s ORDER by parentpath ' + sql_limit_n_offset
    rowlist = self.dbtree.do_select_with_sql_without_tuplevalues(sql)
    print('len', len(rowlist), 'sql', sql)
    self.offset += self.sql_select_limit
    return rowlist

  def verify_sha1s_in_db(self):
    """
    1 id 2 hkey 3 name 4 parentpath 5 is_file 6 sha1 7 bytesize 8 mdatetime

    """
    rowlist = self.fetch_rows_with_offset()
    if len(rowlist) < self.sql_select_limit:
      self.db_fetch_ended = True
    for rowtuplevalues in rowlist:
      name = rowtuplevalues[2]
      parentpath = rowtuplevalues[3]
      sha1 = rowtuplevalues[5]
      sha1hex = sha1.hex()
      self.n_item += 1
      print(self.n_item, ':: name', name, 'parentpath', parentpath)
      print(sha1hex)
      self.verify_sha1_repeats(sha1)

  def process(self):
    n_rows = self.dbtree.count_rows()
    print('n_rows', n_rows)
    while not self.db_fetch_ended:  # db_fetch_ended will become True when len(rowlist) < self.sql_select_limit
      self.verify_sha1s_in_db()

  def keep_one_record_delete_all_others(self, id_to_stay, dbfilerecords, dbtree):
    for dbfilerecord in dbfilerecords:
      _id = dbfilerecord[0]
      if _id == id_to_stay:
        continue
      dbtree.delete_row_by_id(_id)
      self.n_rows_deleted += 1
      if dbtree.mountpath and os.path.isdir(dbtree.mountpath):
        os_equiv_filepath = dn.DirNode.recompose_abspath_w_mountpath(dbfilerecord, dbtree)
        if os.path.isfile(os_equiv_filepath):
          os.remove(os_equiv_filepath)


class RepeatsGrabber:

  def __init__(self, mountpath):
    self.mountpath = mountpath
    self.dbtree = dbt.DBDirTree(self.mountpath)
    self.total_files_in_db = 0
    self.total_sha1s_in_db = 0
    self.n_unique_files_in_db = self.dbtree.count_unique_sha1s_as_int()
    self.n_repeated_files = self.total_files_in_db - self.n_unique_files_in_db
    self.n_windows_open = 0
    self.n_processed_repeated_files = 0
    self.count_stats()

  def count_stats(self):
    self.total_files_in_db = self.dbtree.count_rows_as_int()
    self.total_sha1s_in_db = self.dbtree.count_unique_sha1s_as_int()

  @property
  def total_repeats_in_db(self):
    return self.total_files_in_db - self.total_sha1s_in_db

  def open_window_explorer_for_user(self, n_repeats, sha1):
    print('Opening Window for', sha1.hex(), 'with', n_repeats, 'repeats')
    sql = 'SELECT * FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (sha1, )
    rows = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    print('Number of rows found', len(rows))
    for n_row, row in enumerate(rows):
      self.n_processed_repeated_files += 1
      idx = self.dbtree.fieldnames.index('name')
      name = row[idx]
      _, ext = os.path.splitext(name)
      # if ext in defaults.EXTENSIONS_IN_SHA1_VERIFICATION:
      self.go_open_windows_for_repeats(n_repeats, row, n_row)

  def go_open_windows_for_repeats(self, n_repeats, row, n_row):
    idx = self.dbtree.fieldnames.index('name')
    name = row[idx]
    idx = self.dbtree.fieldnames.index('parentpath')
    parentpath = row[idx]
    _, ext = os.path.splitext(name)
    print(
      'Extension', ext,
      self.n_processed_repeated_files, 'of', self.total_files_in_db,
      n_row, 'repeats =', n_repeats, '[', name, '] =>', parentpath
    )
    print('-'*40)
    middlepath = parentpath.lstrip('/')
    folderpath = os.path.join(self.mountpath, middlepath)
    filepath = os.path.join(folderpath, name)
    if os.path.isdir(folderpath) and os.path.isfile(filepath):
      comm = 'caja "%s"' % folderpath
      os.system(comm)
      self.n_windows_open += 1
      msg = 'Type anything to continue for the next windows open:'
      screen_msg = str(self.n_windows_open) + ' ' + msg
      _ = input(screen_msg)

  def fetch_distinct_sha1s(self):
    sql = 'SELECT DISTINCT sha1, COUNT(id) as c FROM %(tablename)s GROUP by sha1 having c > 1;'
    generator_rows = self.dbtree.do_select_sql_n_tuplevalues_w_limit_n_offset(sql)
    for generated_rows in generator_rows:
      for i, row in enumerate(generated_rows):
        sha1 = row[0]
        n_repeats = row[1]
        print(
          i+1, '/', self.total_repeats_in_db,
          'count', n_repeats, sha1.hex()
        )
        if sha1 == hm.EMPTY_SHA1_AS_BIN:
          print('Jumping EMPTY_SHA1 ::', hm.EMPTY_SHA1HEX_STR)
          continue
        self.open_window_explorer_for_user(n_repeats, sha1)

  def as_dict(self):
    outdict = {
      'mountpath': self.mountpath,
      'n_files_in_db': self.total_files_in_db,
      'n_unique_files_in_db': self.n_unique_files_in_db,
      'total_files_in_db': self.total_files_in_db,
      'total_sha1s_in_db': self.total_sha1s_in_db,
      'total_repeats_in_db': self.total_repeats_in_db,
      'n_processed_repeated_files': self.n_processed_repeated_files,
    }
    return outdict

  def report(self):
    outstr = '''    RepeatsGrabber's end of processing reporting:
    =========================
    mountpath = %(mountpath)s
    n_files_in_db = %(n_files_in_db)d
    n_unique_files_in_db = %(n_unique_files_in_db)d
    total_files_in_db = %(total_files_in_db)d
    total_sha1s_in_db = %(total_sha1s_in_db)d
    total_repeats_in_db = %(total_repeats_in_db)d
    n_processed_repeated_files : %(n_processed_repeated_files)d''' \
      % self.as_dict()
    print(outstr)

  def process(self):
    self.fetch_distinct_sha1s()
    self.report()


def process2():
  start_time = datetime.datetime.now()
  print('start_time', start_time)
  mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  verifier = RepeatVerifier(mountpath)
  verifier.process()
  finish_time = datetime.datetime.now()
  print('finish_time', finish_time)
  elapsed_time = finish_time - start_time
  print('elapsed_time', elapsed_time)


def process():
  start_time = datetime.datetime.now()
  print('start_time', start_time)
  mountpath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  grabber = RepeatsGrabber(mountpath)
  grabber.process()
  finish_time = datetime.datetime.now()
  print('finish_time', finish_time)
  elapsed_time = finish_time - start_time
  print('elapsed_time', elapsed_time)


if __name__ == '__main__':
  process()
