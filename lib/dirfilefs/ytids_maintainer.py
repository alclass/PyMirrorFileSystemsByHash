#!/usr/bin/env python3
"""
ytids_maintainer.py

Choices made for class YtidsTxtNSqliteMaintainer:
---------
1) either basedir (or rootpath) may be given or not and sqlite & txt files are looked up by path-descending
2) in either of the two cases above, an exception will be raised if
   2.1) the rootpath does not exist
   2.2) the the sqlite file does not exist
   2.3) the txt "may" not exist, case in which an exception is not raised

Obs:
1) the rootpath (or basedir) is common both to the sqlite file and the txt file
   (ie the two should be together in the same folder)
   rootpath either given to __init__() or discoverable by descending directories
   descending hypothesis:
   1.1) if an initial directory is given, descending happens starting down from it
   1.2) if not, descending happens starting down from "current dir"
2) the filename for the Sqlite file is pre-determined by "local_settings" ie it's parameterized
3) the filename for the txt file is either "free" (ie any one given)
   or pre-determined by a prefix given in "local_settings"
   in which case it's looked up by os.listdir() seeking a file matching the prefix
"""
import os
import default_settings as ds
import lib.dirfilefs.ytids_functions as ytfs
import sqlite3


def subtract_list_from_other_via_set_diff(lista, elems_to_remove):
  """
  This method removes the elements in elems_to_remove from lista if they exist in the latter
  Example:
    s1= set(['a','b','c'])
    s2= set(['a','b','d'])
    s1 = s1 - s2  # or s1.difference(s2)
  the result is s1 = set(['c']) ie elements 'a' and 'b', existing in s2, are removed from s1
  the return output will be converted "back" to type 'list'
    obs: the inputs may be anything convertable to type 'set', even sets themselves)

  Concerning memory size or slowness for this operation:
    worry 1) the sqlite-dbs may have more than 100k ytids
    worry 1b) this may turn this operation slow or memory-crashable
    comment 1) the inputs here are expected to be sized less than 1k ytids,
      for youtube-id.txt are usually 'playlist sizeable' (ie have average sizes of playlists)
    conclusion 1) the inputs for this function will be sized as 'playlists' (1k records)
      not as sqlite-dbs (100k records)
    * in case growth is in need, this function may be refactored to partition the ytids list into smaller chunks
       at least avoiding a memory-crash;
       to avoiding slowness a multithreaded approach might be looked up
  """
  s1 = set(lista)
  s2 = set(elems_to_remove)
  remains_in_s1 = s1 - s2
  lista = list(remains_in_s1)
  return lista


def retrieveremove_ytids_existing_in_extra_sqlitefilepath(ytids, sqlitefilepath):
  if ytids is None or len(ytids) == 0:
    # notice that if ytids comes in as None it will return as []
    return []
  if sqlitefilepath is None or not os.path.isfile(sqlitefilepath):
    scr_msg = 'Sqlitefile [%s] for an extra ytid look-up not given or does not exist.' % sqlitefilepath
    print(scr_msg)
    return ytids
  ytids_found = []
  n_ytids = len(ytids)
  print('n_ytids =', n_ytids, 'Looking up sqlitefile', sqlitefilepath)
  if n_ytids == 0:
    return ytids
  questionmarks_sqlinstr = '?,' * n_ytids
  questionmarks_sqlinstr = questionmarks_sqlinstr.rstrip(',')
  questionmarks_list = ['?'] * n_ytids
  sql = 'SELECT ytid FROM ytids WHERE ytid IN (' + questionmarks_sqlinstr + ');'
  # print(sql)
  tuplevalues = tuple(questionmarks_list)
  conn = sqlite3.connect(sqlitefilepath)
  cursor = conn.cursor()
  dbret = cursor.execute(sql, tuplevalues)
  rows = dbret.fetchall()
  for row in rows:
    ytid = row[0]
    ytids_found.append(ytid)
  cursor.close()
  conn.close()
  if len(ytids_found) > 0:
    ytids = subtract_list_from_other_via_set_diff(ytids, ytids_found)
  return ytids


def retrieveremove_ytids_existing_in_extra_sqlitefilepaths(ytids, sqlitefilenames=(), sqlitefolderpath=None):
  if ytids is None or len(ytids) == 0:
    # notice that if ytids comes in as None it will return as []
    # the default empty-tuple for the input-parameter is because the IDE complains mutation value with [] (empty-list)
    return []
  if sqlitefilenames is None or len(sqlitefilenames) == 0:
    print('There are no sqlitefilenames for extra ytid look-ups.')
    return ytids
  if sqlitefolderpath is None or not os.path.isdir(sqlitefolderpath):
    print('Sqlite folderpath for extra ytid look-ups not given or does not exist.')
    print(' =>', sqlitefolderpath)
    return ytids
  sqlitefilepaths = list(map(lambda fn: os.path.join(sqlitefolderpath, fn), sqlitefilenames))
  for sqlitefilepath in sqlitefilepaths:
    ytids = retrieveremove_ytids_existing_in_extra_sqlitefilepath(ytids, sqlitefilepath)
    if len(ytids) == 0:
      return []
  return ytids


def retrieveremove_ytids_existing_in_extra_sqlitedirpath(ytids, sqlitefolderpath=None):
  if ytids is None or len(ytids) == 0:
    # notice that if ytids comes in as None it will return as []
    return []
  if sqlitefolderpath is None or not os.path.isdir(sqlitefolderpath):
    print('Sqlite folderpath for extra ytid look-ups not given or does not exist.')
    print(' =>', sqlitefolderpath)
    return ytids
  folderfilenames = os.listdir(sqlitefolderpath)
  sqlitefilenames = list(filter(lambda fn: fn.endswith('.sqlite'), folderfilenames))
  if len(sqlitefilenames) == 0:
    print('There are no sqlite files available for an extra ytid look-up inside the sqlite folderpath.')
    print('sqlitefolderpath =>', sqlitefolderpath)
    return ytids
  return retrieveremove_ytids_existing_in_extra_sqlitefilepaths(ytids, sqlitefilenames, sqlitefolderpath)


class YtidsSqliteMaintainer:

  def __init__(
        self,
        bool_find_root_by_pathdesc=True,
        rootdirpath=None,
        txtfilename_if_known=None,
        extrarepo_ytids_sqlitedirpath=None
    ):
    self.sql_ytids = []
    self.sqlite_filename = ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME
    self.prefix_txt_filename = ds.DEFAULT_YTIDS_FILENAME_PREFIX
    self.txt_filename = txtfilename_if_known  # if None, it's still discoverable by its prefix above
    self._sqlite_filepath = None  # for class property sqlite_filepath
    self._ytids_txtfilepath = None  # for class property ytids_txtfilepath
    self.rootdirpath = rootdirpath
    self.extrarepo_ytids_sqlitedirpath = extrarepo_ytids_sqlitedirpath
    self.bool_find_root_by_pathdesc = bool_find_root_by_pathdesc
    if self.bool_find_root_by_pathdesc:
      self.find_root_by_pathdesc()
    else:
      self.verify_sqlitefile_on_workdir_or_raise()

  @property
  def sqlite_filepath(self):
    if self._sqlite_filepath is None:
      self._sqlite_filepath = os.path.join(self.rootdirpath, self.sqlite_filename)
      if not os.path.isfile(self._sqlite_filepath):
        error_msg = 'Error: sqlite file (%s) is missing.' % self._sqlite_filepath
        raise OSError(error_msg)
    return self._sqlite_filepath

  @property
  def ytids_txtfilepath(self):
    if self.txt_filename is None:
      self.discover_txtfilepath_if_any()
      if self.txt_filename is None:
        return None
    if self._ytids_txtfilepath is None:
      self._ytids_txtfilepath = os.path.join(self.rootdirpath, self.txt_filename)
      if not os.path.isfile(self._ytids_txtfilepath):
        error_msg = 'Error: sqlite file (%s) is missing.' % self._ytids_txtfilepath
        raise OSError(error_msg)
    return self._ytids_txtfilepath

  def find_sqlitebasedir_by_pathdesc(self):
    if self.rootdirpath is None:
      self.rootdirpath = os.path.abspath('.')
    fpath = ytfs.get_recurs_descending_sqlfilepath_by_filename(
      self.rootdirpath,
      self.sqlite_filename
     )
    if fpath is None:
      error_msg = 'Error: filepath_for_ytids_sqlite was not discoverable:' \
                  'Sqlite is probably missing descendiing from %s' \
                  % self.rootdirpath
      raise OSError(error_msg)
    self.rootdirpath, _ = os.path.split(fpath)

  def discover_txtfilepath_if_any(self):
    if self.rootdirpath is None or not os.path.isdir(self.rootdirpath):
      error_msg = 'Error: rootdirpath (%s) is either None or missing:' \
                  % str(self.rootdirpath)
      raise OSError(error_msg)
    entries = os.listdir(self.rootdirpath)
    for e in entries:
      if e.startswith(self.prefix_txt_filename):
        self.txt_filename = e
        break
    return self.txt_filename  # may return None

  def find_root_by_pathdesc(self):
    self.find_sqlitebasedir_by_pathdesc()
    self.discover_txtfilepath_if_any()

  def verify_sqlitefile_on_workdir_or_raise(self):
    """
    This method is run only (from an if in __init__) if path-descending is not chosen
      ie attribute bool_find_root_by_pathdesc is False
    """
    if not os.path.isfile(self.sqlite_filepath):
      error_msg = 'Error: folderpath (%s) for the sqlite file does not exist:' \
        % self.rootdirpath
      raise OSError(error_msg)

  def extract_missing_sqlite_ytids_from(self, o_ytids):
    """
    TO-DO: refactor this method to ytfs (yt-functions).
           There is a similar method in dlYouTubeWithIdsOnTxtFile3.py
           that uses list-comprehension instead of for-loop
    """
    missing_ytids = []
    for ytid in o_ytids:
      if ytid not in self.get_sql_ytids():
        missing_ytids.append(ytid)
    return missing_ytids

  def extract_ytids_existing_in_extrarepo(self, ytids):
    print('Looking up possible extrarepos in', self.extrarepo_ytids_sqlitedirpath)
    if self.extrarepo_ytids_sqlitedirpath is None:
      return ytids
    return retrieveremove_ytids_existing_in_extra_sqlitedirpath(ytids, self.extrarepo_ytids_sqlitedirpath)

  def find_diffset_from(self, other_ytids_txtfilepath):
    other_ytids = ytfs.read_ytids_from_filenamebased_textfile(other_ytids_txtfilepath)
    other_ytids = ytfs.get_diffset_from_lists(other_ytids, self.sql_ytids)
    return other_ytids

  def get_sql_ytids(self):
    """
    TO-DO: there is a similar version of this method in dlYouTubeWithIdsOnTxtFile2.py
           that uses a list-comprehension instead of a for-loop
    """
    if len(self.sql_ytids) > 0:
      return self.sql_ytids
    conn = sqlite3.connect(self.sqlite_filepath)
    sql = 'SELECT ytid FROM ytids;'
    cursor = conn.cursor()
    try:
      dbret = cursor.execute(sql)
      self.sql_ytids = []
      for row in dbret.fetchall():
        ytid = row[0]
        self.sql_ytids.append(ytid)
    except sqlite3.OperationalError as e:
      if 'no such table' in str(e):
        ytfs.create_table_if_not_exists_ytids(conn)
    conn.close()
    return self.sql_ytids

  def ytids_bulk_dbselect(self, ytids):
    outlist = []
    conn = sqlite3.connect(self.sqlite_filepath)
    questionmarks = '?,' * len(ytids)
    questionmarks = questionmarks.rstrip(',')
    sql = 'SELECT ytid FROM ytids WHERE ytid IN (' + questionmarks + ');'
    tuplevalues = tuple(ytids)
    cursor = conn.cursor()
    dbret = cursor.execute(sql, tuplevalues)
    rows = dbret.fetchall()
    for row in rows:
      f_ytid = row[0]
      if f_ytid not in ytids:
        outlist.append(f_ytid)
    return outlist

  @property
  def n_sql_ytids(self):
    if len(self.sql_ytids) == 0:
      _ = self.get_sql_ytids()
    return len(self.sql_ytids)

  def __str__(self):
    outline = """<YtidsTxtNSqliteMaintainer>
    Sqlite file path = {sqlite_abspath}
    N of sql_ytids from sql = {n_sql_ytids}
    Txt file Path = {txtfilepath}
    N of sql_ytids from txt = {n_txt_ytids}
""".format(
      sqlite_abspath=self.sqlite_filepath,
      n_sql_ytids=self.n_sql_ytids,
      txtfilepath=self.ytids_txtfilepath,
      n_txt_ytids=len(self.sql_ytids)
    )
    return outline


def adhoctest():
  """
  ytids_folderpath = ds.Paths.get_default_src_datafolder_abspath()

  """
  ytids_basedirpath = '/home/dados/VideoAudio/Yt videos/yt BRA Pol vi/Meteoro tmp yu'
  ytids_filename = 'z_ls-R_contents-name1234.txt'
  print('hi')
  # ytids_folderpath, ytids_filename
  ytid_o = YtidsSqliteMaintainer(False, ytids_basedirpath, ytids_filename)
  # ytid_o.read_ytids()
  print(ytid_o)


def process():
  # insert_difference_in_rootcontentfile()
  adhoctest()


if __name__ == '__main__':
  process()
