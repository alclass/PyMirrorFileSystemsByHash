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
import fs.dirfilefs.ytids_functions as ytfs
import sqlite3


class YtidsSqliteMaintainer:

  def __init__(
        self,
        bool_find_root_by_pathdesc=True,
        rootdirpath=None,
        txtfilename_if_known=None
    ):
    self.sql_ytids = []
    self.sqlite_filename = ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME
    self.prefix_txt_filename = ds.DEFAULT_YTIDS_FILENAME_PREFIX
    self.txt_filename = txtfilename_if_known  # if None, it's still discoverable by its prefix above
    self._sqlite_abspath = None  # for class property sqlite_abspath
    self._txtfile_abspath = None  # for class property txtfile_abspath
    self.rootdirpath = rootdirpath
    self.bool_find_root_by_pathdesc = bool_find_root_by_pathdesc
    if self.bool_find_root_by_pathdesc:
      self.find_root_by_pathdesc()
    else:
      self.verify_rootpath_or_raise()

  @property
  def sqlite_abspath(self):
    if self._sqlite_abspath is None:
      self._sqlite_abspath = os.path.join(self.rootdirpath, self.sqlite_filename)
      if not os.path.isfile(self._sqlite_abspath):
        error_msg = 'Error: sqlite file (%s) is missing.' % self._sqlite_abspath
        raise OSError(error_msg)
    return self._sqlite_abspath

  @property
  def txtfile_abspath(self):
    if self.txt_filename is None:
      self.discover_txtfilepath_if_any()
      if self.txt_filename is None:
        return None
    if self._txtfile_abspath is None:
      self._txtfile_abspath = os.path.join(self.rootdirpath, self.txt_filename)
      if not os.path.isfile(self._txtfile_abspath):
        error_msg = 'Error: sqlite file (%s) is missing.' % self._txtfile_abspath
        raise OSError(error_msg)
    return self._txtfile_abspath

  def find_root_by_pathdesc(self):
    self.find_sqlitebasedir_by_pathdesc()
    self.discover_txtfilepath_if_any()

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

  def verify_rootpath_or_raise(self):
    if not os.path.isdir(self.rootdirpath):
      error_msg = 'Error: folderpath for the sqlite file does not exist:' \
        % self.rootdirpath
      raise OSError(error_msg)

  def find_diffset_from(self, other_ytids_txtfilepath):
    other_ytids = ytfs.read_ytids_from_filenamebased_textfile(other_ytids_txtfilepath)
    other_ytids = ytfs.get_diffset_from_lists(other_ytids, self.sql_ytids)
    return other_ytids

  def get_sql_ytids(self):
    if len(self.sql_ytids) > 0:
      return self.sql_ytids
    conn = sqlite3.connect(self.sqlite_abspath)
    sql = 'select ytid from ytids;'
    cursor = conn.cursor()
    try:
      dbret = cursor.execute(sql)
      self.sql_ytids = []
      for row in dbret.fetchall():
        ytid = row[0]
        self.sql_ytids.append(ytid)
    except sqlite3.OperationalError as e:
      if 'no such table' in str(e):
        ytfs.create_table_ytids(conn)
    conn.close()
    return self.sql_ytids

  def insert_ytids(self, ytids):
    outlist = []
    conn = sqlite3.connect(self.sqlite_abspath)
    questionmarks = '?,' * len(ytids)
    questionmarks = questionmarks.rstrip(',')
    sql = 'select ytid from ytids where ytid in (' + questionmarks + ');'
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
      sqlite_abspath=self.sqlite_abspath,
      n_sql_ytids=self.n_sql_ytids,
      txtfilepath=self.txtfile_abspath,
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
