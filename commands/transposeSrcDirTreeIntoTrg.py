#!/usr/bin/env python3
"""
transposeSrcDirTreeIntoTrg.py
  copies over the contents of a target sqlite dirtree into a source sqlite dirtree

Usage:
  $transposeSrcDirTreeIntoTrg.py <src_dirtree> <trg_dirtree>

Explanation:
  Suppose an external HD mounted on "/media/user/Sci-videos" with a sqlite dirtree in its root folder (the src)
  Suppose there's a transient folder called "Transient" such as
    "/media/user/Sci-videos/Transient"
  Now the trg-sqlite-dirtree in Transient is to be copied over into the root one at "/media/user/Sci-videos"

  For this operation, each entry in the Transient "db", when copied,
    should be prepended by the "Transient" folder itself.
  Example:
     Suppose an entry in Transient such as => '/dirT/fileABC.txt'
     When copied over it should become => '/Transient/dirT/fileABC.txt' (the src entry prepended by '/Transient' in trg)

Example Usage:
  transposeSrcDirTreeIntoTrg.py "/media/user/Sci-videos/Transient" "/media/user/Sci-videos"
"""
import sys
import fs.db.dbdirtree_mod as dbdt
import default_settings as defaults
# import models.entries.dirnode_mod as dn
# import fs.strnlistfs.listfunctions_mod as listf


class Transposer:

  def __init__(self, src_mountpath, trg_mountpath):
    self.src_dbtree = dbdt.DBDirTree(src_mountpath)
    self.trg_dbtree = dbdt.DBDirTree(trg_mountpath)
    self.n_processed_files_in_trg = 0
    self.n_of_transposed_entries = 0
    self._src_total_files_in_db = None
    self._trg_total_files_in_db = None
    self._prepended_path = None

  @property
  def src_mountpath(self):
    return self.src_dbtree.mountpath

  @property
  def trg_mountpath(self):
    return self.trg_dbtree.mountpath

  @property
  def src_total_files_in_db(self):
    if self._src_total_files_in_db is None:
      self._src_total_files_in_db = self.src_dbtree.total_files()
      if self._src_total_files_in_db is None:
        error_msg = 'Error _src_total_files_in_db was not discoverable in db'
        raise ValueError(error_msg)
    return self._src_total_files_in_db

  @property
  def trg_total_files_in_db(self):
    if self._trg_total_files_in_db is None:
      self._trg_total_files_in_db = self.trg_dbtree.total_files()
      if self._trg_total_files_in_db is None:
        error_msg = 'Error _trg_total_files_in_db was not discoverable in db'
        raise ValueError(error_msg)
    return self._trg_total_files_in_db

  def init_prepended_path(self):
    """
    calculates the "prepend path"
    Example:
      Suppose source mountpath is "/media/user/Sci-videos"
      Suppose target mountpath is "/media/user/Sci-videos/Transient"
      In the example above the "prepend path" is "/Transient"
    """
    self._prepended_path = None
    if self.src_mountpath not in self.trg_mountpath:
      error_msg = 'Error: Target path is not within source path'
      raise OSError(error_msg)
    self._prepended_path = self.trg_mountpath[len(self.src_mountpath):]
    print('_prepended_path', self._prepended_path)

  @property
  def prepended_path(self):
    if self._prepended_path is None:
      self.init_prepended_path()
      if self._prepended_path is None:
        error_msg = 'Error: _prepended_path is None\n'
        error_msg += 'src = ' + self.src_mountpath
        error_msg += 'trg = ' + self.trg_mountpath
        raise OSError(error_msg)
    return self._prepended_path

  def prepend_path(self, path):
    if not path.startswith('/'):
      path = '/' + path
    transposed_path = self.prepended_path + path
    if not transposed_path.startswith('/'):
      transposed_path = '/' + transposed_path
    return transposed_path

  def loop_thru_trg_entries(self):
    sql = 'SELECT * from %(tablename)s ORDER BY parentpath;'
    trg_rowlist = self.trg_dbtree.do_select_with_sql_without_tuplevalues(sql)
    self.n_processed_files_in_trg = 0
    for i, trg_row in enumerate(trg_rowlist):
      trg_dirnode_to_src = self.src_dbtree.transform_row_to_dirnode(trg_row)
      prepended_parentpath = self.prepend_path(trg_dirnode_to_src.parentpath)
      self.n_processed_files_in_trg += 1
      seq = self.n_processed_files_in_trg
      trg_dirnode_to_src.parentpath = prepended_parentpath
      print(seq, 'DB-insert new parentpath', trg_dirnode_to_src.parentpath)
      retobj = trg_dirnode_to_src.insert_into_db(self.src_dbtree)
      if retobj:
        self.n_of_transposed_entries += 1
      pass

  def transpose(self):
    self.loop_thru_trg_entries()
    self.report()

  def report(self):
    print('Report:')
    print('=======')
    print('src dirtree:', self.src_dbtree.mountpath)
    print('trg dirtree:', self.trg_dbtree.mountpath)
    print('src total_files_in_db', self.src_total_files_in_db)
    print('trg total_files_in_db', self.trg_total_files_in_db)
    print('n_of_transposed_entries', self.n_of_transposed_entries)
    print('n_processed_dirs in trg', self.n_processed_files_in_trg)


def show_help_cli_msg_if_asked():
  for arg in sys.argv:
    if arg in ['-h', '--help']:
      print(__doc__)
      sys.exit(0)


def adhoc_test1():
  src_mountpath = '/home/dados/VideoAudio/Yt videos/yt BRA EUR etc vi/BRA Pol vi/Plantão Sams vi/rootA'
  trg_mountpath = '/home/dados/VideoAudio/Yt videos/yt BRA EUR etc vi/BRA Pol vi/Plantão Sams vi/rootA/rootB'
  transposer = Transposer(src_mountpath, trg_mountpath)
  transposer.transpose()


def process():
  show_help_cli_msg_if_asked()
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  transposer = Transposer(src_mountpath, trg_mountpath)
  transposer.transpose()


if __name__ == '__main__':
  process()
