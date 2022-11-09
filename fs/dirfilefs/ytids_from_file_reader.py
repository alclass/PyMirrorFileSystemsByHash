#!/usr/bin/env python3
"""
maintainSqliteRootFolderYtidsRepo.py

strfs.is_str_a_64enc()
strfs.is_str_an_11hchar__64enc()

  def read_ytids_from_ytidsonly_textfile(self):
    self.ytids = []
    self.ytids = read_ytids_from_ytidsonly_textfile(self.ytidsonly_filepath)
    return self.ytids

"""
import os
import fs.strnlistfs.strfunctions_mod as strfs
import default_settings as ds


def get_diffset_from_lists(list1, list2):
  outlist = []
  for e in list1:
    if e not in list2:
      outlist.append(e)
  return outlist


def read_ytids_from_ytidsonly_textfile(filepath):
  ytids = []
  with open(filepath) as fd:
    for line in fd.readlines():
      sup_ytid = line.lstrip(' \t').rstrip(' \t\r\n')
      if len(sup_ytid) >= 11:
        sup_ytid = sup_ytid[:11]
        if strfs.is_str_a_64enc(sup_ytid):
          ytids.append(sup_ytid)
  return ytids


def read_ytids_from_filenamebased_textfile(ytids_filepath):
  ytids = []
  with open(ytids_filepath) as fd:
    for line in fd.readlines():
      filename = line.lstrip(' \t').rstrip(' \t\r\n')
      name, _ = os.path.splitext(filename)
      if len(name) > 12:
        if name[-12] == '-':
          supposed_ytid = name[-11:]
          if strfs.is_str_a_64enc(supposed_ytid):
            ytids.append(supposed_ytid)
  return ytids


def create_empty_file_in(folderpath, filename):
    fpath = os.path.join(folderpath, filename)
    print('Creating empty file ((', fpath, '))')
    fd = open(fpath, 'w')
    fd.write('')
    fd.close()


def lookup_recurs_sqlfilepath_or_none(ytids_folderpath, filename=None):
  if filename is None:
    filename = ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME
  entries = os.listdir(ytids_folderpath)
  for e in entries:
    if e == filename:
      return ytids_folderpath
  # proposed filename does not logically exist, so code below is safe
  if ytids_folderpath == '/':
    return None
  ytids_folderpath, _ = os.path.split(ytids_folderpath)
  return lookup_recurs_sqlfilepath_or_none(ytids_folderpath, filename)


def lookup_ytids_prefixed_or_none(ytids_folderpath, filenameprefix=None):
  if filenameprefix is None:
    filenameprefix = ds.DEFAULT_YTIDS_FILENAME_PREFIX
  entries = os.listdir(ytids_folderpath)
  for e in entries:
    if e.startswith(filenameprefix):
      filename = e
      return filename
  # proposed filename does not logically exist, so code below is safe
  return None


def lookup_ytids_filename_or_create_empty(ytids_folderpath, filenameprefix):
  filename = lookup_ytids_prefixed_or_none(ytids_folderpath, filenameprefix)
  if filename is not None:
    return filename
  filename = filenameprefix + 'name1234.txt'
  create_empty_file_in(ytids_folderpath, filename)
  return filename


def get_recurs_descending_sqlfilepath_by_filename(ongoingpath=None, filename=None):
  if ongoingpath is None:
    ongoingpath = os.path.abspath('.')
  filename = ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME
  fpath = os.path.join(ongoingpath, filename)
  entries = os.listdir(fpath)
  for e in entries:
    if e == filename:
      return fpath
  if ongoingpath == '/':
    return None
  ongoingpath, _ = os.path.split(ongoingpath)
  return get_recurs_descending_sqlfilepath_by_filename(ongoingpath, filename)


def get_descending_recurs_ytidsfpath_after_prefixname(ongoingpath=None, prefix=None):
  if ongoingpath is None:
    ongoingpath = os.path.abspath('.')
  filename = lookup_ytids_prefixed_or_none(ongoingpath, prefix)
  if filename is not None:
    fpath = os.path.join(ongoingpath, filename)
    return fpath
  elif ongoingpath == '/':
    return None
  ongoingpath, _ = os.path.split(ongoingpath)
  return get_descending_recurs_ytidsfpath_after_prefixname(ongoingpath, filename)


class YtidsFileReader:

  def __init__(self,
        folderpath_for_ytids_sqlite=None,
        folderpath_for_ytids_txtfile=None
    ):
    self.ytids = []
    self._folderpath_for_ytids_sqlite = folderpath_for_ytids_sqlite
    _ = self.folderpath_for_ytids_sqlite
    self._folderpath_for_ytids_txtfile = folderpath_for_ytids_txtfile
    self.filename_for_ytids_sqlite = ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME
    self.discover_sqlfilepath_if_any()
    self._filename_for_ytids_txtfile = None  # this one is discoverable
    self.discover_txtfilepath_if_any()

  def discover_sqlfilepath_if_any(self):
    if self._folderpath_for_ytids_sqlite is None or not os.path.isdir(self._folderpath_for_ytids_sqlite):
      self._folderpath_for_ytids_sqlite = os.path.abspath('.')
    fpath = get_recurs_descending_sqlfilepath_by_filename(
      self._folderpath_for_ytids_sqlite,
      self.filename_for_ytids_sqlite
    )
    if fpath is None:
      error_msg = 'Error: filepath_for_ytids_sqlite was not obtainable:' \
      'Sqlite %s is probably missing' % self.filename_for_ytids_sqlite
      raise OSError(error_msg)
    self._filepath_for_ytids_sqlite = fpath
    self._folderpath_for_ytids_sqlite, _ = os.path.split(fpath)

  def filepath_for_ytids_sqlite(self):
    if self._filepath_for_ytids_sqlite is not None:
      return self._filepath_for_ytids_sqlite
    self.discover_sqlfilepath_if_any()
    return self._filepath_for_ytids_sqlite

  def discover_txtfilepath_if_any(self):
    if self._folderpath_for_ytids_sqlite is None or not os.path.isdir(self._folderpath_for_ytids_sqlite):
      self._folderpath_for_ytids_sqlite = os.path.abspath('.')
    fpath = get_descending_recurs_ytidsfpath_after_prefixname(
      self._folderpath_for_ytids_sqlite,
      self.filename_for_ytids_sqlite
    )
    if fpath is None:
      error_msg = 'Error: filepath_for_ytids_txtfile was not obtainable:' \
      'txtfile prefix %s is probably missing' % ds.DEFAULT_YTIDS_FILENAME_PREFIX
      raise OSError(error_msg)
    self._folderpath_for_ytids_sqlite, _ = os.path.split(fpath)

  @property
  def folderpath_for_ytids_sqlite(self):
    if self._folderpath_for_ytids_sqlite is None or not os.path.isdir(self._folderpath_for_ytids_sqlite):
      self._folderpath_for_ytids_sqlite = os.path.abspath('.')
    return self._folderpath_for_ytids_sqlite

  @property
  def folderpath_for_ytids_txtfile(self):
    if self._folderpath_for_ytids_txtfile is None or not os.path.isdir(self._folderpath_for_ytids_txtfile):
      self._folderpath_for_ytids_txtfile = os.path.abspath('.')
    return self._folderpath_for_ytids_txtfile

  @property
  def filepath_for_ytids_txtfile(self):
    fpath = os.path.join(self.folderpath_for_ytids_txtfile, self.filename_for_ytids_sqlite)
    return fpath

  def find_diffset_from(self, ytids_txtfilepath):
    other_ytids = read_ytids_from_filenamebased_textfile(ytids_txtfilepath)
    other_ytids = get_diffset_from_lists(other_ytids, self.ytids)
    return other_ytids

  def __str__(self):
    outline = """<YtidsFileReader>
    Path = {ppath}
    N of ytids = {n_ytids}
""".format(ppath=self.filepath_for_ytids_sqlite, n_ytids=len(self.ytids))
    return outline


def adhoctest():
  """
  ytids_folderpath = ds.Paths.get_default_src_datafolder_abspath()

  """
  ytids_folderpath = '/home/dados/VideoAudio/Yt videos/yt BRA Pol vi/Meteoro tmp yu'
  ytids_filename = 'z_ls-R_contents-name1234.txt'
  print('hi')
  # ytids_folderpath, ytids_filename
  ytid_o = YtidsFileReader(ytids_folderpath, ytids_filename)
  # ytid_o.read_ytids()
  print(ytid_o)


def process():
  # insert_difference_in_rootcontentfile()
  adhoctest()


if __name__ == '__main__':
  process()
