#!/usr/bin/env python3
"""
ytids_functions.py

Choices made for class YtidsTxtNSqliteMaintainer:
---------
1) either basedir is set "fix" at the beginning or it's looked up by path-descending
2) in either of the two cases above, an exception is raised if at least the sqlite file is absent
"""
import os
import string
import default_settings as ds
# import fs.strnlistfs.strfunctions_mod as strfs
# import fs.dirfilefs.ytids_functions as ytfs
ENC64CHARS = string.ascii_lowercase + string.ascii_uppercase + string.digits + '-_'


def is_str_a_64enc(str64):
  f = list(map(lambda c: c in ENC64CHARS, str64))
  if False in f:
    return False
  return True


def is_str_an_11hchar__64enc(p_char11_enc64):
  if not p_char11_enc64 or len(p_char11_enc64) != 11:
    return False
  return is_str_a_64enc(p_char11_enc64)


def extract_ytid_from_filename(filename):
  name, _ = os.path.splitext(filename)
  if len(name) > 11:
    if name[-12] == '-':
      ytid = name[-11:]
      if is_str_a_64enc(ytid):
        return ytid
  return None


def extract_ytids_from_filenames(filenames):
  ytids = []
  for filename in filenames:
    ytid = extract_ytid_from_filename(filename)
    if ytid is None:
      continue
    ytids.append(ytid)
  return ytids


def create_table_if_not_exists_ytids(conn):
  cursor = conn.cursor()
  sql = '''
  CREATE TABLE IF NOT EXISTS ytids (
    ytid CHAR(11) NOT NULL UNIQUE
  );
  '''
  # print(sql)
  cursor.execute(sql)
  sql = '''
  CREATE INDEX IF NOT EXISTS ytid ON ytids(ytid);
  '''
  # print(sql)
  cursor.execute(sql)


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
        if is_str_a_64enc(sup_ytid):
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
          if is_str_a_64enc(supposed_ytid):
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
  if filename is None:
    filename = ds.DEFAULT_DEVICEROOTDIR_SQLFILENAME
  filepath = os.path.join(ongoingpath, filename)
  if os.path.isfile(filepath):
    return filepath
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


def adhoctest():
  pass


def process():
  # insert_difference_in_rootcontentfile()
  adhoctest()


if __name__ == '__main__':
  process()
