#!/usr/bin/env python3
"""
  metafilemod.py
  ...
  Written on 2016-12-27 Luiz Lewis
  Rewritten on 2020-08-10 Luiz Lewis
"""
import os
import shutil
import time
import fs.os.fileshexfunctionsMod as fhfM
import models.samodels as sam


class MetaFile:

  def __init__(
      self, mount_abspath, middlepath, filename, isfile=True,
      sha1hex=None, mockmode=False
  ):

    self.error_on_filepath = False  # yet to find out
    self.middlepath = middlepath
    self.filename = filename
    self.isfile = isfile
    if self.middlepath is None:
      self.middlepath = ''
    self.middlepath = self.middlepath.lstrip('/')
    self.mount_abspath = mount_abspath
    self._sha1hex = sha1hex
    self.mockmode = mockmode
    self.previous_filename = None
    self.previous_middlepath = None
    self._os_meta_tuple = None
    self.set_os_meta_tuple()
    # the following line is commeted out in order
    # to avoid (re)calculating the sha1 hash for large files operation which may last dozens of seconds to return
    # self.calc_n_set_sha1hex()

  def does_mid_n_fil_entry_exist_in_db(self, session):
    dbentry = session.query(sam.FSEntryInDB).\
        filter(sam.FSEntryInDB.entryname == self.filename).\
        filter(sam.FSEntryInDB.middlepath == self.middlepath).\
        first()
    return dbentry

  def insert_or_update_mid_n_fil_dbentry(self, session, verifysha1hex=False):
    dbentry = self.does_mid_n_fil_entry_exist_in_db(session)
    if dbentry:
      if verifysha1hex:
        self.calc_n_set_sha1hex()
        if dbentry.sha1hex == self.sha1hex:
          return dbentry, False
        dbentry.sha1hex = self.sha1hex
        session.commit()
        return dbentry, True
      return dbentry, False
    else:
      return self.insert_mid_n_fil_entry_into_db(session)

  def total_equal_sha1hex_other_files(self, session):
    self.calc_n_set_sha1hex()
    if self.error_on_filepath:
      return None
    return session.query(sam.FSEntryInDB).\
        filter(sam.FSEntryInDB.sha1hex == self.sha1hex).\
        count()

  def rename_dbentry_with_self_attrs_for_sha1hex_found(self, dbentry, session):
    """
    This method should be used with care, for there may be file copies in the same
     dirtree, so if this method is used against a copy with different attributes (middlepath,
       filename even modified date) an inconsistent state will happen
    """
    dbentry.middlepath = self.middlepath
    dbentry.entryname = self.filename
    session.commit()
    return dbentry, True

  def insert_mid_n_fil_entry_into_db(self, session):
    dbentry = sam.FSEntryInDB()
    session.add(dbentry)
    dbentry.entryname = self.filename
    dbentry.middlepath = self.middlepath
    self.calc_n_set_sha1hex()
    dbentry.sha1hex = self.sha1hex
    session.commit()
    return dbentry, True

  @property
  def filesfolder_abspath(self):
    ffap = os.path.join(self.mount_abspath, self.middlepath)
    if not self.mockmode:
      if not os.path.isdir(ffap):
        error_msg = 'Error: filesfolder_abspath (%s) does not exist.' \
                    % str(ffap)
        raise OSError(error_msg)
    return ffap

  @property
  def file_abspath(self):
    """
        error_msg = 'Error: filesfolder_abspath (%s) does not exist.' \
                    % str(fap)
        raise OSError(error_msg)
    """
    fap = os.path.join(self.filesfolder_abspath, self.filename)
    if not self.mockmode:
      if not os.path.isfile(fap):
        self.error_on_filepath = True
    return fap

  @property
  def os_meta_tuple(self):
    if self._os_meta_tuple is None:
      self.set_os_meta_tuple()
    return self._os_meta_tuple

  def set_os_meta_tuple(self):
    if self.mockmode:
      return
    try:
      self._os_meta_tuple = os.stat(self.file_abspath)
    except OSError:
      self._os_meta_tuple = ()

  @property
  def sha1hex(self):
    return self._sha1hex

  @sha1hex.setter
  def sha1hex(self, psha1hex):
    if self._sha1hex is not None and len(self._sha1hex) == 40:
      self._sha1hex = psha1hex

  def calc_n_set_sha1hex(self, reset=False):
    if self.error_on_filepath:
      return
    if self.mockmode:
      return
    if self._sha1hex is None or len(self._sha1hex) != 40 or reset:
      start_time = time.time()
      print('Calculatin sha1 for [%s]. Please wait.' % self.filename)
      self._sha1hex = fhfM.generate_sha1hexdigest_from_filepath(self.file_abspath)
      elapsed_time = time.time() - start_time
      print('Took ', elapsed_time, 'elapsed_time', self._sha1hex)

  def move_file_to_another_middepath(self, target_middlepath):
    target_path = os.path.join(self.mount_abspath, target_middlepath)
    if self.file_abspath == target_path:
      return True
    if os.path.isfile(target_path):
      return False
    shutil.move(self.file_abspath, target_path)
    return True

  def copy_to_target_fs(self, trg_mount_abspath):
    trg_folderpath = os.path.join(trg_mount_abspath, self.middlepath)
    try:
      if not os.path.isdir(trg_folderpath):
        os.makedirs(trg_folderpath)
      trg_abspath = os.path.join(trg_folderpath, self.filename)
      shutil.copy2(self.file_abspath, trg_abspath)
    except OSError:
      return False
    return True

  def move_file_within_its_fs_to_relative_middepath_of_another_metafile(self, target_metafile):

    if type(target_metafile) != MetaFile:
      error_msg = 'Error: target_metafile %s passed to move_file_within_its_fs_to_relative_middepath_of_another_metafile() is not type MetaFile' \
                  % str(target_metafile)
      raise OSError(error_msg)

    new_middlepath = target_metafile.middlepath
    new_filesfolder_abspath = os.path.join(self.mount_abspath, new_middlepath)
    new_filename = target_metafile.filename
    new_file_abspath = os.path.join(new_filesfolder_abspath, new_filename)

    if not self.mockmode:
      if os.path.exists(new_file_abspath):
        error_msg = 'File %s already exists, cannot move it.' % new_file_abspath
        print(error_msg)
        return False
        # raise OSError(error_msg)
      shutil.move(self.file_abspath, new_file_abspath)

    self.previous_middlepath = self.middlepath
    self.previous_filename = self.filename
    self.middlepath = new_middlepath
    self.filename = new_filename

    return True

  def as_dict(self):
    return {
      'filename': self.filename,
      'mount_abspath': self.mount_abspath,
      'middlepath': self.middlepath,
      'filesfolder_abspath': self.filesfolder_abspath,
      'file_abspath': self.file_abspath,
      'sha1hex': self.sha1hex,
      'previous_middlepath': self.previous_middlepath,
      'previous_filename': self.previous_filename,
    }

  def __str__(self):
    """
previous_middlepath: %(previous_middlepath)s
previous_filename  : %(previous_filename)s
    """
    outstr = '''
filename           : %(filename)s
mount_abspath      : %(mount_abspath)s
middlepath         : %(middlepath)s
filesfolder_abspath: %(filesfolder_abspath)s
file_abspath       : %(file_abspath)s
sha1hex            : %(sha1hex)s
    ''' % self.as_dict()
    return outstr


def adhoc_test1():
  filename = 'Homo Sapiens.info.txt'
  mount_path = '/media/SAMSUNG_1/'
  middlepath = 'Animals/Vertebrates/Mammals/'
  mockmode = True
  src_metafile = MetaFile(mount_path, middlepath, filename, None, mockmode)
  print('Source:')
  print(src_metafile)
  filename = 'Homo Economicus.odt'
  mount_path = '/media/SAMSUNG_8/'
  middlepath = 'Sociology/The Cities/Looking Ahead/'
  mockmode = True
  trg_metafile = MetaFile(mount_path, middlepath, filename, None, mockmode)
  print('Target:')
  print(trg_metafile)
  trg_metafile.move_file_within_its_fs_to_relative_middepath_of_another_metafile(src_metafile)
  print('Target Again (after move):')
  print(trg_metafile)
  mount_path = '/home/dados/Sw3/SwDv/OSFileSystemSwDv/PyMirrorFileSystemsByHashSwDv/dados/src'
  filename = 'rootf1.txt'
  trg_metafile = MetaFile(mount_path, '', filename)
  print('Target:')
  print(trg_metafile)


def main():
  adhoc_test1()


if __name__ == '__main__':
  main()
