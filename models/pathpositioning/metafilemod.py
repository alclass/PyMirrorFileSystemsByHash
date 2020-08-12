#!/usr/bin/env python3
"""
  metafilemod.py
  ...
  Written on 2016-12-27 Luiz Lewis
  Rewritten on 2020-08-10 Luiz Lewis
"""
import os
import shutil
import fs.hashpackage.hexfunctionsMod as hfM
import fs.hashpackage.fileshexfunctionsMod as fhfM


class MetaFile:

  def __init__(
      self, mount_abspath, middlepath, filename,
      sha1hex=None, mockmode=False
  ):

    self.filename = filename
    self.middlepath = middlepath
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
    self.set_sha1hex_if_none()

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
    fap = os.path.join(self.filesfolder_abspath, self.filename)
    if not self.mockmode:
      if not os.path.isfile(fap):
        error_msg = 'Error: filesfolder_abspath (%s) does not exist.' \
                    % str(fap)
        raise OSError(error_msg)
    return fap

  @property
  def os_meta_tuple(self):
    if self._os_meta_tuple is None:
      self.set_os_meta_tuple()
    return self._os_meta_tuple

  def set_os_meta_tuple(self):
    if self.mockmode:
      return
    self._os_meta_tuple = os.stat(self.file_abspath)


  @property
  def sha1hex(self):
    if self._sha1hex is None:
      self.set_sha1hex_if_none()
    return self._sha1hex

  @sha1hex.setter
  def sha1hex(self, psha1hex):
    if self._sha1hex is not None and len(self._sha1hex) == 40:
      self._sha1hex = psha1hex

  def set_sha1hex_if_none(self, reset=False):
    if self.mockmode:
      return
    if self._sha1hex is None or len(self._sha1hex) != 40 or reset:
      self._sha1hex = fhfM.generate_sha1hexdigest_from_filepath(self.file_abspath)

  def move_inside_src_tree_to_rel_pos_of(self, target_metafile):

    if type(target_metafile) != MetaFile:
      error_msg = 'Error: target_metafile %s passed to move_inside_src_tree_to_rel_pos_of() is not type MetaFile' \
                  % str(target_metafile)
      raise OSError(error_msg)

    new_middlepath = target_metafile.middlepath
    new_filesfolder_abspath = os.path.join(self.mount_abspath, new_middlepath)
    new_filename = target_metafile.filename
    new_file_abspath = os.path.join(new_filesfolder_abspath, new_filename)

    if not self.mockmode:
      if os.path.exists(new_file_abspath):
        error_msg = 'File %s already exists, cannot move it.' % new_file_abspath
        raise OSError(error_msg)
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
    outstr = '''
filename           : %(filename)s
mount_abspath      : %(mount_abspath)s
middlepath         : %(middlepath)s
filesfolder_abspath: %(filesfolder_abspath)s
file_abspath       : %(file_abspath)s
sha1hex            : %(sha1hex)s
previous_middlepath: %(previous_middlepath)s
previous_filename  : %(previous_filename)s
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
  trg_metafile.move_inside_src_tree_to_rel_pos_of(src_metafile)
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
