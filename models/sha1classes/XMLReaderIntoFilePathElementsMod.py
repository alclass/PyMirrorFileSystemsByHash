#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
The main class in this script is XMLReaderIntoFilePathElements.

Its main purpose is to generate a dict with the following information:

  + key is the sha1hex (ie, a sha1sum 40-char hexadecimal string)
  + value is the list of FilePath Elements having that sha1hex-key hash


Some planned application of this generated dict are:

  1) look up repeated files downwards a dir-tree so that repeats may be removed on a disk, freeing space and avoiding versions;
  2) comparison of dir-tree images like, for example, original and back-up on different disks.

'''
__author__ = 'friend'

import os, sys

#from Sha1XMLReaderWriterMod import Sha1XMLReader
from .Sha1sStoredOnFolderMod import Sha1sStoredOnFolder
from .Sha1AndItsFilepathsOnAFolderDictMod import Sha1AndItsFilepathsOnAFolderDict
from .FilePathElementMod import FilePathElement
from .FilePathElementMod import BaseFolder

BASEPATH_DEFAULT = '/media/friend/47EA70867E97290F/Direito HD2Bak/'

class XMLReaderIntoFilePathElements(object):
  '''
  The main purpose of this class is to generate a dict with the following information:

    + key is the sha1hex (ie, a sha1sum 40-char hexadecimal string)
    + value is the list of FilePath Elements having that sha1hex-key hash


  Some planned application of this generated dict are:

    1) look up repeated files downwards a dir-tree so that repeats may be removed on a disk, freeing space and avoiding versions;
    2) comparison of dir-tree images like, for example, original and back-up on different disks.
  '''

  def __init__(self, folder_abspath, basepath=BASEPATH_DEFAULT):
    BaseFolder.set_basepath(basepath)
    self.folder_abspath = folder_abspath
    self.sha1_store_on_folder = Sha1sStoredOnFolder(folder_abspath)
    self.sha1_fpes_dict = Sha1AndItsFilepathsOnAFolderDict()
    self.store_filepath_elements()

  def store_filepath_elements(self):
    sha1sum_and_its_filenames_dict = self.sha1_store_on_folder.get_sha1_filenames_dict()
    fpe_parent = FilePathElement.make_filepath_element_against_abspath(self.folder_abspath)
    for sha1hex in sha1sum_and_its_filenames_dict:
      filenames = sha1sum_and_its_filenames_dict[sha1hex]
      fpes = []
      for filename in filenames:
        fpe = FilePathElement(filename, fpe_parent)
        print('filling in', fpe.__unicode__(), sha1hex)
        self.sha1_fpes_dict[sha1hex] = fpe

  def printout_sha1_fpes_dict(self):
    for sha1hex in self.sha1_fpes_dict:
      fpes = self.sha1_fpes_dict[sha1hex]
      for fpe in fpes:
        print(fpe.__unicode__())

  def get_sha1_fpes_dict(self):
    return self.sha1_fpes_dict



#abspath = '/media/friend/47EA70867E97290F/Direito HD2Bak/9 Direito Penal (videos)/CERS Direito Penal (videos)/'
#abspath = '/media/friend/47EA70867E97290F/Direito HD2Bak/1 Direito Administrativo (videos)/'
#basepath = '/media/friend/47EA70867E97290F/Direito HD2Bak/'
def test1():
  abspath = '/media/friend/47EA70867E97290F/Direito HD2Bak/9 Direito Penal (videos)/CERS Direito Penal (videos)/'
  fpes_reader = XMLReaderIntoFilePathElements(abspath)
  fpes_reader.printout_sha1_fpes_dict()

def test2():
  # get everything
  sha1_fpes_dict_hd_wide = Sha1AndItsFilepathsOnAFolderDict()
  for dirpath, dirnames, filenames in os.walk(BASEPATH_DEFAULT):
    if dirpath == BASEPATH_DEFAULT:
      continue
    print(('In', dirpath))
    fpes_reader = XMLReaderIntoFilePathElements(dirpath)
    sha1_fpes_dict = fpes_reader.get_sha1_fpes_dict()
    sha1_fpes_dict_hd_wide.add(sha1_fpes_dict)
  sha1s_with_no_than_1_element = []
  for sha1hex in sha1_fpes_dict_hd_wide:
    fpes = sha1_fpes_dict_hd_wide[sha1hex]
    print(('sha1hex', sha1hex, 'has', len(fpes), 'element(s).'))
    if len(fpes) > 1:
      sha1s_with_no_than_1_element.append(sha1hex)
  print('sha1s_with_no_than_1_element')
  for sha1hex in sha1s_with_no_than_1_element:
    print(sha1hex)

def process():
  '''
  '''
  pass
  # test1()
  test2()


if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()
  process()
