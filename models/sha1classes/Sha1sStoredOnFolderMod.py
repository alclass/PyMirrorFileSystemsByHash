#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
This class originally was thought of to be a façade to fetch the sha1sums and its filenames on a folder.
The façade idea is that no matter how that information was stored, this script would find out
whether it's a flat file, an XML file or a SQLite storefile.

For the time being, the class in here will only fetch sha1sums and its filenames from flat text store files.

See DEFAULT_FLAT_SHA1_FILENAME below.
'''
__author__ = 'friend'

import  codecs, os, sys
from .Sha1AndItsFilenamesOnAFolderDictMod import Sha1AndItsFilenamesOnAFolderDict

DEFAULT_FLAT_SHA1_FILENAME = 'z-sha1sum.txt'

class Sha1sStoredOnFolder(object):

  def __init__(self, folder_abspath):
    self.sha1storefilepath = os.path.join(folder_abspath, DEFAULT_FLAT_SHA1_FILENAME)
    self.sha1_filenames_dict = Sha1AndItsFilenamesOnAFolderDict()
    self.read_sha1sums_and_files()

  def read_sha1sums_and_files(self):
    '''
    Read the store file and fill in self.sha1_filenames_dict
    '''
    if not os.path.isfile(self.sha1storefilepath):
      return
    sha1_store_file = codecs.open(self.sha1storefilepath, 'r', encoding='utf-8')
    for line in sha1_store_file.readlines():
      line = line.rstrip(' \t\r\n')
      line = str(line)
      try:
        sha1hex = line[:40]
        filename = line[42:]
        self.sha1_filenames_dict[sha1hex] = filename
      except IndexError:
        continue

  def get_sha1_filenames_dict(self):
    return self.sha1_filenames_dict

  def __str__(self):
    return 'sha1store: %d sha1sums in [%s]' %(len(self.sha1_filenames_dict), self.sha1storefilepath)


# abspath = '/media/friend/47EA70867E97290F/Direito HD2Bak/9 Direito Penal (videos)/CursoCEJ Direito Penal (videos)/'
# abspath = '/media/friend/47EA70867E97290F/Direito HD2Bak/1 Direito Administrativo (videos)/AprovaConcursos Direito Administrativo (videos)/'
abspath = '/media/friend/47EA70867E97290F/Direito HD2Bak/1 Direito Administrativo (videos)/'
def test1():
  sha1_store_on_folder = Sha1sStoredOnFolder(abspath)
  sha1_filenames_dict = sha1_store_on_folder.get_sha1_filenames_dict()
  for sha1hex in sha1_filenames_dict:
    print(sha1hex, 'files: ', end=' ')
    for filename in sha1_filenames_dict[sha1hex]:
      print('['+filename+']')

def process():
  '''
  '''
  pass
  test1()

if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()
  process()
