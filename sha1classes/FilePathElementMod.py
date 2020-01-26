#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
This script ...
'''
__author__ = 'friend'

import codecs, copy, os, subprocess, sys, time

lambda_filter_empties_out = lambda  x : x != ''

class FilePathElementError(OSError):
  pass

class BaseFolder(object):

  basepath = None

  @classmethod
  def set_basepath(cls, basepath):
    cls.basepath = basepath

  @classmethod
  def get_basepath(cls):
    if cls.basepath == None:
      return '/'
    return cls.basepath

  @classmethod
  def get_entry_name(cls):
    basepath = cls.get_basepath()
    if basepath == '/':
      return '/'
    return basepath.split('/')[-1]

  @classmethod
  def get_relpath_above_basepath(cls, full_abspath):
    basepath = cls.get_basepath()
    relpath = full_abspath [ len(basepath) : ]
    return relpath


class FilePathElement(object):

  FOLDER = 1
  FILE   = 2

  def __init__(self, entry_name, rel_parent_obj=None):
    self.entry_name = entry_name
    if rel_parent_obj == None:
      # special case of ROOT-OR-BASE folder
      self.rel_parent_obj = None
    else:
      self.rel_parent_obj = rel_parent_obj
    self.entry_type = None # to be known upon issuing self.complement_init() next
    self.complement_init()

  def get_abspath(self):
    if self.rel_parent_obj == None:
      parent_abspath = BaseFolder.get_basepath()
    else:
      parent_abspath = self.rel_parent_obj.get_abspath()
    abspath = os.path.join(parent_abspath, self.entry_name)
    return abspath

  def get_parent_abspath(self):
    if self.rel_parent_obj == None:
      parent_abspath = BaseFolder.get_basepath()
    else:
      parent_abspath = self.rel_parent_obj.get_abspath()
    return parent_abspath

  def verify_entry_name_via_ls_pipe(self):
    '''

    This method is NO LONGER NEEDED due to upgrading from Python2 to Python3

    bool_return = False
    #verify_entry_name_on_a_folder_via_ls_pipe()
    contents = os.listdir(self.get_parent_abspath)
    prefix_contents
    if create_new_dircontents:
    fpipeout_filename = 'z-dircontents-%s.txt' %time.time()
    fpipeout_filepath = os.path.join(self.get_parent_abspath(), fpipeout_filename)
    fpipeout = codecs.open(fpipeout_filepath, 'w')
    return_code = subprocess.call(["ls"], stdout=fpipeout)
    text = fpipeout.read()
    if text.find(self.entry_name) > -1:
      bool_return = True
    fpipeout.close()
    os.remove(fpipeout_filepath)
    return bool_return

    bool_return = False
    sha1sum_filepath = os.path.join(self.get_parent_abspath(), 'z-sha1sum.txt')
    if not os.path.isfile(sha1sum_filepath):
      fpipeout_filename = 'z-dircontents-001.txt' # %time.time()
      fpipeout_filepath = os.path.join(self.get_parent_abspath(), fpipeout_filename)
      if not os.path.isfile(fpipeout_filepath):
        fpipeout = codecs.open(fpipeout_filepath, 'w')
        return_code = subprocess.call(["ls"], stdout=fpipeout)
        fpipeout.close()
      f = codecs.open(fpipeout_filepath, 'r', 'utf-8')
    else:
      f = codecs.open(sha1sum_filepath, 'r', 'utf-8')
    text = f.read()
    if text.find(self.entry_name) > -1:
      bool_return = True
    return bool_return
    '''
    return False

  def complement_init(self):
    parent_abspath = self.get_parent_abspath()
    if self.entry_name not in os.listdir(parent_abspath):
      # A 2nd check is in order (NO LONGER needed):  and not self.verify_entry_name_via_ls_pipe():
      error_msg = 'entry [%s] does not exist in [%s]' %(self.entry_name, parent_abspath)
      print(error_msg)
      raise FilePathElementError(error_msg)
    abspath = os.path.join(parent_abspath, self.entry_name)

    if os.path.isdir(abspath):
      self.entry_type = FilePathElement.FOLDER
    elif os.path.isfile(abspath):
      self.entry_type = FilePathElement.FILE
    else:
      raise FilePathElementError('Only FOLDERs and FILEs are allowed as FilePathElements.')

  def get_entry_type_name(self):
    if self.entry_type == FilePathElement.FOLDER:
      return 'FOLDER'
    elif self.entry_type == FilePathElement.FILE:
      return 'FILE'
    else:
      return 'NONE'

  def get_subentry_names(self):
    entries = os.listdir(self.get_abspath())
    return entries

  def get_subentry_filepath_objs(self):
    filepath_objs = []
    entries = self.get_subentry_names()
    for entry_name in entries:
      fp_obj = FilePathElement(entry_name, self)
      filepath_objs.append(fp_obj)
    return filepath_objs

  @staticmethod
  def make_filepath_element_against_abspath(abspath):
    relpath = BaseFolder.get_relpath_above_basepath(abspath)
    hierarchical_folder_names = relpath.split('/')
    fpe_parent = None # means "BaseFolder"
    hierarchical_folder_names = list(filter(lambda_filter_empties_out, hierarchical_folder_names))
    if len(hierarchical_folder_names) == 0:
      return None
    for dirname in hierarchical_folder_names:
      fpe = FilePathElement(dirname, fpe_parent)
      fpe_parent = fpe.copy()
    return fpe # made from the last dirname inside hierarchical_folder_names

  def copy(self):
    return copy.copy(self)

  def __unicode__(self):
    outstr = 'Entry: [%(entry_name)s] in [%(parent_abspath)s] is %(entry_type)s' \
             %{'entry_name':self.entry_name, 'parent_abspath':self.get_parent_abspath(), 'entry_type':self.get_entry_type_name()}
    return outstr

  def __str__(self):
    return '' + self.__unicode__()


base_dir = '/media/friend/47EA70867E97290F/Direito HD2Bak/'
def test1():
  BaseFolder.set_basepath(base_dir)
  # os.chdir(BaseFolder.get_basepath())
  entries = os.listdir(BaseFolder.get_basepath())
  for entry_name in entries:
    fpe = FilePathElement(entry_name)
    print(fpe)

def test2():
  BaseFolder.set_basepath(base_dir)
  entry_name = '1 Direito Administrativo (videos)'
  fpe_parent = FilePathElement(entry_name)
  entry_name = 'EBEJI Direito Administrativo (videos)'
  fpe_l1 = FilePathElement(entry_name, fpe_parent)
  entries = fpe_l1.get_subentry_filepath_objs()
  for fpo in entries:
    print(fpo)

def test3():
  BaseFolder.set_basepath(base_dir)
  abspath = '/media/friend/47EA70867E97290F/Direito HD2Bak/1 Direito Administrativo (videos)/EBEJI Direito Administrativo (videos)/'
  fpe_parent = FilePathElement.make_filepath_element_against_abspath(abspath)
  print(fpe_parent)


def test4():
  BaseFolder.set_basepath(base_dir)
  seq = 0
  for dirpath, dirnames, filenames in os.walk(BaseFolder.get_basepath()):
    fpe_parent = FilePathElement.make_filepath_element_against_abspath(dirpath)
    for filename in filenames:
      fpe = FilePathElement(filename, fpe_parent)
      print(seq, fpe)
      seq += 1
      if seq > 20:
        return



def process():
  '''
  '''
  pass
  # test1()
  test4()


if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()
  process()