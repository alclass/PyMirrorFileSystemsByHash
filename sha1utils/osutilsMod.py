#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys

def create_path_if_it_doesnt_exist(dir_abspath):
  '''
  This method doesn't have a unit test yet.
  @see unittests below
  '''
  if not os.path.isdir(dir_abspath):
    os.makedirs(dir_abspath)

def verify_path_existence_then_raise_or_return_with_trailing_slash(abspath, allowNonExistent=False):
  if abspath == None:
    raise OSError, 'Directory abspath is not initialized (ie, it is None).'
  if not allowNonExistent:
    if not os.path.isdir(abspath):
      raise OSError, 'Directory abspath %s does not exist.' %abspath
  if not abspath.endswith('/'):
    abspath += '/'
  return abspath


import unittest
class Test1(unittest.TestCase):

  def setUp(self):
    pass
  
  def test_1_verify_added_trailing_slash(self):
    given_abspath = '/a/c/b'
    self.assertFalse(given_abspath.endswith('/'))
    received_abspath = verify_path_existence_then_raise_or_return_with_trailing_slash(given_abspath, allowNonExistent=True)
    self.assertTrue(received_abspath.endswith('/'))

  def test_2_verify_None_and_NonExistentDir_passed_in(self):
    self.assertRaises(OSError, verify_path_existence_then_raise_or_return_with_trailing_slash, None)
    improbable_dir = '/a/c/b'
    if os.path.isdir(improbable_dir):
      return
    self.assertRaises(OSError, verify_path_existence_then_raise_or_return_with_trailing_slash, improbable_dir)


def unittests():
  unittest.main()

def process():
  '''
  '''
  pass

if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()
  process()
