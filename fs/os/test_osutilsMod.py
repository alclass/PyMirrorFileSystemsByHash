#!/usr/bin/env python
"""

"""
import os
import sys
import unittest
import fs.os.osutilsMod as utilM


class Test1(unittest.TestCase):

  def setUp(self):
    pass
  
  def test_1_verify_added_trailing_slash(self):
    given_abspath = '/a/c/b'
    self.assertFalse(given_abspath.endswith('/'))
    received_abspath = utilM.verify_path_existence_then_raise_or_return_with_trailing_slash(
      given_abspath, allow_nonexistent=True
    )
    self.assertTrue(received_abspath.endswith('/'))

  def test_2_verify_None_and_NonExistentDir_passed_in(self):
    self.assertRaises(OSError, utilM.verify_path_existence_then_raise_or_return_with_trailing_slash, None)
    improbable_dir = '/a/c/b'
    if os.path.isdir(improbable_dir):
      return
    self.assertRaises(OSError, utilM.verify_path_existence_then_raise_or_return_with_trailing_slash, improbable_dir)


def unittests():
  unittest.main()


def process():
  """
  """
  pass


if __name__ == '__main__':
  if 'ut' in sys.argv:
    sys.argv.remove('ut')
    unittests()
  process()
