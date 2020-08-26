#!/usr/bin/env python3
"""
  examples:
  1) The general case:
    Suppose the following level 3 path a/b/c/d/e/f with mountpath is a/b/c:
    + abspath is a/b/c/d/e/f
    + entryname should be 'f'
    + middlepath should be 'd/e/f'
    + parent_middlepath should be 'd/e'
    + great_parent_middlepath should be 'd'

  2) None (root) case ie when abspath is the same as mountpath
    Suppose mountpath is a/b/c and abspath is a/b/c
      entryname should be 'c'
      middlepath should be None
      (this is the level 0 (or root) case)
  3) error case of abspath inside mountpath:
    Suppose mountpath is a/b/c and abspath is a/b
      in this case, a ValueError exception should be raised.
  4) the level 1 case, ie, those with empty middlepath
    Suppose mountpath is a/b/c and abspath is a/b/c/d
      + entryname should be 'd'
      + middlepath should be ''
      (this is a level 1 entry, it has an empty middlepath)
"""
import os
import config
import unittest
import fs.os.middlepathmakemod as midpmak

class TestCase(unittest.TestCase):

  def test_general_case(self):
    midpathobj = midpmak.MiddlePath(mount_abspath='a/b/c')
    abspath = 'a/b/c/d/e/f'
    expected_middlepath = 'd/e/f'
    returned_middlepath = midpathobj.middle_to_entry(abspath)
    self.assertEqual(expected_middlepath, returned_middlepath)
    expected_parent_middlepath = 'd/e'
    returned_parent_middlepath = midpathobj.middle_to_parent(abspath)
    self.assertEqual(expected_parent_middlepath, returned_parent_middlepath)
    expected_greatparent_middlepath = 'd'
    returned_greatparent_middlepath = midpathobj.middle_to_greatparent(abspath)
    self.assertEqual(expected_greatparent_middlepath, returned_greatparent_middlepath)

  def test_general_case_one_more_level_n_with_extra_slash(self):
    midpathobj = midpmak.MiddlePath(mount_abspath='a/b/c')
    abspath = 'a/b/c/d/e/f/g/'
    expected_middlepath = 'd/e/f/g'
    returned_middlepath = midpathobj.middle_to_entry(abspath)
    self.assertEqual(expected_middlepath, returned_middlepath)
    expected_parent_middlepath = 'd/e/f'
    returned_parent_middlepath = midpathobj.middle_to_parent(abspath)
    self.assertEqual(expected_parent_middlepath, returned_parent_middlepath)
    expected_greatparent_middlepath = 'd/e'
    returned_greatparent_middlepath = midpathobj.middle_to_greatparent(abspath)
    self.assertEqual(expected_greatparent_middlepath, returned_greatparent_middlepath)

  def test_root_n_level1_for_foldercases(self):
    # root case
    midpathobj = midpmak.MiddlePath(mount_abspath='a/b/c')
    abspath = 'a/b/c'
    returned_middlepath = midpathobj.middle_to_parent(abspath)
    self.assertIsNone(returned_middlepath)
    # level1 case
    midpathobj = midpmak.MiddlePath(mount_abspath='a/b/c')
    abspath = 'a/b/c/d'
    returned_middlepath = midpathobj.middle_to_parent(abspath)
    self.assertEqual('', returned_middlepath)


def process():
  pass


if __name__ == '__main__':
  process()
