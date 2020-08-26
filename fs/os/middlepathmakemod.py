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


def extract_middlepath_as_complement_from_mountpath_to_abspath(mount_abspath, abspath):
  if len(abspath) < len(mount_abspath):
    return None
  if abspath == mount_abspath:
    return ''
  if not abspath.startswith(mount_abspath):
    return None
  middlepath = abspath[len(mount_abspath):]
  middlepath = middlepath.strip('/')
  return middlepath


class MiddlePath:

  def __init__(self, mount_abspath):
    self.mount_abspath = mount_abspath

  def middle_to_entry(self, entry_abspath):
    return extract_middlepath_as_complement_from_mountpath_to_abspath(
      self.mount_abspath, entry_abspath
    )

  def middle_to_a_childs_entry(self, entry_abspath, entryname):
    try:
      anychildpath = os.path.join(entry_abspath, entryname)
    except IndexError:
      return None
    return self.middle_to_entry(anychildpath)

  def middle_to_parent(self, entry_abspath):
    try:
      entry_abspath = entry_abspath.rstrip('/')
      parentpath, _ = os.path.split(entry_abspath)
    except IndexError:
      return None
    return extract_middlepath_as_complement_from_mountpath_to_abspath(
      self.mount_abspath, parentpath
    )

  def middle_to_greatparent(self, entry_abspath):
    try:
      entry_abspath = entry_abspath.rstrip('/')
      parentpath, _ = os.path.split(entry_abspath)
      greatparentpath, _ = os.path.split(parentpath)
    except IndexError:
      return None
    return extract_middlepath_as_complement_from_mountpath_to_abspath(
      self.mount_abspath, greatparentpath
    )

  def __str__(self):
    return '<MiddlePath abspath={}>'.format(self.mount_abspath)


def adhoc_test():
  # 1
  mount_abspath = '/media/friend/TTC_D2_2T_Orig'
  midpath = MiddlePath(mount_abspath)
  abspath = "/media/friend/TTC_D2_2T_Orig/A _ TTC Arts/M _ TTC Music/" \
            "TTC America's Musical Heritage _i Anthony Seeger _f Univ of California"
  middlepath = midpath.middle_to_entry(abspath)
  print('adhoc_test 1')
  print('mount_abspath', mount_abspath)
  print('abspath', abspath)
  print('middlepath to entry =>', '['+middlepath+']')

  # 2
  mount_abspath = '/knowledgetree/science/'
  midpath = MiddlePath(mount_abspath)
  abspath = "/knowledgetree/science/physics/relativity/einstein.txt"
  middlepath = midpath.middle_to_parent(abspath)
  print('='*30)
  print('adhoc_test 2')
  print('mount_abspath', mount_abspath)
  print('abspath', abspath)
  print("middlepath to file's folder =>", '['+ middlepath + ']')

  # 3
  mount_abspath = '/knowledgetree/science/'
  midpath = MiddlePath(mount_abspath)
  abspath = "/knowledgetree/science/physics/relativity/einstein/"
  childs_entryname = 'entryname'
  middlepath = midpath.middle_to_a_childs_entry(abspath, childs_entryname)
  print('='*30)
  print('adhoc_test 3')
  print('mount_abspath', mount_abspath)
  print('abspath', abspath)
  print('childs_entryname', childs_entryname)
  print('middlepath =>', '['+ middlepath + ']')

  # 4
  mount_abspath = '/knowledgetree/science/'
  abspath = "/knowledgetree/science/physics/relativity/einstein"
  middlepath = midpath.middle_to_entry(abspath)
  parent_middlepath = midpath.middle_to_parent(abspath)
  greatparent_middlepath = midpath.middle_to_greatparent(abspath)
  print('='*30)
  print('adhoc_test 4')
  print('mount_abspath', mount_abspath)
  print('abspath', abspath)
  print('middlepath to entry =>', '[' + middlepath + ']')
  print('middlepath to parent =>', '[' + parent_middlepath + ']')
  print('middlepath to greatparent =>', '[' + greatparent_middlepath + ']')
  print('midpath obj =>', '[' + str(midpath) + ']')


def process():
  adhoc_test()


if __name__ == '__main__':
  process()
