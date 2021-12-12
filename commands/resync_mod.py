#!/usr/bin/env python3
"""
mirror2trees.py

This script does basically two things:
  1) it moves target-tree files to the relative position, in the target-tree itself, that exists in the source-tree;
  2) it copies to the target-tree missing files that exists in the source-tree;

Things this script doesn't do:
  1) This script DOESN'T do removals.
  2) This script DOESN'T do the inverse of the two operations above.

The complete mirroring effect, encompassing these two non-scoped operations above,
  is undertake by other scripts in this system / Python package.
"""
import os.path
import sys
import fs.db.dbdirtree_mod as dbdt
import default_settings as ls
from commands.walkup_dirtree_files import FileSweeper


def get_default_args():
  src_mountpath = os.path.join(ls.Paths.get_datafolder_abspath(), 'src')
  trg_mountpath = os.path.join(ls.Paths.get_datafolder_abspath(), 'trg')
  return src_mountpath, trg_mountpath


def get_args_or_default():
  try:
    src_mountpath = sys.argv[1]
    trg_mountpath = sys.argv[2]
  except IndexError:
    src_mountpath, trg_mountpath = get_default_args()
  if not os.path.isdir(src_mountpath) or not os.path.isdir(trg_mountpath):
    pline = '''Parameter paths (either given or defaulted):
        -------------------
        src_mountpath %(src_mountpath)s
        trg_mountpath %(trg_mountpath)s
        -------------------
         => either one or both do not exist.
        ''' % {'src_mountpath': src_mountpath, 'trg_mountpath': trg_mountpath}
    print(pline)
    sys.exit(1)
  return src_mountpath, trg_mountpath


def resync_tree(mountpath):
  sweeper = FileSweeper(mountpath)
  sweeper.walkup_dirtree_files()
  dbtree = dbdt.DBDirTree(mountpath)
  dbtree.delete_rows_not_existing_on_dirtree(mountpath)


def resync_trees(dirtrees):
  for mountpath in dirtrees:
    resync_tree(mountpath)


def process():
  """
  """
  dirtrees = get_args_or_default()
  resync_trees(dirtrees)  # [src_mountpath, trg_mountpath]


if __name__ == '__main__':
  process()
