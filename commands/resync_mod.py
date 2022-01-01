#!/usr/bin/env python3
"""
resync_mod.py
This script runs:
 1) thw walkup_dirtree_to_verify_possible_moveupdates (to resync os-entries with db-entries) and
 2) delete_rows_not_existing_on_dirtree() which deletes db-entries with no corresponding os-entries
"""
import fs.db.dbdirtree_mod as dbdt
import default_settings as ds
from commands.walkup_dirtree_files import FileSweeper


def resync_tree(mountpath):
  sweeper = FileSweeper(mountpath)
  sweeper.walkup_dirtree_files()
  dbtree = dbdt.DBDirTree(mountpath)
  dbtree.delete_rows_not_existing_on_dirtree(mountpath)


def process():
  """
  """
  src_mountpath, _ = ds.get_src_n_trg_mountpath_args_or_default()
  resync_tree(src_mountpath)  # [src_mountpath, trg_mountpath]


if __name__ == '__main__':
  process()
