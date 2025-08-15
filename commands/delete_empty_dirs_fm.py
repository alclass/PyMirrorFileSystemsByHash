#!/usr/bin/env python3
"""
delete_empty_dirs_fm.py

Deletes / removes empty directory in the target dirtree.

Usage:
  $delete_empty_dirs_fm.py <prunepath>

Example:
  $delete_empty_dirs_fm.py "/Science/Physics/Einsteinian Relativity"

Explanation:
  The above (hypothetical) example will remove all empty directories inside folder "Einsteinian Relativity".
"""
import os
import sys
import default_settings as defaults
import lib.dirfilefs.dir_n_file_fs_mod as df


def show_help_cli_msg_if_asked():
  for arg in sys.argv:
    if arg in ['-h', '--help']:
      print(__doc__)
      sys.exit(0)


def prune_dirtree_from_prunepath(prunepath):
  if not os.path.isdir(prunepath):
    error_msg = 'Error: prunepath (%s) is not a directory' % prunepath
    raise OSError(error_msg)
  n_visited, n_removed, n_failed = df.prune_dirtree_deleting_empty_folders(prunepath)
  print('Report delete_empty_dirs_fm.py')
  print('prune path', prunepath)
  print('n_visited =', n_visited, 'n_removed =', n_removed, 'n_failed =', n_failed)


def process():
  """
  """
  show_help_cli_msg_if_asked()

  prunepath, _ = defaults.get_src_n_trg_mountpath_args_or_default()
  prune_dirtree_from_prunepath(prunepath)


if __name__ == '__main__':
  process()
