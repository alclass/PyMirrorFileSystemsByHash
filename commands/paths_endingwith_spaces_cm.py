#!/usr/bin/env python3
"""
paths_endingwith_spaces_cm.py
"""
import os
import sys
import default_settings as defaults


class PathsEndingWithSpacesVerifier:

  def __init__(self, mountpath):
    self.mountpath = mountpath
    self.current_base_abspath = None
    self.n_entry_renames = 0
    self.folders_walked = 0
    self.n_restarts = 0

  def rename_entry_rstripping(self, entry, newname):
    old_abspath = os.path.join(self.current_base_abspath, entry)
    new_abspath = os.path.join(self.current_base_abspath, newname)
    if not os.path.exists(old_abspath):
      return False
    if os.path.exists(new_abspath):
      return False
    self.n_entry_renames += 1
    print(self.n_entry_renames, 'Renaming:')
    print('FROM:', old_abspath)
    print('TO:', new_abspath)
    os.rename(old_abspath, new_abspath)
    return True

  def treat_entries(self, entries):
    """
    """
    break_walk_up_dir = False
    for entry in entries:
      transitioned_name = entry.rstrip(' \t\r\n')
      if transitioned_name != entry:
        if self.rename_entry_rstripping(entry, transitioned_name):
          break_walk_up_dir = True
    return break_walk_up_dir

  def walk_updir_n_verify_if_paths_endswith_spaces(self, restart=False):
    """
    os.walk() is a generator, once a folder is renamed, there's no need to restart the dir-walk back to the beginning
    (the code below was no longer necessary)
    if break_walk_up_dir:
      # restart current os-dir-walk
      return self.walk_updir_n_verify_if_paths_endswith_spaces(restart=True)
    """
    if restart:
      self.n_restarts += 1
    for self.current_base_abspath, folders, files in os.walk(self.mountpath):
      self.folders_walked += 1
      print(self.folders_walked, self.current_base_abspath)
      entries = folders + files
      _ = self.treat_entries(entries)

  def report(self):
    print('')
    print('mountpath', self.mountpath)
    print('current_base_abspath', self.current_base_abspath)
    print('folders_walked', self.folders_walked)
    print('n_entry_renames', self.n_entry_renames)
    print('n_restarts', self.n_restarts)

  def process(self):
    self.walk_updir_n_verify_if_paths_endswith_spaces()
    self.report()


def get_default_args():
  src_mountpath = os.path.join(defaults.Paths.get_datafolder_abspath(), 'src')
  trg_mountpath = os.path.join(defaults.Paths.get_datafolder_abspath(), 'trg')
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


def process():
  """
  """
  ori_mountpath, _ = get_args_or_default()  # bak_mountpath
  verifier = PathsEndingWithSpacesVerifier(ori_mountpath)
  verifier.process()


if __name__ == '__main__':
  process()
