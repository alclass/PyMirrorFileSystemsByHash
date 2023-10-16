#!/usr/bin/env python3
"""
copyMoveMp3sWithPyMirrorMarker.py
  copies or moves mp3-files between "PyMirror" data disks.

Basically, it dir-walks the source root directory and, when finding mp3-files,
    copies or moves them to the target disk.
  If the source folder, in which the mp3-files are located, has a marker
    (conventioned to an empty file named ".move_mp3s_pymirror_marker"),
    the mp3s are moved, otherwise (no marker present) they are copied.

The idea and purpose of this program is to allow the creation of a mp3 repository from a
    larger video/audio file repository.  The "move strategy" is on the user-level,
    ie organized by the user herself.

For example, sometimes audio is extracted from videos leaving both on disk,
    these may be moved out to the mp3s repo.

Notice also that the source folder, in case of "move", contains the conventioned empty file marker
  mentioned above which is left for both signalling "move" and also for telling the
  mp3-audio-generator, another program in this system, that the content has already been mp3-converted.

Usage:
  $copyMoveMp3sWithPyMirrorMarker.py <source-root-dir> <target-root-dir>

Example:
  $copyMoveMp3sWithPyMirrorMarker.py "/media/user/Disk1" "/media/user/Mp3Disk/mp3s"

In the example above, every mp3-file "below" Disk1 will be either copied or moved
  to its equivalent "middlepath" "below" "/media/user/Mp3Disk/mp3s".

By middlepath, it's meant the paths from the root directory to the file,
  ie, suppose there is a mp3-file in:

  source:  "/media/user/Disk1/Science/History/The Beginnings of the Big Bang Theory.mp3"
    its middlepath is "Science/History", thus the targefile will be:
  target:  "/media/user/Mp3Disk/mp3s/Science/History/The Beginnings of the Big Bang Theory.mp3"

In a nutshell, the source dirtree structure is preserved in the target dirtree.
"""
import os
import shutil
import sys
# import fs.os.osfunctions as osfs
SIGNMOVE_MARKER_FILENAME = '.move_mp3s_pymirror_marker'


class Mp3sCopierMover:

  def __init__(self, source_rootpath, target_rootpath):
    self.ongoingpath = None
    self.isMove = False
    self.hasFoundMp3s = False
    self.n_moved = 0
    self.n_copied = 0
    self.source_rootpath = source_rootpath
    self.target_rootpath = target_rootpath
    self.treat_paths()

  @property
  def middlepath(self):
    midpath = self.ongoingpath[len(self.source_rootpath):]
    # midpath cannot begin with a "/" for it will be right-joined to a rootpath
    midpath = midpath.lstrip('/')
    return midpath

  def treat_paths(self):
    if self.source_rootpath is None or not os.path.isdir(self.source_rootpath):
      error_msg = 'Error: source_rootpath does not exist [%s].' % self.source_rootpath
      raise OSError(error_msg)
    if self.target_rootpath is None or not os.path.isdir(self.target_rootpath):
      error_msg = 'Error: target_rootpath does not exist [%s].' % self.target_rootpath
      raise OSError(error_msg)

  def mount_source_filepath(self, filename):
    ppath = os.path.join(self.source_rootpath, self.middlepath)
    source_filepath = os.path.join(ppath, filename)
    return source_filepath

  def mount_target_filepath(self, filename):
    ppath = os.path.join(self.target_rootpath, self.middlepath)
    if not os.path.isdir(ppath):
      os.makedirs(ppath)
    target_filepath = os.path.join(ppath, filename)
    return target_filepath

  def copy_move_if_applied(self, filenames):
    if not self.hasFoundMp3s:
      return
    for filename in filenames:
      try:
        src_filepath = self.mount_source_filepath(filename)
        trg_filepath = self.mount_target_filepath(filename)
      except TypeError:
        continue
      if self.isMove:
        print('-'*40)
        print('Moving to', trg_filepath)
        shutil.move(src_filepath, trg_filepath)
        self.n_moved += 1
        print(self.n_moved, 'moved', filename)
      else:
        print('-'*40)
        print('Copying to', trg_filepath)
        shutil.copy(src_filepath, trg_filepath)
        self.n_copied += 1
        print(self.n_copied, 'copied', filename)

  def lookup_n_copymove_if_applied(self, filenames):
    mp3filenames = sorted(filter(lambda e: e.endswith('.mp3'), filenames))
    if len(mp3filenames) == 0:
      return []
    self.hasFoundMp3s = True
    # to this point, there is at least one mp3s in folder
    # it needs to decide whether to copy or move (it's move if an empty file marker is present)
    # looks up marker ".move_mp3s_pymirror_marker"
    if SIGNMOVE_MARKER_FILENAME in filenames:
      self.isMove = True
    return mp3filenames

  def walkdirtree(self):
    """
    TO-DO: pre-count all mp3 files so that the user may have a notion of
       processing progress duration program execution (example: copying/moving file i of n).
    """
    for self.ongoingpath, _, filenames in os.walk(self.source_rootpath):  # _ is not used foldernames
      self.hasFoundMp3s = False
      self.isMove = False
      mp3filenames = self.lookup_n_copymove_if_applied(filenames)
      if len(mp3filenames) == 0:
        continue
      self.copy_move_if_applied(mp3filenames)


def get_args():
  source_rootpath = None
  target_rootpath = None
  try:
    source_rootpath = sys.argv[1]
    target_rootpath = sys.argv[2]
    return source_rootpath, target_rootpath
  except (AttributeError, IndexError, NameError):
    pass
  return source_rootpath, target_rootpath


def adhoctest():
  """
  """
  pass


def process():
  source_rootpath, target_rootpath = get_args()
  cm = Mp3sCopierMover(source_rootpath, target_rootpath)
  cm.walkdirtree()


if __name__ == '__main__':
  """
  adhoctest()
  pass
  """
  process()
