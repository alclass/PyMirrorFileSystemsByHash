#!/usr/bin/env python3
"""
copyMoveExtFileWithPyMirrorMarker.py
  copies or moves files, with a certain extension (mp3 defaulted), between directory trees.

Basically, it dir-walks the source root directory and, when finding the files under the extension,
    copies or moves them to the target disk.
  If the source folder has a marker
    (conventioned to an empty file named ".move_exts_pymirror_marker"),
    the source files are moved, otherwise (no marker present) they are copied.

The idea and purpose behind this program is to allow the creation of repository from a
    larger video/audio file repository.  The "move strategy" is on the user-level,
    ie organized by the user herself.

For example, sometimes mp3 audio is extracted from videos leaving both
  the video and the audio on disk, these may be moved out to the other dirtree (or disk) repo.

Notice also that the source folder, in case of "move", contains the conventioned empty file marker
  mentioned above which is left in folder for both signalling "move" and also for telling the
  mp3-audio-generator (or similar), probably another program in this system,
  that the content has already been mp3-converted.

Usage:
  $copyMoveExtFileWithPyMirrorMarker.py <source-root-dir> <target-root-dir> [<starting-relpath>] [<extension>]

Where:
  <source-root-dir>  => the system's absolute path starting the dirtree from which files will be copied or moved
  <target-root-dir>  => the system's absolute path starting the dirtree to which files will be copied or moved
  [<starting-relpath>]  => [optional] the relative path in source from which copy/move inspection will start
  [extension]  => [optional] the file extension (default is mp3)
                  Obs: at this time, the 'extension' is a bit "experimental",
                       the system has been tested with the default mp3, so use it with care.

Example:
  $copyMoveExtFileWithPyMirrorMarker.py "/media/user/Disk1" "/media/user/Mp3Disk/mp3s"

In the example above, every mp3-file "below" Disk1 will be either copied or moved
  to its equivalent "middlepath" "below" "/media/user/Mp3Disk/mp3s".

By middlepath, it's meant the relative paths from the root directory to the file,
  As an example, suppose there is a mp3-file whose path:
  source:  "/media/user/Disk1/Science/History/Talks/The Beginnings of the Big Bang Theory.mp3"
    in the context, its middlepath is "Science/History/Talks", thus the targefile will be:
           "/media/user/Mp3Disk/mp3s/Science/History/Talks/The Beginnings of the Big Bang Theory.mp3"

In a nutshell, middlepath's purpose is to help preserve dirtree structure is in target.
"""
import datetime
import os
import shutil
import subprocess
import sys
# import fs.os.osfunctions as osfs
SIGNMOVE_MARKER_FILENAME = '.move_exts_pymirror_marker'
DEFAULT_DOT_EXT = '.mp3'
COMM_FOR_TOTAL_TO_INTERPOL = 'ls -R *%s | wc'

class FileCopierMover:

  def __init__(self, source_rootpath, target_rootpath, starting_relpath=None, dot_ext=None):
    self.ongoingpath = None
    self.isMove = False
    self.hasFoundExtFiles = False
    self.begin_time = datetime.datetime.now()
    self.end_time = None
    self.total_files = 0
    self.n_moved = 0
    self.n_failed_move = 0
    self.n_copied = 0
    self.n_failed_copy = 0
    self.n_source_missing = 0
    self.n_alredy_exist = 0
    self.dot_ext = dot_ext
    self.source_rootpath = source_rootpath
    self.target_rootpath = target_rootpath
    self.treat_src_trg_paths_n_ext()
    self.starting_relpath = starting_relpath
    self._starting_abspath = None  # will be set (lazily) on its first access

  @property
  def starting_abspath(self):
    """
    @see set_starting_abspath() for explanation.
    """
    if self._starting_abspath is not None:
      return self._starting_abspath
    self.set_starting_abspath()
    if self._starting_abspath is not None:
      return self._starting_abspath
    return None

  @property
  def runduration(self):
    return self.end_time - self.begin_time

  def mark_end_time(self):
    self.end_time = datetime.datetime.now()

  def treat_src_trg_paths_n_ext(self):
    if self.dot_ext is None:
      self.dot_ext = DEFAULT_DOT_EXT
    if self.source_rootpath is None or not os.path.isdir(self.source_rootpath):
      error_msg = 'Error: source_rootpath does not exist [%s].' % self.source_rootpath
      raise OSError(error_msg)
    if self.target_rootpath is None or not os.path.isdir(self.target_rootpath):
      error_msg = 'Error: target_rootpath does not exist [%s].' % self.target_rootpath
      raise OSError(error_msg)

  def set_starting_abspath(self):
    """
    If param starting_relpath was given, starting_abspath = join(src_path, starting_relpath)
    If not given | it does not exist, starting_abspath = src_path

    Init param starting_relpath is used to 'advance'/"jump", so to say,
      the dir-walking process.
    Example: suppose:
      1 a 'large' disk sourcing at /media/user/Disk1  ("large" also means it'll take a long time);
      2 the user wants to, from this disk, copy/move only a directory at
        /media/user/Disk1/Science/Biology/DNA/Talks, with dir-walking the whole "large" disk;
    In the example above, the starting_relpath would be:
      starting_relpath = "Science/Biology/DNA/Talks"
    and the full command:
      $copyMoveExtFileWithPyMirrorMarker.py "/media/user/Disk1" "<target-dir>" "Science/Biology/DNA/Talks"
    """
    if self.starting_relpath is None:
      self._starting_abspath = self.source_rootpath
      return
    try:
      self.starting_relpath = self.starting_relpath.lstrip('/')
      ppath = os.path.join(self.source_rootpath, self.starting_relpath)
      if os.path.isdir(ppath):
        self._starting_abspath = ppath
        return
    except (AttributeError, TypeError):
      pass
    self._starting_abspath = self.source_rootpath

  @property
  def middlepath(self):
    midpath = self.ongoingpath[len(self.source_rootpath):]
    # midpath cannot begin with a "/" for it will be right-joined to a rootpath
    midpath = midpath.lstrip('/')
    return midpath

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
    if not self.hasFoundExtFiles:
      return
    for filename in filenames:
      try:
        src_filepath = self.mount_source_filepath(filename)
        trg_filepath = self.mount_target_filepath(filename)
        # if source file does not exist, loop on
        if not os.path.isfile(src_filepath):
          self.n_source_missing += 1
          continue
        # if target file already exists, loop on
        if os.path.isfile(trg_filepath):
          self.n_alredy_exist += 1
          continue
      except TypeError:
        continue
      if self.isMove:
        print('-'*40)
        print('Moving to', trg_filepath)
        try:
          shutil.move(src_filepath, trg_filepath)
          self.n_moved += 1
          print('Moved', self.n_moved, '/', self.total_files, filename)
        except OSError:
          self.n_failed_move += 1
          print('Failed move', self.n_failed_move, '/', self.total_files, filename)
      else:
        print('-'*40)
        print('Copying to', trg_filepath)
        try:
          shutil.copy(src_filepath, trg_filepath)
          self.n_copied += 1
          print('Copied', self.n_copied, '/', self.total_files, filename)
        except OSError:
          self.n_failed_copy += 1
          print('Failed copy', self.n_failed_copy, '/', self.total_files, filename)

  def lookup_n_copymove_if_applied(self, filenames):
    extfilenames = sorted(filter(lambda e: e.endswith(self.dot_ext), filenames))
    if len(extfilenames) == 0:
      return []
    self.hasFoundExtFiles = True
    # to this point, there is at least one mp3s in folder
    # it needs to decide whether to copy or move (it's move if an empty file marker is present)
    # looks up marker ".move_exts_pymirror_marker"
    if SIGNMOVE_MARKER_FILENAME in filenames:
      self.isMove = True
    return extfilenames

  def walkdirtree(self):
    """
    TO-DO: pre-count all mp3 files so that the user may have a notion of
       processing progress duration program execution (example: copying/moving file i of n).
    """
    for self.ongoingpath, _, filenames in os.walk(self.starting_abspath):  # _ is not used foldernames
      self.hasFoundExtFiles = False
      self.isMove = False
      mp3filenames = self.lookup_n_copymove_if_applied(filenames)
      if len(mp3filenames) == 0:
        continue
      self.copy_move_if_applied(mp3filenames)

  def countfiles_via_ospipe(self):
    """
    This method is not working, @see conventional method (with os.walk()) below
    """
    comm_to_find_total_files = COMM_FOR_TOTAL_TO_INTERPOL % self.dot_ext
    print('Counting files:', comm_to_find_total_files)
    input_pp = comm_to_find_total_files.split(' ')
    result = subprocess.run(input_pp, stdout=subprocess.PIPE)
    bytes_result = result.stdout
    try:
      str_result = str(bytes_result)
      output_pp = str_result.split(' ')
      self.total_files = int(output_pp[0])  # the total number is the first element from "ls | wc"
      print('Total files:', self.total_files)
    except (AttributeError, IndexError, ValueError):
      self.total_files = -1
      print('Total files:', self.total_files, ':: system failed to count files. Moving on.')

  def countfiles(self):
    if self.total_files is not None and self.total_files > 0:
      return
    self.total_files = 0
    scrmsg = 'Counting total files (extension "%s"). Please wait.' % self.dot_ext
    print(scrmsg)
    for self.ongoingpath, _, filenames in os.walk(self.starting_abspath):  # _ is not used foldernames
      total_in_folder = len(list(filter(lambda e: e.endswith(self.dot_ext), filenames)))
      self.total_files += total_in_folder
    at_here = datetime.datetime.now()
    count_duration = at_here - self.begin_time
    scrmsg = 'Finished counting. It took ' + str(count_duration) + ' sec.'
    print(scrmsg)
    scrmsg = 'Total files (extension "%s") = ' % self.total_files
    print(scrmsg)

  def process(self):
    self.countfiles()
    self.walkdirtree()
    self.mark_end_time()
    self.report()

  def attrdict(self):
    pdict = {
      'source_rootpath': self.source_rootpath,
      'target_rootpath': self.target_rootpath,
      'starting_relpath': self.starting_relpath,
      'runduration': self.runduration,
      'total_files': self.total_files,
      'n_copied': self.n_copied,
      'n_moved': self.n_moved,
      'n_source_missing': self.n_source_missing,
      'n_alredy_exist': self.n_alredy_exist,
    }
    return pdict

  def report(self):
    outstr = """Report <FileCopierMover total_files = {total_files}>
    source_rootpath = {source_rootpath}
    target_rootpath = {target_rootpath}
    starting_relpath =  {starting_relpath}
    runduration = {runduration}
    n_copied = {n_copied}
    n_moved = {n_moved}
    n_source_missing = {n_source_missing}
    n_alredy_exist = {n_alredy_exist}
    """.format(**self.attrdict())
    return outstr

  def __str__(self):
    return self.report()


def get_args():
  for arg in sys.argv:
    if arg in ['-h', '--help']:
      print(__doc__)
      sys.exit(0)
  source_rootpath = None
  target_rootpath = None
  starting_relpath = None
  extension = None
  try:
    source_rootpath = sys.argv[1]
    target_rootpath = sys.argv[2]
    starting_relpath = sys.argv[3]
    extension = sys.argv[4]
    return source_rootpath, target_rootpath
  except (AttributeError, IndexError, NameError):
    pass
  return source_rootpath, target_rootpath, starting_relpath, extension


def adhoctest():
  """
  """
  pass


def process():
  source_rootpath, target_rootpath, starting_relpath, extension = get_args()
  fcm = FileCopierMover(source_rootpath, target_rootpath, starting_relpath, extension)
  fcm.process()


if __name__ == '__main__':
  """
  adhoctest()
  pass
  """
  process()
