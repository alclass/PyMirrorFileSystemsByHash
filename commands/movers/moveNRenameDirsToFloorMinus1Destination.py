#!/usr/bin/env python3
"""
commands/movers/moveNRenameDirsToFloorMinus1Destination.py

This script was created with the following scenario in mind:
  1) suppose a folder with videos
  2) suppose a subfolder with audio extracted from these videos
  3) let then move this subfolder to another disk (or dirtree)
  4) place and rename this audio folder to the same "relative level" in destination

Let us see this in an example:

  1 - suppose the audio extracted is in directory:
    => /media/driveA/videorepo/Science/Physics/Quantum/2025_Talks/mp3_converted
    (the files there may be mp3's)
  2 - suppose the root (or base) folder to the above directory is:
    => /media/driveA/videorepo
  (observing the two paths above, one sees that
    the relative path is "Science/Physics/Quantum/2025_Talks/mp3_converted",
    i.e., taking "videorepo" as its root folder, the relative path starts from there)
  3 - suppose that the absolute destination folder is the following:
    => /media/driveB/audiorepo
  4 - so, the destination folder should be the absolute destination root folder
      plus the relative path, and, under the approach here, minus the "mp3_converted", i.e.:
    => /media/driveB/audiorepo/Science/Physics/Quantum/2025_Talks

  In a nutshell, the bash command for accomplishing the example above would be:
    $mv "/media/driveA/videorepo/Science/Physics/Quantum/2025_Talks/mp3_converted"
       "/media/driveB/audiorepo/Science/Physics/Quantum/2025_Talks"

This script can do it from an os-dir-walk, i.e., doing all the moves from a directory upward
  (or downward as one see it),
  moving all directories named "mp3-converted", whereever they are within source,
  in the same manner of the example above

-------------------------------------
Usage:
  $moveNRenameDirsToFloorMinus1Destination.py
    --src <source-root-abspath>
    --dst <destination-root-abspath>
    [--relpath <relative-path-if-chosen>]
    [--audiofolder <foldername_where_the_audio_is_located>]

Where:

  --src is the source abspath equivalent to its "root folder"
  --dst is the destination abspath equivalent to its "root folder"
  optional: --relpath is the relative path from which os.walk() will begin its traversal (@see also below)
  optional: --audiofolder is the name of the folder where the audio to be moved resides
         it defaults to the foldername "mp3_converted"

Examples: (the same example as above but now written in command-form)

1) first formulation (of two)

$moveNRenameDirsToFloorMinus1Destination.py
  --src "/media/driveA/videorepo"
  --dst "/media/driveB/audiorepo"
  --audiofolder "mp3_converted"

In the (first) example above, the program will search for all folders named "mp3_converted"
  upwards (or downwards as one sees it) the source directory tree,
  as the program finds it, it moves its audiofiles to their equivalent places in destination.

2) second formulation (of two) (using parameter --relpath)

$moveNRenameDirsToFloorMinus1Destination.py
  --src "/media/driveA/videorepo"
  --dst "/media/driveB/audiorepo"
  --relpath "Science/Physics/Quantum/2025_Talks"
  --audiofolder "mp3_converted"

The second example above does the same as the first
  if one considers the mp3_converted folder just mentioned, but, as a difference,
  this formulation does not traverse the whole source dirtree. It starts its traversal
  from the "mp3_converted" folder just mentioned.

Notice lastly that the parameter --audiofolder with "mp3_converted" is not necessary,
  because this parameter-value is the default if none is given.
"""
import argparse
import datetime
import os
import shutil
DEFAULT_AUDIO_FOLDERNAME = "mp3s_converted"
parser = argparse.ArgumentParser(description="Move audiofiles from one dirtree to another"
                                             " taking it to one directory below in destination.")
parser.add_argument("--src", type=str,
                    help="Absolute source directory path")
parser.add_argument("--dst", type=str,
                    help="Absolute destination directory path")
parser.add_argument("--relpath", type=str, default=None,
                    help="Relative path to 'advance' the traversal starting point that searches for the audiofolder")
parser.add_argument("--audiofolder", type=str, default=DEFAULT_AUDIO_FOLDERNAME,
                    help="conventioned audio foldername for all folders to be moved")
args = parser.parse_args()
AUDIO_DOT_EXTENSIONS = ['.mp3', '.m4a', '.mpa']  # .webm is not included because it's also used for video


class FloorMinus1MoverRenamer:

  def __init__(
      self, src_rootpath: str, dst_rootpath: str,
      start_relpath: str = None, tomove_foldername: str = None
    ):
    self.start_time = datetime.datetime.now()
    self.end_time = None
    self.src_rootpath = src_rootpath
    self.dst_rootpath = dst_rootpath
    self.start_relpath = start_relpath
    self.treat_src_dst_folders()
    self.tomove_foldername = tomove_foldername or DEFAULT_AUDIO_FOLDERNAME
    self.current_abspath = None
    self.n_files_processed = 0
    self.total_filerepeats = 0
    self.n_src_not_exists = 0
    self.n_dst_exists = 0
    self.n_ongoing_audiofile = 0
    self.n_folders_processed = 0
    self.n_moved = 0
    self.failed_moved = 0

  def treat_src_dst_folders(self):
    errormsgs = []
    raise_error = False
    if not os.path.isdir(self.src_rootpath):
      errmsg = f"Error: SOURCE directory [{self.src_rootpath}] does not exist."
      errormsgs.append(errmsg)
      raise_error = True
    if not os.path.isdir(self.dst_rootpath):
      errmsg = f"Error: DESTINATION directory [{self.src_rootpath}] does not exist."
      errormsgs.append(errmsg)
      raise_error = True
    if raise_error:
      errmsg = '\n'.join(errormsgs)
      raise OSError(errmsg)
    self.treat_start_relpath()

  def treat_start_relpath(self):
    if self.start_relpath is None:
      return
    if not isinstance(self.start_relpath, str):
      # start_relpath must be str else it should be None
      self.start_relpath = None
    # test path existence
    self.start_relpath = self.start_relpath.lstrip('/')  # relpaths cannot begin with "/"
    ppath = os.path.join(self.src_rootpath, self.start_relpath)
    if not os.path.isdir(ppath):
      # if relpath does not form a valid path with source path, set it to None
      self.start_relpath = None

  @property
  def walk_start_abspath(self) -> str:
    if self.start_relpath is None:
      return self.src_rootpath
    return os.path.join(self.src_rootpath, self.start_relpath)

  @property
  def runduration(self) -> datetime.datetime | None:
    if self.end_time is None:
      return None
    return self.end_time - self.start_time

  @property
  def relpath(self) -> str:
    relative_path = self.current_abspath[len(self.src_rootpath):]
    relative_path = relative_path.lstrip('/')
    return relative_path

  def get_src_abspath_for_filename(self, filename: str) -> str:
    src_fp = os.path.join(self.current_abspath, filename)
    return src_fp

  @property
  def relpath_minus_audiofolder(self) -> str:
    """
    When moving the audio files, it descends, so to say, to destination, one directory level,
      i.e., it gets moved to its equivalent parent directory in destination
      (@see also the example in the module's docstr)
    """
    raise_error = False
    errors = []
    pp = self.relpath.split('/')
    lastfoldername = pp[-1]  # it expects the conventioned audiofoldername (currently: mp3_converted)
    if lastfoldername != self.tomove_foldername:
      # oh, oh, error
      raise_error = True
      errmsg = f"Error: relpath [{self.relpath}] does not contain [{self.tomove_foldername}] as last folder"
      errors.append(errmsg)
    if raise_error:
      errmsg = '\n'.join(errors)
      raise OSError(errmsg)
    relpath_minus_audiofoldername = '/'.join(pp[:-1])  # if pp[:-1] is empty, join() forms '' (the empty str)
    return relpath_minus_audiofoldername

  def get_dst_abspath_for_recipient_folder(self) -> str:
    recipient_folder_abspath = os.path.join(self.dst_rootpath, self.relpath_minus_audiofolder)
    recipient_folder_abspath = recipient_folder_abspath.rstrip('/')  # it may happen if relpath is empty
    if not os.path.isdir(recipient_folder_abspath):
      os.makedirs(recipient_folder_abspath, exist_ok=True)
    return recipient_folder_abspath

  def get_dst_abspath_for_filename(self, filename: str) -> str:
    recipient_folder_abspath = self.get_dst_abspath_for_recipient_folder()
    dst_fp = os.path.join(recipient_folder_abspath, filename)
    return dst_fp

  def write_labelfile_informing_audio_was_moved(self, local_moved):
    if local_moved == 0:
      return
    txtfilename = f'z-audiomoved-on_{self.start_time}.txt'
    txtfilepath = os.path.join(self.current_abspath, txtfilename)
    fd = open(txtfilepath, 'w')
    line = f"{txtfilename}\n"
    line += f"number of audiofiles moved = {local_moved}"
    fd.write(line)
    fd.close()
    scrmsg = f"Written post-move txt file [{txtfilename}]"
    print(scrmsg)

  def move_audiofiles_in_folder(self, audiofiles):
    local_moved = 0
    for audiofilename in audiofiles:
      self.n_ongoing_audiofile += 1
      seq = self.n_ongoing_audiofile
      scrmsg = f"{seq} processing audiofile [{audiofilename}]"
      print(scrmsg)
      scrmsg = f"\t@ [{self.relpath}]"
      print(scrmsg)
      src_fp = self.get_src_abspath_for_filename(audiofilename)
      dst_fp = self.get_dst_abspath_for_filename(audiofilename)
      if not os.path.isfile(src_fp):
        scrmsg = f"\tfile [{audiofilename}] is missing in source, not moving, continuing."
        print(scrmsg)
        self.n_src_not_exists += 1
        continue
      if os.path.isfile(dst_fp):
        scrmsg = f"\tfile [{audiofilename}] is present in destination, not moving, continuing."
        print(scrmsg)
        self.n_dst_exists += 1
        continue
      # ok, move can happen
      try:
        # shutil.move(src_fp, dst_fp)
        self.n_moved += 1
        local_moved += 1
        scrmsg = f"\t Moved local={local_moved}/uptilnow={self.n_moved}/ongo={self.n_ongoing_audiofile}"
        print(scrmsg)
      except (IOError, OSError) as e:
        scrmsg = f"\tfailed moving [{audiofilename}] | {e}"
        print(scrmsg)
        self.failed_moved += 1
    self.write_labelfile_informing_audio_was_moved(local_moved)

  def go_move_audio_in_folder(self, files):
    audiofiles = list(filter(lambda fn: fn.endswith(tuple(AUDIO_DOT_EXTENSIONS)), files))
    if len(audiofiles) > 0:
      self.move_audiofiles_in_folder(audiofiles)

  def process_current_folder(self, files):
    _, topfoldername = os.path.split(self.current_abspath)
    if topfoldername == self.tomove_foldername:
      scrmsg = f"{self.n_folders_processed} | {self.current_abspath}"
      print(scrmsg)
      self.go_move_audio_in_folder(files)

  def process(self):
    """
    The process method loops os.walk(startpath) processing each folder through its traversal.
    The processing is to move audiofiles
      inside a convention-named folder to another dirtree (@see also the module's docstr above).
    """
    scrmsg = f" => Starting traversal at [{self.walk_start_abspath}]"
    print(scrmsg)
    for self.current_abspath, dirs, files in os.walk(self.walk_start_abspath):
      print('passing on', self.current_abspath)
      print('dirs', dirs)
      print('files', files)
      self.n_folders_processed += 1
      self.n_files_processed += len(files)
      self.process_current_folder(files)
    self.end_time = datetime.datetime.now()
    self.report()

  def report(self):
    print(self)

  def __str__(self):
    outstr = f"""{self.__class__.__name__}
    start_time = {self.start_time}
    end_time = {self.end_time}
    run duration = {self.runduration}
    src_rootpath = {self.src_rootpath}
    dst_rootpath = {self.dst_rootpath}
    walk_start_abspath = {self.walk_start_abspath}
    tomove_foldername = {self.tomove_foldername}
    current_abspath = {self.current_abspath}
    n_files_processed = {self.n_files_processed}
    total_filerepeats = {self.total_filerepeats}
    n_src_not_exists = {self.n_src_not_exists}
    n_dst_exists = {self.n_dst_exists}
    total trav audiofiles = {self.n_ongoing_audiofile} 
    n_moved = {self.n_moved}
    failed_moved = {self.failed_moved}
    """
    return outstr


def get_args():
  src = args.src
  dst = args.dst
  relpath = args.relpath
  audiofoldername = args.audiofolder
  return src, dst, relpath, audiofoldername


def process():
  """
  """
  src, dst, relpath, audiofoldername = get_args()
  mover = FloorMinus1MoverRenamer(src, dst, relpath, audiofoldername)
  mover.process()


if __name__ == '__main__':
  process()
