#!/usr/bin/env python3
"""
commands/movers/moveNRenameDirsToFloorMinus1Destination.py

This script was created with the following scenario in mind:
  1) suppose a folder with videos
  2) suppose a subfolder with audio extracted from these videos
  3) let then move this subfolder to another disk (or dirtree)
  4) place and rename this audio folder to the same relative level when the videos are

Example:
  1 - suppose the audio extracted is in directory:
  => /media/driveA/videorepo/Science/Physics/Quantum/2025_Talks/mp3_converted
  2 - suppose the root (or base) folder to the above directory is:
  => /media/driveA/videorepo
  (one see that the relative path is "Science/Physics/Quantum/2025_Talks/mp3_converted")
  3 - suppose that the absolute destination folder is the following:
  => /media/driveB/audiorepo

  So, the destination folder should be the absolute root folder plus the relative path,
      minus the "mp3_converted", i.e.:
  /media/driveB/audiorepo/Science/Physics/Quantum/2025_Talks

In a nutshell, the move will be the following:
  $mv "/media/driveA/videorepo/Science/Physics/Quantum/2025_Talks/mp3_converted"
     "/media/driveB/audiorepo/Science/Physics/Quantum/2025_Talks"

This script can do it from an os-dir-walk, i.e., doing all the moves from a directory upward
  (or downward as one see it),
  moving all directories named "mp3-converted"
  in the manner of the example above

Usage:
$moveNRenameDirsToFloorMinus1Destination.py
  --src <source-root-abspath>
  --dst <destination-root-abspath>
  [--audiofolder <foldername_where_the_audio_is_located>]

Where:

  --src is the source abspath equivalent to its "root folder"
  --dst is the destination abspath equivalent to its "root folder"
  optional: --audiofolder is the name of the folder where the audio to be moved resides
         it defaults to the foldername "mp3_converted"

Example: (the same example as above but now written in command-form)

$moveNRenameDirsToFloorMinus1Destination.py
  --src "/media/driveA/videorepo"
  --dst "/media/driveB/audiorepo"
  --audiofolder "mp3_converted"

In the example above, the program will seached for all folders named "mp3_converted"
  upwards (or downwards as one sees it) the source directory tree,
  as the program finds it, it moves it to their equivalent places in destination.

Notice lastly that the parameter --audiofolder with "mp3_converted" is not necessary,
  because this parameter-value is the default if none is given.
"""
import argparse
import datetime
import os
import shutil
DEFAULT_AUDIO_FOLDERNAME = "mp3_converted"
parser = argparse.ArgumentParser(description="Move audiofiles from one dirtree to another"
                                             " taking it to one directory below in destination.")
parser.add_argument("--src", type=str,
                    help="Absolute source directory path")
parser.add_argument("--dst", type=str,
                    help="Absolute destination directory path")
parser.add_argument("--audiofolder", type=str, default=DEFAULT_AUDIO_FOLDERNAME,
                    help="conventioned audio foldername for all folders to be moved")
args = parser.parse_args()
AUDIO_DOT_EXTENSIONS = ['.mp3', '.m4a', '.mpa']  # .webm is not included because it's also used for video


class FloorMinus1MoverRenamer:

  def __init__(self, src_rootpath: str, dst_rootpath: str, tomove_foldername: str = None):
    self.start_time = datetime.datetime.now()
    self.end_time = None
    self.src_rootpath = src_rootpath
    self.dst_rootpath = dst_rootpath
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

  @property
  def runduration(self) -> datetime.datetime | None:
    if self.end_time is None:
      return None
    return self.end_time - self.start_time

  @property
  def relpath(self) -> str:
    relative_path = self.current_abspath[self.src_rootpath:]
    relative_path = relative_path.lstrip('/')
    return relative_path

  def get_src_abspath_for_filename(self, filename: str) -> str:
    src_fp = os.path.join(self.current_abspath, filename)
    return src_fp

  def get_dst_abspath_for_recipient_folder(self) -> str:
    recipient_folder_abspath = os.path.join(self.dst_rootpath, self.relpath)
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
        shutil.move(src_fp, dst_fp)
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

    """
    for self.current_abspath, files, dirs in os.walk(self.src_rootpath):
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
    tomove_foldername = {self.tomove_foldername}
    current_abspath = {self.current_abspath}
    n_files_processed = {self.n_files_processed}
    total_filerepeats = {self.total_filerepeats}
    n_src_not_exists = {self.n_src_not_exists}
    n_dst_exists = {self.n_dst_exists}
    n_ongoing_audiofile = {self.n_ongoing_audiofile} 
    n_moved = {self.n_moved}
    failed_moved = {self.failed_moved}
    """
    return outstr


def get_args():
  src = args.src
  dst = args.dst
  audiofolder = args.audiofolder
  return src, dst, audiofolder


def process():
  """
  """
  src, dst, audiofoldername = get_args()
  mirror = FloorMinus1MoverRenamer(src, dst, audiofoldername)
  mirror.process()


if __name__ == '__main__':
  process()
