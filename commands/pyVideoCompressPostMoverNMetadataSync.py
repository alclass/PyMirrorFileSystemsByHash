#!/usr/bin/env python3
"""
commands/pyVideoCompressPostMoverNMetadataSync.py

  This script has two main functionalities, ie:
    1 it moves files that do not have a certain set of file extensions
      This is useful as a post-action to a previous videocompression run, where videos were compressed
        to another dirtree
    2 it copies (or sync) the file metadata of video files that are complementary to the set above
      The use case here is motivated by the fact that
        the videocompress process does not copy the metadata to the video files compressed
          to the target dirtree, so a second run for resync'ing these metadata is needed

    Obs: the former third 'delete' functionality was removed from here and put in another script
         (at this time, this 'deleter' script is pyVideoCompressAfterDeleter.py)

  In a nutshell, either a file, in the source treedir, will be moved, or it will be copystat'd.
  (One set [movable] complements the other [copystatable] representing all files in the source treedir.)

Usage:
======

pyVideoCompressPostMoverNMetadataSync.py --input-dir <source_dirtree_abspath>
   --output-dir <tareget_dirtree_abspath> [--copystat] [--move]

Where:

  --ed => means "equalize os-dates",
          shutil.copystat() the metadata in the source videofiles to the target previously compressed ones
          (notice that this pressuposes a previous run of the videocompress script)

  --mc => means "move complement",
          ie all complementing files (*) are moved to the target dirtree
            (*) complementing files are all files not having the video file extensions
          (notice that though this does not pressupose a previous run of the videocompress script,
          it was created for a complementary second run in mind, where the video accompanying files
            (.txt, .pdf etc.) would be placed back together with the videos (now the compressed ones))

  Both parameters are optional: neither can be given, either one or the other may be given, or both.
  But notice that if none is given, nothing will be executed.

Example Usage
==============

  Ex1 $pyVideoPostCompressMoverNDeleter.py --input-dir "/media/user/disk1/Science/Physics"
   --output-dir "/media/user/disk2/Science/Physics" --ed

  Ex2 $pyVideoPostCompressMoverNDeleter.py --input-dir "/media/user/disk1/Science/Physics"
   --output-dir "/media/user/disk2/Science/Physics" --ed --mc

  In the first example above, os-dates will be equalized (resync'd)
  In the second example, metadata will be resync'd and all complementing files
    will be moved over the target dirtree
"""
import argparse
import datetime
import logging
import os
import shutil
import sys
import time
DEFAULT_COMPRESSABLE_DOT_EXTENSIONS = [".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v"]
DEFAULT_RESOLUTION_WIDTH_HEIGHT = (256, 144)  # 256:144
ACCEPTED_RESOLUTIONS = [(256, 144), (426, 240), (640, 360), (854, 480), (1280, 720)]
# Parse command-line arguments
parser = argparse.ArgumentParser(description="Compress videos to a specified resolution.")
parser.add_argument("--input_dir", type=str,
                    help="Directory to process videos from")
parser.add_argument("--output_dir", type=str,
                    help="Directory to save compressed videos")
# move_complementary_files = args.input_dir
parser.add_argument("--move", action='store_true',
                    help="move complementary files, ie files that accompany the videos compress to the target dirtree")
# resync_metadata_to_videos = args.output_dir
parser.add_argument("--copystat", action='store_true',
                    help="equalize file dates or resync metadata to videos, ie copy back with (shutil.copystat())"
                         " original video metadata to the compressed video in the target dirtree")
args = parser.parse_args()


def get_totalelapsed_n_avg_inbetween_compressions(begin_time, n_compressions):
  """
  Returns a triple with:
    totalelapsed: the duration of last compression
    average: average duration for all previous compressions
    now: the actual time marking "compress frontiers"
  :return: totalelapsed, average, now
  """
  now = datetime.datetime.now()
  totalelapsed = now - begin_time
  if n_compressions < 1:
    return 0.0, 0.0  # before the first compression, totalelapsed and average are zero
  average = totalelapsed / n_compressions
  return totalelapsed, average


class Log:
  """
  Defines the logging attributes (main) for errorfile logging
  """
  users_home_dir = os.path.expanduser("~")
  log_folder = f"{users_home_dir}/bin/logs"
  log_filename = f"{time.strftime('%Y-%m-%d_%H-%M-%S')} move complem files & copystat erros.log"
  log_filepath = os.path.join(log_folder, log_filename)

  @classmethod
  def start_logging(cls):
    """
    Configure logging (TODO move the hardcoded folderpaths later to somewhere else - a kind of configfile)
      at any rate, the current path picks up the user's home directory and adds to it "bin/logs"
    """
    logging.basicConfig(
      filename=cls.log_filepath,
      level=logging.ERROR,
      format='%(asctime)s - %(levelname)s - %(message)s'
    )


# Function to get video resolution
class FileFromToDirTreeMetadataResyncNMover:

  nonmovable_file_extensions = DEFAULT_COMPRESSABLE_DOT_EXTENSIONS

  def __init__(
      self, src_rootdir_abspath, trg_rootdir_abspath,
      bool_move_complementary_files=False, bool_copystat_for_nonmovable_files=False,
  ):
    self.src_rootdir_abspath = src_rootdir_abspath  # source root directory (or scrdirtree) abspath
    self.trg_rootdir_abspath = trg_rootdir_abspath  # target root directories (or trgdirtree) abspath
    # the complementary files are the video accompanying ones, ie the complement set of eligible_videofile_extensions
    self.bool_move_complementary_files = bool_move_complementary_files
    self.bool_copystat_for_nonmovable_files = bool_copystat_for_nonmovable_files
    self.treat_params()
    self.src_currdir_abspath = None  # its trg equivalent is a class property (i.e., dynamically found)
    # counters
    self.n_dir_in_iter = 0  # counts each directory coming up via os.walk()
    self.total_dirs = 0  # total of directories in the source dirtree
    self.n_movable_file_in_iter = 0  # counts each file coming up via os.walk() and it's movable
    self.n_sel_ext_videofile_in_iter = 0  # counts each file that has the eligible video extensions (mp4, mkv, etc.)
    self._n_file_in_iter = None  # [for @property] equals to n_movable_file_in_iter + n_sel_ext_videofile_in_iter
    self.n_allfiles_in_iter = 0  # the same as the one above but this is counted directly
    self.total_files_to_move = 0  # pre-counted total of movable files
    self.total_selected_videofiles = 0  # pre-counted file total that have the eligible video extensions mp4, mkv, etc
    self._total_files = None  # [for @property] equals to total_selected_videofiles + total_files_to_move
    self.n_files_metadata_copiedover = 0  # number of metadata copies without error
    self.n_files_moved_over = 0  # number of files moved over without error or existence-clash
    self.n_files_not_existing_in_src = 0  # counts videofiles that don't exist in the source (were moved out?)
    self.n_files_not_existing_in_trg = 0  # counts videofiles that don't exist in the source (were moved out?)
    self.n_files_existing_in_trg_when_move = 0  # counts videofiles that exist in the destination
    # counters of (error) exception-caughting
    # ===========
    self.n_failed_files_move_over = 0  # number of move-file attempts caught by the OSError exception
    self.n_failed_metadata_copiedover = 0  # number of shutil.copystat() attempts caught by the OSError exception
    # ===========
    self.begin_time = datetime.datetime.now()  # it marks script's begintime
    self.end_time = None  # will mark script's endtime at the report calling time

  def treat_params(self):
    if not os.path.isdir(self.src_rootdir_abspath):
      errmsg = f"Error: source dirtree path {self.src_rootdir_abspath} does not exist."
      raise ValueError(errmsg)

  @property
  def n_file_in_iter(self):
    """
    equals to n_movable_file_in_iter + n_sel_ext_videofile_in_iter
    """
    if self._n_file_in_iter is None:
      self._n_file_in_iter = self.n_movable_file_in_iter + self.n_sel_ext_videofile_in_iter
    return self._n_file_in_iter

  @property
  def total_files(self):
    """
    equals to total_selected_videofiles + total_files_to_move
    """
    if self._total_files is None:
      self._total_files = self.total_files_to_move + self.total_selected_videofiles
    return self._total_files

  @property
  def relative_working_dirpath(self):
    """
    The relative path is the path beyond srctree_abspath
      and is algorithmically given by a string 'subtraction' so to say, i.e.,
        relative_dirpath = src_currdir_abspath[len(srctree_abspath): ]

    The relative path can then be used to form the target directory absolute path
      that receives the compressed video

    :return _relative_working_dirpath: the relative path as an object's (dynamical) property
    """
    _relative_working_dirpath = self.src_currdir_abspath[len(self.src_rootdir_abspath):]
    # relative_working_dirpath should not begin with / or else it will not work!
    if _relative_working_dirpath.startswith('/'):
      _relative_working_dirpath = _relative_working_dirpath.lstrip('/')
    return _relative_working_dirpath

  @property
  def trg_currdir_abspath(self) -> os.path:
    """
    if the target folder does not exist, create it

    Here are the steps to derive trg_currdir_abspath:
      1) the first interactive variable received from os.walk()
         contains the ongoing abspath
      2) subtracting the srcrootdir from it, one gets the
         relative ongoing dirpath
      3) adding the relative path to trgrootdir, one gets the
         absolute ongoing dirpath
    """
    try:
      _trg_currdir_abspath = os.path.join(self.trg_rootdir_abspath, self.relative_working_dirpath)
    except (OSError, ValueError) as e:
      errmsg = f"In the method that derives the relative working path => {e}"
      raise OSError(errmsg)
    if os.path.isfile(_trg_currdir_abspath):
      errmsg = f"Name {_trg_currdir_abspath} exists as file, program aborting at this point."
      raise OSError(errmsg)
    os.makedirs(_trg_currdir_abspath, exist_ok=True)
    return _trg_currdir_abspath

  def get_curr_output_file_abspath(self, filename: str) -> os.path:
    """
    To get the curr_output_file_abspath,
      add the filename to self.trg_currdir_abspath

    Notice that, by design, the output file must be
      in the same relative path as the input file

    Example:
      a) suppose the following context with directories and files:
        src_abspath = '/media/user/disk1'
        trg_abspath = '/media/user/disk2'
        relativepath = '/sciences/physics/quantum_phys'
        filename = 'quantum_gravity.pdf'

      b) joining the "pieces" of this example, the abspath for the input file is:
        scr_file_abspath = '/media/user/disk1/sciences/physics/quantum_phys/quantum_gravity.pdf'

      b) joining the "pieces" of this example, the abspath for the output file is:
        trg_file_abspath = '/media/user/disk2/sciences/physics/quantum_phys/quantum_gravity.pdf'

      Notice that the only difference is in the root dir-abspath-part,
        relative-path and filename are the same
    """
    curr_output_file_abspath = os.path.join(self.trg_currdir_abspath, filename)
    return curr_output_file_abspath

  def get_curr_input_file_abspath(self, filename) -> os.path:
    """
    @see docstring above for get_curr_output_file_abspath()
    """
    return os.path.join(self.src_currdir_abspath, filename)

  def show_final_report(self):
    self.end_time = datetime.datetime.now()
    elapsed_time = self.end_time - self.begin_time
    scrmsg = f"""=========================================
    Report after videocompressing
    =========================================
    src_rootdir_abspath = {self.src_rootdir_abspath}
    trg_rootdir_abspath = {self.trg_rootdir_abspath}
    error_log_file      = {Log.log_filepath}
    -----------------------------------------
    number of dirs visited = {self.n_dir_in_iter}
    total dirs = {self.total_dirs}
    number of movable files visited = {self.n_movable_file_in_iter}
    total movable files = {self.total_files_to_move}
    number of copystats iterated = {self.n_sel_ext_videofile_in_iter}
    total copystat files = {self.total_selected_videofiles}
    number of files visited = {self.n_file_in_iter}
    total files = {self.total_files}
    -----------------------------------------
    n_failed_files_move_over = {self.n_failed_files_move_over}
    n_failed_metadata_copiedover = {self.n_failed_metadata_copiedover}
    n_files_existing_in_trg_when_move = {self.n_files_existing_in_trg_when_move}
    n_files_not_existing_in_src = {self.n_files_not_existing_in_src}
    n_files_not_existing_in_trg = {self.n_files_not_existing_in_trg}
    -----------------------------------------
    begin_time = {self.begin_time}
    end_time = {self.end_time}
    elapsed_time = {elapsed_time}
    """
    print(scrmsg)

#   def process_command(self, filename):
#     input_file_abspath = self.get_curr_input_file_abspath(filename)

  def is_file_by_its_extension_movable(self, filename):
    """
    This method looks for the complement set of compressable_dot_extensions
      ie the files that do not have the video file extensions
    """
    if filename.endswith(tuple(self.nonmovable_file_extensions)):
      # file is a video, not to be moved in-between dirtrees
      return False
    # file is not a video, to be moved over
    return True

  def is_file_a_video_by_extension(self, filename):
    return not self.is_file_by_its_extension_movable(filename)

  def copystat(self, filename):
    if not self.is_file_a_video_by_extension(filename):
      # file is not a video, so it's not the case for sync'ing dates metadata
      return False
    self.n_files_metadata_copiedover += 1
    numbering = (f"n_file_in_iter={self.n_file_in_iter} | n_movable_file_in_passing={self.n_files_metadata_copiedover}"
                 f" | totalvideos={self.total_selected_videofiles} | totalfiles={self.total_files}")
    scrmsg = f"{numbering} | visiting filename = {filename}"
    print(scrmsg)
    input_file_abspath = self.get_curr_input_file_abspath(filename)
    if not os.path.isfile(input_file_abspath):
      self.n_files_not_existing_in_src += 1
      numbering = (f"n_files_not_existing_in_src={self.n_files_not_existing_in_src}"
                   f" | n_sel_ext_videofile_in_iter={self.n_sel_ext_videofile_in_iter}"
                   f" | total_selected_videofiles={self.total_selected_videofiles} | totalfiles={self.total_files}")
      print(f"{numbering} | file does not exist (or was moved out) in source.")
      return False
    output_file_abspath = self.get_curr_output_file_abspath(filename)
    if not os.path.isfile(output_file_abspath):
      self.n_files_not_existing_in_src += 1
      numbering = (f"n_files_not_existing_in_trg={self.n_files_not_existing_in_trg}"
                   f" | n_sel_ext_videofile_in_iter={self.n_sel_ext_videofile_in_iter}"
                   f" | total_selected_videofiles={self.total_selected_videofiles} | totalfiles={self.total_files}")
      print(f"{numbering} | file does not exist (or was moved out) in target.")
      return False
    try:
      scrmsg = f"\tCopying stat:"
      print(scrmsg)
      scrmsg = f"\tfrom: {input_file_abspath}"
      print(scrmsg)
      shutil.copystat(input_file_abspath, output_file_abspath)
      return True
    except (OSError, IOError) as e:
      # logging and printing the error context
      self.n_failed_metadata_copiedover += 1
      strline = "-" * 35
      print(strline)
      logging.error(strline)
      numbering = (f"n_failed_metadata_copiedover={self.n_failed_metadata_copiedover}"
                   f" | n_sel_ext_videofile_in_iter={self.n_sel_ext_videofile_in_iter}"
                   f" | total_selected_videofiles={self.total_selected_videofiles} | totalfiles={self.total_files}")
      errmsg = f"{numbering} | filepath = {input_file_abspath}) \n\tError = {e}"
      logging.error(errmsg)
      print(errmsg)
      return False

  def move_files_from_to_dirtrees(self, filename):
    """
    Moves files from the source dirtree to the destination dirtree
    This method is called if self.do_move is True

    :param filename:
    :return: bool_moved_occurred: boolean
    """
    if not self.is_file_by_its_extension_movable(filename):
      # file is not eligible for moving because it's a video looked up for compression (in a previous script)
      return False
    self.n_movable_file_in_iter += 1
    numbering = (f"n_file_in_iter={self.n_file_in_iter} | n_movable_file_in_passing={self.n_movable_file_in_iter}"
                 f" | totalvideos={self.total_selected_videofiles} | totalfiles={self.total_files}")
    scrmsg = f"{numbering} | visiting filename = {filename}"
    print(scrmsg)
    input_file_abspath = self.get_curr_input_file_abspath(filename)
    output_file_abspath = self.get_curr_output_file_abspath(filename)
    if not os.path.isfile(input_file_abspath):
      self.n_files_not_existing_in_src += 1
      numbering = (f"not-moved not-exist={self.n_files_not_existing_in_src} | movableiter={self.n_movable_file_in_iter}"
                   f" | totalmovable={self.total_files_to_move} | totalfiles={self.total_files}")
      print(f"{numbering} | file does not exist (or was moved out) in source.")
      return False
    if os.path.isfile(output_file_abspath):
      self.n_files_existing_in_trg_when_move += 1
      numbering = (f"n_files_existing_in_trg={self.n_files_existing_in_trg_when_move}"
                   f" | movableiter={self.n_movable_file_in_iter}"
                   f" | movabletotal={self.total_files_to_move} | totalfiles={self.total_files}")
      print(f"{numbering} | video filename already exists in target.")
      return False
    try:
      shutil.move(input_file_abspath, output_file_abspath)
      return True
    except (OSError, IOError) as e:
      # logging and printing the error context
      self.n_failed_files_move_over += 1
      strline = "-" * 35
      print(strline)
      logging.error(strline)
      numbering = (f"n_failed_files_move_over={self.n_failed_files_move_over}"
                   f" | movableiter={self.n_movable_file_in_iter}"
                   f" | moveabletotal={self.total_files_to_move} | totalfiles={self.total_files}")
      errmsg = f"{numbering} | filepath = {input_file_abspath}) \n\tError = {e}"
      logging.error(errmsg)
      print(errmsg)
      return False

  def copystat_and_or_move_files_if_case(self, files):
    for filename in files:
      self.n_allfiles_in_iter += 1
      if self.bool_move_complementary_files:
        self.move_files_from_to_dirtrees(filename)
      if self.bool_copystat_for_nonmovable_files:
        self.copystat(filename)

  def process_in_oswalk(self):
    self.n_sel_ext_videofile_in_iter = 0
    self.n_movable_file_in_iter = 0
    # self.n_file_in_iter is dynamically obtain by summing the former two counters
    for self.src_currdir_abspath, _, files in os.walk(self.src_rootdir_abspath):
      self.n_dir_in_iter += 1
      n_files_in_dir = len(files)
      scrmsg = (f"n_dir_in_iter={self.n_dir_in_iter} | total_dirs={self.total_dirs} "
                f"| going to check {n_files_in_dir} files in [{self.src_currdir_abspath}]")
      print(scrmsg)
      self.copystat_and_or_move_files_if_case(files)

  def confirm_videoprocessing_with_the_counting(self):
    scrmsg = f""" =========================
    *** Confirmation needed
    Confirm the videocompressing with the following counts:
      log_filepath    = {Log.log_filepath}
      source root dir = {self.src_rootdir_abspath}
      target root dir = {self.trg_rootdir_abspath}
      target extensions = {self.nonmovable_file_extensions}
      total_files       = {self.total_files}
      n_dirs_for_reaching = {self.total_dirs}
      n_videos_yet_to_statsync = n/a
      n_non_videos_yet_to_move = n/a
    -----------------------
    [Y/n] ? [ENTER] means Yes
    """
    ans = input(scrmsg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def confirm_queued_files_deletion(self):
    scrmsg = f""" =========================
    *** Confirmation needed
    Confirm deletion of these files:
      log_filepath    = {Log.log_filepath}
      source root dir = {self.src_rootdir_abspath}
      target root dir = {self.trg_rootdir_abspath}
      target extensions = {self.nonmovable_file_extensions}
      total_files       = {self.total_files}
      n_dirs_to_process = {self.total_dirs}
      n_videos_yet_to_compress = n/a
    -----------------------
    [Y/n] ? [ENTER] means Yes
    """
    ans = input(scrmsg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def count_dir_files_under_selected_extensions(self, files):
    n_files_under_selected_extensions = 0
    for filename in files:
      if filename.endswith(tuple(self.nonmovable_file_extensions)):
        n_files_under_selected_extensions += 1
    return n_files_under_selected_extensions

  def precount_dirs_n_files(self):
    print("Precounting dirs & files for videocompressing")
    print(f"\tLooking and counting those with these extensions: {self.nonmovable_file_extensions}")
    print("\tPlease, wait counting directories and files up dirtree")
    self.total_dirs = 0
    self.total_files_to_move = 0
    self.total_selected_videofiles = 0
    for self.src_currdir_abspath, _, files in os.walk(self.src_rootdir_abspath):
      n_videos_w_selected_extension = self.count_dir_files_under_selected_extensions(files)
      if n_videos_w_selected_extension > 0:
        self.total_dirs += 1
        self.total_selected_videofiles += n_videos_w_selected_extension
        n_dirfiles_to_move = len(files) - n_videos_w_selected_extension
        self.total_files_to_move += n_dirfiles_to_move
        scrmsg = f" => In directory [{self.src_currdir_abspath}]"
        print(scrmsg)
        scrmsg = f" => n_movable={n_dirfiles_to_move} n_copystat={n_videos_w_selected_extension}"
        print(scrmsg)

  def process(self):
    """

    """
    if not self.bool_move_complementary_files and not self.bool_copystat_for_nonmovable_files:
      scrmsg = f"""
      No action chosen:
      
      -----------------
      1) use parameter --move for "move complementary files"
         from source dirtree to destination dirtree
      
      -----------------
      2) use parameter --copystat for "videocompressed metadata resync"
         (or resync whatever files under the selected extensions [videos or not])
         copying the metadata in the source dirtree files to destination dirtree"""
      print(scrmsg)
      return False
    self.precount_dirs_n_files()
    self.process_in_oswalk()
    if not self.confirm_queued_files_deletion():
      return False
    self.show_final_report()
    return True


def get_cli_args():
  """
  Required parameters:
    src_rootdir_abspath & trg_rootdir_abspath

  Optional parameter:
    resolution_tuple

  :return: srctree_abspath, trg_rootdir_abspath, resolution_tuple
  """
  try:
    if args.h or args.help:
      print(__doc__)
      sys.exit(0)
  except AttributeError:
    pass
  src_rootdir_abspath = args.input_dir
  trg_rootdir_abspath = args.output_dir
  move_complementary_files = args.move
  resync_metadata_to_videos = args.copystat
  return src_rootdir_abspath, trg_rootdir_abspath, move_complementary_files, resync_metadata_to_videos


def confirm_cli_args_with_user():
  src_rootdir_abspath, trg_rootdir_abspath, move_complementary_files, resync_metadata_to_videos = get_cli_args()
  print(src_rootdir_abspath, trg_rootdir_abspath, move_complementary_files, resync_metadata_to_videos)
  if src_rootdir_abspath is None or not os.path.isdir(src_rootdir_abspath):
    errmsg = f"Source directory [{src_rootdir_abspath}] does not exist. Please, run --help for params or retry."
    raise OSError(errmsg)
  if trg_rootdir_abspath is None or not os.path.isdir(trg_rootdir_abspath):
    errmsg = f"Target directory [{trg_rootdir_abspath}] does not exist. Please, run --help for params or retry."
    raise OSError(errmsg)
  print('Paramters')
  print('='*20)
  scrmsg = f"Source directory = [{src_rootdir_abspath}]"
  print(scrmsg)
  scrmsg = f"Target directory = [{trg_rootdir_abspath}]"
  print(scrmsg)
  scrmsg = f"move_complementary_files = [{move_complementary_files}]"
  print(scrmsg)
  scrmsg = f"resync_metadata_to_videos = [{resync_metadata_to_videos}]"
  print(scrmsg)
  scrmsg = f"Error-log file = [{Log.log_filepath}]"
  print(scrmsg)
  print('='*20)
  scrmsg = "The parameters are okay? (Y/n) [ENTER] means Yes "
  ans = input(scrmsg)
  print('='*20)
  confirmed = False
  if ans in ['Y', 'y', '']:
    confirmed = True
  return confirmed, src_rootdir_abspath, trg_rootdir_abspath, move_complementary_files, resync_metadata_to_videos


def adhoc_test2():
  now = datetime.datetime.now()
  begin_time = datetime.datetime.now() - datetime.timedelta(hours=1)
  n_passing = 4
  totalelapsed, avg = get_totalelapsed_n_avg_inbetween_compressions(begin_time, n_passing)
  scrmsg = f"totalelapsed={totalelapsed}, avg={avg}, now={now}, n_passing={n_passing}, begin={begin_time}"
  print(scrmsg)


def adhoc_test1():
  src_rootdir_abspath = args.input_dir
  trg_rootdir_abspath = args.output_dir
  print('src_rootdir_abspath', src_rootdir_abspath)
  print('trg_rootdir_abspath', trg_rootdir_abspath)
  for ongoing, dirs, files in os.walk(src_rootdir_abspath):
    print('ongoing', ongoing)
    relpath = ongoing[len(src_rootdir_abspath):]
    print('relpath', relpath)
    print('dirs', dirs)
    print('files', files[:2])


def process():
  Log.start_logging()
  tuple5d = confirm_cli_args_with_user()
  confirmed, src_rootdir_abspath, trg_rootdir_abspath, move_complementary_files, copystat_for_nonmovable_files = tuple5d
  if confirmed:
    moveretc = FileFromToDirTreeMetadataResyncNMover(
      src_rootdir_abspath=src_rootdir_abspath,
      trg_rootdir_abspath=trg_rootdir_abspath,
      bool_move_complementary_files=move_complementary_files,
      bool_copystat_for_nonmovable_files=copystat_for_nonmovable_files,
    )
    moveretc.process()
    return True
  logging.shutdown()
  return False


if __name__ == '__main__':
  """
  adhoc_test1()
  adhoc_test2()
  """
  process()
