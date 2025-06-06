#!/usr/bin/env python3
"""
commands/pyVideoCompressAfterDeleter.py
  Deletes videos with selected-file-extension
  (Ideally, these videos were compressed in a former run and may then be removed (deleted).

Usage:
======

$pyVideoPostCompressMoverNDeleter.py --input-dir <source_dirtree_abspath>
   --output-dir <tareget_dirtree_abspath> [--ed] [--mc] [--dc]

Where:

  --dc => means "delete (those) compressed",
          ie delete files that have their compressed counterparts in the target dirtree
           (for this batch deletion, one user confirmation is required)
  --ed => means "equalize os-dates",
          copystat() the metadata in the source videofiles to the target previously compressed ones
  --mc => means "move complement",
          ie all complementing files (*) are moved to the target dirtree
            (*) complementing files are all files not having the video file extensions


Example Usage
==============

  Ex1 $pyVideoPostCompressMoverNDeleter.py --input-dir "/media/user/disk1/Science/Physics"
   --output-dir "/media/user/disk2/Science/Physics" --ed --mc

  Ex2 $pyVideoPostCompressMoverNDeleter.py --input-dir "/media/user/disk1/Science/Physics"
   --output-dir "/media/user/disk2/Science/Physics" --dc

  In the first example, os-dates will be equalized and all complementing files
    will be moved over the target dirtree

  In the second example, larger videofiles that were (previously) compressed
     will be deleted (a one-time user confirmation is required)

"""
import argparse
import datetime
import logging
import os
import shutil
import subprocess
import sys
import time
DEFAULT_RESOLUTION_WIDTH_HEIGHT = (256, 144)  # 256:144
DEFAULT_COMPRESSABLE_DOT_EXTENSIONS = [".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v"]
ACCEPTED_RESOLUTIONS = [(256, 144), (426, 240), (640, 360), (854, 480), (1280, 720)]
# Parse command-line arguments
parser = argparse.ArgumentParser(description="Compress videos to a specified resolution.")
parser.add_argument("--input_dir", type=str, default="videos/",
                    help="Directory to process videos from")
parser.add_argument("--output_dir", type=str, default="compressed_videos/",
                    help="Directory to save compressed videos")
parser.add_argument("--resolution", type=str, default="256:144",
                    help="Target resolution (e.g., 256:144)")
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
  log_filename = f"{time.strftime('%Y-%m-%d_%H-%M-%S')} videos deleter errors.log"
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


def extract_width_n_height_from_cli_resolution_arg(p_args):
  """
  Extract target width and height from CLI argument
  Used named p_args for argument, because the IDE notices it as a shadow name
    (i.e., there is a variable called 'args' in the program scope below)

  The name of the CLI argument is 'resolution' (if it's changed, it should be updated here)

  :param p_args: the CLI argument object
  :return resolution_tuple: a double (2-tuple) consisting of target_width, target_height
  """
  try:
    target_width, target_height = map(int, p_args.resolution.split(":"))
    resolution_tuple = target_width, target_height
    if resolution_tuple not in ACCEPTED_RESOLUTIONS:
      scrmsg = f"Resolution format not in list {ACCEPTED_RESOLUTIONS}, plase retry."
      print(scrmsg)
      exit(1)
    return resolution_tuple
  except AttributeError:
    return DEFAULT_RESOLUTION_WIDTH_HEIGHT
  except ValueError:
    scrmsg = "Invalid resolution format. Please use WIDTH:HEIGHT (e.g., 256:144)."
    print(scrmsg)
    exit(1)
  # no returning from here, this last line is unreachable (IDE confirmed!)


# Function to get video resolution
def get_actual_video_resolution_of(video_path):
  cmd = [
    "ffprobe", "-v", "error",
    "-select_streams", "v:0",
    "-show_entries", "stream=width,height",
    "-of", "csv=p=0", video_path
  ]
  try:
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    width, height = map(int, result.stdout.strip().split(","))
    return width, height
  except Exception as e:
    print(f"Error checking resolution for {video_path}: {e}")
  return None, None


class FileMoveNDeleterFromToDirTree:

  compressable_dot_extensions = DEFAULT_COMPRESSABLE_DOT_EXTENSIONS

  def __init__(
      self, src_rootdir_abspath, trg_rootdir_abspath,
      do_move=True, replicate_osdates=False, delete_if_compressed=False,
  ):
    self.src_rootdir_abspath = src_rootdir_abspath  # source root directory (or scrdirtree) abspath
    self.trg_rootdir_abspath = trg_rootdir_abspath  # target root directories (or trgdirtree) abspath
    self.do_move = do_move
    self.replicate_osdates = replicate_osdates
    self.delete_if_compressed = delete_if_compressed
    self.treat_params()
    self.src_currdir_abspath = None  # its trg equivalent is a class property (i.e., dynamically found)
    self.files_deletion_queue = []
    self.n_file_does_not_exist = 0
    self.n_videos_yet_to_delete = 0
    self.n_files_not_deleted = 0
    self.n_failed_delete = 0
    self.total_files = 0
    self.n_file_passing = 0  # counts each file coming up via os.walk()
    self.n_video_passing = 0  # counts each file that has the eligible video extensions (mp4, mkv, etc.)
    self.n_videos_in_dirtree = 0  # counts each file that has the eligible video extensions (mp4, mkv, etc.)
    self.n_dir_passing = 0  # counts each directory coming up via os.walk()
    self.n_dirs_for_reaching = 0
    self.n_videos_for_deletion = 0
    self.n_files_moved_over = 0
    self.n_videos_out_of_files = 0
    self.n_not_videodeletable = 0
    self.n_files_metadata_recovered = 0  # originally, the metadata recovery is aimed at videos previously compressed
    self.n_failed_files_moved_over = 0
    self.n_failed_file_deletes = 0
    self.n_files_not_existing_in_src = 0  # counts videofiles that don't exist in the source (were moved out?)
    self.n_files_not_existing_in_trg = 0  # counts videofiles that don't exist in the source (were moved out?)
    self.begin_time = datetime.datetime.now()  # it marks script's begintime
    self.end_time = None  # will mark script's endtime at the report calling time

  def treat_params(self):
    if not os.path.isdir(self.src_rootdir_abspath):
      errmsg = f"Error: source dirtree path {self.src_rootdir_abspath} does not exist."
      raise ValueError(errmsg)

  @property
  def relative_working_dirpath(self):
    """
    The relative path is the path beyond srctree_abspath
      and is given by a 'subtraction' so to say, i.e.,
        relative_dirpath = src_currdir_abspath[len(srctree_abspath): ]

    relative path can then be used to form the target directory
      that receives the compressed video
    :return _relative_working_dirpath: the relative path as an object's (dynamical) property
    """
    _relative_working_dirpath = self.src_currdir_abspath[len(self.src_rootdir_abspath):]
    # relative_working_dirpath should not begin with /
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
    total dirs visited = {self.n_dir_passing}
    total dirs for processing = {self.n_dirs_for_reaching}
    total files visited = {self.n_file_passing}
    total videos visited = {self.n_video_passing}
    total files = {self.total_files}
    total videos in dirtree = {self.n_videos_in_dirtree}
    -----------------------------------------
    begin_time = {self.begin_time}
    end_time = {self.end_time}
    elapsed_time = {elapsed_time}
    """
    print(scrmsg)

  def process_command(self, filename):
    input_file_abspath = self.get_curr_input_file_abspath(filename)

  def is_file_by_extension_eligible_to_move(self, filename):
    if filename.endswith(tuple(self.compressable_dot_extensions)):
      return False
    return True

  def move_files_from_to_dirtrees(self, filename):
    """
    Moves files from the source dirtree to the destination dirtree
    This method is called if self.do_move is True

    :param filename:
    :return: bool_moved_occurred: boolean
    """
    if not self.is_file_by_extension_eligible_to_move(filename):
      # the file is not eligible for moving because it's a video looked up for compression
      return False
    numbering = (f"passing={self.n_file_passing} | videosprocessed={self.n_video_passing}"
                 f" | totalvideos={self.n_videos_in_dirtree} | totalfiles={self.total_files}")
    scrmsg = f"{numbering} | visiting filename = {filename}"
    print(scrmsg)
    input_file_abspath = self.get_curr_input_file_abspath(filename)
    output_file_abspath = self.get_curr_output_file_abspath(filename)
    if not os.path.isfile(input_file_abspath):
      self.n_file_does_not_exist += 1
      numbering = (f"not-copied={self.n_file_does_not_exist} | reached={self.n_file_passing}"
                   f" | total={self.n_videos_for_deletion}")
      print(f"{numbering} | video file does not exist (or was moved out) in source.")
      return False
    if os.path.isfile(output_file_abspath):
      self.n_videos_for_deletion += 1
      numbering = (f"not-copied={self.n_videos_skipped} | reached={self.n_file_passing}"
                   f" | total={self.n_videos_for_compression}")
      print(f"{numbering} | video filename already exists in target.")
      return False
    try:
      # shutil.move(input_file_abspath, output_file_abspath)
      return True
    except (OSError, IOError) as e:
      # logging and printing the error context
      self.n_failed_files_moved_over += 1
      strline = "-" * 35
      print(strline)
      logging.error(strline)
      numbering = (f"failed_copy={self.n_failed_videos_copied_over} | reached={self.n_file_passing}"
                   f" | total={self.n_videos_for_compression}")
      errmsg = f"{numbering} | videopath = {input_file_abspath}) \n\tError = {e}"
      logging.error(errmsg)
      print(errmsg)
      return False

  def was_chosen_complementary_move(self, filename):
    if not self.do_move:
      return False
    return self.move_files_from_to_dirtrees(filename)

  def was_chosen_videofile_metadata_recuperation(self, filename):
    """

    :param filename:
    :return:
    """
    if not self.do_recuperate_metadata:
      return False
    return self.recuperate_metadata(filename)

  def delete_files_in_queue(self):
    for seq, to_delete_filepath in enumerate(self.files_deletion_queue):
      try:
        os.remove(to_delete_filepath)
        self.n_deleted += 1
        scrmsg = f"\tDeleting {self.n_deleted} | {seq} | file=[{to_delete_filepath}]"
        print(scrmsg)
      except (OSError, IOError) as e:
        # logging and printing the error context
        self.n_failed_delete += 1
        strline = "-" * 35
        print(strline)
        logging.error(strline)
        numbering = (f"failed_delete={self.n_failed_delete} | reached={self.n_file_passing}"
                     f" | totalfiles={self.total_files}")
        errmsg = f"{numbering} | file_to_delete = {to_delete_filepath} \n\tError = {e}"
        logging.error(errmsg)
        print(errmsg)

  def queue_up_target_file_for_later_deletion(self, filename):
    """
    Deletes files in the destination that also namewise exist in the source,
      i.e., what matters is the mere filename presence,
      a hash or any content checking is not done

    Use this method with caution, for the deletes cannot be undoned

    The rationale here is to delete videofiles that have been previously compressed.
    Because a quality check of the compress is still not developed,
      the ideal is that the user may have taken a sampling inspection of the compressed videos

    :param filename:
    :return:
    """
    # input_file_abspath = self.get_curr_input_file_abspath(filename)
    target_file_abspath = self.get_curr_output_file_abspath(filename)
    if not os.path.isfile(target_file_abspath):
      self.n_files_not_deleted += 1
      numbering = (f"not-deletedd={self.n_files_not_deleted} | reached={self.n_file_passing}"
                   f" | videos_in_dirtree={self.n_videos_in_dirtree}")
      print(f"{numbering} | video file does not exist (or was moved out) in source.")
      return False
    try:  # copy this part to the real delete method
      # os.remove(target_file_abspath)
      self.files_deletion_queue.append(target_file_abspath)
      return True
    except (OSError, IOError) as e:
      # logging and printing the error context
      self.n_failed_delete += 1
      strline = "-"*35
      print(strline)
      logging.error(strline)
      numbering = (f"failed_delete={self.n_failed_delete} | reached={self.n_file_passing}"
                   f" | totalfiles={self.n_total_files}")
      errmsg = f"{numbering} | videopath = {target_file_abspath}) \n\tError = {e}"
      logging.error(errmsg)
      print(errmsg)
      return False

  def verify_queuedelete_for_a_previously_processed_videofile(self, filename):
    if filename.endswith(tuple(self.compressable_dot_extensions)):
      return self.queue_up_target_file_for_later_deletion(filename)
    # count file is not delete-queueable
    self.n_not_videodeletable += 1

  def process_files_in_folder(self, files):
    for filename in files:
      self.verify_queuedelete_for_a_previously_processed_videofile(filename)

  def process_in_oswalk(self):
    self.n_file_passing = 0
    for self.src_currdir_abspath, _, files in os.walk(self.src_rootdir_abspath):
      self.n_dir_passing += 1
      n_files_in_dir = len(files)
      scrmsg = (f"Visited dirs {self.n_dir_passing} | total dirs {self.n_dirs_for_compression} "
                f"| going to check {n_files_in_dir} files in [{self.src_currdir_abspath}]")
      print(scrmsg)
      self.process_files_in_folder(files)

  def confirm_videoprocessing_with_the_counting(self):
    scrmsg = f""" =========================
    *** Confirmation needed
    Confirm the videocompressing with the following counts:
      log_filepath    = {Log.log_filepath}
      source root dir = {self.src_rootdir_abspath}
      target root dir = {self.trg_rootdir_abspath}
      target extensions = {self.compressable_dot_extensions}
      total_files       = {self.total_files}
      n_dirs_to_process = {self.n_dirs_for_compression}
      n_videos_for_compression = {self.n_videos_for_compression}
      n_videos_yet_to_compress = {self.n_videos_yet_to_compress}
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
      target extensions = {self.compressable_dot_extensions}
      total_files       = {self.total_files}
      n_dirs_to_process = {self.n_dirs_for_reaching}
      ! n_files_not_deleted = {self.n_files_not_deleted}
      n_videos_yet_to_delete = {self.n_videos_yet_to_delete}
    -----------------------
    [Y/n] ? [ENTER] means Yes
    """
    ans = input(scrmsg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def count_grouped_files_under_video_extensions(self, files):
    n_videos_for_compression = 0
    for filename in files:
      self.total_files += 1
      if filename.endswith(tuple(self.compressable_dot_extensions)):
        n_videos_for_compression += 1
        scrmsg = f"{n_videos_for_compression} counting, {self.n_videos_for_compression} counted"
        print(scrmsg)
        scrmsg = f"{filename} in [{self.src_currdir_abspath}]"
        print(scrmsg)
    return n_videos_for_compression

  def precount_dirs_n_files(self):
    print("Precounting dirs & files for videocompressing")
    print(f"looking and counting those with extensions: {self.compressable_dot_extensions}")
    self.total_files = 0
    self.n_dirs_for_reaching = 0
    self.n_videos_out_of_files = 0
    for self.src_currdir_abspath, _, files in os.walk(self.src_rootdir_abspath):
      n_videos_out_of_files = self.count_grouped_files_under_video_extensions(files)
      if n_videos_out_of_files > 0:
        self.n_dirs_for_reaching += 1
        self.n_videos_out_of_files += n_videos_out_of_files

  def process(self):
    """

    """
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
  resolution_tuple = extract_width_n_height_from_cli_resolution_arg(args)
  return src_rootdir_abspath, trg_rootdir_abspath, resolution_tuple


def confirm_cli_args_with_user():
  src_rootdir_abspath, trg_rootdir_abspath, resolution_tuple = get_cli_args()
  print(src_rootdir_abspath, trg_rootdir_abspath, resolution_tuple)
  if not os.path.isdir(src_rootdir_abspath):
    scrmsg = "Source directory [{src_rootdir_abspath}] does not exist. Please, retry."
    print(scrmsg)
    return False
  if not os.path.isdir(trg_rootdir_abspath):
    scrmsg = "Target directory [{trg_rootdir_abspath}] does not exist. Please, retry."
    print(scrmsg)
    return False
  print('Paramters')
  print('='*20)
  scrmsg = f"Source directory = [{src_rootdir_abspath}]"
  print(scrmsg)
  scrmsg = f"Target directory = [{trg_rootdir_abspath}]"
  print(scrmsg)
  scrmsg = f"Resolution = [{resolution_tuple}]"
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
  return confirmed, src_rootdir_abspath, trg_rootdir_abspath, resolution_tuple


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
  confirmed, src_rootdir_abspath, trg_rootdir_abspath, resolution_tuple = confirm_cli_args_with_user()
  if confirmed:
    moveretc = FileMoveNDeleterFromToDirTree(src_rootdir_abspath, trg_rootdir_abspath, resolution_tuple)
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
