#!/usr/bin/env python3
"""
commands/pyVideoCompressAfterDeleter.py
  Deletes files with selected-file-extension from up a directory tree
   The original use case was for videofiles videocompressed to another dirtree
     in a former run and then available for removal (deletion).)

Usage:
======

$pyVideoCompressAfterDeleter.py
   --delfiles_rootdir <rootdir_from_where_deletes_will_happen>
   --mirrored_rootdir <rootdir_which_contains_the_mirror_copied_or_compressed>

Where:

  --delfiles_rootdir => the root directory from which out-mirrored (copied or compressed) files will be deleted
  --mirrored_rootdir => the root directory that contains the mirrored (copied or compressed) files

Example Usage
==============

$pyVideoCompressAfterDeleter.py
   --delfiles_rootdir "/media/user/SSD EEEngSci D1"
   --mirrored_rootdir "/media/user/EESci SSD2T orig"

"""
import argparse
import datetime
import logging
import os
import subprocess
import sys
import time
DEFAULT_RESOLUTION_WIDTH_HEIGHT = (256, 144)  # 256:144
DEFAULT_COMPRESSABLE_DOT_EXTENSIONS = [".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v"]
ACCEPTED_RESOLUTIONS = [(256, 144), (426, 240), (640, 360), (854, 480), (1280, 720)]
# Parse command-line arguments
parser = argparse.ArgumentParser(description="Compress videos to a specified resolution.")
parser.add_argument("--delfiles_rootdir", type=str, default="videos/",
                    help="Directory delfiles_rootdir")
parser.add_argument("--mirrored_rootdir", type=str, default="compressed_videos/",
                    help="Directory ")
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
  log_filename = f"{time.strftime('%Y-%m-%d_%H-%M-%S')} file under extensions deleter errors.log"
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


class FileDeleterAsMirroredInDirTrees:

  dot_extensions_for_deletion = DEFAULT_COMPRESSABLE_DOT_EXTENSIONS

  def __init__(self, delfiles_rootdir, mirrored_rootdir):
    self.delfiles_rootdir = delfiles_rootdir  # source root directory (or scrdirtree) abspath
    self.mirrored_rootdir = mirrored_rootdir  # target root directories (or trgdirtree) abspath
    self.treat_params()
    self.src_currdir_abspath = None  # its trg equivalent is a class property (i.e., dynamically found)
    self.files_deletion_queue = []
    self.n_videos_yet_to_delete = 0  # an iter variable ie a variable for displaying progress of how many yet to go
    self.n_effective_deletes = 0
    self.n_failed_delete = 0
    self.total_files = 0
    self.n_files_in_dir_for_deletion = 0
    self.n_dirs_in_iter = 0
    self.total_dirs = 0
    self.n_file_in_iter = 0  # counts each file coming up via os.walk()
    self.n_deletable_file_in_iter = 0  # counts each file that has the eligible video extensions (mp4, mkv, etc.)
    self.total_deletable = 0  # counts each file that has the eligible video extensions (mp4, mkv, etc.)
    self.n_dir_in_iter = 0  # counts each directory coming up via os.walk()
    self.n_dirs_for_reaching = 0
    # self.n_files_in_for_deletion = 0 this is a property as len(files_deletion_queue)
    # self.n_not_videodeletable = 0 this is also a property as total_files - total_videos
    self.n_failed_file_deletes = 0
    self.n_files_not_existing_in_src = 0
    self.n_files_not_existing_in_trg = 0
    self.begin_time = datetime.datetime.now()  # it marks script's begintime
    self.end_time = None  # will mark script's endtime at the report calling time

  def treat_params(self):
    if not os.path.isdir(self.delfiles_rootdir):
      errmsg = f"Error: source dirtree path {self.delfiles_rootdir} does not exist."
      raise ValueError(errmsg)

  @property
  def n_files_in_deletion_queue(self):
    return len(self.files_deletion_queue)

  @property
  def n_files_not_deletable(self):
    return self.total_files - self.total_deletable

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
    _relative_working_dirpath = self.src_currdir_abspath[len(self.delfiles_rootdir):]
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
      _trg_currdir_abspath = os.path.join(self.mirrored_rootdir, self.relative_working_dirpath)
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
    src_rootdir_abspath = {self.delfiles_rootdir}
    trg_rootdir_abspath = {self.mirrored_rootdir}
    error_log_file      = {Log.log_filepath}
    -----------------------------------------
    total dirs visited = {self.n_dir_in_iter}
    total dirs for processing = {self.n_dirs_for_reaching}
    total files visited = {self.n_file_in_iter}
    total videos visited = {self.n_deletable_file_in_iter}
    total files = {self.total_files}
    total videos in dirtree = {self.total_deletable}
    -----------------------------------------
    begin_time = {self.begin_time}
    end_time = {self.end_time}
    elapsed_time = {elapsed_time}
    """
    print(scrmsg)

  def is_file_by_extension_eligible_to_move(self, filename):
    if filename.endswith(tuple(self.dot_extensions_for_deletion)):
      return False
    return True

  def delete_files_in_queue(self):
    for seq, to_delete_filepath in enumerate(self.files_deletion_queue):
      try:
        os.remove(to_delete_filepath)
        self.n_effective_deletes += 1
        scrmsg = f"\t{seq}/{self.n_effective_deletes} => deleting | file=[{to_delete_filepath}]"
        print(scrmsg)
      except (OSError, IOError) as e:
        # logging and printing the error context
        self.n_failed_delete += 1
        strline = "-" * 35
        print(strline)
        logging.error(strline)
        numbering = (f"failed_delete={self.n_failed_delete} | reached={self.n_file_in_iter}"
                     f" | totalfiles={self.total_files}")
        errmsg = f"{numbering} | file_to_delete = {to_delete_filepath} \n\tError = {e}"
        logging.error(errmsg)
        print(errmsg)

  def check_existence_of_mirrored_file_ifnot_return_false(self, filename):
    mirrored_file_abspath = self.get_curr_output_file_abspath(filename)
    if not os.path.isfile(mirrored_file_abspath):
      self.n_files_not_existing_in_trg += 1
      numbering = (f"n_files_not_deletable={self.n_files_not_deletable} | n_file_in_iter={self.n_file_in_iter}"
                   f" | total_deletable={self.total_deletable}")
      errmsg = f"""Error: mirrored file of its deletable does not exist. Cannot continue. Aborting program.
      {numbering}
      filepath = {mirrored_file_abspath}
      """
      logging.error(errmsg)
      print(errmsg)
      return False
    return True

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
    if not filename.endswith(tuple(self.dot_extensions_for_deletion)):
      return False
    deletable_file_abspath = self.get_curr_input_file_abspath(filename)
    if not os.path.isfile(deletable_file_abspath):
      self.n_files_not_existing_in_src += 1
      numbering = (f"n_files_not_deletable={self.n_files_not_deletable} | n_file_in_iter={self.n_file_in_iter}"
                   f" | total_deletable={self.total_deletable}")
      errmsg = f"""Error: deletable file does not exist. Cannot continue. Aborting program.
      {numbering}
      filepath = {deletable_file_abspath}
      """
      raise OSError(errmsg)
    if not self.check_existence_of_mirrored_file_ifnot_return_false(filename):
      return False
    self.n_deletable_file_in_iter += 1
    scrmsg = f"n_deletable_file_in_iter={self.n_deletable_file_in_iter} => queueing file [{filename}]"
    print(scrmsg)
    self.files_deletion_queue.append(deletable_file_abspath)
    return True

  def queue_up_deletable_files_for_later_deletion(self, files):
    for filename in files:
      self.n_file_in_iter += 1
      self.queue_up_target_file_for_later_deletion(filename)

  def process_in_oswalk(self):
    self.n_file_in_iter = 0
    for self.src_currdir_abspath, _, files in os.walk(self.delfiles_rootdir):
      self.n_dir_in_iter += 1
      n_files_in_dir = len(files)
      scrmsg = (f"n_dir_in_iter={self.n_dir_in_iter} | total dirs {self.total_dirs} "
                f"| going to check {n_files_in_dir} files in [{self.src_currdir_abspath}]")
      print(scrmsg)
      self.queue_up_deletable_files_for_later_deletion(files)

  def confirm_videoprocessing_with_the_counting(self):
    scrmsg = f""" =========================
    *** Confirmation needed
    Confirm the videocompressing with the following counts:
      log_filepath    = {Log.log_filepath}
      source root dir = {self.delfiles_rootdir}
      target root dir = {self.mirrored_rootdir}
      target extensions = {self.dot_extensions_for_deletion}
      total_files       = {self.total_files}
      total_dirs        = {self.total_dirs}
      n_files_for_deletion = {self.n_files_in_deletion_queue}
    -----------------------
    [Y/n] ? [ENTER] means Yes
    """
    ans = input(scrmsg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def do_delete(self):
    self.n_effective_deletes = 0
    total_deletable = len(self.files_deletion_queue)
    for i, filepath in enumerate(self.files_deletion_queue):
      seq = i + 1
      numbering = f"{self.n_effective_deletes}/{seq} of {total_deletable} of {self.total_files}"
      try:
        os.remove(filepath)
        self.n_effective_deletes += 1
        scrmsg = f"{numbering} => deleted [{filepath}]"
        print(scrmsg)
      except (OSError, IOError) as e:
        self.n_failed_delete += 1
        errmsg = f"{self.n_failed_delete} {numbering} \n => failed delete for {filepath} \n {e}"
        logging.error(errmsg)
        print(errmsg)

  def confirm_queued_files_deletion(self):
    for i, filepath in enumerate(self.files_deletion_queue):
      seq = i + 1
      scrmsg = f"{seq} to delete [{filepath}]"
      print(scrmsg)
    scrmsg = f""" =========================
    *** Confirmation needed
    -----------------------
      log_filepath    = {Log.log_filepath}
      source root dir = {self.delfiles_rootdir}
      target root dir = {self.mirrored_rootdir}
      target extensions = {self.dot_extensions_for_deletion}
      total_files     = {self.total_files}
      total_dirs      = {self.total_dirs}
      n_deletable      = {len(self.files_deletion_queue)}
    -----------------------
    Confirm deletion of the files above:
    [Y/n] ? [ENTER] means Yes
    """
    ans = input(scrmsg)
    if ans in ['Y', 'y', '']:
      return True
    return False

  def count_grouped_files_under_video_extensions(self, files):
    n_files_in_dir_for_deletion = 0
    for filename in files:
      self.total_files += 1
      if filename.endswith(tuple(self.dot_extensions_for_deletion)):
        n_files_in_dir_for_deletion += 1
        scrmsg = f"n_files_in_dir_for_deletion={n_files_in_dir_for_deletion} counting, {self.total_deletable} counted"
        print(scrmsg)
        scrmsg = f"{filename} in [{self.src_currdir_abspath}]"
        print(scrmsg)
    return n_files_in_dir_for_deletion

  def precount_dirs_n_files(self):
    print("Precounting dirs & files for videocompressing")
    print(f"looking and counting those with extensions: {self.dot_extensions_for_deletion}")
    self.total_files = 0
    self.n_dirs_in_iter = 0
    self.n_files_in_dir_for_deletion = 0
    for self.src_currdir_abspath, _, files in os.walk(self.delfiles_rootdir):
      n_files_in_dir_for_deletion = self.count_grouped_files_under_video_extensions(files)
      if n_files_in_dir_for_deletion > 0:
        self.n_dirs_in_iter += 1
        self.n_files_in_dir_for_deletion += n_files_in_dir_for_deletion

  def process(self):
    """

    """
    self.precount_dirs_n_files()
    self.process_in_oswalk()
    if not self.confirm_queued_files_deletion():
      return False
    self.do_delete()
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
  delfiles_rootdir = args.delfiles_rootdir
  mirrored_rootdir = args.mirrored_rootdir
  return delfiles_rootdir, mirrored_rootdir


def confirm_cli_args_with_user():
  delfiles_rootdir, mirrored_rootdir = get_cli_args()
  print(delfiles_rootdir, mirrored_rootdir)
  if not os.path.isdir(delfiles_rootdir):
    scrmsg = "Source directory [{src_rootdir_abspath}] does not exist. Please, retry."
    print(scrmsg)
    return False
  if not os.path.isdir(mirrored_rootdir):
    scrmsg = "Target directory [{trg_rootdir_abspath}] does not exist. Please, retry."
    print(scrmsg)
    return False
  print('Paramters')
  print('='*20)
  scrmsg = f"delfiles_rootdir = [{delfiles_rootdir}]"
  print(scrmsg)
  scrmsg = f"mirrored_rootdir = [{mirrored_rootdir}]"
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
  return confirmed, delfiles_rootdir, mirrored_rootdir


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
  confirmed, delfiles_rootdir, mirrored_rootdir = confirm_cli_args_with_user()
  if confirmed:
    deleter = FileDeleterAsMirroredInDirTrees(delfiles_rootdir, mirrored_rootdir)
    deleter.process()
    return True
  logging.shutdown()
  return False


if __name__ == '__main__':
  """
  adhoc_test1()
  adhoc_test2()
  """
  process()
