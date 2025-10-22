#!/usr/bin/env python3
"""
~/bin/localuserpylib/regexfs/filenamevalidator_cls.py
"""
import os.path
import re
# from uTubeRetrieveSubtitles import pass_ytvideoid_thru_probable_filter

ALLOWED_EXTENSIONS = {'mp4', 'mp3', 'mkv', 'webm', 'm4v', 'm4a', 'avi', 'wmv'}
# NTFS reserved names (case-insensitive)
RESERVED_NAMES = {
  "CON", "PRN", "AUX", "NUL",
  *{f"COM{i}" for i in range(1, 10)},
  *{f"LPT{i}" for i in range(1, 10)}
}
# Regex pattern for full filename validation
GENETIC_NTFS_NAMEPATTERN = re.compile(
  r'^(?P<basename>[^<>:"/\\|?*\r\n]+?)\.(?P<ext>[A-Za-z0-9]+)$'
)
# Regex pattern for full filename validation
FILENAME_W_YTID_PATTERN = re.compile(
  # old r'^(?P<basename>[^<>:"/\\|?*\r\n]+?)\s(?P<id>\[[A-Za-z0-9_-]{11}])\.(?P<ext>[A-Za-z0-9]+)$'
  r'^.+?\s\[(?P<ytid>[A-Za-z0-9_-]{11})]\.[A-Za-z0-9]+$'
)


def get_basename_n_ext_if_ntfs_entryname_is_valid(filename) -> tuple:
  """
  Checks if the base name is valid under NTFS rules.
  """
  if not filename or filename.upper() in RESERVED_NAMES:
    return None, None
  if filename[-1] in {' ', '.'}:
    return None, None
  if len(filename) > 256:
    return None, None
  match = GENETIC_NTFS_NAMEPATTERN.match(filename)
  if not match:
    return None, None
  basename = match.group('basename')
  ext = match.group('ext')
  # whether ext is a valid one (i.e., inside an allowed list) is postponed
  return basename, ext


def get_ytid_if_filename_complies_w_ytid_within_squarebrackets(filename) -> str | None:
  """
  Returns True if the filename passes the following two conditions:

    C1 - pattern validation (pattern is the ytid enclosed by square-brackets)
    C2 - has an audio-or-video file-extension

  older-text
    This function implements a 2-level filter:
    both NTFS and pattern validation.
    C3 - NTFS filename validation

  """
  match = FILENAME_W_YTID_PATTERN.match(filename)
  if not match:
    return None
  ytid = match.group('ytid').lower()
  return ytid


def change_fileextension_in_fp_fr_w(filepath_base: str, dot_ext_to_change: str) -> str | None:
  """
  Takes a filepath and changes it fileextension returning the changed filepath

  :param filepath_base:
  :param dot_ext_to_change:
  :return: recomposed_filepath: str | None
  """
  try:
    folderpath, filename = os.path.split(filepath_base)
    name, dot_ext = os.path.splitext(filepath_base)
    # recompose new filename
    recomposed_filename = f"{name}{dot_ext_to_change}"
    # recompose new filepath
    recomposed_filepath = os.path.join(folderpath, recomposed_filename)
    return recomposed_filepath
  except ValueError:
    pass
  return None


class FilenameValidator:

  # Allowed file extensions
  ALLOWED_EXTENSIONS = ALLOWED_EXTENSIONS
  # NTFS reserved names (case-insensitive)
  RESERVED_NAMES = RESERVED_NAMES
  # Regex pattern for full filename validation
  FILENAME_W_YTID_PATTERN = FILENAME_W_YTID_PATTERN
  GENETIC_NTFS_NAMEPATTERN = GENETIC_NTFS_NAMEPATTERN

  def __init__(self, filename: str):
    self.filename = filename
    self.ytid = None
    self.basename = None
    self.ext = None
    self.bool_video_ext_valid = None
    self.bool_ytid_enclosed_valid = None
    self.bool_valid_ntfs_name = None
    self.process()

  @property
  def dot_ext(self):
    if self.ext:
      return f".{self.ext}"
    return None

  @property
  def is_filename_a_valid_ytdlp(self):
    return self.bool_valid_ntfs_name and self.bool_ytid_enclosed_valid and self.bool_video_ext_valid

  @property
  def recomposed_filename(self):
    if self.basename is None:
      return "<invalid NTFS filename>"
    _recomposed_fn = f"{self.basename}{self.dot_ext}"
    return _recomposed_fn

  def find_n_set_basename_n_ext_if_fn_is_ntfs_valid(self):
    """
    Checks if the base name is valid under NTFS rules.
    """
    self.bool_valid_ntfs_name = False
    self.basename, self.ext = get_basename_n_ext_if_ntfs_entryname_is_valid(self.filename)
    if self.basename:
      self.bool_valid_ntfs_name = True

  def find_n_set_filename_w_ytid_validity(self):
    """
    Returns True if the filename passes both NTFS and pattern validation.
    """
    self.bool_ytid_enclosed_valid = False
    ytid = get_ytid_if_filename_complies_w_ytid_within_squarebrackets(self.filename)
    if ytid:
      self.bool_ytid_enclosed_valid = True
      self.ytid = ytid

  def find_n_set_ytid(self):
    """
    Extracts the 11-character ENC64 ID if the filename is valid.
    """
    if self.bool_ytid_enclosed_valid is None:
      self.find_n_set_filename_w_ytid_validity()
    if not self.bool_ytid_enclosed_valid:
        return
    match = self.FILENAME_W_YTID_PATTERN.match(self.filename)
    if match:
      self.ytid = match.group('ytid')

  def is_ext_a_valid_audio_video_one(self):
    self.bool_video_ext_valid = False
    if self.ext in ALLOWED_EXTENSIONS:
      self.bool_video_ext_valid = True

  def process(self):
      """
      """
      self.find_n_set_basename_n_ext_if_fn_is_ntfs_valid()
      self.find_n_set_filename_w_ytid_validity()
      self.is_ext_a_valid_audio_video_one()
      self.find_n_set_ytid()

  def __str__(self):
    """
      print("✅ Valid filename")
      print("❌ Invalid filename")
    """
    outstr = f"""FilenameValidador ✅❌🎯:
    filename = {self.filename}
    ytid = {self.ytid} | basename = [{self.basename}] | ext = [{self.ext}] | dot_ext = [{self.dot_ext}]
    recomposed filename = {self.recomposed_filename}
    --------------------------
    valid enclosed ytid = {self.bool_ytid_enclosed_valid}
    valid ntfs filename = {self.bool_valid_ntfs_name} 
    valid videoext = {self.bool_video_ext_valid}
    --------------------------
    is_filename_a_valid_ytdlp ? {'Yes' if self.is_filename_a_valid_ytdlp else 'No'}
    """
    return outstr


def adhoc_test1():
  filename = "cool video [abcABC123-_].mp4"
  fival = FilenameValidator(filename)
  print(fival)
  filename = "cool video abcABC123-_.mp4"
  fival = FilenameValidator(filename)
  print(fival)
  filename = "bad<name abcABC123-_.mp4"
  fival = FilenameValidator(filename)
  print(fival)
  filename = "bad<name [abcABC123-_].mp4"
  fival = FilenameValidator(filename)
  print(fival)


def process():
  pass


if __name__ == '__main__':
  """
  adhoctest1()
  process()
  """
  adhoc_test1()
  process()
