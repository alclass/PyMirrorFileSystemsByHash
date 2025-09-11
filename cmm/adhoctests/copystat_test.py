#!/usr/bin/env python3
"""
cmm/adhoctests/copystat_test.py

parser.add_argument("--copystat", action='store_true',
                    help="equalize file dates or resync metadata to videos, ie copy back with (shutil.copystat())"
                         " original video metadata to the compressed video in the target dirtree")

"""
from pathlib import Path
import datetime
import argparse  # Parse command-line arguments
parser = argparse.ArgumentParser(description="Compress videos to a specified resolution.")
parser.add_argument("--inputfile", type=str,
                    help="inputfile from which metadata will be extraction for display")
parser.add_argument("--outputfile", type=str,
                    help="inputfile from which metadata will be extraction for display")
args = parser.parse_args()


class FileInfo:

  def __init__(self, filepath):
    self.filepath = filepath
    self.filepath_obj = Path(filepath)
    self.file_size = self.filepath_obj.stat().st_size
    self.last_modified_timestamp = None
    self.creation_timestamp = None
    self.access_timestamp = None
    self.last_modified_datetime = None
    self.creation_datetime = None
    self.access_datetime = None
    self.set_timestamps()
    self.set_datetimes()

  def set_timestamps(self):
    # Accessing metadata attributes
    self.last_modified_timestamp = self.filepath_obj.stat().st_mtime
    self.creation_timestamp = self.filepath_obj.stat().st_ctime
    self.access_timestamp = self.filepath_obj.stat().st_atime

  def set_datetimes(self):
    # Convert timestamps to datetime objects
    self.last_modified_datetime = datetime.datetime.fromtimestamp(self.last_modified_timestamp)
    self.creation_datetime = datetime.datetime.fromtimestamp(self.creation_timestamp)
    self.access_datetime = datetime.datetime.fromtimestamp(self.access_timestamp)

  def __str__(self):
    """
    ------------------
    Last Modified Time: {self.last_modified_timestamp}
    Creation Time: {self.creation_timestamp}
    Last Access Time: {self.access_timestamp}
    """
    outstr = f""" => filepath: {self.filepath}
    File Size: {self.file_size} bytes
    ------------------
    Last Modified Time: {self.last_modified_datetime}
    Creation Time: {self.creation_datetime}
    Last Access Time: {self.access_datetime}
    """
    return outstr


def adhoc_test1():
  pass


def process():
  """
  infile = FileInfo(args.input_file)
  print(infile)
  outfile = FileInfo(args.output_file)
  print(outfile)

  """

  fi1 = FileInfo(args.inputfile)
  fi2 = FileInfo(args.outputfile)
  print('******************')
  print(fi1)
  print('******************')
  print(fi2)


if __name__ == '__main__':
  """
  """
  process()
