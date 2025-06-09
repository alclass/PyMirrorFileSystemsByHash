#!/usr/bin/env python3
"""
fs/videoaudio/audiofs.py
  At the moment of writing, this lib-module contains a function to test whether
    a supposed videofile is just in fact an audiofile without video
    (this is done via lib-package ffmpeg-python)

Ffmpeg wiki docs at: https://trac.ffmpeg.org/wiki

About how ffmpeg-python was installed
  (on Linux Ubuntu-Mate (24.02)):

  -----------------------
  pipx install --include-deps ffmpeg-python
    installed package ffmpeg-python 0.2.0, installed using Python 3.12.3.
    These apps are now globally available
      - futurize
      - pasteurize
  done! ✨ 🌟 ✨
  -----------------------

  Obs:
  1 to note that the 2 apps above required the use of parameter --include-deps with pipx
  2 installing to the local user with pipx, though the messages seen above did not work
  3 but installing to the project's venv from inside PyCharm worked
"""
import ffmpeg
import subprocess


def is_audio_file(filepath):
  """"
  """
  try:
      probe = ffmpeg.probe(filepath, select_streams='a')
      return bool(probe['streams'])
  except ffmpeg.Error:
      return False


def is_video_corrupt(video_path):
  """

  Method 1 Using try-except block: This method attempts to run a basic ffmpeg command that copies the video stream to
  a null output. If the video is corrupt, ffmpeg will likely throw an error, which can be caught using a try-except
  block.
  """
  # noinspection PyProtectedMember
  try:
    ffmpeg.input(video_path).output('null', f='null').run()
    return False  # Video is likely good
  except ffmpeg._run.Error:
    return True   # Video is likely corrupt


def test_is_video_corrupt_1():
  # Example
  video_file = "your_video.mp4"  # revise and put a parameter with your video file path
  if is_video_corrupt(video_file):
    print(f"{video_file} is corrupt.")
  else:
    print(f"{video_file} is good.")


def is_video_corrupt_2(video_path):
  """
  Method 2 Check the return code: This method executes a ffmpeg command and checks the return code. A non-zero
  return code usually indicates an error, suggesting the video is corrupt.
  """
  command = ['ffmpeg', '-v', 'error', '-i', video_path, '-f', 'null', '-']
  i_process = subprocess.run(command, capture_output=True, text=True)
  return i_process.returncode != 0


def test_is_video_corrupt_2():
  # Example
  video_file = "your_video.mp4"  # revise and put a parameter with your video file path
  if is_video_corrupt(video_file):
    print(f"{video_file} is corrupt.")
  else:
    print(f"{video_file} is good.")


def is_video_corrupt_3(video_path):
  """
  Method 3 Check for error messages in stderr: This method captures the standard error output from ffmpeg and looks
  for specific error messages. If any of these messages are found, the video is likely corrupt.
  """
  command = ['ffmpeg', '-v', 'error', '-i', video_path, '-f', 'null', '-']
  i_process = subprocess.run(command, capture_output=True, text=True)
  if i_process.returncode != 0:
    return True
  stderr_output = i_process.stderr
  error_messages = ["error", "corrupt", "invalid"]
  for error in error_messages:
    if error in stderr_output.lower():
      return True
  return False


def test_is_video_corrupt_3():
  # Example
  video_file = "your_video.mp4"  # revise and put a parameter with your video file path
  if is_video_corrupt(video_file):
    print(f"{video_file} is corrupt.")
  else:
    print(f"{video_file} is good.")


def adhoc_test1():
  # Example usage
  file_path = 'your_file.mp4'  # a non-existent file will return False
  if is_audio_file(file_path):
    print(f"{file_path} is an audio file.")
  else:
    print(f"{file_path} is not an audio file.")


def process():
  pass


if __name__ == '__main__':
  """
  adhoc_test2()
  """
  process()
  adhoc_test1()
