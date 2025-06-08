#!/usr/bin/env python3
"""
piping_osexec_return.py
"""
import subprocess
COMM_FOR_TOTAL_TO_INTERPOL = 'ls -R *%s | wc'


def countfiles():
  """

  except (AttributeError, IndexError, ValueError):
    total_files = -1
    print('Total files:', total_files, ':: system failed to count files. Moving on.')

  """
  dot_ext = '.py'
  comm_to_find_total_files = COMM_FOR_TOTAL_TO_INTERPOL % dot_ext
  print('Counting files:', comm_to_find_total_files)
  # input_pp = comm_to_find_total_files.split(' ')
  proc = subprocess.Popen(['ls', '|', 'wc'], stdout=subprocess.PIPE)  # args=['-R', '*.py']
  bytes_result = proc.stdout.read()
  # try:
  str_result = 'r=' + str(bytes_result)
  output_pp = str_result.split(' ')
  # total_files = int(output_pp[0])  # the total number is the first element from "ls | wc"
  print('str_result:', str_result, bytes_result)
  # print('Total files:', total_files)


def adhoctest():
  countfiles()


def process():
  pass


if __name__ == '__main__':
  """
  adhoctest()
  process()
  """
  adhoctest()
