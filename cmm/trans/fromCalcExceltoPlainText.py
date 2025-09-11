#!/usr/bin/env python3
"""
DirTreeMirror_PrdPrjSw:
  cmm/trans/fromCalcExceltoPlainText.py
Description below.

  (script-writing is ongoing!)


"""
import os

import pandas as pd



def excel_to_plain_text(excel_file_path, output_text_file_path, sheet_name=0):
  """
  Converts a specified sheet from an Excel/Calc file to a plain text file.

  Args:
      excel_file_path (str): The path to the input Excel/Calc file (.xlsx, .xls, .ods).
      output_text_file_path (str): The path to the output plain text file.
      sheet_name (int or str, optional): The sheet to convert. Can be an integer
        (0 for the first sheet, 1 for the second, etc.)
        or the name of the sheet as a string.
        Defaults to 0 (the first sheet).
  """
  try:
    # Read the Excel file into a pandas DataFrame
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)

    # Convert the DataFrame to a string representation and write to a text file
    # index=False prevents writing the DataFrame index to the text file
    # na_rep='' handles NaN values by replacing them with an empty string
    with open(output_text_file_path, 'w', encoding='utf-8') as f:
        df.to_string(f, index=False, na_rep='')
    print(f"Successfully converted '{excel_file_path}' (sheet: {sheet_name}) to '{output_text_file_path}'")

  except FileNotFoundError:
    print(f"Error: The file '{excel_file_path}' was not found.")
  except Exception as e:
    print(f"An error occurred: {e}")


# Example usage:
def convert_to_plain_text():
  input_excel = "your_spreadsheet.xlsx"  # Replace with your Excel/Calc file name
  output_text = "output_data.txt"      # Replace with your desired output text file name

  # Create a dummy Excel file for demonstration if it doesn't exist
  try:
    dummy_data = {'Name': ['Alice', 'Bob', 'Charlie'],
                  'Age': [30, 24, 35],
                  'City': ['New York', 'London', 'Paris']}
    dummy_df = pd.DataFrame(dummy_data)
    dummy_df.to_excel(input_excel, index=False)
    print(f"Created a dummy Excel file: '{input_excel}'")
  except Exception as e:
    print(f"Could not create dummy Excel file (might already exist): {e}")

  excel_to_plain_text(input_excel, output_text)

# To convert a specific sheet by name:
# excel_to_plain_text(input_excel, "output_sheet2.txt", sheet_name="Sheet2")


class ExcelToTextConverter:

  def __init__(self, src_rofo_abspath, dst_rofo_abspath):
    self.src_rofo_abspath, self.dst_rofo_abspath = src_rofo_abspath, dst_rofo_abspath
    self.currfo_abspath = None  # current folder abspath from os.walk()

  def get_src_filepath(self, filename):
    fileabspath = os.path.join(self.currfo_abspath, filename)
    return fileabspath


  def convert_files(self, filenames):
    for filename in filenames:
      excel_file_path = get_src_filepath(filename)
      output_text_file_path =
      excel_to_plain_text(excel_file_path, output_text_file_path, sheet_name=0)



  def process(self):
    for self.currfo_abspath, dirnames, filenames in os.walk(self.src_rofo_abspath):
      self.convert_files(filenames)



def adhoctest1():
  pass


def process():
  pass


if __name__ == '__main__':
  """
  adhoctest1()
  process()
  """
  adhoctest1()
  process()
