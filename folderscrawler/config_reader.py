#!/usr/bin/env python3
import os
from pathlib import Path

# this list is shared just inside class Config below
# do not use it elsewhere! (the mutability issue!)
config_filename = 'local_settings.cfg'
config_variables = [
  'BASE_FOLDER_ORIGIN',
  'BASE_FOLDER_TARGET',
]

class Config:

  def __init__(self):
    self.config_variables = config_variables
    self.read_n_set_config_file()

  def read_n_set_config_file(self):
    '''
    The technique using Path(__file__).resolve() works both
      in the folder where this script is located and also
      in some other folder, ie, open(configfile).read() picks
      up the local txt correctly
    :return:
    '''
    thisfile = Path(__file__).resolve()
    root_folder = thisfile.parents[1]
    # parent_folder = thisfile.parent
    config_abspath = os.path.abspath(os.path.join(root_folder, config_filename))
    lines = open(config_abspath).read().split('\n')
    for line in lines:
      for config_variable in self.config_variables:
        if line.startswith(config_variable):
          _, value = line.split('=')
          pyline = "self." + config_variable + " = " + value + ""
          #print (pyline)
          exec(pyline)
  def print_config_vars(self):
    print ('Printing config vars:')
    print ('--------------------:')
    for config_variable in self.config_variables:
      pyline = config_variable + " = " + eval('self.' + config_variable)
      print (pyline)

def print_config_vars_from_outside():
  c = Config()
  for config_variable in c.config_variables:
    pyline = config_variable + " = " + eval('c.' + config_variable)
    print(pyline)

def test_it():
  c = Config()
  c.print_config_vars()
  print('--------------------print_config_vars_from_outside():')
  print_config_vars_from_outside()

if __name__ == '__main__':
  test_it()
