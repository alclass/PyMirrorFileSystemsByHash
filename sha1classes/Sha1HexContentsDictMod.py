#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
TwoFileSystemsWideComparatorModule
'''

import __init__
import local_settings as ls
from Sha1FileOn2DisksComparatorMod import Sha1FileOn2DisksComparator


class Sha1HexContentsDict(dict):
  '''
  This class is a one-method inheritance of dict.
  Nothing except __getitem__() has been overridden.
  This choice has two important implications, which are:
  1) the "value" of this class is an instance of Sha1FileOn2DisksComparator,
     which is created empty at a first __getitem__() to its key (a sha1hex number)  
  2) The use of this child class should consider __setitem__() to be private,
     ie, obj[k]=v should never be used. 
  '''
  
  def __getitem__(self, sha1hex):
    '''
    This is the only method that is inherited from parent class dict
    The first time obj[k] (__getitem__() itself) is issued, 
      a Sha1FileOn2DisksComparator object is instantiated empty to its key k
    '''
    if not self.has_key(sha1hex):
      sha1FileComparator = Sha1FileOn2DisksComparator(sha1hex)
      self.__setitem__(sha1hex, sha1FileComparator)
      return sha1FileComparator
    # implicit else, if sha1FileComparator has already been instantiated, it's available in the dict 
    sha1FileComparator = super(Sha1HexContentsDict, self).__getitem__(sha1hex)
    return sha1FileComparator

    
def process():
  pass
  
if __name__ == '__main__':
  process()
  #unittest.main()
