#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
'''
import glob, os

def process():
  pys = glob.glob('*.py')
  for py in pys:
    print 'Unit Tests for', py
    name, _ = os.path.splitext(py)
    exec('import %s' %name)
    try:
      eval(name).unittest.main()
    except AttributeError:
      pass
  
if __name__ == '__main__':
  process()
