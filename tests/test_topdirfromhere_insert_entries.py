#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import classes.FolderTreeWalkerMod as treewalk

def test1():
  treewalker = treewalk.FolderTreeWalker()
  treewalker.walk_top_down()

def main():
  test1()

if __name__ == '__main__':
  main()
