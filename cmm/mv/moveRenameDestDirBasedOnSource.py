#!/usr/bin/env python3
"""
cmm/mv/moveRenameDestDirBasedOnSource.py
Moves (or renames) a destination directory based on a source path.

Example:

  1) suppose source dir is:
    /media/user/disk1/Areas/Science/Phys/Quantu
  2) suppose destination  dir is:
    /media/user/disk2/Areas/xpto/SciencePo/Phys/Quantu
  3) now notice that one wants to rename destination dirname "SciencePo'
    to the dirname in source (Science) (subdirectories "Phys/Quantu" are dropped in simplication)

Then, for this example, the parameters, to give as input, are:

  1) --droot (destination root absolute path): "/media/user/disk2"
  2) --smid (source middlepath): "Areas/Science"
  3) --dmid (destination middlepath): "Areas/xpto/SciencePo"

The middlepath is the complement relativepath to the root abspath that makes up the full path.
Or, in other words: fullpath = rootpath + middlepath

For this example, the move command could be represented as:

mv "/media/user/disk2/Areas/xpto/SciencePo"
   "/media/user/disk2/Areas/Science"

Notice that the operation happens on the destination.
"""
import os
import shutil
import llib.db.dbdirtree_mod as dbdt
import models.entries.dirnode_mod as dn
import default_settings as defaults
import cmm.clean.dbclean.dbentry_deleter_those_without_corresponding_osentry_cm as dbentr_del


class MoveCommandMaker:

  def __init__(self, droot, dmid, smid):
    self.dst_rootpath = droot
    self.dst_middlepath = dmid
    self.src_middlepath = smid

  @property
  def new_dst_fullpath(self):
    _new_dst_fp = os.path.join(self.dst_rootpath, self.src_middlepath)
    return _new_dst_fp

  @property
  def old_dst_fullpath(self):
    _old_dst_fp = os.path.join(self.dst_rootpath, self.dst_middlepath)
    return _old_dst_fp

  def move(self):
    src = self.old_dst_fullpath
    dst = self.new_dst_fullpath
    comm = f'mv "{src}" "{dst}"'
    scrmsg = f'Executing move command: [{comm}]'
    print(scrmsg)
    os.system(comm)

  def process(self):
    self.move()


def get_args():

  return droot, dmid, smid


def process():
  """
  """
  droot, dmid, smid = get_args()
  mover = MoveCommandMaker(droot, dmid, smid)
  mover.process()


if __name__ == '__main__':
  process()
