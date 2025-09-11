#!/usr/bin/env python3
"""
cmm/deleteYtidFilesWhenIntersectionBetween2DirTrees.py
  Deletes repeated ytid files either from the source or destination dirtree,
    the user chooses from which one

This script does the following:
  1) it calls cmm/reportDifferentYdidsFromToDirTree.py
     to find out the ytids coincidences between two dirtrees;
     (this call is actually class-inheritance)
  2) loops each coincidence and asks the user which one is to delete,
     i.e., whether to delete from src or dst

import lib.db.dbdirtree_mod as dbdt
import models.entries.dirnode_mod as dn
import default_settings as defaults
import lib.strnlistfs.strfunctions_mod as strf
import lib.dirfilefs.dir_n_file_fs_mod as dirf
"""
import os
import cmm.rep.reportDifferentYdidsFromToDirTree as modEquals  # .YtidsComparatorReporter
rule40dashsigns = '-'*40
rule40equalsigns = '='*40


class IntersectionBetween2DirTreesDeleter(modEquals.YtidsComparatorReporter):
  """
  This class processes the deletion of repeated ytid files
    either in source or destination, the user chooses from which one
  """

  def __init__(self, src, dst):
    super().__init__(src, dst)
    self._src_ytid_fp_tuplelist = None
    self._dst_ytid_fp_tuplelist = None
    self.n_deleted = 0

  @property
  def src_ytid_fp_tuplelist(self):
    """
    "Dynamic" attribute that contains tuple list of SOURCE dirtree ytid's and their filepaths
    In the context of this class, it encompasses those ytid's that are in the intersection set,
      i.e., ytid's that exist both in SOURCE and DESTINATION
    """
    if self._src_ytid_fp_tuplelist is None:
      ytids = self.ytids_in_both
      self._src_ytid_fp_tuplelist = modEquals.get_tuplelist_ytids_filepaths_fr_ytids(ytids, self.src_sqlitefilepath)
      # sorting maintains both src & dst in order (element to element)
      self._src_ytid_fp_tuplelist.sort(key=lambda tupl: tupl[0])
    return self._src_ytid_fp_tuplelist

  @property
  def dst_ytid_fp_tuplelist(self):
    """
    "Dynamic" attribute that contains tuple list of DESTINATION dirtree ytid's and their filepaths
    In the context of this class, it encompasses those ytid's that are in the intersection set
      i.e., ytid's that exist both in SOURCE and DESTINATION
    """
    if self._dst_ytid_fp_tuplelist is None:
      ytids = self.ytids_in_both
      self._dst_ytid_fp_tuplelist = modEquals.get_tuplelist_ytids_filepaths_fr_ytids(ytids, self.dst_sqlitefilepath)
      # sorting maintains both src & dst in order (element to element)
      self._dst_ytid_fp_tuplelist.sort(key=lambda tupl: tupl[0])
    return self._dst_ytid_fp_tuplelist

  @property
  def src_ytid_fp_dict(self):
    """
    Contains the dict's version of src_ytid_fp_tuplelist

    It returns the dict on-the-fly without keeping an attribute in the object
      after 'return' (not as an iter-object, the memory allocation still happens upon call);
      the tuplelist version is kept in-object once initialized lazily via its @property.
    This above is said because if data is large enough, one solution would be to use an iter-object kept in disk.
    """
    pdict = {tupl[0]: tupl[0] for tupl in self.src_ytid_fp_tuplelist}
    return pdict

  @property
  def dst_ytid_fp_dict(self):
    """
    Contains the dict's version of dst_ytid_fp_tuplelist

    @see also docstr for @property src_ytid_fp_dict
    """
    pdict = {tupl[0]: tupl[0] for tupl in self.dst_ytid_fp_tuplelist}
    return pdict

  def delete_intersection_file_upon_user_confirmation(self, todel_src_filepath, todel_dst_filepath):
    ans = input('\t\tDelete [1 src] or [2 dst] (type number and press [ENTER] or Ctrl+C to interrupt program)')
    todel_filepath = None
    if ans == '1':
      todel_filepath = todel_src_filepath
    elif ans == '2':
      todel_filepath = todel_dst_filepath
    if todel_filepath:
      os.remove(todel_filepath)
      scrmsg = f"\t\t Deleted [{todel_filepath}]"
      print(scrmsg)
      self.n_deleted += 1

  def process_del_intersection_files_in_either_src_or_dst(self):
    """
    Prints to stdout the intersection set: inter(src, dst)
    """
    scrmsg = 'Delete intersection set either src or dst:'
    print(scrmsg)
    if len(self.ytids_in_both) == 0:
      scrmsg = '\tno ytids_in_both. Returning.'
      print(scrmsg)
      return
    total = len(self.src_ytid_fp_tuplelist)
    for i, src_ytid_fp_tuple in enumerate(self.src_ytid_fp_tuplelist):
      seq = i + 1
      print(rule40dashsigns)
      line = f"{seq}/{total}: which one to delete ?"
      print(line)
      src_ytid, src_filepath = src_ytid_fp_tuple
      todel_src_filepath = self.get_srcfilepath_w_relpath(src_filepath)
      if not os.path.isfile(todel_src_filepath):
        scrmsg = f'\tJumping over, src file [{todel_src_filepath}] is NOT present'
        print(scrmsg)
        continue
      dst_ytid, dst_filepath = self.dst_ytid_fp_tuplelist[i]
      todel_dst_filepath = self.get_dstfilepath_w_relpath(dst_filepath)
      if not os.path.isfile(todel_dst_filepath):
        scrmsg = f'\tJumping over, dst file [{todel_dst_filepath}] is NOT present'
        print(scrmsg)
        continue
      if src_ytid != dst_ytid:
        errmsg = f"Error ytid_scr={src_ytid} != ytid_dst={dst_ytid} are not equal."
        raise ValueError(errmsg)
      print(rule40equalsigns)
      line = f"""[1 scr] \tytid={src_ytid} | [{self.src_mountpath}] 
      => [{src_filepath}]"""
      print(line)
      print(rule40equalsigns)
      line = f"""[2 dst] \tytid={dst_ytid} | [{self.dst_mountpath}] 
      => [{dst_filepath}]"""
      print(line)
      print(rule40equalsigns)
      self.delete_intersection_file_upon_user_confirmation(todel_src_filepath, todel_dst_filepath)

  def process(self):
    self.process_del_intersection_files_in_either_src_or_dst()

  def __str__(self):
    outstr = str(super(self))
    outstr += f"\t n_deleted {self.n_deleted}"
    return outstr


def process():
  src_abspath, dst_abspath, _, _, _ = modEquals.get_args()
  deleter = IntersectionBetween2DirTreesDeleter(src_abspath, dst_abspath)
  deleter.process()


if __name__ == '__main__':
  """
  adhoctest()
  """
  process()
