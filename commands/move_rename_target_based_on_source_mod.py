#!/usr/bin/env python3
"""
move_rename_target_based_on_source_mod.py
"""
import os
import shutil
import fs.db.dbdirtree_mod as dbdt
import models.entries.dirnode_mod as dn
import default_settings as defaults


class MoveRename:

  def __init__(self, ori_mountpath, bak_mountpath):
    self.ori_dbtree = dbdt.DBDirTree(ori_mountpath)
    self.bak_dbtree = dbdt.DBDirTree(bak_mountpath)
    self.n_moverename = 0

  def move_trg_based_on_src(self, src_dirnode, trg_dirnode):
    trg_middlepath = trg_dirnode.path
    trg_middlepath = trg_middlepath.lstrip('/')
    trg_oldpath = os.path.join(self.bak_dbtree.mountpath, trg_middlepath)
    src_middlepath = src_dirnode.path
    src_middlepath = src_middlepath.lstrip('/')
    trg_newpath = os.path.join(self.bak_dbtree.mountpath, src_middlepath)
    if not os.path.isfile(trg_oldpath):
      return False
    if os.path.isfile(trg_newpath):
      return False
    shutil.move(trg_oldpath, trg_newpath)
    self.n_moverename += 1
    return True

  def dbupdate_trg_middlepath_with_sr(self, src_dirnode, trg_dirnode):
    sql = '''
    UPDATE %(tablename)s SET
      name=?,
      parentpath=?,
    WHERE
      sha1=?;
    '''
    name = src_dirnode.name
    parentpath = src_dirnode.parentpath
    sha1 = trg_dirnode.sha1
    tuplevalues = (name, parentpath, sha1)
    return self.bak_dbtree.do_update_with_sql_n_tuplevalues(sql, tuplevalues)

  def compare_src_sha1_position_in_trg(self, sha1, _id):
    """
    IMPORTANT:
      this script requires (as a constraint) that sha1's must be unique in dirtree
        (a previous script normalizes this).

    """
    sql = 'SELECT * FROM %(tablename)s WHERE id=?;'
    tuplevalues = (_id, )
    fetched_rows = self.ori_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if not fetched_rows or len(fetched_rows) != 1:
      return False
    src_row = fetched_rows[0]
    sql = 'SELECT * FROM %(tablename)s WHERE sha1=?;'
    tuplevalues = (sha1, )
    fetched_rows = self.bak_dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if not fetched_rows or len(fetched_rows) != 1:
      return False
    trg_row = fetched_rows[0]
    src_dirnode = dn.DirNode.create_with_tuplerow(src_row, self.ori_dt.fieldnames)
    trg_dirnode = dn.DirNode.create_with_tuplerow(trg_row, self.bak_dt.fieldnames)
    print(src_dirnode.path, '?', trg_dirnode.path)
    if src_dirnode.path != trg_dirnode.path:
      if self.move_trg_based_on_src(src_dirnode, trg_dirnode):
        self.dbupdate_trg_middlepath_with_sr(src_dirnode, trg_dirnode)

  def traverse_src_nodes(self):
    sql = 'SELECT DISTINCT sha1, COUNT(id), id FROM %(tablename)s GROUP BY sha1;'
    generator_rows = self.ori_dbtree.do_select_sql_n_tuplevalues_w_limit_n_offset(sql, None)
    for rows in generator_rows:
      for row in rows:
        sha1 = row[0]
        n_repeats = row[1]
        _id = row[2]
        print(_id, n_repeats, sha1.hex()[:20])
        if n_repeats == 1:
          self.compare_src_sha1_position_in_trg(sha1, _id)

  def report(self):
    print('Report:')
    print('=======')
    print('dirtrees:', self.ori_dbtree.mountpath, self.bak_dbtree.mountpath)
    print('n_moverename', self.n_moverename)

  def process(self):
    self.traverse_src_nodes()
    self.report()


def process():
  """
  """
  src_mountpath, trg_mountpath = defaults.get_src_n_trg_mountpath_args_or_default()
  moverenamer = MoveRename(src_mountpath, trg_mountpath)
  moverenamer.process()


if __name__ == '__main__':
  process()
