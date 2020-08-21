#!/usr/bin/env python3
"""

"""
import copy
import hashlib
import os
import models.samodels as sam
# import fs.db.sqlalchemy_conn as con
import string
import config

hexdigits = string.digits + 'abcdef'


def add_bins(b1, b2):
  b1 = b1.strip('0b')
  b2 = b2.strip('0b')
  return b1 + b2


class DirNode:
  """
  This class works instantiating top-down and does not work bottom-up.
  (ie, it is important not to fill nodes bottom-up, unless a pool mechanism is deviced.)

  Here follows the explanation for the above statement:
  In words, it's necessary to instante a parent before instanciating a child.
    (A rercusive mechanism might autoinstantiate parents bottom-up, in a kind of look-up process,
     if a pool is planned available.
    Because there's no pool in this implementation, instantiation must happen top-down.
  """

  def __init__(self, dirname, parentnode=None, parentpath=None):
    self.parentnode = None
    self.parentpath = None
    # this setting must happen before _treat_integrated_parentnode_n_parentpath()
    self.dirname = dirname
    self.sha1hex = None
    self.children_dirnodes = []
    self._abspath = None
    self._middlepath = None
    self._level = None
    self._nsubnodes = None
    self._root = None
    self._treat_integrated_parentnode_n_parentpath(parentnode, parentpath)

  def _treat_integrated_parentnode_n_parentpath(self, parentnode, parentpath):
    if parentnode is None and parentpath is None:
      error_msg = 'Error: both parentnode and parentpath are None, cannot continue'
      raise ValueError(error_msg)
    if parentnode is not None and parentpath is not None:
      error_msg = 'Error: both parentnode and parentpath are not None, cannot continue'
      raise ValueError(error_msg)
    if parentnode is None:
      self.parentnode = None  # this is the case of the root node
      self.parentpath = parentpath
      return
    self.parentnode = parentnode
    # self.parentpath is set in the parent node's method add_sub()
    self.parentnode.add_subdir(self)

  @property
  def abspath(self):
    if self._abspath is not None:
      return self._abspath
    if self.parentpath is None:
      return None
    self._abspath = os.path.join(self.parentpath, self.dirname)
    return self._abspath

  def add_subdir(self, subdirnode):
    if self.sha1hex is not None:
      error_msg = 'Error: cannot add a new subdirnode after sha1hex having been calculated'
      raise IndexError(error_msg)
    if type(subdirnode) != DirNode:
      error_msg = 'Error: trying to append a subdirnode that is not of class DirNode'
      raise ValueError(error_msg)
    for child in self.children_dirnodes:
      if subdirnode.dirname == child.dirname:
        return False
    if subdirnode.parentpath is not None:
      if subdirnode.parentpath != self.abspath:
        return False
    else:
      subdirnode.parentpath = self.abspath
    subdirnode.parentnode = self
    self.children_dirnodes.append(subdirnode)
    # every new child added demands an updating to nsubnodes
    _ = self.count__n_set_nsubnodes()
    return True

  @property
  def parentdirname(self):
    if self.parentnode:
      return self.parentnode.dirname
    return ''

  @property
  def level(self):
    if self._level is None:
      self._level = self.get_n_levels()
    return self._level

  def reset_level(self):
    self._level = None

  @property
  def nsubnodes(self):
    if self._nsubnodes is None:
      _ = self.count__n_set_nsubnodes()
    return self._nsubnodes

  def count__n_set_nsubnodes(self):
    nsubnodes = len(self.children_dirnodes)
    for child in self.children_dirnodes:
      nsubnodes += child.count__n_set_nsubnodes()
    self._nsubnodes = nsubnodes
    return nsubnodes

  def get_n_levels(self, level_counter=0):
    """
    Consider this "private" method only callable from level() or recursively by itself
      all other "calls" must be to level as property as this method sets private attribute _level
    """
    if self.parentnode:
      level_counter += 1
      return self.parentnode.get_level(level_counter)
    return level_counter

  def get_root(self):
    if self.parentnode is None:
      return self
    return self.parentnode.get_root()

  @property
  def root(self):
    if self._root is None:
      self._root = self.get_root()
    return self._root

  @property
  def middlepath(self):
    if self._middlepath is None:
      self.calc_middlepath()
    return self._middlepath

  @property
  def middlepathforchildren(self):
    return self.middlepath + '/' + self.dirname

  def calc_middlepath(self):
    """
    Obs:
      1) by definition, root does not have a middlepath, it returns None;
      2) also by definition, root's immediate children have as their middlepaths their own dirnames;
      3) basically, excepting for root, middlepaths are calculated as:
          middlepath = self.abspath[len(self.root.abspath):]
          middlepath = middlepath.strip('/')
    Examples:
      1) if a child node has path: mount_dir_root/abc/def, its middlepath is 'abc';
      2) if a child node has path: mount_dir_root/level1/level2/level3, its middlepath is 'level1/level2';
    """
    if self._root == self:
      return None
    parentabspath, _ = os.path.split(self.abspath)
    if len(parentabspath) == len(self.root.abspath):
      self._middlepath = ''
    middlepath = parentabspath[len(self.root.abspath):]
    middlepath = middlepath.strip('/')
    self._middlepath = middlepath

  def fill_in_to_dbentry(self, session, dbentry=None):
    if self.root == self:
      return None
    if dbentry is None:
      dbentry = session.query(sam.FSEntryInDB).\
        filter(sam.FSEntryInDB.entryname == self.dirname).\
        filter(sam.FSEntryInDB.middlepath == self.middlepath).\
        filter(not sam.FSEntryInDB.isfile).\
        first()
    if dbentry is None:
      dbentry = sam.FSEntryInDB()
      session.add(dbentry)
    # maybe dbentry exists in db
    dbentry.entryname = self.dirname
    dbentry.middlepath = self.middlepath
    dbentry.isfile = False
    self.calc_entry_sha1hex(session)
    dbentry.sha1hex = self.sha1hex
    return dbentry

  def calc_sha1hex_n_save_into_db(self, session):
    self.calc_entry_sha1hex(session)

  def calc_entry_sha1hex(self, session=None):
    if session is None:
      sha1sconcatenated = self.concatenate_sha1hex_topdownindepth()
    else:
      sha1sconcatenated = self.concatenate_sha1hex_topdownindepth_via_db(session)
    pbytes = bytes(sha1sconcatenated, encoding='utf8')
    h = hashlib.new('sha1')
    h.update(pbytes)
    self.sha1hex = h.hexdigest()

  def concatenate_sha1hex_topdownindepth(self):
    sha1sconcatenated = self.concatenate_sha1hex_of_children_files()
    for child_dirnode in self.children_dirnodes:
      sha1sconcatenated += child_dirnode.concatenate_sha1hex_topdownindepth()
    if sha1sconcatenated == '':
      sha1sconcatenated = config.EMPTYFILE_SHA1HEX
    return sha1sconcatenated

  def concatenate_sha1hex_topdownindepth_via_db(self, session):
    sha1sconcatenated = ''
    for child_dirnode in self.children_dirnodes:
      sha1sconcatenated += child_dirnode.concatenate_sha1hex_topdownindepth_via_db(session)
    sha1sconcatenated = self.concatenate_sha1hex_of_children_files_via_db(session)
    if sha1sconcatenated == '':
      sha1sconcatenated = config.EMPTYFILE_SHA1HEX
    return sha1sconcatenated

  def concatenate_sha1hex_of_children_files(self):
    entries = os.listdir(self.abspath)
    entries = sorted(entries)
    fullentries = [os.path.join(self.abspath, e) for e in entries]
    fullentries = filter(lambda e: not os.path.isdir, fullentries)
    sha1sconcatenated = ''
    for fullentry in fullentries:
      content = open(fullentry, 'rb').read()
      h = hashlib.new('sha1')
      h.update(content)
      sha1sconcatenated += h.hexdigest()
    return sha1sconcatenated

  def concatenate_sha1hex_of_children_files_via_db(self, session):
    dbentries = session.query(sam.FSEntryInDB).\
        filter(sam.FSEntryInDB.middlepath == self.middlepathforchildren).\
        filter(sam.FSEntryInDB.isfile).\
        order_by(sam.FSEntryInDB.entryname).\
        all()
    sha1sconcatenated = ''
    for dbentry in dbentries:
      sha1sconcatenated += dbentry.sha1hex
    return sha1sconcatenated

  @property
  def is_leaf(self):
    if len(self.children_dirnodes) == 0:
      return True
    return False

  @property
  def stamp(self):
    return '<Node "%s" p="%s" l=%d s=%d>' % (self.dirname, self.parentdirname, self.level, self.nsubnodes)

  @property
  def stamp2(self):
    return 'sha1 %s | middle %s | rootname %s' % (self.sha1hex, self.middlepath, self.root.dirname)

  def __str__(self):
    spaces = ' ' * 2 * self.level
    outstr = spaces + '%s\n' % self.stamp
    outstr += spaces + '%s\n' % self.stamp2
    for child in self.children_dirnodes:
      outstr += str(child)
    return outstr


def fetch_dirnames_in_dirpath_return_sorted_paths_n_dirnames_list(abspath):
  entries = os.listdir(abspath)
  absentries = map(lambda p: os.path.join(abspath, p), entries)
  absentries = filter(lambda p: os.path.isdir(p), absentries)
  path_n_dirname_tuplelist = [os.path.split(p) for p in absentries]
  path_n_dirname_tuplelist = sorted(path_n_dirname_tuplelist, key=lambda e: e[1])
  return path_n_dirname_tuplelist


def fetch_dirnames_in_dirpath_return_sorted_dirnames(abspath):
  entries = os.listdir(abspath)
  absentries = map(lambda p: os.path.join(abspath, p), entries)
  absentries = filter(lambda p: os.path.isdir(p), absentries)
  dirnames = [os.path.split(p)[1] for p in absentries]
  dirnames = sorted(dirnames)
  return dirnames


count = 0


def jump_indepth_one_step(p_dirnode):
  global count
  dirnode = copy.copy(p_dirnode)
  count += 1
  print(count, 'processing', dirnode.dirname)
  dirnames = fetch_dirnames_in_dirpath_return_sorted_dirnames(dirnode.abspath)
  if len(dirnames) == 0:
    return
  while len(dirnames) > 0:
    dirname = dirnames[0]
    child_dirnode = DirNode(dirname, parentnode=dirnode)
    del dirnames[0]
    jump_indepth_one_step(child_dirnode)

  # absdirnames = reversed(sorted(dirnames))
  # dirname = dirnames.pop()


def traverse_topdown_leftright(dirnode, session=None):
  print('Entered', dirnode.stamp)
  dirnodes = copy.copy(dirnode.children_dirnames)
  while len(dirnodes) > 0:
    next_dirnode = dirnodes[0]
    del dirnodes[0]
    traverse_topdown_leftright(next_dirnode, session)
  print('Exhausted', dirnode.stamp)
  if session is None:
    dirnode.calc_entry_sha1hex()
  else:
    _ = dirnode.fill_in_to_dbentry(session)
    session.commit()
  print('sha1hex', dirnode.stamp2)
  return


def load_tree_n_traverse_topdown_leftright(dirnode, session=None):
  jump_indepth_one_step(dirnode)
  traverse_topdown_leftright(dirnode, session)


def process_nodes():
  # current_dir = '/home/dados/Sw3/SwDv/OSFileSystemSwDv/PyMirrorFileSystemsByHash/fs/'
  rootnode = DirNode('fs', parentpath='/home/dados/Sw3/SwDv/OSFileSystemSwDv/PyMirrorFileSystemsByHash')
  jump_indepth_one_step(rootnode)
  print('================ dirnode ====================')
  print(rootnode)
  traverse_topdown_leftright(rootnode)
  print('last')
  print(rootnode)


def adhoc_test1():
  h1 = 0x5
  sh1 = hex(h1)
  h2 = 0x6
  sh2 = hex(h2)
  print(h1, h2)
  h3 = h1 + h2
  sh3 = sh1 + sh2
  print(h3, sh3)


def process():
  process_nodes()


if __name__ == '__main__':
  process()
