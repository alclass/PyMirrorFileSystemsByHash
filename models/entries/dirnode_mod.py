#!/usr/bin/env python3
import datetime
import hashlib
import os
import fs.hashfunctions.hash_mod as hm
LF = '\n'
PREFIX_FOR_FILES_LINEPATH = 'F '


class MockDirEntryType:
  DIR = 'DIR'
  FILE = 'FILE'
  MAX_DIRS_LEVEL = 500


MockDET = MockDirEntryType


def from_dbres_tuple_to_dirnode(restuple):
  r = restuple
  node = DirNode(
    name=r[1],
    parentpath=r[2],
    sha1=r[3],
    bytesize=r[4],
    mdatetime=r[5]
  )
  print(node)
  return node


class DirNode:
  """
  This class has two dimensions, so to say. One considers classmethods functionality;
    the other models a DirNode object.

  The constructor of this class should be considered PRIVATE
    ie client callers should not issue it directly,
    instead they should issue classmethod get_dirnode_instance() (the factory design-pattern).

  DEPRECATED The DirNode dict storage
  ========================
  Every instantiated dirnode, at the moment of instantiation, gets stored into a dict.
  The hash (key) for the dict today is the dirnode path itself eg /folder1/folder2/file3.
  (Refactoring to come: the hash (key) above mentioned will be changed in the future
  for something that is at the same time collision-safe and memory-saving.

  The DirNode (object) model
  ==========================
  DirNode has a name, a parent, an etype (DIR|FILE), children, fpath (the full path) etc.
  See code below for futher info.

  The root dirnode has no parent and has a conventioned name which is just a '/' (forward slash).
  (Its name attribute internally is None.)

  The main class application
  ==========================
  The DirNode mimics somehow a directory entry common (and ubuquitous) in operating system.
  One envisioned use of this class is to compare two different dirtrees (directory trees) and
  apply copy, move and delete operations on them as an application of mirroring
  and backing up disks.
  """

  _dirnodes_instantiated = {}
  _root = None

  @staticmethod
  def recompose_abspath_w_mountpath(dbfilerecord, dbtree):
    idx = dbtree.fieldnames.index('name')
    name = dbfilerecord[idx]
    idx = dbtree.fieldnames.index('parentpath')
    parentpath = dbfilerecord[idx]
    middlepath = os.path.join(parentpath, name)
    middlepath = middlepath.lstrip('/')
    os_equiv_filepath = os.path.join(dbtree.mountpath, middlepath)
    return os_equiv_filepath

  @classmethod
  def instantiate_foldernode(cls, npath):
    if npath is None or npath == '/':
      return cls.root
    parentpath, name = os.path.split(npath)
    dirnode = DirNode(
      name=name,
      parentpath=parentpath,
      sha1=None,
      bytesize=None,
      mdatetime=None
    )
    return dirnode

  @classmethod
  def fetch_node_from_db(cls, dbtree, npath, is_file=False):  # eg '/folder1/file.txt'
    """

    """
    if not is_file:
      # directories / folders are virtual dirnodes ie they do not need to be stored in db
      return cls.instantiate_foldernode(npath)
    sql = 'SELECT * from %(tablename)s WHERE hkey=(?);'
    hkey = hm.HashSimple(npath).num
    tuplevalues = (hkey,)
    dbres_tuplelist = dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(dbres_tuplelist) == 0:
      return None
    dirnode = from_dbres_tuple_to_dirnode(dbres_tuplelist[0])
    fpath = os.path.join(dirnode.parentpath, dirnode.name)
    if npath != fpath:
      error_msg = 'DataError: path (%s) used to find record via its hash is different than path found (%s)'\
                  % (npath, fpath)
      raise ValueError(error_msg)
    return dirnode

  @classmethod
  def fetch_or_create_node_from_db(cls, dbtree, npath, is_file=False, sha1=None, bytesize=None, mdatetime=None):
    node = cls.fetch_node_from_db(dbtree, npath)
    if node is not None:
      return node
    return cls.create_n_return_node_in_db(npath, is_file, sha1, bytesize, mdatetime)

  @classmethod
  def create_n_return_node_in_db(cls, dbtree, npath, sha1=None, bytesize=None, mdatetime=None):
    if npath is None or npath == '/':
      return cls.root
    parentpath, name = os.path.split(npath)
    bytesize = bytesize
    mdatetime = mdatetime
    tuplevalues = (None, name, parentpath, sha1, bytesize, mdatetime)
    question_marks = '?, ' * len(tuplevalues)
    question_marks = question_marks.rstrip(', ')
    sql = "insert into %(tablename)s VALUES (" + question_marks + ");"
    # check existence
    selectsql = 'select * from  %(tablename)s where name=? and parentpath=?;'
    fetched_list = dbtree.do_select(selectsql, (name, parentpath))
    if fetched_list is None or len(fetched_list) == 0:
      # record does not exists, an insert should be tried
      _ = dbtree.do_insert_with_sql_n_tuplevalues(sql, tuplevalues)
    else:
      # id exists, an update should be tried
      row_found = fetched_list[0]
      new_row = (row_found[0], name, parentpath, sha1, bytesize, mdatetime)
      dbtree.do_update_with_all_fields_with_tuplevalues(new_row)
    return cls.fetch_node_from_db(dbtree, npath)

  @staticmethod
  def create_with_tuplerow(tuplerow, fieldnames):
    if tuplerow is None:
      return None
    if len(tuplerow) < len(fieldnames):
      return None
    _id = tuplerow[0]  # _id is always index 0
    idx = fieldnames.index('name')
    name = tuplerow[idx]
    idx = fieldnames.index('parentpath')
    parentpath = tuplerow[idx]
    idx = fieldnames.index('sha1')
    sha1 = tuplerow[idx]
    idx = fieldnames.index('bytesize')
    bytesize = tuplerow[idx]
    idx = fieldnames.index('mdatetime')
    mdatetime = tuplerow[idx]
    dirnode = DirNode(name, parentpath, sha1, bytesize, mdatetime)
    dirnode.db_id = _id
    return dirnode

  def get_db_id(self):
    return self.db_id

  def set_db_id(self, db_id):
    self.db_id = db_id

  def has_same_size_n_date(self, bytesize, mdatetime):
    if self.bytesize == bytesize and self.mdatetime == mdatetime:
      return True
    return False

  @classmethod
  def get_root_cls(cls):
    if cls._root:
      return cls._root
    # create root dirnode and store it into _dirnodes_instantiated and in class attribute _root
    cls._root = DirNode(None, None, True)
    cls._dirnodes_instantiated['/'] = cls._root
    return cls._root

  @classmethod
  def printline_nodes_with_filenotation_if_any(cls):
    for fpath in cls._dirnodes_instantiated:
      dn = cls._dirnodes_instantiated[fpath]
      fileprefixifneeded = ''
      if dn.type == MockDET.FILE:
        fileprefixifneeded = 'F '
      line = fileprefixifneeded + fpath
      print(line)

  def __init__(self, name, parentpath=None, sha1=None, bytesize=None, mdatetime=None):
    """
    This constructor should be CONSIDERED "private"
    The idea is that a unique path maps to a unique instantiated object
    The problem to solve is to avoid, as happens now, having multiple objects for the same path.

    if parent is not None:
      if type(parent) != DirNode:
        error_msg = 'Runtime error: parent %s is not of type DirNode' % parent
        raise RuntimeError(error_msg)
    """
    self.db_id = None
    self.name = name
    self.parentpath = parentpath
    self._fpath = None
    if self.parentpath is None:
      self._root = self
    self._fpath = None
    self.parent = None
    self._treename = None  # only root needs this attribute filled-in
    self.type = MockDET.DIR
    self.sha1 = sha1
    self.bytesize = bytesize
    self.mdatetime = mdatetime
    # self.treat_attributes(None)
    """
    if not self.is_root:
      self.parent.children.append(self)
    """

  def as_dict(self):
    outdict = {
      'name': self.name,
      'parentpath': self.parentpath,
      'sha1': self.sha1,
      'bytesize': self.bytesize,
      'mdatetime': self.mdatetime,
    }
    return outdict

  @property
  def fieldvalue_dict(self):
    _fieldnames_dict = {
      'name': self.name, 'parentpath': self.parentpath, 'sha1': self.sha1,
      'bytesize': self.bytesize, 'mdatetime': self.mdatetime
    }
    return _fieldnames_dict

  def update_db_name_n_parentpath(self, name, parentpath, dbtree):
    if self.db_id is None:
      return False
    sql = '''UPDATE %(tablename)s SET
      name=?,
      parentpath=?
    WHERE
      id=?;
    '''
    tuplevalues = (name, parentpath, self.db_id)
    return dbtree.do_update_with_sql_n_tuplevalues(sql, tuplevalues)

  def update_db_with_fetched_row(self, fetched_row, dbtree):
    dirnodedict = self.fieldvalue_dict
    return dbtree.do_update_with_dict_n_fetchedrow(dirnodedict, fetched_row)

  def insert_into_db(self, dbtree):
    dirnodedict = self.fieldvalue_dict
    # try db table keys existence before trying to insert it
    sql = 'SELECT * FROM %(tablename)s WHERE name=? AND parentpath=?;'
    tuplevalues = (dirnodedict['name'], dirnodedict['parentpath'])
    fetched_list = dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if fetched_list and len(fetched_list) > 0:
      fetched_row = fetched_list[0]
      return self.update_db_with_fetched_row(fetched_row, dbtree)
    return dbtree.do_insert_with_dict(dirnodedict)

  def db_fetch_siblings(self, dbtree):
    siblings = []
    sql = 'SELECT * FROM %(tablename)s WHERE parentpath=?;'
    tuplevalues = (self.parentpath, )
    fetched_rows = dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    for dictrow in fetched_rows:
      idx = dbtree.fieldnames.index('name')
      sibling_name = dictrow[idx]
      idx = dbtree.fieldnames.index('parentpath')
      sibling_parentpath = dictrow[idx]
      idx = dbtree.fieldnames.index('sha1')
      sibling_sha1 = dictrow[idx]
      idx = dbtree.fieldnames.index('bytesize')
      sibling_bytesize = dictrow[idx]
      idx = dbtree.fieldnames.index('mdatetime')
      sibling_mdatetime = dictrow[idx]
      dirnode = DirNode(
        name=sibling_name,
        parentpath=sibling_parentpath,
        sha1=sibling_sha1,
        bytesize=sibling_bytesize,
        mdatetime=sibling_mdatetime
      )
      siblings.append(dirnode)
    return siblings

  @property
  def fpath(self):
    """
    fpath is in fact a middlepath starting with '/'.
    To form the absolute "full" path see next method get_abspath_with_mountpath(mountpath)
      ie the full abspath depends on the mountpath.

    Two obs:
      1) if name is '/' (or None), both name and parentpath will be set to '/' and None respectively;
      2) the os.path.join(parentpath, name) will happen on the condition that both have types in
         [str, bytes, os.PathLike], otherwise a Value Error exception will be raised.
      On the same note, the client script must fill in parentpath and name attributes appropriately,
        mainly when these come from db using the fieldnames.index() approach.
    """
    # 1st case: return self._fpath rightaway if it's not None
    if self._fpath is not None:
      return self._fpath
    # 2nd case: if name is None or '/' (root), _fpath should also be '/' (on the way, guarantee that parentpath is None)
    if self.name is None or self.name == '/':
      self.name = '/'
      self.parentpath = None
      self._fpath = '/'
      return self._fpath
    # 3rd case: if name is None, set name to '/' (root) and recall the method itself
    if type(self.name) in [str, bytes, os.PathLike]:
      if type(self.parentpath) in [str, bytes, os.PathLike]:
        self._fpath = os.path.join(self.parentpath, self.name)
        return self._fpath
    error_msg = 'Runtime (logical) error: program cannot derive path from name (%s) and parentpath (%s).' \
                % (self.name, self.parentpath)
    raise ValueError(error_msg)

  def get_folderabspath_with_mountpath(self, mountpath):
    try:
      middlepath = self.parentpath.lstrip('./')
      return os.path.join(mountpath, middlepath)
    except (AttributeError, TypeError):
      error_msg = 'Error (attr or type ) in get_folderabspath_with_mountpath(): self.parentpath = [%s]' \
                  ' and mountpath is [%s]' % (str(self.parentpath), str(mountpath))
      raise ValueError(error_msg)

  def get_abspath_with_mountpath(self, mountpath):
    """
    fpath (above) is in fact a middlepath. This method forms the absolute "full" path
      relative to a mountpath. "mountpath" is not an attribute here, instead it's fed by a parameter.
    Notice that the abspath exists relative to mountpath. If mountpath changes, so will the abspath.
    """
    middlepath = self.fpath
    middlepath = middlepath.lstrip('/')
    if middlepath == '':
      return mountpath
    return os.path.join(mountpath, middlepath)

  def does_dirnode_exist_in_disk(self, mountpath):
    abspath = self.get_abspath_with_mountpath(mountpath)
    return os.path.exists(abspath)

  @classmethod
  def fetch_dirnode_by_id_n_db(cls, row_id, dbtree):
    sql = 'SELECT * FROM %(tablename)s WHERE id=?;'
    tuplevalues = (row_id, )
    tuplerowlist = dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if tuplerowlist and len(tuplerowlist) > 0:
      tuplerow = tuplerowlist[0]
      return cls.create_with_tuplerow(tuplerow, dbtree.fieldnames)
    return None

  def is_leaf(self, dbtree):
    if type == MockDET.FILE:
      return True
    sql = 'select * from %(tablename) where parentpath=?;'
    tuplevalues = (self.fpath,)
    tuplelist = dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(tuplelist) == 0:
      return True
    return False

  def unlink(self):
    # this must be the first command because self.path depends on name and parent which will be reset below
    if not self.is_leaf:
      error_msg = 'Error: cannot delete a non-leaf node, ie cannot delete a folder with contents.'
      raise ValueError(error_msg)
    del DirNode._dirnodes_instantiated[self.path]
    if self.is_root:
      self._root = None
    else:
      self.parent.children.remove(self)
    self.empty_node()

  def empty_node(self):
    """
    self.name = None
    self.parent = None
    self.type = None
    self._root = None
    self.children = []
    self.bytesize = None
    self.sha1hex = None
    self.mdate = None
    """
    pass

  def is_target_in_the_same_pathposition(self, trg_dirnode):
    if self.path == trg_dirnode.path:
      return True
    return False

  def treat_attributes(self, do_root):
    """
    Hypotheses verified here:
    1) raise an error if root has parent or name not None (both must be None for root -- and only for root, see 3 below)
    2) raise error when parent is FILE (parent should never be a FILE, it can only be a DIR)
    3) raise error if any non-root fails to have both name and parent as non-None
    """
    # error if root has parent or name not None
    if do_root:
      if self.name is not None or self.parent is not None:
        error_msg = 'root must have both name & parent as None'
        raise ValueError(error_msg)
      if self.type != MockDET.DIR:
        error_msg = 'root must be a DIR'
        raise ValueError(error_msg)
      return
    # error when parent is FILE
    if self.parent and self.parent.type == MockDET.FILE:
      error_msg = 'parent cannot be a FILE'
      raise ValueError(error_msg)
    # any non-root must have both name and parent as non-None
    elif self.name is None or self.parent is None:
      error_msg = 'name or parent can only be None for root'
      raise ValueError(error_msg)

  @property
  def sha1hex(self):
    if self.sha1 is None:
      return None
    return self.sha1.hex()

  @property
  def root(self):
    """
    Order of finding out root:
    1) look up class attribute self._root
    2) verify is_root() [this option, in theory, will never happen because the first one will be taken]
    3) navigate upwards back to root, in finding it setting class attribute self._root, then returning it
    If all 3 above fails, there is a logical error in code and an exception is raised.
    """
    if self._root is not None:
      return self._root
    if self.is_root:
      self._root = self
      return self
    dirnode = DirNode(
      name='/',
      parentpath=None,
      sha1=None,
      bytesize=None,
      mdatetime=None
    )
    self._root = dirnode

  @property
  def is_root(self):
    """
    The condition:
          if self == self._root:
      is not tested here because this is_root() verification is called earlier from __init__()
      when deciding whether or not to parent.append(self)
    Because self._root is only set later, a False would return and
      an append() would be called upon a None (root's parent) which raises an exception.
    """
    if self.name is None and self.parent is None:
        return True
    return False

  @property
  def is_dir(self):
    if self.type == MockDET.DIR:
      return True
    return False

  @property
  def is_file(self):
    if self.type == MockDET.FILE:
      return True
    return False

  @property
  def n_levels(self):
    if self.is_root:
      return 0
    if self.parent.is_root:
      return 1
    current_node = self
    _n_levels = 0
    while not current_node.is_root:
      # do not use current_node.root (instead of is_root) probably because of the order of setting (may infinite loop)
      _n_levels += 1
      current_node = current_node.parent
    return _n_levels

  @property
  def linepath(self):
    """
    linepath is the same as path exception for FILE type when linepath = 'F ' + path ie files receive prefix 'F '
    """
    if self.type == MockDET.FILE:
      return PREFIX_FOR_FILES_LINEPATH + self.path
    return self.path

  @property
  def path(self):
    """
    Historical note: this method was previously recursive.
    After this first choice, the change was to implement a navigation
      node by node upwards back to root with a while-loop.
    However, at this moment, the implementation keeps the path a the key (hash) to the object in the store dict.
    Because of this detail, this method should be avoided and the key/hash should be used instead.

    Example:
       path = '/a/b/c'
    This is generated by this method but, as said above, it's also kept in the dict.
    # previous code
    if self.is_root:
        return '/'
    obj = self
    ongoingpath = ''
    whiledepth = 0
    while 1:
      whiledepth += 1
      if whiledepth > MockDET.MAX_DIRS_LEVEL:
        print('whiledepth > MAX_WHILE_DEPTH')
        break
      if obj.is_root:
        break
      ongoingpath = '/' + obj.name + ongoingpath
      obj = obj.parent
    return ongoingpath
    """
    if self.parentpath is None:
      return '/'
    return os.path.join(self.parentpath, self.name)

  def dbupdate_new_path_to(self, mold_dirnode, dbtree):
    if mold_dirnode is None or dbtree is None:
      return None
    sql = '''UPDATE %(tablename)s SET 
      name=?,
      parentpath=?
    WHERE
      name=? AND,
      parentpath=? AND
      sha1=?;
    '''
    newname = mold_dirnode.name
    newparentpath = mold_dirnode.parentpath
    oldname = self.name
    oldparentpath = self.parentpath
    tuplevalues = (newname, newparentpath, oldname, oldparentpath, self.sha1)
    return dbtree.do_update_with_sql_n_tuplevalues(sql, tuplevalues)

  def form_path_with_mountprefix(self, mountprefix):
    midpath = self.path.lstrip('/')
    return os.path.join(mountprefix, midpath)

  def __lt__(self, other_node):
    if other_node is None:
      return False
    if self.name < other_node.name:
      return True
    return False

  def __str__(self):
    name = self.name
    if name is None:
      name = '//root//'
    pdict = {
      'name': name, 'parentpath': self.parentpath, 'sha1hex': self.sha1hex,
      'bytesize': self.bytesize, 'mdatetime': self.mdatetime}
    outstr = '''DirNode name = {name} 
    parentpath = {parentpath}
    sha1hex = {sha1hex}
    bytesize = {bytesize}
    mdatetime = {mdatetime}
    '''.format(**pdict)
    return outstr


def adhoc_test():
  name = 'file1'
  parentpath = '/folder1/secondç'
  sha_obj = hashlib.sha1()
  strdata = 'dafbn bnç~pafsdkç'.encode('utf8')
  sha_obj.update(strdata)
  sha1 = sha_obj.digest()
  bytesize = 1000
  mdatetime = datetime.datetime.now()
  node = DirNode(
    name=name,
    parentpath=parentpath,
    sha1=sha1,
    bytesize=bytesize,
    mdatetime=mdatetime
  )
  print(node)


def process():
  adhoc_test()


if __name__ == '__main__':
  process()
