#!/usr/bin/env python3
import copy
import datetime
import hashlib
import math
import os.path
import lib.hashfunctions.hash_mod as hm
import lib.db.dbdirtree_mod as dbu
import models.entries.dirnode_mod as dn

LF = '\n'
PREFIX_FOR_FILES_LINEPATH = 'F '


class MockDirEntryType:
  DIR = 'DIR'
  FILE = 'FILE'
  MAX_DIRS_LEVEL = 500


MockDET = MockDirEntryType


def get_linepathlist_from_dbtuplelist(tuplelist):
  fpaths = []
  for tuplerec in tuplelist:
    name = tuplerec[2]
    parentpath = tuplerec[3]
    fpath = os.path.join(parentpath, name)
    fpaths.append(fpath)
  return fpaths


def extract_all_paths_from_path(fpath):
  if fpath is None or fpath == '/':
    return ['/']
  pp = fpath.split('/')
  if len(pp) == 0:
    return []
  if pp[0] == '':
    del pp[0]
  if len(pp) == 0:
    return []
  size = len(pp)
  outlist = []
  for i in range(1, size+1):
    cpath = '/' + '/'.join(pp[0:i])
    cpath = cpath.rstrip('/')
    if cpath == '':
      continue
    outlist.append(cpath)
  if len(outlist) > 0 and outlist[0] != '/':
    outlist.insert(0, '/')
  return outlist


def expand_tree_with_fpaths(fpaths):
  fpaths.sort()
  tmptreeset = set('/')
  for fpath in fpaths:
    outlist = extract_all_paths_from_path(fpath)
    for chunk in outlist:
      tmptreeset.add(chunk)
  tmptreelist = list(tmptreeset)
  tmptreelist.sort()
  # second pass: put the prefix 'F ' in the original linepaths in fpaths
  second_pass_outlist = []
  for linepath in tmptreelist:
    if linepath in fpaths:
      # linepath is FILE
      newlinepath = 'F ' + linepath
      second_pass_outlist.append(newlinepath)
    else:
      second_pass_outlist.append(linepath)
  tree_repr_str = '\n'.join(second_pass_outlist)
  return tree_repr_str


def normalize_repr_str(repr_str):
  """
  normalize_repr_str() has THREE operations, ie:
  1) removes beginning '\n'
  2) removes, line by line, beginning ' \t'
  3) removes ending '\n'
  """
  if repr_str is None:
    return None
  repr_str = repr_str.lstrip('\n')
  if repr_str in ['', '/']:
    return '/'
  outstr = ''
  for line in repr_str.split('\n'):
    line = line.lstrip(' \t')
    if len(line) > 1:
      # ie a linepath may not end with '/' except root itself which has linepath equals to '/' (/ starts and ends it)
      line = line.rstrip('/')
    if line == '':
      continue
    outstr += line + LF
  # remove last line-feed
  outstr = outstr.rstrip('\n')
  return outstr


def transform_linepathlist_to_linepathstr(missing_files):
  outstr = '\n'.join(missing_files)
  return normalize_repr_str(outstr)


class DirTree:

  treenames_dict = {}
  # MAX_DIRS_LEVEL = MockDET.MAX_DIRS_LEVEL

  def __init__(self, treename, mountpath=None):
    """
    This constructor should be considered PRIVATE
      ie to get a dirtree use instead the classmethod add_dirtree_by_tname_n_pxpath(treename, prefix)
    """
    self.mountpath = mountpath
    self.dbtree = dbu.DBDirTree(self.mountpath)
    self.treename = treename
    self._nodes_dict = {}
    self._all_sha1hexes = None
    self.n_clean_dbtable = 0
    self.root = None
    self.init_root()

  def verify_node_in_db(self, node):
    if node is None:
      return None
    verified = False
    tuplelist = self.dbtree.fetch_row_by_id(node.hashkey)
    if len(tuplelist) == 1:
      tuplevalues = tuplelist[0]
      if node.name == tuplevalues[2]:
        if node.parentpath == tuplevalues[3]:
          if node.is_file == tuplevalues[4]:
            if node.sha1 == tuplevalues[5]:
              if node.bytesize == tuplevalues[6]:
                verified = True
    return verified

  def add_node_to_tree_in_db_n_return_it(self, node):
    nd = node
    tuplevalues = (None, nd.hashkey, nd.name, nd.parentpath, nd.is_file, nd.sha1, nd.bytesize, nd.mdatetime)
    bool_res = self.dbtree.do_insert_or_update_with_tuplevalues(tuplevalues)
    if bool_res:
      return node
    tuplelist = self.dbtree.fetch_row_by_id(nd.hashkey)
    if len(tuplelist) > 0:
      return node
    return None

  @property
  def tablename(self):
    return 'dbtree_' + self.treename

  def add_filenode_with_linepath_n_content_n_return_it(self, linepath, content, pdatetime=None):
    parentpath, name = os.path.split(linepath)
    sha1o = hashlib.sha1()
    sha1o.update(content.encode('utf8'))
    sha1bin = sha1o.digest()
    bytesize = len(content)
    mdatetime = pdatetime
    if pdatetime is None:
      mdatetime = datetime.datetime.now()
    node = dn.DirNode(
      name=name,
      parentpath=parentpath,
      sha1=sha1bin,
      bytesize=bytesize,
      mdatetime=mdatetime
    )
    return self.add_node_to_tree_in_db_n_return_it(node)

  @classmethod
  def put_root_into_a_dirtree_by_name_n_get_dirtree(cls, root, treename):
    dirtree = cls.get_dirtree_by_name(treename)
    dirtree.reset_root(root)
    return dirtree

  @classmethod
  def get_dirtree_by_name(cls, treename):
    if treename in cls.treenames_dict:
      return cls.treenames_dict[treename]
    tree = DirTree(treename)
    cls.treenames_dict[treename] = tree
    return tree

  @classmethod
  def add_n_get_dirtree_by_tname_n_pxpath(cls, treename, prefixpath):
    if treename in cls.treenames_dict:
      t = cls.treenames_dict[treename]
      t.mountpath = prefixpath
      return t
    t = DirTree(treename, prefixpath)
    cls.treenames_dict[treename] = t
    t.mountpath = prefixpath
    return t

  def does_node_exist_via_linepath(self, linepath):
    if linepath is None:
      return False
    if linepath.startswith('F '):
      linepath = linepath[2:]
    if linepath in self._nodes_dict:
      return True
    return False

  def get_or_make_node_by_linepath(self, linepath):
    """
    strpath is the '/'-joined string
    when path is FILE, it starts with string "F " (F plus a space)
    """
    if linepath is None:
      return self.root
    last_node_is_file = False
    if linepath.startswith('F '):
      linepath = linepath[2:]
      last_node_is_file = True
    current_parent_node = self.root
    if linepath in self._nodes_dict:
      return self._nodes_dict[linepath]
    cumulative_path = ''
    current_node = None
    pp = linepath.split('/')
    for cpath in pp[1:]:
      cumulative_path += '/' + cpath
      if cumulative_path in self._nodes_dict:
        current_node = self._nodes_dict[cumulative_path]
      else:
        if cpath == pp[-1] and last_node_is_file:
          current_node = dn.DirNode(cpath, current_parent_node)
          current_node._is_file = MockDET.FILE
        else:
          current_node = dn.DirNode(cpath, current_parent_node)
        # the one instantiated must be stored into dict (notice each node must be instantiated only once)
        self._nodes_dict[cumulative_path] = current_node
      current_parent_node = current_node
    if current_node is not None:
      self._nodes_dict[linepath] = current_node
    return current_node

  @property
  def all_sha1hexes(self):
    if self._all_sha1hexes is not None:
      return self._all_sha1hexes
    self._all_sha1hexes = {}
    linepaths_list = self.as_tree_only_filelinepaths_as_list()
    for linepath in linepaths_list:
      node = self.get_or_make_node_by_linepath(linepath)
      self._all_sha1hexes[node.sha1hex] = linepath
    return self._all_sha1hexes

  def dbinsert_dirnode(self, dirnode):
    tuplevalues = (
      None, dirnode.name, dirnode.parentpath,
      dirnode.sha1, dirnode.bytesize, dirnode.mdatetime
    )
    return self.dbtree.do_insert_or_update_with_tuplevalues(tuplevalues)

  def as_tree_only_filelinepaths_as_list(self):
    linepaths_str = self.as_tree_only_files_linepath_repr_str()
    if linepaths_str is None:
      return []
    linepaths = linepaths_str.split('\n')
    filelinepaths_as_list = []
    for line in linepaths:
      if line == '':
        # double empties is the result of split('\n'), triple of split('\n\n') and so on
        continue
      filelinepaths_as_list.append(line)
    return filelinepaths_as_list

  def print_tree_graphlines_as_interable(self):
    stack = [self.root]
    zfill_n = math.floor(math.log(self.n_of_nodes, 10) + 1)
    n_node = 0
    while len(stack) > 0:
      node = stack.pop()
      n_node += 1
      if node.name is None:
        outline = str(n_node).zfill(zfill_n) + ' ' + '/'
      else:
        outline = str(n_node).zfill(zfill_n) + ' ' + '---| ' * node.n_levels + node.name
      yield outline
      substack = copy.copy(node.children)
      substack.sort()
      substack.reverse()
      stack += substack
    lastline = 'N of nodes = ' + str(self.n_of_nodes)
    yield lastline

  def get_all_tree_nodes_as_interable(self):
    stack = [self.root]
    while len(stack) > 0:
      node = stack.pop()
      yield node
      substack = copy.copy(node.children)
      substack.sort()
      substack.reverse()
      stack += substack

  def get_file_tree_nodes_as_interable(self):
    for node in self.get_all_tree_nodes_as_interable():
      if node.type != MockDET.FILE:
        continue
      yield node

  def print_whole_dirtree_node_by_node(self):
    outstr = str(self.root) + '\n'
    stack = [self.root]
    while len(stack) > 0:
      node = stack.pop()
      outstr += str(node) + '\n'
      substack = copy.copy(node.children)
      substack.sort()
      substack.reverse()
      stack += substack
    return outstr

  def get_missing_file_nodes_with_sha1(self, other_tree):
    missing_file_nodes = []
    for ot_node in other_tree.get_file_tree_nodes_as_interable():
      if ot_node.sha1hex not in self.all_sha1hexes:
        missing_file_nodes.append(ot_node)
    return missing_file_nodes

  def get_missing_file_nodes_with_sha1_as_set(self, other_tree):
    return set(self.get_missing_file_nodes_with_sha1(other_tree))

  def get_missing_files_as_repr_str_list_compared_to(self, other_tree):
    ot_linepath_repr_str = other_tree.as_tree_only_files_linepath_repr_str()
    ot_linepaths = ot_linepath_repr_str.split('\n')
    linepaths_str = self.as_tree_only_files_linepath_repr_str()
    linepaths = linepaths_str.split('\n')
    missing_files = []
    for ot_linepath in ot_linepaths:
      if ot_linepath not in linepaths:
        missing_files.append(ot_linepath)
    return missing_files

  def get_missing_files_as_repr_str_compared_to(self, other_tree):
    missing_files = self.get_missing_files_as_repr_str_list_compared_to(other_tree)
    return transform_linepathlist_to_linepathstr(missing_files)

  def copy(self, origin_node, target_tree):
    """
    delete_node_if_copy_fails = False
    if not bool_exists:
      delete_node_if_copy_fails = True
    """
    print('Copying')
    linepath = origin_node.linepath
    # bool_exists = target_tree.does_node_exist_via_linepath(linepath)
    target_node = target_tree.get_or_make_node_by_linepath(linepath)
    print('source:', origin_node.form_path_with_mountprefix(self.mountpath))
    print('target:', target_node.form_path_with_mountprefix(target_tree.mountpath))

  def move(self, origin_node, target_tree):
    origin_path = self.mountpath + origin_node.path
    target_path = target_tree.mountpath + origin_node.path
    print('Move')
    print(origin_path)
    print(target_path)
    # add it to target tree
    # _ = dn.DirNode.get_dirnode_instance(target_path)
    # remove it from source tree
    origin_node.unlink()

  def copy_all_entries_to(self, other_dt):
    other_dt.add_nodes_with_tree_repr_str(self.as_tree_repr_str())

  def reset_root(self, root):
    self.root = root
    self.root.treename = self.treename
    self._nodes_dict['/'] = self.root

  def init_root(self):
    """
    """
    self.root = dn.DirNode('/', None)
    self.root.treename = self.treename
    self._nodes_dict['/'] = self.root

  def add_nodes_with_tree_repr_str(self, tree_repr_str):
    """
    Note that after the first "if" the input parameter is normalized by normalize_repr_str(tree_repr_str)
    This will remove, if any, a beginning and ending '\n' and, line by, line, beginning ' \t' (in the lstrip() context)
    """
    if tree_repr_str is None or len(tree_repr_str) == 0:
      return None
    # clean up last \n if any
    tree_repr_str = normalize_repr_str(tree_repr_str)
    lines = tree_repr_str.split('\n')
    for line in lines:
      # clean up left spaces
      if not (line.startswith('/') or line.startswith('F /')):
        error_msg = 'path char must either begins with a / (slash) or with "F /" (it was %s)' \
                    % line
        raise ValueError(error_msg)
      node = self.get_or_make_node_by_linepath(line)
      fpath = node.path
      self._nodes_dict[fpath] = node

  @property
  def n_of_nodes(self):
    tuplelist = self.dbtree.fetch_all()
    fpaths = get_linepathlist_from_dbtuplelist(tuplelist)
    result_tree_repr_str = expand_tree_with_fpaths(fpaths)
    if result_tree_repr_str is None or result_tree_repr_str == '':
      return 0
    n_nodes = len(result_tree_repr_str.split('\n'))
    return n_nodes

  def clean_dbtable(self):
    self.n_clean_dbtable += 1
    self.dbtree.delete_all_rows()

  def load_dbtable_with_fpaths_n_contents(self, fpaths_n_contents):
    for tuplerec in fpaths_n_contents:
      linepath = tuplerec[0]
      contents = tuplerec[1]
      _ = self.add_filenode_with_linepath_n_content_n_return_it(linepath, contents)

  def has_linepath_in_tree_with_like_operator(self, linepath):
    sql = 'SELECT * from %(tablename)s WHERE parentpath LIKE ?;'
    tuplevalues = (linepath, )
    tuplelist = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(tuplelist) > 0:
      return True
    return False

  def has_linepath_in_tree(self, linepath):
    if linepath is None:
      return False
    if linepath == '/':
      return True  # ie root is always (at least virtually) in the tree
    # linepath is supposed to begin with prefix 'F ' for files (at this moment every db-record represents a file)
    linepath = linepath.lstrip('F ')
    hkey = hm.HashSimple(linepath).num
    # is_file is a logical complement, though, at this time, we haven't decided to make db a file-only store definitely
    tuplevalues = (hkey, )
    sql = 'SELECT * from %(tablename)s WHERE hkey=?;'
    tuplelist = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(tuplelist) > 0:
      return True
    # this method here finds a file-record, it's still necessary to look for folders with the LIKE operator for parents
    return self.has_linepath_in_tree_with_like_operator(linepath)

  def has_linepath_in_tree_v2(self, linepath):
    """
    This "v2" uses name and parentpath in the WHERE-clause in the SELECT.
    The "v1" above use hkey derived from fpath (which is os.path.join(parentpath, name) excepting for root).
    """
    if linepath is None:
      return False
    if linepath == '/':
      return True  # ie root is always (at least virtually) in the tree
    # linepath is supposed to begin with prefix 'F ' for files (at this moment every db-record represents a file)
    linepath = linepath.lstrip('F ')
    parentpath, name = os.path.split(linepath)
    tuplevalues = (name, parentpath)
    sql = 'SELECT * from %(tablename)s WHERE name=? and parentpath=?;'
    tuplelist = self.dbtree.do_select_with_sql_n_tuplevalues(sql, tuplevalues)
    if len(tuplelist) > 0:
      return True
    # this method here finds a file-record, it's still necessary to look for folders with the LIKE operator for parents
    return self.has_linepath_in_tree_with_like_operator(linepath)

  def as_tree_repr_str(self):
    outstr = ''
    for cpath in self._nodes_dict.keys():
      outstr += cpath + LF
    # remove last line-feed
    outstr = outstr.rstrip('\n')
    return outstr

  def as_tree_linepath_repr_str(self):
    """
    old algo
    ========
    outstr = ''
    for cpath in self._nodes_dict.keys():
      if_file_prefix_notation = ''
      node = self._nodes_dict[cpath]
      if node.type == MockDET.FILE:
        if_file_prefix_notation = 'F '
      outstr += if_file_prefix_notation + cpath + LF
    # remove last line-feed
    outstr = outstr.rstrip('\n')
    return outstr
    """
    fpaths = []
    tuplelist = self.dbtree.fetch_all()
    for tuplerec in tuplelist:
      name = tuplerec[2]
      parentpath = tuplerec[3]
      fpath = os.path.join(parentpath, name)
      fpaths.append(fpath)
    return expand_tree_with_fpaths(fpaths)

  def as_tree_only_files_linepath_repr_str(self):
    """
    This method is planned to return the same repr_str that forms a tree inputing only files.
    This approach values the composition of a tree with files (as its subdirs will be autocreated)
      and ignores leaf directories ie folders that are empty.
    This method uses the return of as_tree_linepath_repr_str() and ajust it for output.
    """
    instr = self.as_tree_linepath_repr_str()
    lines = instr.split('\n')
    outstr = ''
    for line in lines:
      if line.startswith('F '):
        outstr += line + LF
    # remove last line-feed
    outstr = outstr.rstrip('\n')
    return outstr

  def __eq__(self, other):
    """
    This equality operator is not to compare that the objects are the same (ie with the same id),
      instead it just compares the trees themselves for equality.
    """
    str_repr_self = self.as_tree_repr_str()
    str_repr_other = other.as_tree_repr_str()
    if str_repr_self == str_repr_other:
      return True
    return False

  def __str__(self):
    outstr = '''
    treename = {0}
    n of nodes = {1}'''.format(self.treename, self.n_of_nodes)
    return outstr


def process():
  pass


if __name__ == '__main__':
  # process()
  pass
