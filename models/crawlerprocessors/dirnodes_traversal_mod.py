#!/usr/bin/env python3
"""

"""
import hashlib
import os
import models.samodels as sam
import fs.os.utilsmod as osutil
import fs.hashfunctions.hexfunctionsmod as hexfs
import fs.os.middlepathmakemod as midpath
import models.pathpositioning.metafilemod as metaf
import string
import config

hexdigits = string.digits + 'abcdef'


def add_bins(b1, b2):
  b1 = b1.strip('0b')
  b2 = b2.strip('0b')
  return b1 + b2


def create_virtual_root_node(mountpoint_abspath):
  middlepathobj = midpath.MiddlePath(mountpoint_abspath)
  abspath, dirname = os.path.split(mountpoint_abspath)
  return DirNode(dirname, '', middlepathobj)


class DirNode:
  """
  This class worked -- when it was first implemented -- with link nodes, parent to children,
  each child to its parent and root to parent None.

  The above strategy was changed to a new one that included mountpoint_abspath as a way to
  traverse parent to child and vice-versa.  There remains only a children-list in nodes to
    immediate get a node's children.  For a child to get its parent, it just traverses the path
    one dir up, no need to link to it, it may just recreate a node on-the-fly or, perhaps,
    get it from db, supposing no object pool exists, in which case a pool might return it.
    (Duplication is avoided because if a node is recreated, it's not part of the running thread anymore.)

  Ie in the first strategy there was no mountpoint attribute and traversal was done by the cross-links,
  whereas in the second strategy traversal does not depend on cross-link but on mountpoint as a path.

  The first strategy was moved to the museum package (fs.museum) and the second one is here.

  Another important comment is a sql integrity error due to a fetch happening
    inbetween additions not yet committed.

  That one was a tricky problem that was solved changing the moment of db-saving.
    In the former recording strategy, node were saved as traversal happened,
    but part of the traversal was recursive. The new strategy was to one a full pass to find nodes
    and db-record them. Then, a second pass was to calculate sha1's for them.

  The different is suble to notice. When a first pass happens, db-recording may occur
    from top to bottom, whereas when the sha1 second pass happen, db-recording must be bottom to top,
    because parents depend on their children sha1's to get theirs.

  The docstring for the first strategy in fs.museum contains for extra info though code here
    also attempts to explain itself.

  Middlepath rules:
    1) the virtual root node has middlepath None;
    2) first level directories have middlepath as '' (empty string);
    3) second level  directories has middlepath as the names of their parents;
    4) third level directories have middlepath as 'greatparentname/parentname'
    And so on.

  Middlepath examples:

  1) directory 'abc' on mountpoint:
    abspath is mountpoint
    middlepath is ''
    dirname is 'abc'

  2) directory 'abc/def' on mountpoint:
    abspath is mountpoint/abc/def
    middlepath is 'abc'
    dirname is 'def'

  2) directory 'science/physics/relativity' on mountpoint:
    abspath is mountpoint/science/physics/relativity
    middlepath is 'science/physics'
    dirname is 'relativity'

  4) directory mountpoint itself: (notice this one is an exception and is not db-recordable)
    abspath is mountpoint (exception because first level directories will have mountpoint as well
      this is necessary because a root node may be a device mountpoint or even '/' itself)
    middlepath is None (second exception, all other nodes will a string, even if it's the empty string)
    dirname is 'mountpoint-top-dirname' or, if '/', None
  """

  def __init__(self, dirname, middlepath, middlepathobj):
    self.parentnode = None  # auxiliary attribute mainly for external use though it can be used inside
    self.dirname = dirname
    self.middlepathobj = middlepathobj
    self.mountpoint_abspath = middlepathobj.mount_abspath  # mountpoint_abspath
    if middlepath is None:
      error_msg = 'Error: passed middlepath as None to DirNode na=[%s] mi=[%s] mo=[%s]' \
        % (str(dirname), str(middlepath), str(middlepathobj))
      raise ValueError(error_msg)
    self.middlepath = middlepath
    self.sha1hex = None
    self.concatenated_sha1hex = ''
    self.sha1_calculated_children = []
    self._abspath = None
    self._nsubnodes = None  # recursive
    self.children_dirnames = []
    self.set_children_dirnames_via_os()
    # self._level = None  #
    # self._root = None  # root now is the exception node of mountpoint_abspath (not db-recordable)

  def set_children_dirnames_via_os(self):
    self.children_dirnames = osutil.find_directory_entries_in_abspath(self.abspath, sort=True)

  def form_n_return_middlepath_to_a_childs_entry(self, dirname):
    return self.middlepathobj.middle_to_a_childs_entry(self.abspath, dirname)

  def create_childnode_with_name(self, dirname):
    """
    This routine creates a childnode and fills its parentnode with self.
    Notice that the childnode is not added to parent in this method,
      because this adding function is application-dependent and can be deferred to
      the moment this list may be filled in.
    At any rate, at instantiation, an os.listdir() will fill in children_dirnames
      ie children names will come from os at instantiation.
    Notice also this may be changed in the future with a flag
      to fill them in thru db or another flag that makes this class
      work under mockmode, ie without any underlying os or db.
    """
    middlepath = self.form_n_return_middlepath_to_a_childs_entry(dirname)
    childnode = DirNode(dirname, middlepath, self.middlepathobj)
    childnode.parentnode = self
    return childnode

  def is_root(self):
    """
    Notice that middlepath is always None for root and dirname is None when abspath is '/' itself
    Also that middlepath should never be None for any other node.
    """
    if self.middlepath is None:
      return True
    return False

  def is_leaf(self):
    if len(self.children_dirnames) == 0:
      return True
    return False

  @property
  def abspath(self):
    """
    """
    if self._abspath is not None:
      return self._abspath
    if self.middlepath == '':
      # root has conventioned middlepath the empty string ''
      return self.mountpoint_abspath
    self._abspath = os.path.join(self.mountpoint_abspath, self.middlepath)
    return self._abspath

  @property
  def middlepathforchildren(self):
    if self.is_root():
      return self.dirname
    # code must be improved later on and withdraw this try-block
    # see further explanation below
    try:
      pmiddlepathforchildren = self.middlepath + '/' + self.dirname
      pmiddlepathforchildren = pmiddlepathforchildren.strip('/')
      return pmiddlepathforchildren
    # a TypeError will happen when self.middlepath is None
    # which was the old convention for root (now root has middlepath = '')
    except TypeError:
      pass
    return self.dirname

  def get_subdirectories(self, viadb=False):
    if viadb:
      return []  # yet to implement
    if self.is_root():
      abspath_for_children = os.path.join(self.abspath)
    else:
      abspath_for_children = os.path.join(self.abspath, self.dirname)
    subentries = osutil.find_directory_entries_in_abspath(abspath_for_children)
    return subentries

  def add_sha1_calculated_child(self, child):
    if type(child) != DirNode:
      error_msg = 'Error: attempted to add a non DirNode node (%s) to its calculated sha1 list' % str(child)
      raise ValueError(error_msg)
    self.sha1_calculated_children.append(child)

  def get_next_not_yet_sha1_calculated_children(self):
    n_of_calculated = len(self.sha1_calculated_children)
    if n_of_calculated > len(self.children_dirnames) - 1:
      return []
    yet_to_go = self.children_dirnames[n_of_calculated:]
    return yet_to_go

  def find_children_dirnodes(self, viadb=False):
    subentries = self.get_subdirectories(viadb)
    subnodes = []
    for child_dirname in subentries:
      if self.is_root():
        child_middlename = ''
      else:
        child_middlename = self.middlepath + '/' + child_dirname
        child_middlename = child_middlename.strip('/')  # this is in case middlepath is '' (1st level)
      childnode = DirNode(child_dirname, child_middlename, self.middlepathobj)
      subnodes.append(childnode)
    return subnodes

  def get_parent(self):
    if self.is_root():  # root is level 0
      return None
    if self.get_level() == 1:  # ex dirname = 'abc' & middlepath = ''
      return self.get_root()
    if self.get_level() == 2:  # ex dirname = 'def' & middlepath = 'abc'
      parentdirname = self.middlepath
      parentmiddlepath = ''
    else:  # ie level > 2 | ex dirname = 'relativity' & middlepath = 'science/physics'
      pp = self.middlepath.split('/')
      parentdirname = pp[-1]  # ie 'science' in the example above
      parentmiddlepath = '/'.join(pp[:-2])  # '' in the example above
    parentdirnode = DirNode(parentdirname, parentmiddlepath, self.middlepathobj)
    return parentdirnode

  @property
  def parentdirname(self):
    if self.parentnode is None:
      return ''
    return self.parentnode.dirname

  @property
  def level(self):
    """
    Logical attribute derived from a look-up at middlepath.
    It's not kept with an underline attribute such as self._level,
    it's recalculated at every issue on-the-fly.
    """
    return self.get_level()

  def get_level(self):
    """
    Called from @property level.

    Examples:
      1) the two exceptions:
      e1) nodepath is mountpoint is the root node
        its middlepath is None and its level is 0
      e2) nodepath is mountpoint/abc
        its middlepath is '' and its level is 1
      2) the general rule:
      g1) nodepath is mountpoint/abc/def
        its middlepath is 'abc' and its level is 2 (ie len(middlepath.split('/')) + 1)
      g2) nodepath is mountpoint/science/physics/relativity
        its middlepath is 'science/physics' and its level is 3 (ie len(middlepath.split('/')) + 1)
    """
    # exception 1 (root)
    if self.is_root():
      return 0
    # exception 2 (first level directories)
    if self.middlepath == '':
      return 1
    # general rule (second level directories and beyond)
    above_hierarchy_levels = len(self.middlepath.split('/'))
    this_nodes_level = above_hierarchy_levels
    return this_nodes_level

  def get_root(self):
    return create_virtual_root_node(self.mountpoint_abspath)

  def make_parent(self):
    """
    This method reinstantiate parent. Notice it depends on the underlying caller, whether
      it has or does not have parent in a kind of pool, in which case this method should be avoided.
    """
    if self.middlepath is None:
      return None
    if self.middlepath == '':
      return self.get_root()
    pp = self.middlepath.split('/')
    if len(pp) == 1:
      dirname = pp[0]
      middlepath = ''
    else:
      dirname = pp[-1]
      middlepath = '/'.join(pp[:-1])
    parentnode = DirNode(dirname, middlepath, self.middlepathobj)
    return parentnode

  def save_to_db(self, session, docommit=True):
    # root node is not dbsaved, it's always recreated on-the-fly
    if self.is_root() == self:
      return None
    # look up itself in db
    was_changed = False
    # take care! boolean values in sqlite should be int's (0|1)
    dbentry = session.query(sam.FSEntryInDB). \
        filter(sam.FSEntryInDB.entryname == self.dirname). \
        filter(sam.FSEntryInDB.middlepath == self.middlepath). \
        filter(sam.FSEntryInDB.isfile == 0). \
        first()
    if not dbentry:
      was_changed = True
      dbentry = sam.FSEntryInDB()
      session.add(dbentry)
    if dbentry.entryname != self.dirname:
      was_changed = True
      dbentry.entryname = self.dirname
    if dbentry.middlepath != self.middlepath:
      was_changed = True
      dbentry.middlepath = self.middlepath
    if dbentry.isfile != 0:  # self.isfile:
      was_changed = True
      dbentry.isfile = 0
    if dbentry.sha1hex != self.sha1hex:
      was_changed = True
      dbentry.sha1hex = self.sha1hex
    if was_changed and docommit:
      print('db committing')
      session.commit()
    else:
      print('not committing: was_changed =', was_changed, 'docommit =', docommit)
    return dbentry

  def calc_sha1hex_n_save_into_db(self, session):
    self.process_entry_sha1hex(session)

  def concatenate_sha1hexes_from_files(self, session=None):
    if session is not None:
      return self.concatenate_sha1hex_of_children_files_via_db(session)
    fileentries = osutil.find_file_entries_in_abspath(self.abspath)
    for filename in fileentries:
      if self.middlepath is None:
        middlepathforfile = ''
      else:
        middlepathforfile = self.middlepath + '/' + self.dirname
        middlepathforfile = middlepathforfile.strip('/')
      mfile = metaf.MetaFile(self.mountpoint_abspath, middlepathforfile, filename)
      mfile.calc_n_set_sha1hex()
      self.concatenated_sha1hex += mfile.sha1hex

  def concatenate_sha1hexes_from_folders(self, session=None):
    if session is not None:
      return self.concatenate_sha1hexes_from_folders_viadb(session)
    for dirnode in self.sha1_calculated_children:
      self.concatenated_sha1hex += dirnode.concatenated_sha1hex

  def concatenate_sha1hexes_from_folders_viadb(self, session):
    """
    _ = session
    self.concatenated_sha1hex = ''
    """
    pass

  def concatenate_sha1hexes(self, session=None):
    self.concatenated_sha1hex = ''
    self.concatenate_sha1hexes_from_files(session)
    self.concatenate_sha1hexes_from_folders(session)
    if self.concatenated_sha1hex == '':
      self.concatenated_sha1hex = config.EMPTYFILE_SHA1HEX
    # self.sha1hex = hexfs.calc_sha1hex_from_str(self.concatenated_sha1hex)

  def process_entry_sha1hex(self, session=None):
    if self.sha1hex:
      return
    if session is None:
      sha1sconcatenated = self.concatenate_sha1hex_topdownindepth()
    else:
      sha1sconcatenated = self.concatenate_sha1hex_topdownindepth_via_db(session)
    self.calc_n_set_sha1hex_with_concatenated_hexstring(sha1sconcatenated)
    if session:
      self.save_to_db(session)

  def calc_n_set_sha1hex_with_concatenated_hexstring(self, sha1sconcatenated):
    if self.concatenated_sha1hex == '':
      self.concatenated_sha1hex = config.EMPTYFILE_SHA1HEX
    self.sha1hex = hexfs.calc_sha1hex_from_str(sha1sconcatenated)

  def concatenate_sha1hex_topdownindepth(self):
    sha1sconcatenated = self.concatenate_sha1hex_of_children_files()
    for child_dirnode in self.children_dirnames:
      sha1sconcatenated += child_dirnode.concatenate_sha1hex_topdownindepth()
    if sha1sconcatenated == '':
      sha1sconcatenated = config.EMPTYFILE_SHA1HEX
    return sha1sconcatenated

  def concatenate_sha1hex_topdownindepth_via_db(self, session):
    sha1sconcatenated = ''
    for child_dirnode in self.children_dirnames:
      sha1sconcatenated += child_dirnode.concatenate_sha1hex_topdownindepth_via_db(session)
    sha1sconcatenated = self.concatenate_sha1hex_of_children_files_via_db(session)
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
  def stamp(self):
    return '<Node "%s" p="%s" l=%d>' % (self.dirname, self.parentdirname, self.level)

  @property
  def stamp2(self):
    rootdirname = self.get_root().dirname
    if rootdirname is None:
      rootdirname = '~[root]'
    return 'sha1 %s | middle %s | rootname %s' % (self.sha1hex, self.middlepath, rootdirname)

  def __str__(self):
    spaces = ' ' * 2 * self.level
    outstr = spaces + '%s\n' % self.stamp
    outstr += spaces + '%s\n' % self.stamp2
    for child in self.children_dirnames:
      outstr += str(child)
    return outstr


def find_leftmost_bottommost_folderabspath(abspath):
  entries = osutil.find_directory_entries_in_abspath(abspath)
  if len(entries) == 0:
    return abspath
  leftmost_entry = entries[0]
  leftmost_entry_abspath = os.path.join(abspath, leftmost_entry)
  return find_leftmost_bottommost_folderabspath(leftmost_entry_abspath)


traversal_stack = []


class BottomUpTraversal:
  """
  This class adds two method in class DirNode, they are:
    1) add_sha1_calculated_child(node) => appends to a list of known/calculated sha1 files belonging to a folder;
    2) get_next_not_yet_sha1_calculated_children() => returns children that are not yet sha1-calculated;
       "to the right" means that children entries are alphanumerically sorted;

    These two methods help know at what moment a node's sha1 is ready to be joint together,
      ie when all children are sha1-known, a parent may gather its own sha1 and move up tree,
      recursive until the whole tree is computed.
  """

  def __init__(self, leftmost_bottommost_abspath, mountpoint_abspath):
    self.middlepathobj = midpath.MiddlePath(mountpoint_abspath)
    self.leftmost_bottommost_abspath = leftmost_bottommost_abspath
    self.mountpoint_abspath = mountpoint_abspath
    middlepath = self.middlepathobj.middle_to_entry(self.leftmost_bottommost_abspath)
    _, dirname = os.path.split(self.leftmost_bottommost_abspath)
    self.currentnode = DirNode(dirname, middlepath, self.middlepathobj)
    self.currentnode.process_entry_sha1hex()
    self.currentparent = self.currentnode.get_parent()
    if self.currentparent is None:  # ie, node is root
      return
    self.currentparent.add_sha1_calculated_child(self.currentnode)
    if self.currentnode.is_root():
      return
    self.traverse()

  def traverse(self):
    print('traversing')
    print('current node', self.currentnode)
    print('current parent', self.currentparent)
    self.go_up()
    if self.currentparent == self.currentnode.is_root():
      return  # out of recursion
    self.traverse()  # into recursion

  def go_up(self):
    missing_to_the_right = self.currentparent.get_next_not_yet_sha1_calculated_children()
    self.go_to_the_right(missing_to_the_right)
    if self.currentparent == self.currentnode.is_root():
      return  # out of recursion
    self.go_up()  # into recursion

  def go_to_the_right(self, missing_to_the_right):
    if len(missing_to_the_right) == 0:
      self.currentparent.calculate_sha1hex()
      self.currentnode = self.currentparent
      self.currentparent = self.currentnode.get_parent()
      self.currentparent.add_ready_sha1_sibling(self.currentnode)
      return  # out of recursion
    self.currentnode = missing_to_the_right[0]
    del missing_to_the_right[0]
    if self.currentnode.has_children():
      pass  # go_down()
    self.currentnode.concatenate_sha1hexes()
    self.currentparent.add_ready_sha1_sibling(self.currentnode)
    self.go_to_the_right(missing_to_the_right)  # into recursion


def mount_tree_for_bottomup_traversal():
  mountpoint_abspath = config.get_datatree_mountpoint_abspath(source=True)
  leftmost_bottommost_folderabspath = find_leftmost_bottommost_folderabspath(mountpoint_abspath)
  bt = BottomUpTraversal(leftmost_bottommost_folderabspath, mountpoint_abspath)
  bt.traverse()


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


def process():
  pass


if __name__ == '__main__':
  process()
