#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
EqualizerWithEntryMoverMod.py

...

  Written on 2016-12-27 Luiz Lewis
'''
import os
from FileCoordinatorMod import FileCoordinator

class EqualizerWithEntryMover:
    '''
    This class does the moving of files in the target-to-be store space, ie,
      given two stores (eg., two external HDs), all equal files (equal sha1's)
      will have their paths compared.
    Every different path will be moved in the target to become equal with source.
    Eg.:
     /A/path/example/1/file.txt
     /B/path/helloexample/jump2/anyname.info

    So, supposing file anyname.info (in disk B) has the same sha1 as file.txt (in disk A)
      the desired operation is:

       mv /B/path/helloexample/jump2/anyname.info /B/path/example/1/file.txt

    Notice that if folder /B/path/example/1 does not exist, it will be created.

    '''

    def __init__(self, dbSourceTarget):
        # base_abspath_filesystem_source, base_abspath_filesystem_target
        self.equal_sha1s = []
        self.dbSourceTarget = dbSourceTarget


    def find_equal_sha1s(self):

        self.equal_sha1s = []
        sha1s_source = self.dbSourceTarget.source.get_all_sha1s()
        sha1s_target = self.dbSourceTarget.target.get_all_sha1s()

        for sha1sum in sha1s_source:
            if sha1sum in sha1s_target:
                self.equal_sha1s.append(sha1sum)


    def compare_all(self):

        self.find_equal_sha1s()

        for sha1hex in self.equal_sha1s:

            relpath_source, filename_source = self.dbSourceTarget.source.find_relpath_n_filename_by_sha1hex(sha1hex)
            relpath_target, filename_target = self.dbSourceTarget.target.find_relpath_n_filename_by_sha1hex(sha1hex)

            if relpath_source != relpath_target or filename_source != filename_target:

                file_coord_source   = FileCoordinator(
                    filename_source,
                    self.dbSourceTarget.source.get_abspath_to_filesystem_topdir(),
                    relpath_source)

                file_coord_target   = FileCoordinator(
                    filename_target,
                    self.dbSourceTarget.target.get_abspath_to_filesystem_topdir(),
                    relpath_target)

                file_coord_target.move_to_relative_position_of(file_coord_source)


    def filesystem_traverse(self):
        '''
        This is the second technique in which the checking is done by filesystem traversal (ie, os.walk())

        :return:
        '''
        base_abspath_source = self.dbSourceTarget.source.get_abspath_to_filesystem_topdir()
        for current_relfolderpath_source, dirs, files in os.walk(base_abspath_source):
            for filename_source in files:
                sha1sum = self.dbSourceTarget.source.get_sha1_for_entry(current_relfolderpath_source, filename_source)
                current_relfolderpath_target, filename_target = self.dbSourceTarget.target.find_relpath_n_filename_by_sha1hex(sha1sum)
                if (current_relfolderpath_target, filename_target) == (None, None):
                    # copy!
                    pass

                elif current_relfolderpath_source != current_relfolderpath_target or filename_source != filename_target:

                    file_coord_source = FileCoordinator(
                        filename_source,
                        self.dbSourceTarget.source.get_abspath_to_filesystem_topdir(),
                        current_relfolderpath_source)

                    file_coord_target = FileCoordinator(
                        filename_target,
                        self.dbSourceTarget.target.get_abspath_to_filesystem_topdir(),
                        current_relfolderpath_target)

                    file_coord_target.move_to_relative_position_of(file_coord_source)


def main():
  pass

if __name__ == '__main__':
  main()


