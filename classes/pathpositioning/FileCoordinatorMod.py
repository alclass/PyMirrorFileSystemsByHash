#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
FileCoordinatorMod.py

...

  Written on 2016-12-27 Luiz Lewis
'''
import os, shutil

class FileCoordinator:

    def __init__(self, filename, abspath_to_fs_topdir, relpath, mockmode=False):

        self.mockmode = mockmode

        self.filename             = filename
        self.previous_filename    = None
        self.abspath_to_fs_topdir = abspath_to_fs_topdir
        self.relpath              = relpath
        self.previous_relpath     = None

    @property
    def filesfolder_abspath(self):
        return os.path.join(self.abspath_to_fs_topdir, self.relpath)

    @property
    def file_abspath(self):
        return os.path.join(self.filesfolder_abspath, self.filename)


    def move_to_relative_position_of(self, file_coord_reference):

        current_filesfolder_abspath = os.path.join(self.abspath_to_fs_topdir, self.relpath)
        current_file_abspath = os.path.join(current_filesfolder_abspath, self.filename)

        new_relpath = file_coord_reference.relpath
        new_filesfolder_abspath = os.path.join(self.abspath_to_fs_topdir, new_relpath)

        new_filename = file_coord_reference.filename
        new_file_abspath = os.path.join(new_filesfolder_abspath, new_filename)

        if not self.mockmode:
            if os.path.exists(new_file_abspath):
                raise OSError('Dir Entry %s already exists, cannot move it.' %new_file_abspath)
                # return False

        if not self.mockmode:
            shutil.move(current_file_abspath, new_file_abspath)

        self.previous_relpath  = self.relpath
        self.previous_filename = self.filename
        self.relpath  = new_relpath
        self.filename = new_filename

        return True


    def __str__(self):
        outstr = '''
filename            : %(filename)s
previous_filename   : %(previous_filename)s
abspath_to_fs_topdir: %(abspath_to_fs_topdir)s
relfolderpath       : %(relpath)s
previous_relpath    : %(previous_relpath)s
filesfolder_abspath : %(filesfolder_abspath)s
file_abspath        : %(file_abspath)s
        ''' %{

            'filename'            : self.filename,
            'previous_filename'   : self.previous_filename,
            'abspath_to_fs_topdir': self.abspath_to_fs_topdir,

            'relpath'             : self.relpath,
            'previous_relpath'    : self.previous_relpath,

            'filesfolder_abspath' : self.filesfolder_abspath,
            'file_abspath'        : self.file_abspath,

        }
        return outstr


def test1():
    '''

    :return:
    '''
    filename = 'Homo Sapiens.info.txt'
    abspath_to_fs_topdir =  '/media/SAMSUNG_1/'
    relpath = 'Animals/Vertebrates/Mammals/'
    mockmode = True
    file_coord_source = FileCoordinator(filename, abspath_to_fs_topdir, relpath, mockmode)
    print ('Source:')
    print (file_coord_source)


    filename = 'Homo Economicus.odt'
    abspath_to_fs_topdir = '/media/SAMSUNG_8/'
    relpath = 'Sociology/The Cities/Looking Ahead/'
    mockmode = True
    file_coord_target = FileCoordinator(filename, abspath_to_fs_topdir, relpath, mockmode)
    print ('Target:')
    print (file_coord_target)

    file_coord_target.move_to_relative_position_of(file_coord_source)
    print ('Target Again (after move):')
    print (file_coord_target)


def main():
    test1()

if __name__ == '__main__':
    main()