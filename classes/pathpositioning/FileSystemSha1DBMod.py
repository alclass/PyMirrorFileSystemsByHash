#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
FileSystemSha1DBMod.py

...

  Written on 2016-12-27 Luiz Lewis
'''

import os, sqlite3

DEFAULT_SQLITE_FILENAME = 'sha1filesystemdowntree.sqlite'

class FileSystemSha1DB:

    def __init__(self, abspath_to_filesystem_topdir, mockmode=False):
        '''
        Init only keeps abspath_to_filesystem_topdir
        :param abspath_to_filesystem_topdir: this is the path to the filesystem mounted or a subfilesystem
        '''
        self.abspath_to_filesystem_topdir = abspath_to_filesystem_topdir
        self.set_sqlitefilename()

    def set_sqlitefilename(self, sqlitefilename):
        if sqlitefilename == None:
            self.sqlitefilename = DEFAULT_SQLITE_FILENAME
        return

        if mockmode:
            self.sqlitefilename = sqlitefilename
        else: # ie, it's not mockmode, it's "filesystem real"
            # verify file's existence
            test_sqlite_absfile = os.path.join(self.abspath_to_filesystem_topdir, sqlitefilename)
            if os.path.isfile(test_sqlite_absfile):
                self.sqlitefilename = sqlitefilename

    def get_absfilepath_to_fsdb_sqlite(self):
        '''

        :return:
        '''
        abspath_to_fsdb_sqlitefile = os.path.join(
            self.get_abspath_to_filesystem_topdir(),
            self.sqlitefilename
            )
        return abspath_to_fsdb_sqlitefile

    def get_db_connection_for_filesystem(self):
        '''

        :return:
        '''

        dbconn = sqlite3.connect(self.get_absfilepath_to_fsdb_sqlite())
        return dbconn

    def get_sha1_for_entry(self, innerfolderpath, filename):
        '''
        Gets, if it exists, the sha1sum for filename located in innerfolderpath
        :param innerfolderpath:
        :param filename:
        :return:
        '''
        sha1hex  = None
        sql = '''SELECT sha1hex FROM sha1table
        WHERE
          innerfolderpath = ? AND
          filename = ? ;
        '''
        sqldatatuple = innerfolderpath, filename
        dbconn = self.get_db_connection_for_filesystem()
        rows = dbconn.execute(sql, sqldatatuple)
        if rows:
            sha1hex = rows[0][0]
        dbconn.close()
        return sha1hex

    def does_sha1hex_exist(self, p_sha1hex):
        '''

        :param p_sha1hex:
        :return:
        '''

        sha1hex_exists = False
        sql = '''SELECT sha1hex FROM sha1table
        WHERE
          sha1hex = ?;
        '''
        sqldata = (p_sha1hex)
        dbconn = self.get_db_connection_for_filesystem()
        rows = dbconn.execute(sql, sqldata)
        if len(rows) > 0:
            sha1hex_exists = True
        dbconn.close()
        return sha1hex_exists


    def find_relpath_n_filename_by_sha1hex(self, p_sha1hex):

        relpath_n_filename_tuple = (None, None)
        sql = '''SELECT relpath, filename FROM sha1table
        WHERE
          sha1hex = ?;
        '''
        sqldata = (p_sha1hex)
        dbconn = self.get_db_connection_for_filesystem()
        rows = dbconn.execute(sql, sqldata)
        if len(rows) > 0:
            relpath  = rows[0][0]
            filename = rows[0][1]
            relpath_n_filename_tuple = (relpath, filename)
        dbconn.close()
        return relpath_n_filename_tuple


class DBSourceTarget:

    def __init__(self, base_abspath_filesystem_source, base_abspath_filesystem_target):
        self.source = FileSystemSha1DB(base_abspath_filesystem_source)
        self.target = FileSystemSha1DB(base_abspath_filesystem_target)

def test1():
    pass

def main():
    test1()

if __name__ == '__main__':
    main()