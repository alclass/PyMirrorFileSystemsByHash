#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlite3

sql = '''
CREATE TABLE IF NOT EXISTS linked_list (
  id INT,
  parent_id INT
);
'''
# create database
conn = sqlite3.connect('linked_list.sqlite')
cursor = conn.cursor()
cursor.execute(sql)
cursor.execute('INSERT INTO linked_list (id, parent_id) VALUES (0, 0);')
conn.commit()

def is_record_inserted(curr_id, parent_id):
  sql = 'select * from linked_list where id=? and parent_id=?;'
  result = cursor.execute(sql, (curr_id, parent_id))
  if len(result.fetchall()) > 0:
    return True
  return False

traces_str = '''0-1-8-9
0-1-8-10
0-2-12-13-14-21
0-3-4-5-6
0-7'''
traces = traces_str.split('\n')
for trace in traces:
  ids = trace.split('-')
  parent_id = 0
  for curr_id in ids[1:]:
    if not is_record_inserted(curr_id, parent_id):
      sql = 'INSERT INTO linked_list (id, parent_id) VALUES (?, ?);'
      cursor.execute(sql, (curr_id, parent_id))
    parent_id = curr_id
conn.commit()
conn.close()
