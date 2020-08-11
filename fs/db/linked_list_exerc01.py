#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlite3

conn = sqlite3.connect('linked_list.sqlite')
cursor = conn.cursor()
seq = 0

def is_record_inserted(curr_id, parent_id):
  sql = 'SELECT * FROM linked_list WHERE id=? AND parent_id=?;'
  result = cursor.execute(sql, (curr_id, parent_id))
  if len(result.fetchall()) > 0:
    return True
  return False

def create_table_and_insert_root():
  sql = '''
  CREATE TABLE IF NOT EXISTS linked_list (
    id INT PRIMARY KEY NULL,
    parent_id INT
  );
  '''
  # create database
  cursor.execute(sql)
  try:
    cursor.execute('INSERT INTO linked_list (id, parent_id) VALUES (1, 0);')
  except sqlite3.IntegrityError:
    # print 'Not inserting', (1, 0)
    pass
  conn.commit()

def insert_data():
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
        try:
          sql = 'INSERT INTO linked_list (id, parent_id) VALUES (?, ?);'
          print sql
          cursor.execute(sql, (curr_id, parent_id))
        except sqlite3.IntegrityError:
          print 'Not inserting', (curr_id, parent_id)
      parent_id = curr_id
  conn.commit()

n_of_selects = 0
def get_traversal_ids(_id):
  global n_of_selects
  sql = '''SELECT id FROM linked_list WHERE parent_id=%d ORDER BY id;''' %_id
  n_of_selects += 1
  result = cursor.execute(sql)
  traversal_ids = []
  for row in result.fetchall():
    traversal_ids.append(row[0])
  return traversal_ids

paths_register = []; n_of_found_path = 0
def recursive_traversal(prefix_list=[], traversal_ids=[0]):
  global n_of_found_path
  for current_id in traversal_ids:
    current_prefix_list = prefix_list + [current_id]
    current_traversal_ids = get_traversal_ids(current_id)
    if len(current_traversal_ids) > 0:
      print 'current_prefix_list, current_traversal_ids', current_prefix_list, current_traversal_ids
      recursive_traversal(current_prefix_list, current_traversal_ids)
    else:
      # nó folha, imprima-o
      n_of_found_path += 1
      print n_of_found_path, 'º caminho encontrado (ocorrência de nó folha):', current_prefix_list
      paths_register.append(current_prefix_list)

def process_traversal():
  # children of nodes
  print 'Beggining of process_traversal()'
  recursive_traversal()

def process():
  create_table_and_insert_root()
  insert_data()
  process_traversal()
  print paths_register
  print 'n_of_selects =', n_of_selects

if __name__ == '__main__':
  process()

conn.close()
