#!/usr/bin/env python
#-*-encoding:utf8-*-
'''
Created on 14 de mar de 2016

@author: friend
'''
import sqlite3

def get_connection():
  sqlite_filename = 'db_exercise.sqlite'
  conn = sqlite3.connect(sqlite_filename)
  return conn

def create_tables():
  conn = get_connection()
  cursor = conn.cursor()
  
  sql = '''CREATE TABLE IF NOT EXISTS apps_countries (
  id INTEGER PRIMARY KEY,
  country_code TEXT NOT NULL UNIQUE,
  country_name TEXT
  );'''
  cursor.execute(sql)
  sql = '''CREATE TABLE IF NOT EXISTS company (
    company_code TEXT PRIMARY KEY,
    name TEXT,
    origin_country_code TEXT,
    holding_company_code TEXT NULL,
    FOREIGN KEY(origin_country_code) REFERENCES apps_countries(country_code),
    FOREIGN KEY(holding_company_code) REFERENCES company(company_code)
  );'''
  cursor.execute(sql)
  sql = '''CREATE TABLE IF NOT EXISTS employee (
    id INTEGER PRIMARY KEY, -- AUTOINCREMENT,
    name TEXT,
    salary REAL NOT NULL,
    company_code INTEGER,
    FOREIGN KEY(company_code) REFERENCES company(company_code)
  );'''
  cursor.execute(sql)
  conn.commit()
  conn.close()

def form_employees():
  '''
  '''
  txt = u'''João Silva;7500.00;ALPH
John Silva;7500.00;ALPH
João Jones;17500.00;ALPH
Maria Silva;9500.50;GOOG
Mary Silva;8500.00;AMBV
Mariah Jones;7700.00;ECAR'''
  lines = txt.split('\n')
  employees = []
  for i, line in enumerate(lines):
    seq = i + 1
    field_values = line.split(';')
    field_values.insert(0, seq)
    employees.append(tuple(field_values)) 
  return employees

def populate_tables():
  conn = get_connection()
  cursor = conn.cursor()
  # ================
  companies = [ \
    ('GOOG', u'Google Inc.', 'US', 'ALPH'), ('ALPH', u'Alphabet', 'US', None), ('AMBV', u'Ambev Cia.', 'BR', None), \
    ('PETR', u'Petrobras S/A', 'BR', None), ('EXXO', u'Exxon', 'US', None), ('BOMB', u'Bombarbier', 'CA', None), \
    ('EMBR', u'Embraer S/A', 'BR', None), ('ESSO', u'Esso', 'BR', 'EXXO'), ('ECAR', u'Electric Cars', 'CA', 'ALPH'), \
  ]
  sql = 'INSERT INTO company (company_code, name, origin_country_code, holding_company_code) VALUES (?,?,?,?);'
  try:
    cursor.executemany(sql, companies)
  except sqlite3.IntegrityError:
    pass
  # ================
  employees = form_employees()
  sql = 'INSERT INTO employee (id, name, salary, company_code) VALUES (?, ?,?,?);'
  try:
    cursor.executemany(sql, employees)
  except sqlite3.IntegrityError:
    pass
  conn.commit()
  conn.close()

def show_data():
  conn = get_connection()
  sql = '''
  SELECT e.name, salary, co.name, c.country_name FROM employee e, company co, apps_countries c
    WHERE
      e.company_code = co.company_code AND
      c.country_code = co.origin_country_code
    ORDER by e.name;
  '''
  cursor = conn.cursor()
  rows = cursor.execute(sql)
  for row in rows:
    for column_value in row:
      print column_value,
    print

def process():
  create_tables()
  populate_tables()
  show_data()

if __name__ == '__main__':
  process()