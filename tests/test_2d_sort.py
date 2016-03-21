#!/usr/bin/env python
# -*- coding: utf-8 -*-
lol = [ range(5, 15), range(2, 12), range(7, 17), range(10), ]

'''
indices = range(10)
indices.sort(key = lol[1].__getitem__)
for i, sublist in enumerate(lol):
  lol[i] = [sublist[j] for j in indices]
'''

print lol
r = [] # lol[:]
for l in lol:
  rl = l[:]
  rl.reverse()
  r.append(rl)
r.sort(key=lambda e:e[0])
print r