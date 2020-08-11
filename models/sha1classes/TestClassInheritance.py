'''
Created on 2 mars 2014

@author: friend
'''

class A(object):
  
  def __init__(self, a):
    self.a = a
    
  def __str__(self):
    return 'a = ' + str(self.a)

class B(A):
  
  def __init__(self, a, b):
    super(B, self).__init__(a)
    self.b = b

  def __str__(self):
    return 'b = ' + str(self.b)

obj_a = A('a')
obj_b = B('a2', 'b')
print obj_a 
print obj_b