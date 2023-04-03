'''
Created on Nov 14, 2021

@author: nejat
'''


class Disease:

  def __init__(self, d_subtype, d_type):
    self.d_type = d_type
    self.d_subtype = d_subtype
    self.hierarchy_level = self.get_max_hierarchy_level()
      
      
  def get_entry(self):
    return([self.d_subtype, self.d_type])
  
  
  def is_entry_empty(self):
    if self.d_subtype == "" and self.d_type == "":
      return True
    return False
  
  
  # we stick to the disease name in disease 1. We just try to get extra information from disease 2
  def extend_disease_from_another_event_if_possible(self, dis2):
    #dis1 = Disease(self.d_subtype, self.d_type)
    if self.d_type != "" and self.d_type == dis2.d_type:
      if self.d_subtype == "" and dis2.d_subtype != "":
        self.d_subtype = dis2.d_subtype
    #return(dis1)
  
  
  def get_max_hierarchy_level(self):
    if self.d_subtype != "":
      return 1
    return 0
  
  
  def is_hierarchically_included(self, another_dis):
    if self.d_type == another_dis.d_type:
      if another_dis.hierarchy_level < self.hierarchy_level: # e.g. level 1 < level 3
        return True
    return False
  
  
  def hierarchically_includes(self, another_dis):
    if self.d_type == another_dis.d_type:
      if self.hierarchy_level < another_dis.hierarchy_level: # e.g. level 1 < level 3
        return True
    return False
  
  
  def is_identical(self, another_dis):
    if self.hierarchy_level == another_dis.hierarchy_level and self.d_type == another_dis.d_type and self.d_subtype == another_dis.d_subtype:
      return True
    return False  
  
  
  def __repr__(self):
    return "[" + self.get_entry()[0] + "; " + self.get_entry()[1] + "]"
       
  def __str__(self):
    return "[" + self.get_entry()[0] + "; " + self.get_entry()[1] + "]"
       