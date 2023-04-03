'''
Created on Nov 14, 2021

@author: nejat
'''


class Host:

  def __init__(self, dict_data = None):
    self.dict_data = {}
    self.hierarchy_level = 0
    if dict_data is not None:
      self.dict_data = dict_data
      self.hierarchy_level = self.get_max_hierarchy_level()
    
    
  def is_host_info_empty(self):
    return not bool(self.dict_data)
        
  # each entry in 'tuple_data' is organized from general to specialized category
  def add_host_info(self, tuple_data):
    # ex: ("bird", "domestic bird", "black swan")
    # ex: ("bird", "domestic bird", "level2-unknown")
    nb_items = len(tuple_data)
    d_data = self.dict_data # we do not want to use the recursive approach, so we take advantage of shallow copy of the original dict structure
    for i in range(nb_items-1): # except the last item
      if tuple_data[i] not in d_data:
        if i < (nb_items-2):
          d_data[tuple_data[i]] = {}
        else:
          d_data[tuple_data[i]] = []
      d_data = d_data[tuple_data[i]]
    if tuple_data[nb_items-1] not in d_data:
      d_data.append(tuple_data[nb_items-1]) # last item
  #raise Exception('check the type of input parameters in add_host_info()') 
  # update hier level
    self.hierarchy_level = self.get_max_hierarchy_level()
        
             
  def get_entry(self):
    return self.dict_data
 
 
  def dict_depth(self, dic, level = 1):
    found_unknown = True
    if isinstance(dic, dict):
      for k in dic.keys():
        if "unknown" not in k:
          found_unknown = False
    if isinstance(dic, list):
      for k in dic:
        if "unknown" not in k:
          found_unknown = False
    if found_unknown:
      return level-1
    if not isinstance(dic, dict) or not dic or found_unknown:
      return level
    return max(self.dict_depth(dic[key], level + 1) for key in dic)
  
   
  def get_max_hierarchy_level(self):
    return self.dict_depth(self.dict_data) 
  
  
  def merge_with_other_dict_data(self, other_dict_data):
    def unnest(d, keys=[]):
      # input: d2 = {'bird': {'domestic bird': ['poultry', 'aaaa']}}
      # output: [('bird', 'domestic bird', ['poultry', 'aaaa'])]
      result = []
      for k, v in d.items():
          if isinstance(v, dict):
              result.extend(unnest(v, keys + [k]))
          else:
              result.append(tuple(keys + [k, v]))
      return result
    other_tuples = unnest(other_dict_data)
    final_other_tuples = []
    for other_tuple in other_tuples:
      for elt in other_tuple[-1]: # the last item is always a list
        final_other_tuples.append(tuple(list(other_tuple[0:-1])+[elt]))
    for other_tuple in other_tuples:
      self.add_host_info(other_tuple)
    # update hier level
    self.hierarchy_level = self.get_max_hierarchy_level()
    #self.nb_host_cases = other_dict_data.nb_host_cases + 1
      
      
  def is_hierarchically_included(self, another_host):
    if list(self.dict_data.keys())[0] == list(another_host.dict_data.keys())[0]:
      if another_host.hierarchy_level < self.hierarchy_level: # e.g. level 1 < level 3
        return True
    return False


  def hierarchically_includes(self, another_host):
    if list(self.dict_data.keys())[0] == list(another_host.dict_data.keys())[0]:
      if self.hierarchy_level < another_host.hierarchy_level: # e.g. level 1 < level 3
        return True
    return False
  
  
  def is_identical(self, another_dis):
    def unnest(d, keys=[]):
      # input: d2 = {'bird': {'domestic bird': ['poultry', 'aaaa']}}
      # output: [('bird', 'domestic bird', ['poultry', 'aaaa'])]
      result = []
      for k, v in d.items():
          if isinstance(v, dict):
              result.extend(unnest(v, keys + [k]))
          else:
              result.append(tuple(keys + [k, v]))
      return result
    if self.hierarchy_level != another_dis.hierarchy_level:
      return False
    temp = unnest(self.get_entry())
    other_temp = unnest(another_dis.get_entry())
    for i in range(len(temp)):
      if temp[i] != other_temp[i]:                   
        return False
    return True 
  
  
  
  def __repr__(self):
    return(str(self.dict_data))


  def __str__(self):
    return(str(self.dict_data))
  
  
  
