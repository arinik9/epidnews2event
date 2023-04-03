'''
Created on Dec 11, 2021

@author: nejat
'''

from __future__ import annotations
from abc import ABC, abstractmethod

import event

import util
import math



class EventSimilarityStrategy(ABC):
  """
  The Strategy interface declares operations common to all supported versions
  of some algorithm.
  
  The Context uses this interface to call the algorithm defined by Concrete
  Strategies.
  """
  @abstractmethod
  def get_description(self) -> str:
      pass
    
    
  @abstractmethod
  def perform_event_similarity(self, event1:event, event2:event):
      pass


"""
Concrete Strategies implement the algorithm while following the base Strategy
interface. The interface makes them interchangeable in the Context.
"""
class EventSimilarityStrategyManual(EventSimilarityStrategy):
  
  def get_description(self) -> str:
    return("manual_event_similarity")
      
      
  def perform_event_similarity(self, event1:event, event2:event, verbose=False):
    signed_score = 0
    # loc
    loc_score = self.compute_location_similarity(event1.loc, event2.loc)
    if verbose:
      print("loc", loc_score)
    # date
    date_score = self.compute_date_similarity(event1.date, event2.date)
    if verbose:
      print("date", date_score)
    # host
    host_score = self.compute_host_similarity(event1.host, event2.host)
    if verbose:
      print("host", host_score)
    # disease
    disease_score = self.compute_disease_similarity(event1.disease, event2.disease)
    if verbose:
      print("disease", disease_score)
    # symptom
    symptom_score = self.compute_symptom_similarity(event1.symptom, event2.symptom)
    if verbose:
      print("symptom", symptom_score)
    # final score
    signed_score = signed_score + loc_score + date_score + disease_score \
                  + host_score + symptom_score
                  
    # if signed_score > -5:
    #   print("loc", loc_score)
    #   print("date", date_score)
    #   print("host", host_score)
    #   print("disease", disease_score)
    return signed_score


  # remark: root node ROOT, which is something useless, must have level 0
  def compute_location_similarity(self, l1, l2):
    score = -10 # default value >> penalization factor
    hier_related = False
    if l1.spatially_contains(l2) or l1.is_spatially_included(l2) or l1.is_identical(l2):
      hier_related = True
    if hier_related:
      # workaround: since we initially designed that country level is level 0, we change this for this calculation
      # so, the country level is 2
      lvl1 = l1.get_hierarchy_level()
      lvl1 += 2
      lvl2 = l2.get_hierarchy_level()
      lvl2 += 2
      common_ancestor_hier_level = min(lvl1, lvl2)-1
      score = (2*common_ancestor_hier_level)/(lvl1+lvl2)
    return(score)
  
    
  
  def compute_date_similarity(self, t1, t2):
    # Timeliness is the proportion of time saved by detection relative to the other event.
    # source: Jafarpour2015
    L = 21 # max window size # 21 days, i.e. 3 weeks
    diff = abs((t2.date - t1.date).days) # difference in days
    score = 1 - (diff/L)
    # when the time diff is around 90, the score will be around -3 >> put your threshold based on this
    return(score)
  
  
  # remark: root node ROOT, which is something useless, must have level 0
  def compute_host_similarity(self, h1, h2):
    score = -1 # default value >> penalization factor
    hier_related = False
    if h1.hierarchically_includes(h2) or h1.is_hierarchically_included(h2) or h1.is_identical(h2):
      hier_related = True
    if hier_related:
      # so, the bird or human level is 1
      lvl1 = h1.get_max_hierarchy_level()
      #lvl1 += 1
      lvl2 = h2.get_max_hierarchy_level()
      #lvl2 += 1
      common_ancestor_hier_level = min(lvl1, lvl2)-1
      if lvl1+lvl2 == 0:
        return 0
      score = (2*common_ancestor_hier_level)/(lvl1+lvl2)
    return(score)
  
  
  # remark: root node ROOT, which is something useless, must have level 0
  def compute_disease_similarity(self, d1, d2):
    score = -1 # default value >> penalization factor
    hier_related = False
    if d1.hierarchically_includes(d2) or d1.is_hierarchically_included(d2) or d1.is_identical(d2):
      hier_related = True
    if hier_related:
      # so, the AI or WNV level is 1
      lvl1 = d1.get_max_hierarchy_level()
      #lvl1 += 1
      lvl2 = d2.get_max_hierarchy_level()
      #lvl2 += 1
      common_ancestor_hier_level = min(lvl1, lvl2)-1
      score = (2*common_ancestor_hier_level)/(lvl1+lvl2)
    return(score)
  
  
  def compute_symptom_similarity(self, s1, s2):
    score = 0
    # if len(s1.dict_data.keys())>0 and len(s2.dict_data.keys())>0:
    #   common_keys = [x for x in s1.dict_data.keys() if x in s2.dict_data.keys()]
    #   if len(common_keys)>0:
    #     for key in common_keys:
    #       values = s1.dict_data[key]
    #       other_values = s2.dict_data[key]
    #       common_values = [x for x in values if x in other_values]
    #       if len(common_values)>0:
    #         score = score + len(common_values)*10
    #       else:
    #         score = score - 100
    #   else:
    #     score = score - 300
        
    return(score)  
  
  
  
    
