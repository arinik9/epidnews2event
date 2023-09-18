'''
Created on Dec 11, 2021

@author: nejat
'''

from __future__ import annotations
from abc import ABC, abstractmethod

import src.event.event as event




class EventDuplicateIdentificationStrategy(ABC):
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
  def are_two_events_possibly_duplicate(self, event1:event, event2:event):
      pass


"""
Concrete Strategies implement the algorithm while following the base Strategy
interface. The interface makes them interchangeable in the Context.
"""
class EventDuplicateHierIdentificationStrategy(EventDuplicateIdentificationStrategy):
  
  def get_description(self) -> str:
    return("hierarchical_identification")
      
      
  def are_two_events_possibly_duplicate(self, event1:event, event2:event, verbose=False):
    # loc
    loc_ident_or_hier_bool = self.is_loc_ident_or_hier_bool(event1.loc, event2.loc)
    if verbose:
      print("loc", loc_ident_or_hier_bool)
    # date
    same_interval = False
    if abs((event1.date.date-event2.date.date).days) <= 6: # up to 6 days before and after the current event
      same_interval= True
    if verbose:
      print("date (same time interval)", same_interval)
    # host
    host_ident_or_hier_bool = self.is_host_ident_or_hier_bool(event1.host, event2.host)
    if verbose:
      print("host", host_ident_or_hier_bool)
    # disease
    disease_ident_or_hier_bool = self.is_disease_ident_or_hier_bool(event1.disease, event2.disease)
    if verbose:
      print("disease", disease_ident_or_hier_bool)
    # final decision
    if loc_ident_or_hier_bool and host_ident_or_hier_bool and disease_ident_or_hier_bool and same_interval:
      return True
                  
    return False


  def is_loc_ident_or_hier_bool(self, loc1, loc2):
    if loc1.is_spatially_included(loc2) or loc1.spatially_contains(loc2) or loc1.is_identical(loc2):
      return True
    return False
  
    
  
  def is_host_ident_or_hier_bool(self, host1, host2):
    for h1 in host1.get_entry():
      for h2 in host2.get_entry():
        if h1.is_hierarchically_included(h2) or h1.hierarchically_includes(h2) or h1.is_identical(h2):
          return True
    return False
  

  
  def is_disease_ident_or_hier_bool(self, dis1, dis2):
    if dis1.is_hierarchically_included(dis2) or dis1.hierarchically_includes(dis2) or dis1.is_identical(dis2):
      return True
    return False  
  
  
  
  
