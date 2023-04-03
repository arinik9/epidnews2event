'''
Created on Feb 18, 2022

@author: nejat
'''

from abc import ABC, abstractmethod
import pandas as pd

from typing import List
import numpy as np
from copy import deepcopy
import json
import dateutil.parser as parser

import src.consts as consts
import src.event.event as event
import src.event.location as location
import src.event.temporality as temporality
import src.event.symptom as symptom
import src.event.host as host
from src.util_event import retrieve_disease_from_raw_sentence, retrieve_host_from_raw_sentence, contains_ban_related_keyword
from src.util_gis import retrieve_continent_from_country_code
from src.event.timex import convert_timex_str_to_datetime

from sutime import SUTime # temporal entity normalization




class EventRetrievalStrategy(ABC):
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
  def estimate(self, row:pd.Series, sentence_id_to_article_id:dict, df_initial_info_extr: pd.DataFrame, df_articles: pd.DataFrame, \
               keyword_ass:pd.DataFrame, df_keywords_align: pd.DataFrame, geonames_ass:dict, sentence_id_to_text:dict, article_id_to_title:dict) -> List:
      pass
    
    


   
   
    
class EventRetrievalStrategyRelevantSentence(EventRetrievalStrategy):
  
  def get_description(self) -> str:
    return(consts.EVENT_RETRIEVAL_STRATEGY_RELEVANT_SENTENCE)



  def rebuild_location_list_with_spatial_inclusion(self, location_list):
    geonameId_to_loc = {}
    for loc in location_list:
      geonameId_to_loc[loc.get_geoname_id()] = loc
      
    geonameId_list = [loc.get_geoname_id() for loc in location_list]
    unique_geonameId_list = np.unique(geonameId_list)
    
    location_list = [geonameId_to_loc[id] for id in unique_geonameId_list]
        
    geoname_ids_to_remove = []
    if len(location_list)>1:
      for i in range(len(location_list)):
        loc1 = location_list[i]
        for j in range(len(location_list)):
          loc2 = location_list[j]
          if i != j:
            if loc1.get_geoname_id() not in geoname_ids_to_remove and loc2.get_geoname_id() not in geoname_ids_to_remove:
              if loc1.is_spatially_included(loc2):
                geoname_ids_to_remove.append(loc2.get_geoname_id())
    #
    final_location_list = []
    for loc in location_list:
      if loc.get_geoname_id() not in geoname_ids_to_remove:
        final_location_list.append(loc)
    return final_location_list




  def merge_events_with_same_spatial_entity(self, event_list):
    #print("merge_events_with_same_spatial_entity")
    #print(event_list)
    first_event = event_list[0]
    other_event_list = event_list[1:]
    e_final = deepcopy(first_event)
    
    for e in other_event_list:
      # 1) improve disease info 
      if e.disease.is_hierarchically_included(e_final.disease):
        e_final.disease = e.disease
        e_final.sentences = e_final.sentences + "||" + e.sentences
            
      # 2) improve host info
      if e.host.is_hierarchically_included(e_final.host):
        e_final.host = e.host
        e_final.sentences = e_final.sentences + "||" + e.sentences
          
    return e_final  

          
  # we also take into account disease name and host type
  def handle_event_location_redundancy_and_spatial_inclusion(self, event_list):
    final_event_list = []
    #print("handle_event_location_redundancy_and_spatial_inclusion")
    #print(event_list)
    
    # first, regroup by geoname_id
    dict_by_geoname_id = {}
    for e in event_list:
      key = str(e.loc.get_geoname_id())+"_"+e.disease.d_type+"_"+list(e.host.dict_data.keys())[0]
      if key not in dict_by_geoname_id:
        dict_by_geoname_id[key] = []
      dict_by_geoname_id[key].append(e)
       
    # second, compress the event occurring in the same place
    for key in dict_by_geoname_id.keys():
      e_list = dict_by_geoname_id[key]
      e = e_list[0]
      if len(e_list)>1:
        e = self.merge_events_with_same_spatial_entity(e_list)
      final_event_list.append(e)
      
    # third, check spatial inclusion
    event_ids_to_remove = []
    for i in range(len(final_event_list)):
      loc1 = final_event_list[i].loc
      for j in range(len(final_event_list)):
        if i != j:
          loc2 = final_event_list[j].loc
          if loc1.is_spatially_included(loc2) or loc1.get_geoname_id() == loc2.get_geoname_id():
            event_ids_to_remove.append(final_event_list[j].e_id)
    #
    final_event_list2 = []
    for e in final_event_list:
      if e.e_id not in event_ids_to_remove:
        final_event_list2.append(e)
    return final_event_list2
  
  
      
  # example of the "row" object. It is of type 'pd.Series'.
  # -----------------------------------
  # id_articleweb                                                       3WWDYJD6ZB
  # id                                                        [52751799, 52751815]
  # position                                                             [489, 43]
  # token_index                                                            [41, 9]
  # length                                                                [11, 11]
  # type                         ['outbreak-related-location', 'outbreak-relate...
  # label                                                   ['correct', 'correct']
  # value                                           ['South Korea', 'South Korea']
  # keyword_id                                                [154461.0, 154461.0]
  # from_automatic_extraction                                           [1.0, 1.0]
  # geonames_id                                             [1835841.0, 1835841.0]
  # country_feature_code                                          ['PCLI', 'PCLI']
  # sentence_id                                                 [9493873, 9493869]
  # s_classificationlabel_id                                    [[11, 12], [11, 12]]
  # country                                                            South Korea
  # country_freq                                                                 2
  # nb_country                                                                   1
  # Name: 0, dtype: object      
     
      
  def estimate(self, disease_name:str, df_extr_spatial_entities_by_article:pd.DataFrame, sentence_id_to_article_id:dict, \
                sentence_id_to_local_sentence_id: dict, df_articles:pd.DataFrame, \
                df_initial_info_extr: pd.DataFrame, keyword_ass:pd.DataFrame, df_keywords_align: pd.DataFrame, \
                geonames_hierarchy_ass:dict, sentence_id_to_text:dict) -> List:
    # first, assign input params to local variables
    article_id = df_extr_spatial_entities_by_article.iloc[0][consts.COL_ARTICLE_ID_RENAMED]
    article_info = df_articles[df_articles[consts.COL_ID] == article_id].iloc[0]
    self.disease_name = disease_name
    self.df_extr_spatial_entities_by_article = df_extr_spatial_entities_by_article
    self.df_initial_info_extr = df_initial_info_extr
    self.df_keywords_align = df_keywords_align
    self.keyword_ass = keyword_ass
    self.sentence_id_to_article_id = sentence_id_to_article_id
    self.geonames_hierarchy_ass = geonames_hierarchy_ass
    self.sentence_id_to_text = sentence_id_to_text
    self.sentence_id_to_local_sentence_id = sentence_id_to_local_sentence_id
    
    title = article_info[consts.COL_TITLE]
    published_time = article_info[consts.COL_PUBLISHED_TIME]
    t = temporality.Temporality(published_time)
    source = article_info[consts.COL_SOURCE]
    url = article_info[consts.COL_URL]
    
    print("------------------------!!------------------------")
    SLIDING_WINDOW_LENGTH = 4 # 4 next sentences 
    sliding_window_for_history = [] # e.g. 
    sentence_id_list = np.unique(self.df_extr_spatial_entities_by_article[consts.COL_SENTENCE_ID].to_numpy())
    #print(self.sentence_id_to_local_sentence_id, sliding_window_for_history)
    
    final_event_candidates = []
    sentence_id_list = [s_id for s_id in sentence_id_list if s_id != -1] # -1 from those coming from title
    for sentence_id in sentence_id_list:
      local_sentence_id = self.sentence_id_to_local_sentence_id[sentence_id]
      sentence_text = self.sentence_id_to_text[sentence_id]
      #print(sentence_text)
      df_extr_spatial_entities_by_sentence = \
                self.df_extr_spatial_entities_by_article[\
                                     self.df_extr_spatial_entities_by_article["local_sentence_id"] == local_sentence_id]
      
      if len(sliding_window_for_history)>0: 
        # remove the old entries based on time limit
        completed_event_candidates = [entry[1] for entry in sliding_window_for_history if (local_sentence_id-entry[0])>=SLIDING_WINDOW_LENGTH]
        sliding_window_for_history = [entry for entry in sliding_window_for_history if (local_sentence_id-entry[0])<SLIDING_WINDOW_LENGTH]
        final_event_candidates.extend(completed_event_candidates)
                
      if not contains_ban_related_keyword(sentence_text):
        disease_info = retrieve_disease_from_raw_sentence(sentence_text, self.disease_name)
        #print(disease_info)
        host_info = retrieve_host_from_raw_sentence(sentence_text) # host info can be empty
        #print(host_info)
        symptom_info = symptom.Symptom()
        contains_forbidden_hosts = np.any([True if h not in consts.DISEASE_HOST_RELATION[self.disease_name] else False \
                                  for h in host_info.get_entry().keys()])
        has_disease_keywords = (disease_info is not None and self.disease_name in disease_info.get_entry()[1]) # e.g., self.disease_name="AI" and disease_info.get_entry()[1]="HPAI"
        has_host = (not host_info.is_host_info_empty())
        has_new_event_info = (has_disease_keywords and has_host and not contains_forbidden_hosts)  
        elaborate_existing_events = ((disease_info is None or has_disease_keywords) \
                                      and not contains_forbidden_hosts and len(sliding_window_for_history)>0)
        
        if elaborate_existing_events:
          #print("---- update")
          sliding_window_for_history = self.update_existing_event_candidates_from_sentence(sentence_id, local_sentence_id,
                                                               sliding_window_for_history, \
                                                              df_extr_spatial_entities_by_sentence, \
                                                              disease_info, host_info, symptom_info)
          
        if has_new_event_info:
          #print("---- new")
          event_candidates_by_sentences = self.retrieve_new_event_candidates_from_sentence(article_id, title, published_time, \
                                                           sentence_id, local_sentence_id, t, disease_name, source, url,
                                                           sentence_text, df_extr_spatial_entities_by_sentence, \
                                                           disease_info, host_info, symptom_info)
          for e in event_candidates_by_sentences:
            #print(e)
            sliding_window_for_history.append((local_sentence_id, e))
            
            
    # finally, check redundancy, if so, compress them        
    final_event_candidates2 = self.handle_event_location_redundancy_and_spatial_inclusion(final_event_candidates)     
    return final_event_candidates2



  def retrieve_new_event_candidates_from_sentence(self, article_id, title, published_time, sentence_id, local_sentence_id , \
                                                  t, disease_name, source, url,
                                                  sentence_text, df_extr_spatial_entities_by_sentence,
                                                   disease_info, host_info, symptom_info):
    event_candidates = []
    # --------------------------------------------------------
    #has_annual_statement = False
    has_annual_statement = ("this year so far" in sentence_text.lower()) or ("so far this year" in sentence_text.lower())
    # sutime source: https://nlp.stanford.edu/software/sutime.html
    # annotation guideline on Timex: https://timeml.github.io/site/publications/timeMLdocs/annguide_1.2.1.pdf
    #print(sentence_text)
    # converting timex format to datetime: https://stackoverflow.com/questions/55066094/timex-strings-to-datetime-in-python
    pub_date = parser.parse(published_time)
    sutime = SUTime(mark_time_ranges=False, include_range=False)
    res_list = sutime.parse(sentence_text, published_time)
    has_old_date = False
    #print(published_time)
    #print(res_list)
    res_list_date = [ent for ent in res_list if 'timex-value' in ent and ent['type'] == 'DATE']
    timex_str_list = [ent['timex-value'] for ent in res_list_date]
    # {'timex-value': '2022-09-14', 'start': 18, 'end': 26, 'text': 'tomorrow', 'type': 'DATE', 'value': '2022-09-14'}
    found_dates = convert_timex_str_to_datetime(timex_str_list)
    #print(found_dates)
    for found_date in found_dates:
      # diff_days = (pub_date-found_date).days
      #print(abs(pub_date.day-found_date.day))
      if abs(pub_date.day-found_date.day) > 360: # 1 year of difference >> to be sure that it is an old date
        has_old_date = True
        #print("has_old_date")
        break
    # --------------------------------------------------------
    
    if not has_old_date and not has_annual_statement:
      spatial_entity_list = df_extr_spatial_entities_by_sentence["value"].to_list()
      #country_list = df_extr_spatial_entities_by_sentence["country_code"].to_list()
      #spatial_entity_list = [spatial_entity_list[i]+" ("+country_list[i]+")" for i in range(len(spatial_entity_list))]
      #print(spatial_entity_list)
      location_list = [] # TODO: we can regroup locations by country
      for index, row in df_extr_spatial_entities_by_sentence.iterrows():
        name = row[consts.COL_VALUE]
        geoname_id = row[consts.COL_GEONAMES_ID]
        geoname_json_str = row[consts.COL_GEONAMS_JSON]
        geoname_json = json.loads(geoname_json_str)
        lat = geoname_json["lat"]
        lng = geoname_json["lng"]
        fcode = row[consts.COL_FEATURE_CODE]
        country_code = row["country_code"]
        continent = retrieve_continent_from_country_code(country_code)
        hierarchy_data = eval(self.geonames_hierarchy_ass[geoname_id]) # it converts into a list
        
        loc = location.Location(name, geoname_id, geoname_json, lat, lng, country_code, continent, hierarchy_data)
        location_list.append(loc)
        
      #print(location_list)
      final_location_list = self.rebuild_location_list_with_spatial_inclusion(location_list)
      #print(final_location_list)
      # create an event object for each location
  
      sentences = str(sentence_id) + "("+str(local_sentence_id)+")"
      event_candidates = []
      for loc in final_location_list:
        if host_info is None:
          host_info = host.Host()
        e = event.Event(-1, article_id, url, source, loc, t, disease_info, host_info, symptom_info, title, sentences)
        event_candidates.append(e)
        
    return event_candidates
  
  

  def update_existing_event_candidates_from_sentence(self, sentence_id, local_sentence_id, sliding_window_for_history, \
                                                              df_extr_spatial_entities_by_sentence, \
                                                              disease_info, host_info, symptom_info):
    # TODO: check if spatialy inclusion does not exist
    
    # 1) improve locations 
    ids_to_remove = []
    for index, row in df_extr_spatial_entities_by_sentence.iterrows():
      name = row[consts.COL_VALUE]
      geoname_id = row[consts.COL_GEONAMES_ID]
      geoname_json_str = row[consts.COL_GEONAMS_JSON]
      # JSON only allows enclosing strings with double quotes 
      #p = re.compile('(?<!\\\\)\'') # source: https://stackoverflow.com/questions/39491420/python-jsonexpecting-property-name-enclosed-in-double-quotes
      #geoname_json_str = p.sub('\"', geoname_json_str)
      geoname_json = json.loads(geoname_json_str)
      lat = geoname_json["lat"]
      lng = geoname_json["lng"]
      fcode = row[consts.COL_FEATURE_CODE]
      country_code = row["country_code"]
      continent = retrieve_continent_from_country_code(country_code)
      hierarchy_data = eval(self.geonames_hierarchy_ass[geoname_id])
      
      loc = location.Location(name, geoname_id, geoname_json, lat, lng, country_code, continent, hierarchy_data)
      # check if loc is an improvement w.r.t existing ones
      
      entries_to_add = []
      for (id, e) in sliding_window_for_history:
        if loc.is_spatially_included(e.loc):
          e2 = event.Event(-1, e.article_id, e.url, e.source, loc, e.date, e.disease, e.host, e.symptom, e.title, e.sentences)
          if str(sentence_id) not in e.sentences:
            e2.sentences = e2.sentences + ", " + str(sentence_id) + "("+str(local_sentence_id)+")"
          entries_to_add.append((id, e2))
          if e.loc.get_geoname_id() not in ids_to_remove:
            ids_to_remove.append(e.loc.get_geoname_id())
      sliding_window_for_history.extend(entries_to_add)
    sliding_window_for_history = [(id, e) for (id, e) in sliding_window_for_history if e.loc.get_geoname_id() not in ids_to_remove] 
          
    # 2) improve disease info in terms of hierarchy
    if disease_info is not None:
      for (id, e) in sliding_window_for_history:
        if disease_info.is_hierarchically_included(e.disease):
          e.disease = disease_info
          if str(sentence_id) not in e.sentences:
            e.sentences = e.sentences + ", " + str(sentence_id) + "("+str(local_sentence_id)+")"
            
    # 3) improve host info in terms of hierarchy
    if not host_info.is_host_info_empty():
      for (id, e) in sliding_window_for_history:
        if host_info.is_hierarchically_included(e.host) or (list(host_info.get_entry().keys())[0] == "human" and list(host_info.get_entry().keys())[0] != list(e.host.get_entry().keys())[0]):
          e.host = host_info
          if str(sentence_id) not in e.sentences:
            e.sentences = e.sentences + ", " + str(sentence_id) + "("+str(local_sentence_id)+")"
          
    return sliding_window_for_history
  
  
