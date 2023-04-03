'''
Created on Nov 16, 2021

@author: nejat
'''


import os
import pandas as pd
import numpy as np
import csv
from abc import ABC, abstractmethod

import src.util as util
import src.consts as consts
from src.event_retrieval.event_retrieval_strategy import EventRetrievalStrategy
from src.util_event import contains_ban_related_keyword




class AbstractEventRetrieval(ABC):

  def __init__(self, event_retrieval_strategy: EventRetrievalStrategy):
    print("--- using sentence identification strategy: ", event_retrieval_strategy.get_description())
    self.event_retrieval_strategy = event_retrieval_strategy
    
    
  def get_strategy(self):
    return(self.location_identification_strategy)
  
  
  def set_strategy(self, sentence_identification_strategy):
    self.location_identification_strategy = sentence_identification_strategy
    
    
  @abstractmethod
  def perform_event_retrieval(self):
    pass



class EventRetrievalPadiweb(AbstractEventRetrieval):
# for each selected article, find the corresponding sentence ids
# identify the sentence id describing an event
# find keywords based on this sentence id
# construct the tuple
# try to find previous and next sentences to complete the tuple

# a_id = 'ZW76AEWEPX'
# s_indx = S_agg_reverted_indices[a_id] 
# s = S_agg[s_indx:s_indx+1]


# possible entity type:
# - keyword (e.g. "case", "virus", "Japan")
# - outbreak-related-location
# - GPE (e.g. "Hokkaido")
# - ORG
# - disease
# - host
# - outbreak-related-date (e.g. "December")
# - TIME (e.g. "Monday evening") >> no entry for Avian Influenza
# - DATE (e.g. "Monday", "Monday, November 1") >> no entry for Avian Influenza
# - cases (e.g. "23,000")
# - NORP (e.g. "Italian")
# - LOC (e.g. "Hokkaido East")
# - symptom (e.g. "dead")
# - (TODO) published time

# TODO https://datascience.stackexchange.com/questions/12575/similarity-between-two-words

# https://nlp.stanford.edu/software/sutime.shtml
# https://stanfordnlp.github.io/CoreNLP/sutime.html
# https://stanfordnlp.github.io/CoreNLP/pipeline.html
# Heideltime


# A-ADM1, i.e. première division administrative (e.g. région française, état aux USA
# la classe P-PPL, associée le plus souvent à des petites villes, villages, etc.

# The order of the PPL* features goes like : PPLC > PPLA[2-4] > PPL > PPLX > PPLL



  def __init__(self, strategy: EventRetrievalStrategy):
    super().__init__(strategy)
    
    

  def perform_event_retrieval(self, disease_name, input_folder, out_csv_folder, data_folder):
    print("...... beginning of event retrieval from padiweb for " + disease_name + "!")
    self.disease_name = disease_name
    self.input_folder = input_folder
    self.out_csv_folder = out_csv_folder
    self.data_folder = data_folder
    
    signal_filepath = os.path.join(self.input_folder, \
                            consts.PADIWEB_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    signal_info_exists = os.path.isfile(signal_filepath)
    
    self.read_csv_files_as_dataframe()
    
    self.keyword_ass = dict(zip(self.df_keywords[consts.COL_ID],self.df_keywords[consts.COL_NAME]))
    
    self.sentence_id_to_article_id = dict(zip(self.df_sentences[consts.COL_ID], self.df_sentences[consts.COL_ARTICLE_ID]))
    self.sentence_id_to_text = dict(zip(self.df_sentences[consts.COL_ID], self.df_sentences[consts.COL_TEXT]))
    self.article_id_to_title = dict(zip(self.df_articles[consts.COL_ID], self.df_articles[consts.COL_TITLE]))
    self.sentence_id_to_local_sentence_id = dict(zip(self.df_sentences[consts.COL_ID], self.df_sentences["local_sentence_id"]))
    
    # if signal_info_exists:
    #   self.article_id_to_signal_ids = dict(zip(self.df_articles[consts.COL_ID], self.df_articles[consts.COL_SIGNAL_ID]))
    
    self.geonames_hierarchy_ass = dict(zip(self.df_geonames_hierarchy[consts.COL_GEONAMES_ID], self.df_geonames_hierarchy["hierarchy_geoname_id"]))


    ###self.remove_ban_related_articles()
    

    event_candidates = self.retrieve_event_candidates(disease_name)


    # event_candidates = [e for e in event_candidates if e.disease.d_type != ""]
    

    df_event_candidates = self.get_df_from_event_candidates(event_candidates)
    
    # --------------------------------
    # add time relate columns
    dates = pd.to_datetime(df_event_candidates[consts.COL_PUBLISHED_TIME]).to_list()
    day_no_list, week_no_list, biweek_no_list, month_no_list, year_list, season_list = util.retrieve_time_related_info(dates)
    df_event_candidates["day_no"] = day_no_list
    df_event_candidates["week_no"] = week_no_list
    df_event_candidates["biweek_no"] = biweek_no_list
    df_event_candidates["month_no"] = month_no_list
    df_event_candidates["year"] = year_list
    df_event_candidates["season"] = season_list
    # --------------------------------
    
    event_candidates_filepath = os.path.join(self.out_csv_folder, \
                        consts.PADIWEB_EVENT_CANDIDATES \
                         + "_task1=" +  self.event_retrieval_strategy.get_description() + "." + consts.FILE_FORMAT_CSV)
    df_event_candidates.to_csv(event_candidates_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    
    # TO ADD SIGNAL IDS
    # # df_padiweb = pd.read_csv("event_candidates_padiweb_task1=relevant_sentence.csv", sep=";", keep_default_na=False)
    # # df_bahdja = pd.read_csv("articlesweb_bahdja.csv", sep=";", keep_default_na=False)
    # # article_id_to_signal_id = dict(zip(df_bahdja["id"], df_bahdja["signal_id"]))
    # # df_padiweb["signal_id"] = df_padiweb["article_id"].apply(lambda x: article_id_to_signal_id[x] if x in article_id_to_signal_id else "")
    # # df_padiweb.to_csv("event_candidates_padiweb_task1=relevant_sentence_with_signals.csv", sep=";", quoting=csv.QUOTE_NONNUMERIC)


    print("...... end of event retrieval for padiweb !")


    

  def read_csv_files_as_dataframe(self):
    signal_filepath = os.path.join(self.input_folder, \
                        consts.PADIWEB_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    signal_info_exists = os.path.isfile(signal_filepath)
    
    articles_filepath = os.path.join(self.out_csv_folder, \
                           consts.PADIWEB_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_articles = [consts.COL_ID, consts.COL_TITLE, consts.COL_URL, consts.COL_SOURCE, \
                      consts.COL_PUBLISHED_TIME]
    # if signal_info_exists:
    #   cols_articles.append(consts.COL_SIGNAL_ID)
    self.df_articles = pd.read_csv(articles_filepath, usecols=cols_articles, sep=";", keep_default_na=False)
    
    sentences_filepath = os.path.join(self.out_csv_folder, \
                           consts.PADIWEB_EXT_SENTENCES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_sentences = [consts.COL_ID,consts.COL_TEXT,consts.COL_START,consts.COL_END, \
                      consts.COL_ARTICLE_ID, consts.COL_LANG, consts.COL_SENTENCE_CLASSIF_LABEL_ID, "local_sentence_id"]
    self.df_sentences = pd.read_csv(sentences_filepath, usecols=cols_sentences, sep=";", keep_default_na=False)
    
    extr_spatial_entities_filepath = os.path.join(self.out_csv_folder, \
                            consts.PADIWEB_INFO_EXTR_SPATIAL_ENTITIES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_extr_spatial_entities = [consts.COL_ID, consts.COl_POS, consts.COl_TOKEN_INDEX, consts.COl_LENGTH, \
                      consts.COL_TYPE, consts.COL_LABEL, consts.COL_VALUE, \
                      consts.COL_ARTICLE_ID_RENAMED, consts.COL_KEYW_ID, consts.COL_FROM_AUTO_EXTR, \
                      consts.COL_GEONAMES_ID, consts.COL_GEONAMS_JSON, consts.COL_SENTENCE_ID, \
                      consts.COL_FEATURE_CODE, consts.COL_SENTENCE_CLASSIF_LABEL_ID, "local_sentence_id", \
                      "country_code", "updated", "ref_countries_for_geocoding"] 
    self.df_extr_spatial_entities = pd.read_csv(extr_spatial_entities_filepath, usecols=cols_extr_spatial_entities, sep=";", keep_default_na=False)
    self.df_extr_spatial_entities[consts.COL_SENTENCE_ID] = self.df_extr_spatial_entities[consts.COL_SENTENCE_ID].astype(int)
    self.df_extr_spatial_entities["local_sentence_id"] = self.df_extr_spatial_entities["local_sentence_id"].astype(int)


    geonames_hierarchy_filepath = os.path.join(self.out_csv_folder, \
                            consts.GEONAMES_HIERARCHY_INFO_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_geonames_hierarcy = [consts.COL_GEONAMES_ID, "hierarchy_name", "hierarchy_geoname_id"] 
    self.df_geonames_hierarchy = pd.read_csv(geonames_hierarchy_filepath, usecols=cols_geonames_hierarcy, sep=";", keep_default_na=False)

    keywords_filepath = os.path.join(self.input_folder, \
                       consts.PADIWEB_KEYWORD_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_keywords = [consts.COL_ID,consts.COL_NAME,consts.COL_LANG,consts.COL_KEYW_TYPE_ID]
    self.df_keywords = pd.read_csv(keywords_filepath, usecols=cols_keywords, sep=";", keep_default_na=False)
    
    keywords_align_filepath = os.path.join(self.input_folder, \
                         consts.PADIWEB_KEYW_ALIGN_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_keywords_align = [consts.COL_ID, consts.COL_ALIGN_KEYW_ID, consts.COL_KEYW_ID]
    self.df_keywords_align = pd.read_csv(keywords_align_filepath, usecols=cols_keywords_align, sep=";", keep_default_na=False)
    
    # this one is needed for geonames json
    intitial_info_extr_filepath = os.path.join(self.out_csv_folder, \
                        consts.PADIWEB_EXT_INFO_EXTR_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_intitial_info_extr = [consts.COL_ID, consts.COl_POS, consts.COl_TOKEN_INDEX, consts.COl_LENGTH, \
                      consts.COL_TYPE, consts.COL_LABEL, consts.COL_VALUE, \
                      consts.COL_ARTICLE_ID_RENAMED, consts.COL_KEYW_ID, consts.COL_FROM_AUTO_EXTR, \
                      consts.COL_SENTENCE_ID, consts.COL_GEONAMES_ID, consts.COL_GEONAMS_JSON] 
    self.df_initial_info_extr = pd.read_csv(intitial_info_extr_filepath, usecols=cols_intitial_info_extr, sep=";", keep_default_na=False)

   
   
   
  def get_df_from_event_candidates(self, event_candidates):
    signal_filepath = os.path.join(self.input_folder, \
                    consts.PADIWEB_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    signal_info_exists = os.path.isfile(signal_filepath)
    
    nb_events  = len(event_candidates)
    if nb_events == 0:
      print("!! there are no event candidates !!")
      return(-1)
    
    df_event_candidates = pd.DataFrame(columns=( \
                           consts.COL_ID, consts.COL_ARTICLE_ID, consts.COL_URL, consts.COL_SOURCE, \
                           consts.COL_GEONAMES_ID, "geoname_json",
                           "loc_name", "loc_country_code", consts.COL_LOC_CONTINENT, \
                           consts.COL_LAT, consts.COL_LNG, "hierarchy_data", \
                           consts.COL_PUBLISHED_TIME, 
                           # consts.COL_DISEASE_SUBTYPE, 
                           consts.COL_DISEASE,  \
                           # consts.COL_HOST_SUBTYPE, 
                           consts.COL_HOST, consts.COL_SYMPTOM_SUBTYPE, consts.COL_SYMPTOM, \
                           consts.COL_TITLE, consts.COL_SENTENCES
                           ))
    for indx in range(0,nb_events):
      e = event_candidates[indx]
      df_event_candidates.loc[indx] = e.get_event_entry() + [e.title, e.sentences]
      
    # if signal_info_exists:
    #   signal_ids = [self.article_id_to_signal_ids[a_id] for a_id in df_event_candidates[consts.COL_ARTICLE_ID]]
    #   df_event_candidates[consts.COL_SIGNAL_ID] = signal_ids
    
    return df_event_candidates
    



  def retrieve_event_candidates(self, disease_name):
    all_event_candidates = []
    
    article_id_list = np.unique(self.df_extr_spatial_entities[consts.COL_ARTICLE_ID_RENAMED])
    #article_id_list = ["7QZJNVLHTS", "L7CG25TT3N", "RA419Z60HU", "JMO1JRJDJW", "5PF9FM6U2Q", "MDQA110VY0", "4SJJ0Y1X0D", "WAYOPZHA5J", "03WD3TAW5C", "02TL7DYIWC", "3UFCKR6BOV", "3TB9FO1ENQ", "3Z3PDHGCJX", "7V4FWGZU0D", "3SJZPBCWT8"]
    #article_id_list = ["04STR5766F"]
    relevant_article_id_list = self.remove_ban_related_articles(article_id_list)
    #relevant_article_id_list = ['A86TY2VKFO', 'P6CXS552R4', '4TP9S9MRHD', '7CGBSW6XVD', 'HO377ZC5Q8', 'O0ZSJHTLRX', '5RZ4N0CHCV', 'L2056OXVZM', 'CKYEHIHDUG', '7CGBSW6XVD', '7WWPUS79DA', '5YLQ4E5MI0', 'C1XP44ULHW', 'O1VMLG1RGK', 'GQ53J3P5SL', 'TD76M6JV83', 'SQ3ZUSBNSW', '4QWA91MBT6', 'DEPVU4GMA7']

    for indx, article_id in enumerate(relevant_article_id_list):
      print("----------- ", indx, article_id)
      df_extr_spatial_entities_by_article = \
                self.df_extr_spatial_entities[self.df_extr_spatial_entities[consts.COL_ARTICLE_ID_RENAMED] == article_id]
      
      if df_extr_spatial_entities_by_article.shape[0]>0:
        # =================================================================
        # EVENT RETRIEVAL STRATEGY
        curr_event_candidates = self.event_retrieval_strategy.estimate(disease_name, df_extr_spatial_entities_by_article, \
                                                    self.sentence_id_to_article_id, self.sentence_id_to_local_sentence_id, \
                                                    self.df_articles, self.df_initial_info_extr, self.keyword_ass, \
                                                    self.df_keywords_align, self.geonames_hierarchy_ass, self.sentence_id_to_text)
        # =================================================================
        #print("--------------------------------- We extracted ", len(curr_event_candidates), " event candidates on "+disease_name+".")
        if curr_event_candidates is not None and len(curr_event_candidates) > 0:
            all_event_candidates.extend(curr_event_candidates)
    
    # df_selected_sentence_ids = pd.DataFrame(sentence_ids_list, columns =[consts.COL_SENTENCE_ID])
    # df_selected_sentence_ids[consts.COL_ARTICLE_ID_RENAMED] = self.df_info_extr_single_event_est[consts.COL_ARTICLE_ID_RENAMED]
    # selected_sentence_ids_filepath = os.path.join(self.out_csv_folder, \
    #                         consts.PADIWEB_SELECTED_SENTENCE_IDS_FILENAME + "." + consts.FILE_FORMAT_CSV)
    # df_selected_sentence_ids.to_csv(selected_sentence_ids_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    
    return(all_event_candidates)
  
  
  
  # it also verifies if an article id is in df_articles or not >> some inconsistency is possible
  def remove_ban_related_articles(self, article_id_list):
    relevant_article_id_list = []
    for article_id in article_id_list:
      if article_id in self.article_id_to_title:
        title = self.article_id_to_title[article_id]
        if not contains_ban_related_keyword(title):
          relevant_article_id_list.append(article_id)
    return relevant_article_id_list
        
      
    
    