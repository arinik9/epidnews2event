'''
Created on Nov 14, 2021

@author: nejat
'''

from src.preprocessing.preprocessing import PreprocessingPadiweb, PreprocessingBadhjaSignals, PreprocessingEmpresi
from src.event_retrieval.retrieve_event_candidates import EventRetrievalPadiweb
from src.event_clustering.event_clustering import EventClusteringPadiweb

from src.event.event_duplicate_identification_strategy import EventDuplicateHierIdentificationStrategy
from src.event_clustering.event_clustering_strategy import EventClusteringStrategyHierDuplicate

from src.event_retrieval.event_retrieval_strategy import EventRetrievalStrategyRelevantSentence

import os
import src.consts as consts
import pandas as pd
import json
import csv

from src.event_fusion.event_fusion import EventFusionPadiweb, EventFusionPromed
from src.event_fusion.event_fusion_strategy import EventFusionStrategyMaxOccurrence

from src.event_matching.event_matching import EventMatching
from src.event_matching.event_matching_strategy import EventMatchingStrategyPossiblyDuplicate, EventMatchingStrategyEventSimilarity

from src.util_event import read_df_events, read_events_from_df


# from normalize_location import normalize_all_locations_for_padiweb, normalize_all_locations_for_healthmap


# remarks
# - in preprocessing.py, I commented this line:
#    df_sentences_agg = df_sentences_agg[df_sentences_agg[consts.COL_CLASSIF_LABEL_ID].map(len)>1]
# in prepare_auxiliary_dataframes_for_single_event_estimation() of data_reduction.py, I commented this line:
#     consts.CLASSIF_LABEL_DESCR_EPID_ID in x[consts.COL_CLASSIF_LABEL_ID]
# ===> these operations give us more entries in the end




#MAIN_FOLDER = os.path.abspath("..") # the absolute path of the previous level
MAIN_FOLDER = "/home/nejat/Downloads/epidnews2event-dev"

LIB_FOLDER = os.path.join(MAIN_FOLDER, "lib")
DATA_FOLDER = os.path.join(MAIN_FOLDER, "data")
# DATA_SOURCE_GEO_COVERAGE_FOLDER = os.path.join(DATA_FOLDER, "news-websites-geography")


# input folder
IN_FOLDER = os.path.join(MAIN_FOLDER, "in-bahdja") # REMARK: it will be reassigned in main.py

OUT_FOLDER = os.path.join(MAIN_FOLDER, "out-bahdja")
CSV_FOLDER = os.path.join(OUT_FOLDER, "csv")



# ================================================================


IN_PADIWEB_FOLDER = os.path.join(IN_FOLDER, consts.NEWS_SURVEILLANCE_PADIWEB)
IN_BAHDJA_FOLDER = os.path.join(IN_FOLDER, "bahdja")
IN_EMPRESSI_FOLDER = os.path.join(IN_FOLDER, consts.NEWS_DB_EMPRESS_I)

CSV_FOLDER = os.path.join(OUT_FOLDER, "csv")
CSV_PADIWEB_FOLDER = os.path.join(CSV_FOLDER, consts.NEWS_SURVEILLANCE_PADIWEB)
CSV_BAHDJA_FOLDER = os.path.join(CSV_FOLDER, "bahdja")
CSV_EMPRESI_FOLDER = os.path.join(CSV_FOLDER, consts.NEWS_DB_EMPRESS_I)

EVAL_EVENT_CLUSTERING_FOLDER = os.path.join(OUT_FOLDER, "evaluate-event-clustering")
EVAL_PADIWEB_EVENT_CLUSTERING_FOLDER = os.path.join(EVAL_EVENT_CLUSTERING_FOLDER, consts.NEWS_SURVEILLANCE_PADIWEB)

EVAL_EVENT_FUSION_FOLDER = os.path.join(OUT_FOLDER, "evaluate-event-fusion")
EVAL_PADIWEB_EVENT_FUSION_FOLDER = os.path.join(EVAL_EVENT_FUSION_FOLDER, consts.NEWS_SURVEILLANCE_PADIWEB)




def event_and_signal_matching_bahdja(padiweb_event_retrieval_strategy, event_matching_out_filepath):
  bahdja_articles_filepath = os.path.join(CSV_BAHDJA_FOLDER, "articlesweb_bahdja.csv") 
  df_bahdja_articles = pd.read_csv(bahdja_articles_filepath, sep=";", keep_default_na=False)
  article_id_to_signals = dict(zip(df_bahdja_articles["id"], df_bahdja_articles["signal_id"]))
  
  bahdja_signals_filepath = os.path.join(CSV_BAHDJA_FOLDER, "signal_bahdja.csv") 
  df_bahdja_signals = pd.read_csv(bahdja_signals_filepath, sep=";", keep_default_na=False, index_col=False)
  signal_id_to_empresi_id = dict(zip(df_bahdja_signals["id_signal"], df_bahdja_signals["ref_empresi"]))
  

  article_id_to_empresi_ids = {}
  for a_id in article_id_to_signals:
    signals_str = article_id_to_signals[a_id]
    signals = eval(signals_str)
    empresi_ids = [int(signal_id_to_empresi_id[s]) for s in signals if signal_id_to_empresi_id[s] != '' and signal_id_to_empresi_id[s] != 'na']
    article_id_to_empresi_ids[a_id] = empresi_ids
    
  #print(article_id_to_empresi_ids)
  
  # ==========================================
  
  event_candidates_filepath_padiweb = os.path.join(CSV_PADIWEB_FOLDER, "event_candidates_padiweb" \
                                                    + "_task1=" +  padiweb_event_retrieval_strategy + "." + consts.FILE_FORMAT_CSV)
  events_filepath_empresi = os.path.join(CSV_EMPRESI_FOLDER, "events_empresi.csv")
  
  df_event_candidates_padiweb = read_df_events(event_candidates_filepath_padiweb)
  df_events_empresi = read_df_events(events_filepath_empresi)
  
  
  padiweb_article_id_list = df_event_candidates_padiweb["article_id"].to_list()
  
  nb_common_article_ids = len(set(padiweb_article_id_list) & set(article_id_to_empresi_ids.keys()))

  # ============================================
  
  counter = 0
  df_event_matching_list = []
  for a_id in article_id_to_empresi_ids:
    if a_id in padiweb_article_id_list:
      counter += 1
      print("------------", a_id, counter)
      empresi_ids = article_id_to_empresi_ids[a_id]
      #print(empresi_ids)
      #
      df_event_candidates_padiweb_by_article = df_event_candidates_padiweb[df_event_candidates_padiweb["article_id"] == a_id]
      df_events_empresi_by_article = df_events_empresi[df_events_empresi["article_id"].isin(empresi_ids)]
        
      if df_event_candidates_padiweb_by_article.shape[0]>0 and df_events_empresi_by_article.shape[0]>0:
        event_candidates_padiweb_by_article = read_events_from_df(df_event_candidates_padiweb_by_article)
        events_empresi_by_article = read_events_from_df(df_events_empresi_by_article)
        
        event_matching_strategy = EventMatchingStrategyEventSimilarity()
        job_event_matching = EventMatching(event_matching_strategy)
        
        
        print(event_candidates_padiweb_by_article)
        print(events_empresi_by_article)
        df_event_matching = job_event_matching.perform_event_matching_as_dataframe(consts.NEWS_SURVEILLANCE_PADIWEB,\
                                                  event_candidates_padiweb_by_article,\
                                                  consts.NEWS_DB_EMPRESS_I,\
                                                  events_empresi_by_article)
        df_event_matching["padiweb_id"] = a_id
        df_event_matching_list.append(df_event_matching)
        

  df_event_matching_all = pd.concat(df_event_matching_list, axis=0)
  df_event_matching_all["x"] = df_event_matching_all["empres-i_id"]
  
  df_final = df_event_matching_all.groupby("x", group_keys=False).aggregate(lambda x: max(list(x)))
  
  df_final.to_csv(event_matching_out_filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)



def compare_nb_events_to_nb_signals(padiweb_event_retrieval_strategy, result_comp_filepath):
  bahdja_articles_filepath = os.path.join(CSV_BAHDJA_FOLDER, "articlesweb_bahdja.csv") 
  df_bahdja_articles = pd.read_csv(bahdja_articles_filepath, sep=";", keep_default_na=False)
  article_id_to_signals = dict(zip(df_bahdja_articles["id"], df_bahdja_articles["signal_id"]))
  event_candidates_filepath_padiweb = os.path.join(CSV_PADIWEB_FOLDER, "event_candidates_padiweb" \
                                                    + "_task1=" +  padiweb_event_retrieval_strategy + "." + consts.FILE_FORMAT_CSV)
  df_event_candidates_padiweb = read_df_events(event_candidates_filepath_padiweb)
  
  df_agg = df_event_candidates_padiweb.groupby(["article_id"])["id"].aggregate(lambda x: len(list(x))).reset_index()
  df_agg = df_agg.rename(columns={"id": "nb_events_padiweb"})

  article_id_list = df_agg["article_id"].to_list()
  nb_signal_list = [len(eval(article_id_to_signals[a_id])) if a_id in article_id_to_signals else 0 for a_id in article_id_list]
  df_agg["nb signals"] = nb_signal_list
  print(df_agg)
  
  df_agg.to_csv(result_comp_filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)






if __name__ == '__main__':
  
  DISEASE_NAME = consts.DISEASE_AVIAN_INFLUENZA
  
  
  #########################################
  # Empres-i
  #########################################
  print("starting with Empres-i ....")
  try:
    if not os.path.exists(CSV_EMPRESI_FOLDER):
      os.makedirs(CSV_EMPRESI_FOLDER)
  except OSError as err:
     print(err)
  
  prep = PreprocessingEmpresi()
  #prep.perform_preprocessing(DISEASE_NAME, IN_EMPRESSI_FOLDER, CSV_EMPRESI_FOLDER, DATA_FOLDER)
  
  
  
  # #########################################
  # # Padiweb
  # #########################################
  print("starting with Padiweb ....")
  try:
    if not os.path.exists(CSV_PADIWEB_FOLDER):
      os.makedirs(CSV_PADIWEB_FOLDER)
  except OSError as err:
     print(err)
  
  prep = PreprocessingPadiweb()
  #prep.perform_preprocessing(DISEASE_NAME, IN_PADIWEB_FOLDER, CSV_PADIWEB_FOLDER, DATA_FOLDER)
  
  relevant_sentence_event_retrieval_strategy = EventRetrievalStrategyRelevantSentence()
  event_retrieval = EventRetrievalPadiweb(relevant_sentence_event_retrieval_strategy)
  #event_retrieval.perform_event_retrieval(DISEASE_NAME, IN_PADIWEB_FOLDER, CSV_PADIWEB_FOLDER, DATA_FOLDER)
  
  event_duplicate_ident_strategy_manual = EventDuplicateHierIdentificationStrategy()
  event_clustering_strategy = EventClusteringStrategyHierDuplicate()
  event_clustering_padiweb_manual = EventClusteringPadiweb(event_duplicate_ident_strategy_manual, event_clustering_strategy)
  #event_clustering_padiweb_manual.perform_event_clustering(relevant_sentence_event_retrieval_strategy, CSV_PADIWEB_FOLDER, EVAL_PADIWEB_EVENT_CLUSTERING_FOLDER)
  
  max_occurrence_fusion_strategy = EventFusionStrategyMaxOccurrence()
  event_fusion_padiweb = EventFusionPadiweb(max_occurrence_fusion_strategy, relevant_sentence_event_retrieval_strategy, \
                                            event_clustering_padiweb_manual)
  #event_fusion_padiweb.perform_event_fusion(CSV_PADIWEB_FOLDER, EVAL_PADIWEB_EVENT_CLUSTERING_FOLDER, EVAL_PADIWEB_EVENT_FUSION_FOLDER)
  
  

  # #########################################
  # # Bahdja
  # #########################################
  
  print("starting with Bahdja ....")
  try:
    if not os.path.exists(CSV_BAHDJA_FOLDER):
      os.makedirs(CSV_BAHDJA_FOLDER)
  except OSError as err:
     print(err)
  
  prep = PreprocessingBadhjaSignals()
  prep.perform_preprocessing(DISEASE_NAME, IN_BAHDJA_FOLDER, CSV_BAHDJA_FOLDER, CSV_PADIWEB_FOLDER, DATA_FOLDER)
      
  
  # ======================================================================
     
  
  output_dirpath_event_matching = os.path.join(OUT_FOLDER, "event_matching")
  try:
    if not os.path.exists(output_dirpath_event_matching):
      os.makedirs(output_dirpath_event_matching)
  except OSError as err:
     print(err)
     
  event_matching_out_filepath = os.path.join(output_dirpath_event_matching, "result.csv")   
  event_and_signal_matching_bahdja(relevant_sentence_event_retrieval_strategy.get_description(), event_matching_out_filepath)
  
  
  result_comp_filepath = os.path.join(output_dirpath_event_matching, "comparison_nb_events.csv")   
  compare_nb_events_to_nb_signals(relevant_sentence_event_retrieval_strategy.get_description(), result_comp_filepath)

  
  print("ending with ???? ....")

