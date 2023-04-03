'''
Created on Nov 14, 2021

@author: nejat
'''

from src.preprocessing.preprocessing import PreprocessingPadiweb, PreprocessingPromed, PreprocessingEmpresi
from src.event_retrieval.retrieve_event_candidates import EventRetrievalPadiweb
from src.event_clustering.event_clustering import EventClusteringPadiweb, EventClusteringPromed
from src.event_fusion.event_fusion import EventFusionPadiweb, EventFusionPromed
from src.event.event_duplicate_identification_strategy import EventDuplicateHierIdentificationStrategy
from src.event_clustering.event_clustering_strategy import EventClusteringStrategyHierDuplicate
from src.event_retrieval.event_retrieval_strategy import EventRetrievalStrategyRelevantSentence
from src.event_fusion.event_fusion_strategy import EventFusionStrategyMaxOccurrence

import os
import src.consts as consts



# =================================================================

MAIN_FOLDER = "<YOUR_FOLDER>" # TODO

DATA_FOLDER = os.path.join(MAIN_FOLDER, "data")
IN_FOLDER = os.path.join(MAIN_FOLDER, "in")
OUT_FOLDER = os.path.join(MAIN_FOLDER, "out")

# =================================================================




IN_PADIWEB_FOLDER = os.path.join(IN_FOLDER, consts.NEWS_SURVEILLANCE_PADIWEB)
IN_PROMED_FOLDER = os.path.join(IN_FOLDER, consts.NEWS_SURVEILLANCE_PROMED)
IN_EMPRESSI_FOLDER = os.path.join(IN_FOLDER, consts.NEWS_DB_EMPRESS_I)

CSV_FOLDER = os.path.join(OUT_FOLDER, "csv")
CSV_PADIWEB_FOLDER = os.path.join(CSV_FOLDER, consts.NEWS_SURVEILLANCE_PADIWEB)
CSV_PROMED_FOLDER = os.path.join(CSV_FOLDER, consts.NEWS_SURVEILLANCE_PROMED)
CSV_EMPRESI_FOLDER = os.path.join(CSV_FOLDER, consts.NEWS_DB_EMPRESS_I)

EVAL_EVENT_CLUSTERING_FOLDER = os.path.join(OUT_FOLDER, "evaluate-event-clustering")
EVAL_PADIWEB_EVENT_CLUSTERING_FOLDER = os.path.join(EVAL_EVENT_CLUSTERING_FOLDER, consts.NEWS_SURVEILLANCE_PADIWEB)
EVAL_PROMED_EVENT_CLUSTERING_FOLDER = os.path.join(EVAL_EVENT_CLUSTERING_FOLDER, consts.NEWS_SURVEILLANCE_PROMED)

EVAL_EVENT_FUSION_FOLDER = os.path.join(OUT_FOLDER, "evaluate-event-fusion")
EVAL_PADIWEB_EVENT_FUSION_FOLDER = os.path.join(EVAL_EVENT_FUSION_FOLDER, consts.NEWS_SURVEILLANCE_PADIWEB)
EVAL_PROMED_EVENT_FUSION_FOLDER = os.path.join(EVAL_EVENT_FUSION_FOLDER, consts.NEWS_SURVEILLANCE_PROMED)







if __name__ == '__main__':
  
  DISEASE_NAME = consts.DISEASE_AVIAN_INFLUENZA
  #DISEASE_NAME = consts.DISEASE_WEST_NILE_VIRUS
  
  if DISEASE_NAME == consts.DISEASE_WEST_NILE_VIRUS:
    consts.HOST_KEYWORDS_HIERARCHY_DICT["human"] = consts.HOST_KEYWORDS_HIERARCHY_DICT["human"] + \
      [
        {"text": "death", "level": 0, "hierarchy": ("human", "gender-unknown")},
        {"text": "resident", "level": 0, "hierarchy": ("human", "gender-unknown")},
        {"text": "hospital", "level": 0, "hierarchy": ("human", "gender-unknown")},
        {"text": "victim", "level": 0, "hierarchy": ("human", "gender-unknown")}
      ]
    consts.HOST_KEYWORDS_HIERARCHY_DICT["mosquito"] = consts.HOST_KEYWORDS_HIERARCHY_DICT["mosquito"] + \
      [
        {"text": "vector", "level": 0, "hierarchy": ("mosquito", "subtype-unknown")}
      ]      
      
    
  
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
  prep.perform_preprocessing(DISEASE_NAME, IN_EMPRESSI_FOLDER, CSV_EMPRESI_FOLDER, DATA_FOLDER)
  
  
      
        
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
  prep.perform_preprocessing(DISEASE_NAME, IN_PADIWEB_FOLDER, CSV_PADIWEB_FOLDER, DATA_FOLDER)
  
  relevant_sentence_event_retrieval_strategy = EventRetrievalStrategyRelevantSentence()
  event_retrieval = EventRetrievalPadiweb(relevant_sentence_event_retrieval_strategy)
  event_retrieval.perform_event_retrieval(DISEASE_NAME, IN_PADIWEB_FOLDER, CSV_PADIWEB_FOLDER, DATA_FOLDER)
  
  event_duplicate_ident_strategy_manual = EventDuplicateHierIdentificationStrategy()
  event_clustering_strategy = EventClusteringStrategyHierDuplicate()
  event_clustering_padiweb_manual = EventClusteringPadiweb(event_duplicate_ident_strategy_manual, event_clustering_strategy)
  event_clustering_padiweb_manual.perform_event_clustering(relevant_sentence_event_retrieval_strategy, CSV_PADIWEB_FOLDER, EVAL_PADIWEB_EVENT_CLUSTERING_FOLDER)
  
  max_occurrence_fusion_strategy = EventFusionStrategyMaxOccurrence()
  event_fusion_padiweb = EventFusionPadiweb(max_occurrence_fusion_strategy, relevant_sentence_event_retrieval_strategy, \
                                            event_clustering_padiweb_manual)
  event_fusion_padiweb.perform_event_fusion(CSV_PADIWEB_FOLDER, EVAL_PADIWEB_EVENT_CLUSTERING_FOLDER, EVAL_PADIWEB_EVENT_FUSION_FOLDER)
  
  
  print("ending with Padiweb ....")

  
  
  
  #########################################
  # Promed
  #########################################
  print("starting with Promed ....")
  try:
    if not os.path.exists(CSV_PROMED_FOLDER):
      os.makedirs(CSV_PROMED_FOLDER)
  except OSError as err:
     print(err)
     
  KEEP_ONLY_UNOFFICIAL_DATA = False
  
  prep = PreprocessingPromed()
  prep.perform_preprocessing(DISEASE_NAME, IN_PROMED_FOLDER, CSV_PROMED_FOLDER, DATA_FOLDER, KEEP_ONLY_UNOFFICIAL_DATA)
  
  
  event_duplicate_ident_strategy_manual = EventDuplicateHierIdentificationStrategy()
  event_clustering_strategy = EventClusteringStrategyHierDuplicate()
  event_clustering_promed_manual = EventClusteringPromed(event_duplicate_ident_strategy_manual, event_clustering_strategy)
  event_clustering_promed_manual.perform_event_clustering(CSV_PROMED_FOLDER, EVAL_PROMED_EVENT_CLUSTERING_FOLDER)
  
  
  max_occurrence_fusion_strategy = EventFusionStrategyMaxOccurrence()
  event_fusion_promed = EventFusionPromed(max_occurrence_fusion_strategy, \
                                            event_clustering_promed_manual)
  event_fusion_promed.perform_event_fusion(CSV_PROMED_FOLDER, EVAL_PROMED_EVENT_CLUSTERING_FOLDER, EVAL_PROMED_EVENT_FUSION_FOLDER)
  
  print("ending with Promed ....")
  
  
