'''
Created on Nov 14, 2021

@author: nejat
'''

from src.preprocessing.preprocessing import PreprocessingPadiweb, PreprocessingPromed, PreprocessingEmpresi, PreprocessingWahis, PreprocessingApha, PreprocessingAphis
from src.event_retrieval.retrieve_doc_events import EventRetrievalPadiweb, EventRetrievalEmpresi, EventRetrievalWahis, EventRetrievalPromed, EventRetrievalAphis, EventRetrievalApha
from src.event_clustering.event_clustering import EventClusteringPadiweb, EventClusteringPromed,\
                                                EventClusteringEmpresi, EventClusteringWahis, EventClusteringApha, EventClusteringAphis
from src.event_fusion.event_fusion import EventFusionPadiweb, EventFusionPromed, EventFusionEmpresi, EventFusionWahis, EventFusionAphis, EventFusionApha
from src.event.event_duplicate_identification_strategy import EventDuplicateHierIdentificationStrategy
from src.event_clustering.event_clustering_strategy import EventClusteringStrategyHierDuplicate
from src.event_retrieval.event_retrieval_strategy import EventRetrievalStrategyRelevantSentence, EventRetrievalStrategyStructuredData
from src.event_fusion.event_fusion_strategy import EventFusionStrategyMaxOccurrence
from src.event_normalization.normalize_events import NormalizationPadiweb, NormalizationEmpresi, NormalizationWahis, NormalizationPromed, NormalizationApha, NormalizationAphis

import os
import time
import src.consts as consts
import src.user_consts as user_consts

if __name__ == '__main__':
  st = time.time()
  
  #########################################
  # Padiweb
  #########################################
  print("starting with Padiweb ....")
  
  for folder in [consts.PREPROCESSING_RESULT_PADIWEB_FOLDER, consts.NORM_RESULT_PADIWEB_FOLDER, consts.DOC_EVENTS_PADIWEB_FOLDER, \
                  consts.EVENT_CLUSTERING_PADIWEB_FOLDER, consts.CORPUS_EVENTS_PADIWEB_FOLDER]:
    try:
      if not os.path.exists(folder):
        os.makedirs(folder)
    except OSError as err:
      print(err)
  
  prep = PreprocessingPadiweb()
  prep.perform_preprocessing(user_consts.USER_DISEASE_NAME, consts.IN_PADIWEB_FOLDER, consts.PREPROCESSING_RESULT_PADIWEB_FOLDER, \
                              consts.DATA_FOLDER)
  
  norm = NormalizationPadiweb()
  norm.perform_normalization(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_PADIWEB_FOLDER, consts.NORM_RESULT_PADIWEB_FOLDER, \
                              consts.DATA_FOLDER) 
  
  relevant_sentence_event_retrieval_strategy = EventRetrievalStrategyRelevantSentence()
  event_retrieval = EventRetrievalPadiweb(relevant_sentence_event_retrieval_strategy)
  event_retrieval.perform_event_retrieval(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_PADIWEB_FOLDER, consts.NORM_RESULT_PADIWEB_FOLDER, \
                                           consts.DOC_EVENTS_PADIWEB_FOLDER, consts.DATA_FOLDER)
  
  event_duplicate_ident_strategy_manual = EventDuplicateHierIdentificationStrategy()
  event_clustering_strategy = EventClusteringStrategyHierDuplicate()
  event_clustering_padiweb_manual = EventClusteringPadiweb(relevant_sentence_event_retrieval_strategy, event_duplicate_ident_strategy_manual, \
                                                            event_clustering_strategy)
  event_clustering_padiweb_manual.perform_event_clustering(consts.DOC_EVENTS_PADIWEB_FOLDER, consts.EVENT_CLUSTERING_PADIWEB_FOLDER)
  
  max_occurrence_fusion_strategy = EventFusionStrategyMaxOccurrence()
  event_fusion_padiweb = EventFusionPadiweb(relevant_sentence_event_retrieval_strategy, event_clustering_padiweb_manual, max_occurrence_fusion_strategy)
  event_fusion_padiweb.perform_event_fusion(consts.DOC_EVENTS_PADIWEB_FOLDER, consts.EVENT_CLUSTERING_PADIWEB_FOLDER, consts.CORPUS_EVENTS_PADIWEB_FOLDER)
  
  print("ending with Padiweb ....")

  

  
  #########################################
  # Wahis
  #########################################
  print("starting with Wahis ....")
  
  for folder in [consts.PREPROCESSING_RESULT_WAHIS_FOLDER, consts.NORM_RESULT_WAHIS_FOLDER, consts.DOC_EVENTS_WAHIS_FOLDER, \
                 consts.EVENT_CLUSTERING_WAHIS_FOLDER, consts.CORPUS_EVENTS_WAHIS_FOLDER]:
    try:
      if not os.path.exists(folder):
        os.makedirs(folder)
    except OSError as err:
      print(err)
  
  prep = PreprocessingWahis()
  prep.perform_preprocessing(user_consts.USER_DISEASE_NAME, consts.IN_WAHIS_FOLDER, consts.PREPROCESSING_RESULT_WAHIS_FOLDER, \
                              consts.DATA_FOLDER)
  
  norm = NormalizationWahis()
  norm.perform_normalization(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_WAHIS_FOLDER, consts.NORM_RESULT_WAHIS_FOLDER,\
                              consts.DATA_FOLDER) 
  
  structured_data_event_retrieval_strategy = EventRetrievalStrategyStructuredData()
  event_retrieval = EventRetrievalWahis(structured_data_event_retrieval_strategy)
  event_retrieval.perform_event_retrieval(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_WAHIS_FOLDER, \
                                           consts.NORM_RESULT_WAHIS_FOLDER, consts.DOC_EVENTS_WAHIS_FOLDER, consts.DATA_FOLDER)
  
  event_duplicate_ident_strategy_manual = EventDuplicateHierIdentificationStrategy()
  event_clustering_strategy = EventClusteringStrategyHierDuplicate()
  event_clustering_wahis_manual = EventClusteringWahis(structured_data_event_retrieval_strategy, event_duplicate_ident_strategy_manual, event_clustering_strategy)
  event_clustering_wahis_manual.perform_event_clustering(consts.DOC_EVENTS_WAHIS_FOLDER, consts.EVENT_CLUSTERING_WAHIS_FOLDER)
  
  max_occurrence_fusion_strategy = EventFusionStrategyMaxOccurrence()
  event_fusion_wahis = EventFusionWahis(structured_data_event_retrieval_strategy, event_clustering_wahis_manual, max_occurrence_fusion_strategy)
  event_fusion_wahis.perform_event_fusion(consts.DOC_EVENTS_WAHIS_FOLDER, consts.EVENT_CLUSTERING_WAHIS_FOLDER, \
                                          consts.CORPUS_EVENTS_WAHIS_FOLDER)
  
  print("ending with Wahis ....")
  
  
  
  
  #########################################
  # Promed
  #########################################
  print("starting with Promed ....")
  
  for folder in [consts.PREPROCESSING_RESULT_PROMED_FOLDER, consts.NORM_RESULT_PROMED_FOLDER, consts.DOC_EVENTS_PROMED_FOLDER, \
                 consts.EVENT_CLUSTERING_PROMED_FOLDER, consts.CORPUS_EVENTS_PROMED_FOLDER]:
    try:
      if not os.path.exists(folder):
        os.makedirs(folder)
    except OSError as err:
      print(err)
  
  prep = PreprocessingPromed()
  prep.perform_preprocessing(user_consts.USER_DISEASE_NAME, consts.IN_PROMED_FOLDER, consts.PREPROCESSING_RESULT_PROMED_FOLDER, \
                              consts.DATA_FOLDER)
  
  norm = NormalizationPromed()
  norm.perform_normalization(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_PROMED_FOLDER, \
                              consts.NORM_RESULT_PROMED_FOLDER, consts.DATA_FOLDER) 
  
  structured_data_event_retrieval_strategy = EventRetrievalStrategyStructuredData()
  event_retrieval = EventRetrievalPromed(structured_data_event_retrieval_strategy)
  event_retrieval.perform_event_retrieval(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_PROMED_FOLDER, \
                                           consts.NORM_RESULT_PROMED_FOLDER, consts.DOC_EVENTS_PROMED_FOLDER, consts.DATA_FOLDER)
  
  event_duplicate_ident_strategy_manual = EventDuplicateHierIdentificationStrategy()
  event_clustering_strategy = EventClusteringStrategyHierDuplicate()
  event_clustering_promed_manual = EventClusteringPromed(structured_data_event_retrieval_strategy, \
                                                          event_duplicate_ident_strategy_manual, event_clustering_strategy)
  event_clustering_promed_manual.perform_event_clustering(consts.DOC_EVENTS_PROMED_FOLDER, consts.EVENT_CLUSTERING_PROMED_FOLDER)
  
  max_occurrence_fusion_strategy = EventFusionStrategyMaxOccurrence()
  event_fusion_promed = EventFusionPromed(structured_data_event_retrieval_strategy, event_clustering_promed_manual, \
                                           max_occurrence_fusion_strategy)
  event_fusion_promed.perform_event_fusion(consts.DOC_EVENTS_PROMED_FOLDER, consts.EVENT_CLUSTERING_PROMED_FOLDER, \
                                            consts.CORPUS_EVENTS_PROMED_FOLDER)
  
  print("ending with Promed ....")
  
  
  
  #########################################
  # Apha
  #########################################
  print("starting with Apha ....")
  
  for folder in [consts.PREPROCESSING_RESULT_APHA_FOLDER, consts.NORM_RESULT_APHA_FOLDER, consts.DOC_EVENTS_APHA_FOLDER, \
                 consts.EVENT_CLUSTERING_APHA_FOLDER, consts.CORPUS_EVENTS_APHA_FOLDER]:
    try:
      if not os.path.exists(folder):
        os.makedirs(folder)
    except OSError as err:
      print(err)
  
  prep = PreprocessingApha()
  prep.perform_preprocessing(user_consts.USER_DISEASE_NAME, consts.IN_APHA_FOLDER, consts.PREPROCESSING_RESULT_APHA_FOLDER, \
                              consts.DATA_FOLDER)
  
  norm = NormalizationApha()
  norm.perform_normalization(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_APHA_FOLDER, \
                             consts.NORM_RESULT_APHA_FOLDER, consts.DATA_FOLDER) 
  
  structured_data_event_retrieval_strategy = EventRetrievalStrategyStructuredData()
  event_retrieval = EventRetrievalApha(structured_data_event_retrieval_strategy)
  event_retrieval.perform_event_retrieval(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_APHA_FOLDER, \
                                          consts.NORM_RESULT_APHA_FOLDER, consts.DOC_EVENTS_APHA_FOLDER, consts.DATA_FOLDER)
  
  event_duplicate_ident_strategy_manual = EventDuplicateHierIdentificationStrategy()
  event_clustering_strategy = EventClusteringStrategyHierDuplicate()
  event_clustering_apha_manual = EventClusteringApha(structured_data_event_retrieval_strategy, \
                                                     event_duplicate_ident_strategy_manual, event_clustering_strategy)
  event_clustering_apha_manual.perform_event_clustering(consts.DOC_EVENTS_APHA_FOLDER, consts.EVENT_CLUSTERING_APHA_FOLDER)
  
  max_occurrence_fusion_strategy = EventFusionStrategyMaxOccurrence()
  event_fusion_apha = EventFusionApha(structured_data_event_retrieval_strategy, event_clustering_apha_manual, \
                                      max_occurrence_fusion_strategy)
  event_fusion_apha.perform_event_fusion(consts.DOC_EVENTS_APHA_FOLDER, consts.EVENT_CLUSTERING_APHA_FOLDER, \
                                         consts.CORPUS_EVENTS_APHA_FOLDER)
  
  print("ending with Apha ....")
  
  
  #########################################
  # Aphis
  #########################################
  print("starting with Aphis ....")
  
  for folder in [consts.PREPROCESSING_RESULT_APHIS_FOLDER, consts.NORM_RESULT_APHIS_FOLDER, consts.DOC_EVENTS_APHIS_FOLDER, \
                 consts.EVENT_CLUSTERING_APHIS_FOLDER, consts.CORPUS_EVENTS_APHIS_FOLDER]:
    try:
      if not os.path.exists(folder):
        os.makedirs(folder)
    except OSError as err:
      print(err)
  
  prep = PreprocessingAphis()
  prep.perform_preprocessing(user_consts.USER_DISEASE_NAME, consts.IN_APHIS_FOLDER, consts.PREPROCESSING_RESULT_APHIS_FOLDER,\
                              consts.DATA_FOLDER)
  
  norm = NormalizationAphis()
  norm.perform_normalization(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_APHIS_FOLDER,\
                              consts.NORM_RESULT_APHIS_FOLDER, consts.DATA_FOLDER) 
  
  structured_data_event_retrieval_strategy = EventRetrievalStrategyStructuredData()
  event_retrieval = EventRetrievalAphis(structured_data_event_retrieval_strategy)
  event_retrieval.perform_event_retrieval(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_APHIS_FOLDER,\
                                           consts.NORM_RESULT_APHIS_FOLDER, consts.DOC_EVENTS_APHIS_FOLDER, consts.DATA_FOLDER)
  
  event_duplicate_ident_strategy_manual = EventDuplicateHierIdentificationStrategy()
  event_clustering_strategy = EventClusteringStrategyHierDuplicate()
  event_clustering_aphis_manual = EventClusteringAphis(structured_data_event_retrieval_strategy,\
                                                        event_duplicate_ident_strategy_manual, event_clustering_strategy)
  event_clustering_aphis_manual.perform_event_clustering(consts.DOC_EVENTS_APHIS_FOLDER, consts.EVENT_CLUSTERING_APHIS_FOLDER)
  
  max_occurrence_fusion_strategy = EventFusionStrategyMaxOccurrence()
  event_fusion_aphis = EventFusionAphis(structured_data_event_retrieval_strategy, event_clustering_aphis_manual,\
                                         max_occurrence_fusion_strategy)
  event_fusion_aphis.perform_event_fusion(consts.DOC_EVENTS_APHIS_FOLDER, consts.EVENT_CLUSTERING_APHIS_FOLDER, \
                                           consts.CORPUS_EVENTS_APHIS_FOLDER)
  
  print("ending with Aphis ....")



  #########################################
  # Empres-i
  #########################################
  print("starting with Empres-i ....")
  
  for folder in [consts.PREPROCESSING_RESULT_EMPRESI_FOLDER, consts.NORM_RESULT_EMPRESI_FOLDER, consts.DOC_EVENTS_EMPRESI_FOLDER, \
                 consts.EVENT_CLUSTERING_EMPRESI_FOLDER, consts.CORPUS_EVENTS_EMPRESI_FOLDER]:
    try:
      if not os.path.exists(folder):
        os.makedirs(folder)
    except OSError as err:
      print(err)
  
  prep = PreprocessingEmpresi()
  prep.perform_preprocessing(user_consts.USER_DISEASE_NAME, consts.IN_EMPRESSI_FOLDER, consts.PREPROCESSING_RESULT_EMPRESI_FOLDER, \
                              consts.DATA_FOLDER)
  
  norm = NormalizationEmpresi()
  norm.perform_normalization(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_EMPRESI_FOLDER, \
                             consts.NORM_RESULT_EMPRESI_FOLDER, consts.DATA_FOLDER) 
  
  structured_data_event_retrieval_strategy = EventRetrievalStrategyStructuredData()
  event_retrieval = EventRetrievalEmpresi(structured_data_event_retrieval_strategy)
  event_retrieval.perform_event_retrieval(user_consts.USER_DISEASE_NAME, consts.PREPROCESSING_RESULT_EMPRESI_FOLDER, \
                                           consts.NORM_RESULT_EMPRESI_FOLDER, consts.DOC_EVENTS_EMPRESI_FOLDER, consts.DATA_FOLDER)
  
  event_duplicate_ident_strategy_manual = EventDuplicateHierIdentificationStrategy()
  event_clustering_strategy = EventClusteringStrategyHierDuplicate()
  event_clustering_empresi_manual = EventClusteringEmpresi(structured_data_event_retrieval_strategy, \
                                                            event_duplicate_ident_strategy_manual, event_clustering_strategy)
  event_clustering_empresi_manual.perform_event_clustering(consts.DOC_EVENTS_EMPRESI_FOLDER, \
                                                           consts.EVENT_CLUSTERING_EMPRESI_FOLDER)
  
  max_occurrence_fusion_strategy = EventFusionStrategyMaxOccurrence()
  event_fusion_empresi = EventFusionEmpresi(structured_data_event_retrieval_strategy, event_clustering_empresi_manual, \
                                            max_occurrence_fusion_strategy)
  event_fusion_empresi.perform_event_fusion(consts.DOC_EVENTS_EMPRESI_FOLDER, consts.EVENT_CLUSTERING_EMPRESI_FOLDER, \
                                             consts.CORPUS_EVENTS_EMPRESI_FOLDER)
  
  print("ending with Empres-i ....")

  
  
  elapsed_time = time.time()-st
  print('Execution time:', elapsed_time/60, 'minutes')
