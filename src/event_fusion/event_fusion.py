import os
import json
import pandas as pd
import src.event.event as event

import src.util as util

import src.consts as consts


from abc import ABC, abstractmethod

from src.event.location import Location
from src.event.temporality import Temporality
from src.event.host import Host
from src.event.disease import Disease
from src.event.symptom import Symptom

from src.event_fusion.event_fusion_strategy import EventFusionStrategy
from src.event_clustering.event_clustering import AbstractEventClustering
from src.event_retrieval.event_retrieval_strategy import EventRetrievalStrategy



class AbstractEventFusion(ABC):

  def __init__(self, event_fusion_strategy: EventFusionStrategy):
    print("--- using event fusion strategy: ", event_fusion_strategy.get_description())
    self.event_fusion_strategy = event_fusion_strategy

    
  def get_strategy(self):
    return(self.strategy)
  
  def set_strategy(self, strategy):
    self.strategy = strategy
    

      
  @abstractmethod
  def perform_event_fusion(self, event_candidates_filepath, output_dirpath, clustering_filepath, result_filename):
    input_dirpath = output_dirpath.replace("out", "in")
    signal_filepath = os.path.join(input_dirpath, consts.PADIWEB_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    signal_info_exists = os.path.isfile(signal_filepath)
    
    res_event_clustering = pd.read_csv(clustering_filepath, header=None)[0].to_numpy()
    
    cols_event_candidates = [consts.COL_ID, consts.COL_ARTICLE_ID, consts.COL_URL, consts.COL_SOURCE, \
                        consts.COL_GEONAMES_ID, "geoname_json", "loc_name", "loc_country_code", consts.COL_LOC_CONTINENT, \
                        consts.COL_LAT, consts.COL_LNG, "hierarchy_data", consts.COL_PUBLISHED_TIME, consts.COL_DISEASE,  \
                       consts.COL_HOST, consts.COL_SYMPTOM_SUBTYPE, consts.COL_SYMPTOM, \
                        consts.COL_TITLE, consts.COL_SENTENCES
                       ]
    if signal_info_exists:
      cols_event_candidates.append(consts.COL_SIGNAL_ID)
      
    df_event_candidates = pd.read_csv(event_candidates_filepath, usecols=cols_event_candidates, sep=";", keep_default_na=False)

    # =====================================================================
    # this method does not affect the data frame
    self.event_fusion_strategy.perform_preprocessing(df_event_candidates, res_event_clustering)
    # =====================================================================

    event_canditates = []
    for index, row in df_event_candidates.iterrows():
      loc = Location(row["loc_name"], row[consts.COL_GEONAMES_ID], json.loads(row["geoname_json"]), \
                     row[consts.COL_LAT], row[consts.COL_LNG], row["loc_country_code"], row[consts.COL_LOC_CONTINENT], \
                     eval(row["hierarchy_data"]))
      t = Temporality(row[consts.COL_PUBLISHED_TIME])
      disease_tuple = eval(row[consts.COL_DISEASE])
      dis = Disease(disease_tuple[0], disease_tuple[1])
      h = Host(json.loads(row[consts.COL_HOST]))
      sym = Symptom()
      sym.load_dict_data_from_str(row[consts.COL_SYMPTOM_SUBTYPE], row[consts.COL_SYMPTOM])
      e = event.Event(int(row[consts.COL_ID]), str(row[consts.COL_ID])+"-"+str(row[consts.COL_ARTICLE_ID]), row[consts.COL_URL], \
                      row[consts.COL_SOURCE], loc, t, dis, h, sym, row[consts.COL_TITLE], row[consts.COL_SENTENCES])
      event_canditates.append(e)
      
    
    id_to_event = {}
    for e in event_canditates:
      id_to_event[e.e_id] = e
    
    # TO ADD SIGNAL IDS
    # # df_padiweb = pd.read_csv("events_padiweb_task1=relevant_sentence_task2=hierarchical_identification-hier_duplicate_task3=max-occurrence.csv", sep=";", keep_default_na=False)
    # # df_bahdja = pd.read_csv("articlesweb_bahdja.csv", sep=";", keep_default_na=False)
    # # article_id_to_signal_id = dict(zip(df_bahdja["id"], df_bahdja["signal_id"]))
    # # df_padiweb["signal_id"] = df_padiweb["article_id"].apply(lambda x: [article_id_to_signal_id[s] if s in article_id_to_signal_id else "" for s in eval(x)])
    # # df_padiweb.to_csv("events_padiweb_task1=relevant_sentence_task2=hierarchical_identification-hier_duplicate_task3=max-occurrence_with_signals.csv", sep=";", quoting=csv.QUOTE_NONNUMERIC)
    
    final_signal_ids = []
    event_id_to_signal_id = {}
    if signal_info_exists:
      signal_ids_list = [eval(s) for s in df_event_candidates[consts.COL_SIGNAL_ID]]
      df_event_candidates["signal_ids"] = signal_ids_list
      event_id_to_signal_id = dict(zip(df_event_candidates[consts.COL_ID], signal_ids_list))
    
    events = []
    # res_event_clustering does not contain a result of hard clustering ==> check the file structure
    for grouping_info_str in res_event_clustering:
      e = id_to_event[int(grouping_info_str.split(",")[0])] # for single events
      if "," in grouping_info_str: # for more than one event
        e_list = [id_to_event[int(id)] for id in grouping_info_str.split(",")]
        # ===========================================================================
        # EVENT FUSION
        e = self.event_fusion_strategy.merge_event_candidates(e_list)
        # ===========================================================================

      if signal_info_exists:
        pass
      #   curr_signal_ids_str = row["signal_ids"]
      #   curr_signal_ids = curr_signal_ids_str.split(",")
      #   curr_signal_ids = [int(i) for i in curr_signal_ids]
      #   merged_signal_ids = curr_signal_ids
      #   if e_list_str != "NO MERGING" and e_list_str != "-1":
      #     e_list = e_list_str.split(",")
      #     e_list = [int(i) for i in e_list]
      #     for e_other_id in e_list:
      #       merged_signal_ids.append(event_id_to_signal_id[e_other_id])
      #   final_signal_ids.append(list(set(merged_signal_ids)))

      events.append(e)
      
    df_events = self.get_df_from_events(events, final_signal_ids)
    
    # --------------------------------
    # add time relate columns
    dates = pd.to_datetime(df_events[consts.COL_PUBLISHED_TIME]).to_list()
    day_no_list, week_no_list, biweek_no_list, month_no_list, year_list, season_list = util.retrieve_time_related_info(dates)
    df_events["day_no"] = day_no_list
    df_events["week_no"] = week_no_list
    df_events["biweek_no"] = biweek_no_list
    df_events["month_no"] = month_no_list
    df_events["year"] = year_list
    df_events["season"] = season_list
    # --------------------------------
    
    events_filepath = os.path.join(output_dirpath, result_filename)
    df_events.to_csv(events_filepath, sep=";")






  def get_df_from_events(self, events, signal_ids):
    nb_events  = len(events)
    if nb_events == 0:
      print("!! there are no events !!")
      return(-1)
    
    df_events = pd.DataFrame(columns=( \
                           consts.COL_ID, consts.COL_ARTICLE_ID, consts.COL_URL, consts.COL_SOURCE, \
                          consts.COL_GEONAMES_ID, "geoname_json", "loc_name", "loc_country_code", consts.COL_LOC_CONTINENT, \
                          consts.COL_LAT, consts.COL_LNG, "hierarchy_data", consts.COL_PUBLISHED_TIME, consts.COL_DISEASE,  \
                         consts.COL_HOST, consts.COL_SYMPTOM_SUBTYPE, consts.COL_SYMPTOM, \
                          consts.COL_TITLE, consts.COL_SENTENCES
                           ))
    for indx in range(0,nb_events):
      e = events[indx]
      if not isinstance(e.article_id, list) and not isinstance(e.source, list) and not isinstance(e.url, list):
        e = event.Event(e.e_id, [e.article_id], [e.url], [e.source], e.loc, \
                         e.date, e.disease, e.host, e.symptom, e.title, e.sentences)
      print(e.get_event_entry())
      # df_events.loc[indx] = e.get_event_entry() + [e.title, e.sentences]
      df_events.loc[indx] = e.get_event_entry() + ["", ""]
      
    if len(signal_ids)>0:
      df_events[consts.COL_SIGNAL_ID] = signal_ids
    
    df_events[consts.COL_ID] = df_events.index+1
    return df_events





class EventFusionPadiweb(AbstractEventFusion):

  def __init__(self, event_fusion_strategy: EventFusionStrategy, \
               event_retrieval_strategy:EventRetrievalStrategy, \
               event_clustering:AbstractEventClustering):
    super().__init__(event_fusion_strategy)
    self.event_retrieval_strategy = event_retrieval_strategy
    self.event_clustering = event_clustering
    

    
       
  def perform_event_fusion(self, out_csv_folder, out_eval_clustering_folder, out_eval_fusion_folder):
    self.out_csv_folder = out_csv_folder
    self.out_eval_clustering_folder = out_eval_clustering_folder
    self.out_eval_fusion_folder = out_eval_fusion_folder
    
    choice_strategy_descr_prev = "task1=" + self.event_retrieval_strategy.get_description()
    choice_strategy_descr = choice_strategy_descr_prev + "_task2=" + self.event_clustering.get_description()
    event_candidates_filepath = os.path.join(self.out_csv_folder, \
                                consts.PADIWEB_EVENT_CANDIDATES \
                                 + "_" + choice_strategy_descr_prev + "." + consts.FILE_FORMAT_CSV)
    output_dirpath = self.out_eval_fusion_folder
    #if os.path.exists(output_dirpath):
    #  shutil.rmtree(output_dirpath)
    try:
      if not os.path.exists(output_dirpath):
        os.makedirs(output_dirpath)
    except OSError as err:
       print(err)
       
    clustering_filepath = os.path.join(self.out_eval_clustering_folder, \
                                 consts.RESULT_FILENAME_EVENT_CLUSTERING \
                                 + "_" + choice_strategy_descr + "." + consts.FILE_FORMAT_TXT)
    result_filename = consts.PADIWEB_EVENTS + "_" + choice_strategy_descr \
                           + "_task3=" + self.event_fusion_strategy.get_description() + "." + consts.FILE_FORMAT_CSV
    
    super().perform_event_fusion(event_candidates_filepath, output_dirpath, clustering_filepath, result_filename)
    
    
    
class EventFusionPromed(AbstractEventFusion):

  def __init__(self, event_fusion_strategy: EventFusionStrategy, event_clustering:AbstractEventClustering):
    super().__init__(event_fusion_strategy)
    self.event_clustering = event_clustering

    
  def perform_event_fusion(self, out_csv_folder, out_eval_clustering_folder, out_eval_fusion_folder):
    self.out_csv_folder = out_csv_folder
    self.out_eval_clustering_folder = out_eval_clustering_folder
    self.out_eval_fusion_folder = out_eval_fusion_folder
    
    choice_strategy_descr = "task2=" + self.event_clustering.get_description()
    event_candidates_filepath = os.path.join(self.out_csv_folder, \
                                consts.HEALTHMAP_EVENT_CANDIDATES + "." + consts.FILE_FORMAT_CSV)
    output_dirpath = self.out_eval_fusion_folder
    #if os.path.exists(output_dirpath):
    #  shutil.rmtree(output_dirpath)
    try:
      if not os.path.exists(output_dirpath):
        os.makedirs(output_dirpath)
    except OSError as err:
       print(err)
       
    clustering_filepath = os.path.join(self.out_eval_clustering_folder, \
                                        consts.RESULT_FILENAME_EVENT_CLUSTERING + "_" + choice_strategy_descr + "." + consts.FILE_FORMAT_TXT)
    result_filename = consts.HEALTHMAP_EVENTS + "_" + choice_strategy_descr + "_task3=" + self.event_fusion_strategy.get_description() + "." + consts.FILE_FORMAT_CSV
    super().perform_event_fusion(event_candidates_filepath, output_dirpath, clustering_filepath, result_filename)
        
        

class EventFusionEmpresi(AbstractEventFusion):

  def __init__(self, event_fusion_strategy: EventFusionStrategy, event_clustering:AbstractEventClustering):
    super().__init__(event_fusion_strategy)
    self.event_clustering = event_clustering

    
  def perform_event_fusion(self, out_csv_folder, out_eval_clustering_folder, out_eval_fusion_folder):
    self.out_csv_folder = out_csv_folder
    self.out_eval_clustering_folder = out_eval_clustering_folder
    self.out_eval_fusion_folder = out_eval_fusion_folder
    
    choice_strategy_descr = "task2=" + self.event_clustering.get_description()
    event_candidates_filepath = os.path.join(self.out_csv_folder, \
                                consts.EMPRESI_EVENT_CANDIDATES + "." + consts.FILE_FORMAT_CSV)
    output_dirpath = self.out_eval_fusion_folder
    #if os.path.exists(output_dirpath):
    #  shutil.rmtree(output_dirpath)
    try:
      if not os.path.exists(output_dirpath):
        os.makedirs(output_dirpath)
    except OSError as err:
       print(err)
       
    clustering_filepath = os.path.join(self.out_eval_clustering_folder, \
                                        consts.RESULT_FILENAME_EVENT_CLUSTERING + "_" + choice_strategy_descr + "." + consts.FILE_FORMAT_TXT)
    result_filename = consts.EMPRESI_EVENTS + "_" + choice_strategy_descr + "_task3=" + self.event_fusion_strategy.get_description() + "." + consts.FILE_FORMAT_CSV
    
    super().perform_event_fusion(event_candidates_filepath, output_dirpath, clustering_filepath, result_filename)
        
        
        
