import os
import json
import pandas as pd
import src.event.event as event


import src.consts as consts

from src.event_retrieval.event_retrieval_strategy import EventRetrievalStrategy
from abc import ABC, abstractmethod

from src.event.location import Location
from src.event.temporality import Temporality
from src.event.host import Host
from src.event.disease import Disease
from src.event.symptom import Symptom

from src.event.event_duplicate_identification_strategy import EventDuplicateIdentificationStrategy
from src.event_clustering.event_clustering_strategy import EventClusteringStrategy



class AbstractEventClustering(ABC):

  def __init__(self, event_duplicate_ident_strategy:EventDuplicateIdentificationStrategy, event_clustering_strategy:EventClusteringStrategy):
    self.event_duplicate_ident_strategy = event_duplicate_ident_strategy
    self.event_clustering_strategy = event_clustering_strategy
    

  def get_description(self):
    return(self.event_duplicate_ident_strategy.get_description() + "-" + self.event_clustering_strategy.get_description())
    
        
  @abstractmethod
  def perform_event_clustering(self, event_candidates_filepath, output_dirpath, result_filename):
    cols_event_candidates = [consts.COL_ID, consts.COL_ARTICLE_ID, consts.COL_URL, consts.COL_SOURCE, \
                            consts.COL_GEONAMES_ID, "geoname_json", "loc_name", "loc_country_code", consts.COL_LOC_CONTINENT, \
                            consts.COL_LAT, consts.COL_LNG, "hierarchy_data", consts.COL_PUBLISHED_TIME, consts.COL_DISEASE,  \
                           consts.COL_HOST, consts.COL_SYMPTOM_SUBTYPE, consts.COL_SYMPTOM, \
                            consts.COL_TITLE, consts.COL_SENTENCES
                           ]
    df_event_candidates = pd.read_csv(event_candidates_filepath, usecols=cols_event_candidates, sep=";", keep_default_na=False)


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
      e = event.Event(int(row[consts.COL_ID]), row[consts.COL_ARTICLE_ID], row[consts.COL_URL], \
                      row[consts.COL_SOURCE], loc, t, dis, h, sym, row[consts.COL_TITLE], row[consts.COL_SENTENCES])
      event_canditates.append(e)

    # perform clustering
    self.event_clustering_strategy.perform_clustering(self.event_duplicate_ident_strategy, event_canditates, output_dirpath, result_filename)
    

    

class EventClusteringDoNothing(AbstractEventClustering):

  def __init__(self, event_duplicate_ident_strategy:EventDuplicateIdentificationStrategy, event_clustering_strategy:EventClusteringStrategy):
    super().__init__(event_duplicate_ident_strategy, event_clustering_strategy)
    
  def perform_event_clustering(self):
    pass
  
  
  
    
    
class EventClusteringPadiweb(AbstractEventClustering):

  def __init__(self, event_similarity_strategy:EventDuplicateIdentificationStrategy, event_clustering_strategy:EventClusteringStrategy):
    super().__init__(event_similarity_strategy, event_clustering_strategy)
    


  def perform_event_clustering(self, event_retrieval_strategy:EventRetrievalStrategy, out_csv_folder, out_eval_clustering_folder):
    self.out_csv_folder = out_csv_folder
    self.out_eval_clustering_folder = out_eval_clustering_folder
    
    prev_choice_strategy_descr = "task1=" + event_retrieval_strategy.get_description()
    choice_strategy_descr = prev_choice_strategy_descr + "_task2=" + self.get_description()
    event_candidates_filepath = os.path.join(self.out_csv_folder, \
                                consts.PADIWEB_EVENT_CANDIDATES + "_" + prev_choice_strategy_descr + "." + consts.FILE_FORMAT_CSV)
    output_dirpath = self.out_eval_clustering_folder

    try:
      if not os.path.exists(output_dirpath):
        os.makedirs(output_dirpath)
    except OSError as err:
       print(err)
       
    result_filename = consts.RESULT_FILENAME_EVENT_CLUSTERING + "_" + choice_strategy_descr + "." + consts.FILE_FORMAT_TXT
    super().perform_event_clustering(event_candidates_filepath, output_dirpath, result_filename)
    
    
    
class EventClusteringPromed(AbstractEventClustering):

  def __init__(self, event_similarity_strategy:EventDuplicateIdentificationStrategy, event_clustering_strategy:EventClusteringStrategy):
    super().__init__(event_similarity_strategy, event_clustering_strategy)


  def perform_event_clustering(self, out_csv_folder, out_eval_clustering_folder):
    self.out_csv_folder = out_csv_folder
    self.out_eval_clustering_folder = out_eval_clustering_folder
    
    choice_strategy_descr = "task2=" + self.get_description()
    
    event_candidates_filepath = os.path.join(self.out_csv_folder, \
                                consts.HEALTHMAP_EVENT_CANDIDATES + "." + consts.FILE_FORMAT_CSV)
    output_dirpath = self.out_eval_clustering_folder

    try:
      if not os.path.exists(output_dirpath):
        os.makedirs(output_dirpath)
    except OSError as err:
       print(err)
   
    result_filename = consts.RESULT_FILENAME_EVENT_CLUSTERING  + "_" + choice_strategy_descr + "." + consts.FILE_FORMAT_TXT
    super().perform_event_clustering(event_candidates_filepath, output_dirpath, result_filename)





class EventClusteringEmpresi(AbstractEventClustering):

  def __init__(self, event_duplicate_ident_strategy:EventDuplicateIdentificationStrategy, event_clustering_strategy:EventClusteringStrategy):
    super().__init__(event_duplicate_ident_strategy, event_clustering_strategy)


  def perform_event_clustering(self, out_csv_folder, out_eval_clustering_folder):
    self.out_csv_folder = out_csv_folder
    self.out_eval_clustering_folder = out_eval_clustering_folder
    
    choice_strategy_descr = "task2=" + self.get_description()
    
    event_candidates_filepath = os.path.join(self.out_csv_folder, \
                                consts.EMPRESI_EVENT_CANDIDATES + "." + consts.FILE_FORMAT_CSV)
    output_dirpath = self.out_eval_clustering_folder

    try:
      if not os.path.exists(output_dirpath):
        os.makedirs(output_dirpath)
    except OSError as err:
       print(err)
   
    print(event_candidates_filepath)
    result_filename = consts.RESULT_FILENAME_EVENT_CLUSTERING  + "_" + choice_strategy_descr + "." + consts.FILE_FORMAT_TXT
    super().perform_event_clustering(event_candidates_filepath, output_dirpath, result_filename)
        
        