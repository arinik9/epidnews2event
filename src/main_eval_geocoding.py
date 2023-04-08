'''
Created on Nov 14, 2021

@author: nejat
'''

from src.preprocessing.preprocessing import PreprocessingPadiweb
from src.evaluate.evaluate_geocoding_result import evaluate_all_spatial_info_extr_results
from src.prepare_input.retrieve_sentences import retrieve_sentences_from_all_raw_texts
from src.prepare_input.retrieve_gpe import retrieve_gpe_from_all_raw_texts
from src.prepare_input.retrieve_geonames_details import retrieve_geonames_details_for_ground_truth
from src.prepare_input.retrieve_geonames_hierarchy import retrieve_geonames_hierarchy_for_ground_truth


import os
import src.consts as consts
import pandas as pd
import json


# from normalize_location import normalize_all_locations_for_padiweb, normalize_all_locations_for_healthmap


# remarks
# - in preprocessing.py, I commented this line:
#    df_sentences_agg = df_sentences_agg[df_sentences_agg[consts.COL_CLASSIF_LABEL_ID].map(len)>1]
# in prepare_auxiliary_dataframes_for_single_event_estimation() of data_reduction.py, I commented this line:
#     consts.CLASSIF_LABEL_DESCR_EPID_ID in x[consts.COL_CLASSIF_LABEL_ID]
# ===> these operations give us more entries in the end




#MAIN_FOLDER = os.path.abspath("..") # the absolute path of the previous level
MAIN_FOLDER = "<YOUR_FOLDER>"

LIB_FOLDER = os.path.join(MAIN_FOLDER, "lib")
DATA_FOLDER = os.path.join(MAIN_FOLDER, "data")
# DATA_SOURCE_GEO_COVERAGE_FOLDER = os.path.join(DATA_FOLDER, "news-websites-geography")


# input folder
IN_FOLDER = os.path.join(MAIN_FOLDER, "in-geocoding") # REMARK: it will be reassigned in main.py
IN_RAW_FOLDER = os.path.join(IN_FOLDER, "raw")
OUT_FOLDER = os.path.join(MAIN_FOLDER, "out-geocoding")
CSV_FOLDER = os.path.join(OUT_FOLDER, "csv")

EVAL_GEOCODING_FOLDER = os.path.join(OUT_FOLDER, "evaluate-geocoding")



if __name__ == '__main__':
  
  DISEASE_NAME = consts.DISEASE_AVIAN_INFLUENZA
  


  # ########################################################################
  # PREPARE INPUT
  articles_filepath = os.path.join(IN_FOLDER, "articlesweb.csv")
  sentences_filepath = os.path.join(IN_FOLDER, "sentences_with_labels.csv")
  extr_info_filepath = os.path.join(IN_FOLDER, "extracted_information.csv")
  
  retrieve_sentences_from_all_raw_texts(articles_filepath, sentences_filepath)
  retrieve_gpe_from_all_raw_texts(articles_filepath, extr_info_filepath)
  # ########################################################################


  # ########################################################################
  # PREPARE GROUND-TRUTH
  articles_filepath = os.path.join(IN_FOLDER, "articlesweb.csv")
  ground_truth_folder_path = IN_RAW_FOLDER
  geonames_data_filepath = os.path.join(IN_FOLDER, "ground_truth_geonames_data.csv")
  geonames_hierarchy_data_filepath = os.path.join(IN_FOLDER, "ground_truth_geonames_hierarchy.csv")
  
  retrieve_geonames_details_for_ground_truth(articles_filepath, ground_truth_folder_path, geonames_data_filepath)
  retrieve_geonames_hierarchy_for_ground_truth(articles_filepath, ground_truth_folder_path, geonames_hierarchy_data_filepath)
  # ########################################################################
  
      
        
  # #########################################
  # # Padiweb
  # #########################################
  print("starting with geocoding eval ....")
  try:
    if not os.path.exists(CSV_FOLDER):
      os.makedirs(CSV_FOLDER)
  except OSError as err:
     print(err)
  
  prep = PreprocessingPadiweb()
  prep.perform_preprocessing(DISEASE_NAME, IN_FOLDER, CSV_FOLDER, DATA_FOLDER)
  

  # --------------------------------------------------------------------
  
  filename = "extr_spatial_entities_info.csv"
  filepath = os.path.join(CSV_FOLDER, filename)
  df_estimated_spatial_info_extr = pd.read_csv(filepath, sep=";", keep_default_na=False)

  true_resuls_folder_path = os.path.join(IN_FOLDER, "raw")

  filepath = os.path.join(IN_FOLDER, "ground_truth_geonames_hierarchy.csv")
  df_geonames_hierarchy1 = pd.read_csv(filepath, sep=";", keep_default_na=False)
  filepath = os.path.join(CSV_FOLDER, "geonames_hierarchy.csv")
  df_geonames_hierarchy2 = pd.read_csv(filepath, sep=";", keep_default_na=False)
  df_geonames_hierarch = pd.concat([df_geonames_hierarchy1, df_geonames_hierarchy2])
  df_geonames_hierarch = df_geonames_hierarch.drop_duplicates(subset=['geonames_id'])
  df_geonames_hierarch = df_geonames_hierarch.astype({'geonames_id':'int'})
  geonames_hierarchy_ass = dict(zip(df_geonames_hierarch["geonames_id"], df_geonames_hierarch["hierarchy_name"]))

  filepath = os.path.join(IN_FOLDER, "ground_truth_geonames_data.csv")
  df_geonames = pd.read_csv(filepath, sep=";", keep_default_na=False)
  geonames_id_json_ass = dict(zip(df_geonames["geonames_id"], df_geonames["geonames_json"]))
  #print(json.loads(geonames_id_json_ass[1668285]))
  output_folder = EVAL_GEOCODING_FOLDER
  
  try:
    if not os.path.exists(output_folder):
      os.makedirs(output_folder)
  except OSError as err:
     print(err)
     
  evaluate_all_spatial_info_extr_results(df_estimated_spatial_info_extr, true_resuls_folder_path, \
                                          geonames_id_json_ass, geonames_hierarchy_ass, output_folder)
  
  print("ending with geocoding eval ....")

