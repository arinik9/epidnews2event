'''
Created on Apr 1, 2023

@author: nejat
'''

import os

from src.prepare_input.retrieve_geonames_details import retrieve_geonames_details_for_ground_truth
from src.prepare_input.retrieve_geonames_hierarchy import retrieve_geonames_hierarchy_for_ground_truth


# =================================================================

#MAIN_FOLDER = os.path.abspath("..") # the absolute path of the previous level
MAIN_FOLDER = "/home/nejat/eclipse/github/epidnews2event"

#LIB_FOLDER = os.path.join(MAIN_FOLDER, "lib")
DATA_FOLDER = os.path.join(MAIN_FOLDER, "data")
# DATA_SOURCE_GEO_COVERAGE_FOLDER = os.path.join(DATA_FOLDER, "news-websites-geography")

IN_FOLDER = os.path.join(MAIN_FOLDER, "in-geocoding")
IN_RAW_FOLDER = os.path.join(IN_FOLDER, "raw")

# =================================================================


if __name__ == '__main__':
  
  articles_filepath = os.path.join(IN_FOLDER, "articlesweb.csv")
  ground_truth_folder_path = IN_RAW_FOLDER
  geonames_data_filepath = os.path.join(IN_FOLDER, "ground_truth_geonames_data.csv")
  geonames_hierarchy_data_filepath = os.path.join(IN_FOLDER, "ground_truth_geonames_hierarchy.csv")
  
  retrieve_geonames_details_for_ground_truth(articles_filepath, ground_truth_folder_path, geonames_data_filepath)
  
  retrieve_geonames_hierarchy_for_ground_truth(articles_filepath, ground_truth_folder_path, geonames_hierarchy_data_filepath)
  
  