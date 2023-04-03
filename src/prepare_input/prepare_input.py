'''
Created on Apr 1, 2023

@author: nejat
'''

import os

from src.prepare_input.retrieve_sentences import retrieve_sentences_from_all_raw_texts
from src.prepare_input.retrieve_gpe import retrieve_gpe_from_all_raw_texts

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
  sentences_filepath = os.path.join(IN_FOLDER, "sentences_with_labels.csv")
  extr_info_filepath = os.path.join(IN_FOLDER, "extracted_information.csv")
  
  #retrieve_sentences_from_all_raw_texts(articles_filepath, sentences_filepath)
  
  #retrieve_gpe_from_all_raw_texts(articles_filepath, extr_info_filepath)
  
  