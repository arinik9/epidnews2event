'''
Created on Nov 16, 2021

@author: nejat
'''

import os
import json
import math

import pandas as pd
import numpy as np

import src.consts as consts

import dateutil.parser as parser

import shutil

from abc import ABC, abstractmethod


import string


import time


import csv
import re
import requests


import src.util as util

import src.event.event as event
import src.event.location as location
import src.event.temporality as temporality
import src.event.disease as disease
import src.event.symptom as symptom
import src.event.host as host

from src.util_gis import retrieve_continent_from_country_code

from src.util_event import retrieve_disease_from_raw_sentence, retrieve_host_from_raw_sentence

from tldextract import extract

from src.media_sources.news_source_geo_coverage import perform_media_source_resolution

import spacy

# https://gis.stackexchange.com/questions/308508/difference-between-geodetic-distance-and-great-circle-distance
# https://github.com/geopy/geopy
# https://geopy.readthedocs.io/en/latest/
from geopy.geocoders import Nominatim

from src.geocoding.relative_spatial_entity import is_relative_spatial_entity, idenitfy_relative_spatial_cardinal
from src.geocoding.geocode import geocode_batch_with_nominatim, geocode_batch_with_arcgis, geocode_batch_with_geonames, geocode_with_geonames, geocode_raw_with_geonames
from src.geocoding.enhance_geonames_data_with_hierarchy import get_loc_hierarchy, enhance_geonames_data_with_hierarchy
    
from iso3166 import countries

from src.geocoding.prepare_spatial_entity_lookup_table import build_spatial_entity_lookup_table

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.util import ngrams








class AbstractPreprocessing(ABC):
  
  
  def bind_article_ids_to_signal_ids(self):
    
    df_paths_agg = self.df_paths.groupby([consts.COL_ARTICLE_ID]).aggregate(temp_col=(consts.COL_ID, lambda x: list(x)))
    df_paths_agg = df_paths_agg.rename(columns={"temp_col": consts.COL_ID_PATH_LIST})

    path_id_to_signal_id = dict()
    for index, row in self.df_paths_signal.iterrows():
      p_id = row[consts.COL_PATH_ID_RENAMED]
      if p_id not in path_id_to_signal_id:
        path_id_to_signal_id[p_id] = []
      path_id_to_signal_id[p_id].append(row[consts.COL_SIGNAL_ID_RENAMED])
    
    signal_ids = []
    for index, row in df_paths_agg.iterrows():
      s_ids = []
      p_ids = row[consts.COL_ID_PATH_LIST]
      for p_id in p_ids:
        if p_id in path_id_to_signal_id:
          s_id = path_id_to_signal_id[p_id]
          s_ids = s_ids + s_id
      signal_ids.append(list(set(s_ids)))
    
    df_paths_agg[consts.COL_SIGNAL_ID] = signal_ids
    df_paths_agg = df_paths_agg[df_paths_agg[consts.COL_SIGNAL_ID].map(len)>0]
    return(df_paths_agg)
  
  
  def write_aggregated_paths_file_into_file(self, paths_agg_filepath):
    df_paths_agg = self.bind_article_ids_to_signal_ids()
    df_paths_agg.to_csv(paths_agg_filepath, index=True, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    df_paths_agg[consts.COL_ARTICLE_ID] = df_paths_agg.index.get_level_values(consts.COL_ARTICLE_ID)
    return(df_paths_agg)
  
  
  def extend_articles_info_with_signal_ids(self, df_paths_agg, articles_final_filepath):
    article_id_to_signal_ids = dict(zip(df_paths_agg["article_id"],df_paths_agg["signal_id"]))
    articles_final = self.df_articles[self.df_articles["id"].isin(article_id_to_signal_ids.keys())]
    
    signal_ids_list = []
    for index, row in articles_final.iterrows():
      article_id = row["id"]
      s_ids = article_id_to_signal_ids[article_id]
      signal_ids_list.append(s_ids) 
    
    articles_final["signal_id"] = signal_ids_list
    articles_final.to_csv(articles_final_filepath, index=True, quotechar='"',sep=";")




  # example
  # url = "http://news.google.com/news/url?sa=t&fd=R&ct2=us&usg=AFQjCNGVMSrg2dLxRQjoY7IsVq4KnyhJSQ&
  #   clid=c3a7d30bb8a4878e06b80cf16b898331&cid=52780061392433&ei=GZjBW4C7FqPbzAa3qpCIBg
  #   &url=https://www.channelnewsasia.com/news/asia/china-reports-new-h5n6-bird-flu-outbreak-in-hunan-province-10808126"
  def trim_url_custom(self, url):
    #print(url)
    http_count = url.count("http")
    if http_count > 0:
      url_part_by_http = url.split("http")
      part_of_interest = url_part_by_http[http_count]
      # https://developer.salesforce.com/forums/?id=906F00000008pc4IAA
      part_of_interest = part_of_interest.replace("%2F","/")
      part_of_interest = part_of_interest.split("//")[1]
      twitter_count = url.count("twitter")
      if twitter_count>0:
        # https://twitter.com/FluTrackers/statuses/1077629051630702593
        if "/status" in part_of_interest:
          final_source = part_of_interest.split("/status")[0]
        else:
          final_source = part_of_interest.split("/statuses")[0]
      else:
        final_source = part_of_interest.split("/")[0]
        
      # https://redirect.viglink.com?u=sdsfsdfds
      # The ? indicates that there are variables coming in.
      res = re.match(r"^(.)+\?", final_source)
      if res is not None:
        final_source = final_source.split("?", 1)[0]
        
      if "wwwnc." in final_source:
        final_source = final_source.split("wwwnc.")[1]
      if "www." in final_source:
        final_source = final_source.split("www.")[1]
      

      if final_source == "oie.int":
        final_source = "wahis.oie.int"

      # ---------------------------
      # some manual cleaning rules
      final_source = re.sub('^e\.', '', final_source)
      final_source = re.sub('^m\.', '', final_source)
      final_source = re.sub('^english\.', '', final_source)
      final_source = re.sub('^eng\.', '', final_source)
      final_source = re.sub('^en\.', '', final_source)
      final_source = re.sub('^fr\.', '', final_source)
      final_source = re.sub('^french\.', '', final_source)
      final_source = re.sub('^it\.', '', final_source)
      final_source = re.sub('^blog\.', '', final_source)
      final_source = re.sub('^blogs\.', '', final_source)
      final_source = re.sub('^world\.', '', final_source)
      final_source = re.sub('^in\.', '', final_source)
      final_source = re.sub('^www01\.', '', final_source)
      final_source = re.sub('^www1\.', '', final_source)
      final_source = re.sub('^www2\.', '', final_source)
      final_source = re.sub('^www3\.', '', final_source)
      final_source = re.sub('^wap\.', '', final_source)
      if "indiatimes.com" in final_source:
        final_source = "indiatimes.com"
      if "yna.co.kr" in final_source:
        final_source = "yna.co.kr"
      if "kbs.co.kr" in final_source:
        final_source = "kbs.co.kr"
      if "china.org.cn" in final_source:
        final_source = "china.org.cn"        
      if "reuters.com" in final_source:
        final_source = "reuters.com"
      if "sputniknews.com" in final_source:
        final_source = "sputniknews.com"   
      if "dvm360.com" in final_source:
        final_source = "dvm360.com"
      if "yahoo.com" in final_source:
        final_source = "yahoo.com"  
      if "who.int" in final_source:
        final_source = "who.int" 
      if "francetvinfo.fr" in final_source:
        final_source = "francetvinfo.fr"
      if "cbslocal.com" in final_source:
        final_source = "cbslocal.com"
      if "gelocal.it" in final_source:
        final_source = "gelocal.it"
      if "ctvnews.ca" in final_source:
        final_source = "ctvnews.ca"  
      if "floridaweekly.com" in final_source:
        final_source = "floridaweekly.com"      
      if "advocatemag.com" in final_source:
        final_source = "advocatemag.com"
      if "euronews.com" in final_source:
        final_source = "euronews.com"
      if "repubblica.it" in final_source:
        final_source = "repubblica.it"
      if "corriere.it" in final_source:
        final_source = "corriere.it"
      if "timesreview.com" in final_source:
        final_source = "timesreview.com"
      if "news12.com" in final_source:
        final_source = "news12.com"
      if "newschannelnebraska.com" in final_source:
        final_source = "newschannelnebraska.com"
      if "springer.com" in final_source:
        final_source = "springer.com"

      return(final_source)
    return(url)
  
  
  
  
  def update_geocoding_results_in_DB(self, out_folder, text):
    print("in update_geocoding_results_in_DB()")
    filepath = os.path.join(out_folder, "geonames_results_in_batch" + "." + consts.FILE_FORMAT_CSV)
    df_batch_geonames_results = pd.read_csv(filepath, sep=";", keep_default_na=False)
    results = {} # each key corresponds to a column in the future file
    for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST):
      results[str(i)] = []
    results["text"] = []
    locs = geocode_raw_with_geonames(text)
    print("locs:", locs)
    if locs is not None and len(locs)>0:
      for i in range(len(locs)):
        results[str(i)].append(json.dumps(locs[i], ensure_ascii=False))
      for i in range(len(locs), consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST):
        results[str(i)].append(json.dumps("-1"))
      results["text"].append(text)
      df_curr = pd.DataFrame(results)
      df_batch_geonames_results = df_batch_geonames_results.append(df_curr, ignore_index=True)
      
      df_batch_geonames_results.to_csv(filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    else:
      print("NOT FOUND for: ", text)
      
    df_batch_geonames_results = pd.read_csv(filepath, sep=";", keep_default_na=False)
    inverted_indices_for_df_batch_geonames_results = dict(zip(df_batch_geonames_results["text"], df_batch_geonames_results.index))
    return(df_batch_geonames_results, inverted_indices_for_df_batch_geonames_results)



  def update_geonames_hierarhcy_results_in_DB(self, out_folder, geonameId):
    print(out_folder)
    geonames_hierarchy_filepath = os.path.join(out_folder, \
                            consts.GEONAMES_HIERARCHY_INFO_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_geonames_hierarcy = [consts.COL_GEONAMES_ID, "hierarchy_name", "hierarchy_geoname_id"] 
    df_geonames_hierarchy = pd.read_csv(geonames_hierarchy_filepath, usecols=cols_geonames_hierarcy, sep=";", keep_default_na=False)
    
    hierarchy_result = get_loc_hierarchy(geonameId)
    df_curr = pd.DataFrame({consts.COL_GEONAMES_ID: [geonameId], \
                            "hierarchy_name": [str(hierarchy_result["name"])], \
                            "hierarchy_geoname_id": [str(hierarchy_result["geoname_id"])]
                          })
    df_geonames_hierarchy = df_geonames_hierarchy.append(df_curr, ignore_index=True)
  
    df_geonames_hierarchy.to_csv(geonames_hierarchy_filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
      
    df_batch_geonames_results = pd.read_csv(geonames_hierarchy_filepath, sep=";", keep_default_na=False)
    geonames_hierarchy_ass = dict(zip(df_geonames_hierarchy[consts.COL_GEONAMES_ID], df_geonames_hierarchy["hierarchy_geoname_id"]))
    return(df_batch_geonames_results, geonames_hierarchy_ass)  


  # the aim of this method is just to populate 'geonames_results_in_batch' file for admin1 names
  def search_geonames_for_admin1_loc_list(self, output_folder, df_events):
    filepath = os.path.join(output_folder, "geonames_results_in_batch" + "." + consts.FILE_FORMAT_CSV)
    print(filepath)
    if not os.path.exists(filepath):
      # create an empty file 
      columns = [str(i) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] + ["text"]
      df = pd.DataFrame(columns=columns)
      df.to_csv(filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    #
    df_batch_geonames_results = pd.read_csv(filepath, sep=";", keep_default_na=False)
    inverted_indices_for_df_batch_geonames_results = dict(zip(df_batch_geonames_results["text"], df_batch_geonames_results.index))


    filepath = os.path.join(output_folder, consts.GEONAMES_HIERARCHY_INFO_FILENAME + "." + consts.FILE_FORMAT_CSV)
    if not os.path.exists(filepath):
      # create an empty file 
      columns = [consts.COL_GEONAMES_ID, "hierarchy_name", "hierarchy_geoname_id"] 
      df = pd.DataFrame(columns=columns)
      df.to_csv(filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    #
    df_geonames_hierarchy = pd.read_csv(filepath, sep=";", keep_default_na=False)
    geonames_hierarchy_ass = dict(zip(df_geonames_hierarchy[consts.COL_GEONAMES_ID], df_geonames_hierarchy["hierarchy_geoname_id"]))

    # ----------------------------
    location_list = []
    for index, row in df_events.iterrows():
      print("----", str(index)+"/"+str(df_events.shape[0]), row["city"] + ", " + row["region"] + ", " + row["country"])
      region_name = row["region"] + ", " + row["country"]
      if row["region"] == "":
        region_name = row["country"]
      
      #print("---------------", index, place_name)
      res = {"geonamesId": "-1", "name": "-1", "country_code": "-1", "raw_data": "-1"}
      # ==================================================
      # retrieve the results from our database
      if region_name not in inverted_indices_for_df_batch_geonames_results:
        print("girdi")
        df_batch, inverted_indices = self.update_geocoding_results_in_DB(output_folder, region_name)
        df_batch_geonames_results = df_batch
        inverted_indices_for_df_batch_geonames_results = inverted_indices


  def retrieve_loc_list(self, output_folder, df_events):
    filepath = os.path.join(output_folder, "geonames_results_in_batch" + "." + consts.FILE_FORMAT_CSV)
    print(filepath)
    if not os.path.exists(filepath):
      # create an empty file 
      columns = [str(i) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] + ["text"]
      df = pd.DataFrame(columns=columns)
      df.to_csv(filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    #
    df_batch_geonames_results = pd.read_csv(filepath, sep=";", keep_default_na=False)
    inverted_indices_for_df_batch_geonames_results = dict(zip(df_batch_geonames_results["text"], df_batch_geonames_results.index))


    filepath = os.path.join(output_folder, consts.GEONAMES_HIERARCHY_INFO_FILENAME + "." + consts.FILE_FORMAT_CSV)
    print(filepath)
    if not os.path.exists(filepath):
      # create an empty file 
      columns = [consts.COL_GEONAMES_ID, "hierarchy_name", "hierarchy_geoname_id"] 
      df = pd.DataFrame(columns=columns)
      df.to_csv(filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    #
    df_geonames_hierarchy = pd.read_csv(filepath, sep=";", keep_default_na=False)
    geonames_hierarchy_ass = dict(zip(df_geonames_hierarchy[consts.COL_GEONAMES_ID], df_geonames_hierarchy["hierarchy_geoname_id"]))

    # ----------------------------
    location_list = []
    for index, row in df_events.iterrows():
      print("----", str(index)+"/"+str(df_events.shape[0]), row["city"] + ", " + row["region"] + ", " + row["country"])
      region_name = ""
      if row["region"] != "":
        region_name = row["region"] + ", " + row["country"]
        region_gindx = inverted_indices_for_df_batch_geonames_results[region_name]
        row_region_geonames_results = df_batch_geonames_results.iloc[region_gindx]
        geonames_results = [json.loads(row_region_geonames_results[str(i)]) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] # we access the columns
        region_geoname_json = geonames_results[0] # get the first result from geonames
        region_geoname = region_geoname_json["adminName1"]
      # ----------------------------------------------------
      place_name = row["city"] + ", " + row["country"]
      if row["city"] == "" and region_name != "":
        place_name = region_name
      elif row["city"] == "" and region_name == "":
        place_name = row["country"]
      
      #print("---------------", index, place_name)
      res = {"geonamesId": "-1", "name": "-1", "country_code": "-1", "raw_data": "-1"}
      # ==================================================
      # retrieve the results from our database
      if place_name not in inverted_indices_for_df_batch_geonames_results:
        print("girdi for:", place_name)
        df_batch, inverted_indices = self.update_geocoding_results_in_DB(output_folder, place_name)
        df_batch_geonames_results = df_batch
        inverted_indices_for_df_batch_geonames_results = inverted_indices
        
        
      gindx = inverted_indices_for_df_batch_geonames_results[place_name]
      row_geonames_results = df_batch_geonames_results.iloc[gindx]
      # print(row_geonames_results["0"])
      # {"adminCode1": "00", "lng": "34.75", "geonameId": 294640, "toponymName": "State of Israel", "countryId": "294640", "fcl": "A", "population": 8883800, "countryCode": "IL", "name": "Israel", "fclName": "country, state, region,...", "countryName": "Israel", "fcodeName": "independent political entity", "adminName1": "", "lat": "31.5", "fcode": "PCLI"}

      
      loc = location.Location(place_name, -1, -1, -1, -1, -1, -1, {})
      if row_geonames_results["0"] == "-1":
        print("Error in geonames with place name=",place_name)
      if row_geonames_results["0"] != "-1":
        print(row_geonames_results["0"])
        geonames_results = [json.loads(row_geonames_results[str(i)]) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] # we access the columns
        # --------------------------------------------
        # get the geoname results with the correct admin1 id
        geoname_json = geonames_results[0] # get the first result from geonames # default
        for geoname_json_cand in geonames_results:
          if geoname_json_cand != "-1" and row["city"] != "" and geoname_json_cand["adminName1"] == region_geoname: # if row["city"] == "", then we take the first json result
            geoname_json = geoname_json_cand
            break
        # --------------------------------------------
        
        if "countryCode" in geoname_json: # it is not a continent or sea
          geoname_id = geoname_json["geonameId"]
          # ---------------------------------------
          if geoname_id not in geonames_hierarchy_ass:
              df_geonames_hierarchy, geonames_hierarchy_ass = \
                self.update_geonames_hierarhcy_results_in_DB(output_folder, geoname_id)
          #print("DB ACCESS!!!!")
          hierarchy_data = eval(geonames_hierarchy_ass[geoname_id])
          # ---------------------------------------
          name = geoname_json["toponymName"]
          lat = geoname_json["lat"]
          lng = geoname_json["lng"]
          fcode = geoname_json["fcode"]
          country_code_alpha2 = geoname_json["countryCode"]
          country_code_alpha3 = countries.get(country_code_alpha2).alpha3
          continent = retrieve_continent_from_country_code(country_code_alpha3)
          loc = location.Location(name, geoname_id, geoname_json, lat, lng, country_code_alpha3, continent, hierarchy_data)
      
      location_list.append(loc)
    return(location_list)
  

  @abstractmethod
  def perform_preprocessing(self):
    pass











class PreprocessingPadiweb(AbstractPreprocessing):
  
  
  def perform_preprocessing(self, disease_name, input_folder, out_csv_folder, data_folder):
    print("...... beginning of preprocessing for padiweb !")
    self.disease_name = disease_name
    self.input_folder = input_folder
    self.out_csv_folder = out_csv_folder
    self.data_folder = data_folder
    
    self.read_csv_files_as_dataframe()
    
    self.copy_csv_files_from_input()

    # ===========================================================================================
    # PART 1.2: Enhancing and preprocessing the following information:
    #            - URL
    #            - media source name
    #            - publication date
    # ===========================================================================================      
      
    # retrieve source names from the corresponding urls, and overwrite the csv event file
    source_names = [self.trim_url_custom(url) for url in self.df_articles[consts.COL_URL]]
    self.df_articles[consts.COL_SOURCE] = source_names
    articles_filepath = os.path.join(self.out_csv_folder, \
                       consts.PADIWEB_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    
    self.df_articles["p_date"] = self.df_articles[consts.COL_PUBLISHED_TIME].apply(parser.parse)
    self.df_articles["p_date2"] = self.df_articles["p_date"].apply(lambda x: str(x.day)+"/"+str(x.month)+"/"+str(x.year))
    self.df_articles.groupby([consts.COL_TITLE, "p_date2"])[consts.COL_ID, consts.COL_URL, consts.COL_SOURCE, consts.COL_PUBLISHED_TIME, \
                     consts.COL_TEXT, consts.COL_PROCESSED_TEXT, consts.COL_LANG].aggregate(lambda x: list(x)[0])
    
    self.df_articles["source_td"] = self.df_articles["source"].apply(lambda x: extract(x)[1])
    self.df_articles.loc[self.df_articles["source_td"] == "www", "source_td"] = self.df_articles.loc[self.df_articles["source_td"] == "www", "source"]
    self.df_articles.loc[self.df_articles["source_td"] == "gov.uk", "source_td"] = self.df_articles.loc[self.df_articles["source_td"] == "gov.uk", "source"]
    self.df_articles.loc[self.df_articles["source_td"] == "gov.ie", "source_td"] = self.df_articles.loc[self.df_articles["source_td"] == "gov.ie", "source"]
    self.df_articles.loc[self.df_articles["source_td"] == "gov.scot", "source_td"] = self.df_articles.loc[self.df_articles["source_td"] == "gov.scot", "source"]

    # ===========================================================================================
    # PART 1.3: Remove duplicates based on:
    #            - title
    #            - url base
    #            - publication date
    # ===========================================================================================
    
    df_groupby = self.df_articles.groupby([consts.COL_TITLE, "source_td", "p_date2"])[consts.COL_ID, consts.COL_URL, consts.COL_SOURCE, consts.COL_PUBLISHED_TIME, \
                     consts.COL_TEXT, consts.COL_PROCESSED_TEXT, consts.COL_LANG].aggregate(lambda x: list(x)[0])
                     
    df_groupby.to_csv(articles_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    
    
    
    
    
    # ===========================================================================================
    # PART 2: We use the following dataset to retrieve the country information regarding media sources
    #          publishing news articles: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/4F7TZW
    #        In this dataset, if I remember correctly, it basically lists the national news sources, 
    #          but there is no distinction between regional/local or national. 
    # ===========================================================================================

    self.read_csv_files_as_dataframe() # reload them, especially for articles
    
    
    news_source_url_list = self.df_articles[consts.COL_URL].to_numpy()
    out_filepath = os.path.join(self.out_csv_folder, "media_source_resolution" + "." + consts.FILE_FORMAT_CSV)
    df_media_source = perform_media_source_resolution(news_source_url_list, self.data_folder)
    df_media_source[consts.COL_ARTICLE_ID] = self.df_articles[consts.COL_ID]
    df_media_source.to_csv(out_filepath, index=False, quoting=csv.QUOTE_NONNUMERIC)
    
    
    
    # ===========================================================================================
    # PART 3: Extract spatial entities from the titles and perform geocoding.
    #          These spatial entities will serve as an initial bias 
    #          for correctly geocoding difficult spatial entities found in the main text.
    #          In the end, it produced the file "title_locations_final_with_geonames.csv". 
    #          This file has the following columns:
    #          - article_id
    #          - media_source_country_code
    #          - country_bias
    #          - final_value
    #          - final_geonames_json
    #            (Note that there might be multiple spatial entities in the same title)
    # ===========================================================================================
    
    self.retrieve_location_info_from_title()
    
    self.retrieve_location_info_from_title_with_knowledge_base()
    
    self.apply_gecoding_for_title_locations()
    
    self.deduce_from_title_geocoding_results()
    
    
    
    # ===========================================================================================
    # PART 4: Add local sentence id into "extended_sentences_with_labels.csv"
    #        Normally, PADI-web uses gloabl sentence ids. But, it is also useful to to know 
    #          the first sentence of a given article. This is why we extract the local ids
    # ===========================================================================================
        
    a_ids = self.df_sentences[consts.COL_ARTICLE_ID].to_numpy()
    nb_row = self.df_sentences.shape[0]
    article_ending_sentence_indexes = {}
    for i in range(0,nb_row):
      a_id = a_ids[i]
      if a_id not in article_ending_sentence_indexes:
        article_ending_sentence_indexes[a_id] = i # i: starting index
    
    article_starting_sentence_indexes = {}
    for i in range(0,nb_row):
      a_id = a_ids[i]
      article_starting_sentence_indexes[a_id] = i # i: ending index       
    
    self.df_sentences["local_sentence_id"] = -1
    unique_a_ids = np.unique(a_ids)
    for a_id in unique_a_ids:
      start_index = article_starting_sentence_indexes[a_id]
      end_index = article_ending_sentence_indexes[a_id]
      nb_sents = start_index-end_index+1 # they are in desc order
      print(a_id, start_index, end_index, nb_sents)
      self.df_sentences.iloc[end_index:(start_index+1)]["local_sentence_id"] = range(nb_sents) # range(nb_sents-1, -1, -1) # decreasing order, until we reach 0 index.

    self.df_sentences = self.df_sentences.sort_values(by=[consts.COL_ARTICLE_ID, "local_sentence_id"], ascending=True)
    
    filepath = os.path.join(self.out_csv_folder, \
                          consts.PADIWEB_EXT_SENTENCES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    self.df_sentences.to_csv(filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC) # TODO not used in the rest of the code
    
    
    # after the 'groupby' operation, I would not find a solution to access the desired column
    # as a workaround, I reread the dataframe.
    ##df_sentences_agg = pd.read_csv(filepath, sep=";", keep_default_na=False)
    self.sentence_id_to_local_id_ass = dict(zip(self.df_sentences[consts.COL_ID], self.df_sentences["local_sentence_id"]))
 
 
 
     
    # ===========================================================================================
    # PART 5.1: 1) Create "inverted_article_indices_for_sentences"
    #         2) It extends the initial "extracted_information.csv" file with these information:
    #            - 
    # ===========================================================================================
    
    nb_row = self.df_sentences.shape[0]
    a_ids = self.df_sentences[consts.COL_ARTICLE_ID].to_numpy()
    
    # inverted indexes for sentences: this is before filtering
    # because the sentences with ids != 12 can have some additional information 
    self.inverted_article_indices_for_sentences = {}
    for i in range(0,nb_row):
      a_id = a_ids[i]
      if a_id not in self.inverted_article_indices_for_sentences:
        self.inverted_article_indices_for_sentences[a_id] = i # i: starting index
    
    print(np.unique(a_ids).shape)    
    # >>> S_agg_reverted_indices
    # {'DEOL28QKIP': 0, 'MO5GLFVP9X': 2, '8QSAIG2LAH': 8, 'P068SXF7LZ': 14, 'M9Y31PW9YJ': 19, '8Q1P443EDC': 43, 'R9K3R99XTF': 63, 'PONHFDNH5G': 67, 'M04B9PUD06': 79, 'QLYNYYQOFH': 97, 'G7QZQ1KAN0': 105, '9IXRWPVJR1': 115, 'XJ90HGTPYT': 121, 'JQG87NSQ6Q': 134, 'NDUJ0T87P2': 147, 'I0MN9I2TR8': 160, 'D59DLD0ODO': 174, ... }
    
    
    self.extend_info_extr_with_sentence_id_and_geoanmes_and_classif_label()
    
    
    
    
    # ===========================================================================================
    # PART 5.2: Keep only relevant entities/keywords of relevant sentences from relevant articles
    #          In the end, it creates the file 'extended_extracted_information.csv'
    #            - sentence global id
    #            - sentence local id
    #            - sentence classification id
    #            - feature code
    # ===========================================================================================
    
    # scenario: the classification label of a given sentence is not "11 12", but it contains outbreak related loc 
    df_ext_info_outbreaks = self.df_info_extr[self.df_info_extr["type"] == "outbreak-related-location"]
    # oubtreaks_sentence_ids_11_12 = df_ext_info_outbreaks[df_ext_info_outbreaks[consts.COL_SENTENCE_CLASSIF_LABEL_ID] == "11 12"]
    oubtreaks_sentence_ids_other = df_ext_info_outbreaks[df_ext_info_outbreaks[consts.COL_SENTENCE_CLASSIF_LABEL_ID] != "11 12"]
    sentence_ids_to_adjust = oubtreaks_sentence_ids_other[consts.COL_SENTENCE_ID].to_list()
    #
    self.df_info_extr.loc[self.df_info_extr[consts.COL_SENTENCE_ID].isin(sentence_ids_to_adjust), [consts.COL_SENTENCE_CLASSIF_LABEL_ID]] = "11 12"
    #
    self.df_sentences.loc[self.df_sentences[consts.COL_ID].isin(sentence_ids_to_adjust), [consts.COL_SENTENCE_CLASSIF_LABEL_ID]] = "11 12"
    
    # filter aggregated sentences by classification label
    drop_vals = [k for k in self.df_sentences.index \
                 if str(consts.CLASSIF_LABEL_CURR_EVENT_ID)+" "+str(consts.CLASSIF_LABEL_DESCR_EPID_ID) \
                  != self.df_sentences.loc[k, consts.COL_SENTENCE_CLASSIF_LABEL_ID]]
    df_sentences_filtered = self.df_sentences.drop(drop_vals)
    filepath = os.path.join(self.out_csv_folder, \
                          consts.PADIWEB_RELEVANT_SENTENCES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    df_sentences_filtered.to_csv(filepath, index=True, sep=";", quoting=csv.QUOTE_NONNUMERIC) # TODO not used in the rest of the code
    
    
    filepath = os.path.join(self.out_csv_folder, consts.PADIWEB_EXT_INFO_EXTR_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    self.df_info_extr.to_csv(filepath, index=True, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    

    
    # ===========================================================================================
    # PART 6: Geocoding spatial entities in the main text of the articles 
    #            (with the help of the geocoded spatial entities found in the titles)
    #          In the end, it produces the file "extr_spatial_entities_info.csv", which has these information:
    #          - id_articleweb
    #          - local_sentence_id
    #          - value (i.e. spatial entity final name)
    #          - geonames_id
    #          - geonames_json
    #          - updated: if my geocoding method changed the GeoNames result provided by PADI-web. 1 if updated
    #          - prev_value (i.e. spatial entity previous name, as provided by PADI-web)
    #          - prev_geonames_json
    #          - ref_countries_for_geocoding
    #          - country_code
    # ===========================================================================================
  
    self.process_spatial_entities_in_sentences_for_all_articles()
    
    # find the hierarchical information of the geocoded spatial entities found in the previous method
    self.extract_geonames_hierarchy_info_for_all_spatial_entities()
    

    print("...... end of preprocessing for padiweb !")






  def retrieve_in_batch_geocoding_results_for_all_articles(self):
    filepath = os.path.join(self.out_csv_folder, consts.PADIWEB_EXT_INFO_EXTR_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    df_ext_info_extr = pd.read_csv(filepath, sep=";", keep_default_na=False)
    # df_ext_info_extr[consts.COL_ARTICLE_CLASSIF_LABEL_ID] = df_ext_info_extr[consts.COL_ARTICLE_CLASSIF_LABEL_ID].replace("", "[]")
    # df_ext_info_extr = df_ext_info_extr[df_ext_info_extr[consts.COL_ARTICLE_CLASSIF_LABEL_ID] != '[]']
    # df_ext_info_extr[consts.COL_ARTICLE_CLASSIF_LABEL_ID] = [eval(x) for x in df_ext_info_extr[consts.COL_ARTICLE_CLASSIF_LABEL_ID]]
    # filter aggregated sentences by classif label
    drop_vals = [k for k in df_ext_info_extr.index \
                 if str(consts.CLASSIF_LABEL_CURR_EVENT_ID)+" "+str(consts.CLASSIF_LABEL_DESCR_EPID_ID)\
                  not in df_ext_info_extr.loc[k, consts.COL_SENTENCE_CLASSIF_LABEL_ID]]
    df_ext_info_extr_only_outbreak = df_ext_info_extr.drop(drop_vals)
    
    spatial_entities_list = []
    article_id_list = np.unique(df_ext_info_extr_only_outbreak[consts.COL_ARTICLE_ID_RENAMED].to_numpy())
    print("in total, there are " + str(len(article_id_list)) + " articles.")
    for article_id in article_id_list:
      print("----------- ", article_id)
      df_ext_info_by_article = df_ext_info_extr_only_outbreak[df_ext_info_extr_only_outbreak[consts.COL_ARTICLE_ID_RENAMED] == article_id]
      
      df_spatial_entities = df_ext_info_by_article[~((df_ext_info_by_article["geonames_id"] == 'NULL') | (df_ext_info_by_article["geonames_id"] == '-1'))]
      df_spatial_entities = df_spatial_entities[df_spatial_entities["type"] == "outbreak-related-location"]
      df_spatial_entities["updated"] = 0
      df_spatial_entities["prev_geonames_json"] = -1
      df_spatial_entities["ref_countries_for_geocoding"] = -1
      df_country = df_spatial_entities[df_spatial_entities["feature_code"] == "PCLI"]
      df_other = df_spatial_entities[(df_spatial_entities["feature_code"] != "PCLI") & (df_spatial_entities["feature_code"] != "CONT")]
      spatial_entities_list.extend(df_other["value"].to_list() )
    #
    spatial_entities_list = np.unique(spatial_entities_list)
    
    print("in total, there are " + str(len(spatial_entities_list)) + " spatial entities.")
    results = {} # each key corresponds to a column in the future file
    for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST):
      results[i] = []
    results["text"] = []
    for i in range(len(spatial_entities_list)):
      text = spatial_entities_list[i]
      locs = geocode_raw_with_geonames(text)
      if locs is not None:
        for i in range(len(locs)):
          results[i].append(json.loads(locs[i].raw))
        for i in range(len(locs), consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST):
          results[i].append("-1")
        results["text"].append(text)
    df = pd.DataFrame(results)
    filepath = os.path.join(self.out_csv_folder, "geonames_results_in_batch" + "." + consts.FILE_FORMAT_CSV)
    df.to_csv(filepath, index=True, sep=";", quoting=csv.QUOTE_NONNUMERIC)




  # There are 2 possibilities:
  #  1) we process the abbreviation
  #  2) if convert_spatial_entity_abbreviation_into_name != "-1", on top of step 1, we also go a step further and converts the abbreviation into a name
  #
  # In any case, we treat four abbreviation cases:
  # 1) if all the letters are in uppercase (from 2 to 4 letters)
  # 2) the text with dots, e.g. U.K or U.S.A. etc.
  # 3) the first letter is in uppercase and the second and third ones (from 2 to 3 letters) are in lowercase, e.g. Fla >> Floria, Ill >> Illinois
  # 4) the text ended with a dot, e.g., Minn. >> Minnesota
  def process_spatial_entity_abbreviation(self, text, country_code_alpha2="-1"):
    # The reason why we do not treat multi words, event though there can be an abbreviation
    # good example: LA country
    # bad example: St. Louis
    if text.lower() in consts.BAN_LIST_FOR_SPATIAL_ENTITY_EXTRACTION: # source: https://www.dictionary.com/e/dr-vs-phd-vs-md/
      return text
    
    init_text = text 
    rgx = "^([A-Z][.])+" # e.g., turn "U.K." into "UK"
    if re.sub(rgx, "", text) == "" and len(text)<=8: #1 letter + 1 dot, for 4 times
      text = text.replace(".", "")
    rgx = "^([A-Z][ ])+[A-Z]$" # e.g., turn "U K" or "U S A" into "UK"
    if re.sub(rgx, "", text) == "":
      text = text.replace(" ", "")
    rgx = "^[A-Z]&[A-Z]$" # e.g., turn "U&K" into "UK"
    if re.sub(rgx, "", text) == "":
      text = text.replace("&", "")
    rgx = "^[A-Z]-[A-Z]$" # e.g., turn "U-K" into "UK"
    if re.sub(rgx, "", text) == "":
      text = text.replace("-", "")
    # rgx = "^[A-Z][a-z]{1,2}$" # e.g., turn "Pa" into "PA"
    # if re.sub(rgx, "", text) == "" and text.lower() not in consts.BAN_LIST_FOR_SPATIAL_ENTITY_EXTRACTION:
    #   text = text.upper()
    rgx = "^[A-Z][a-z]{1,3}[.]{1,1}$" # e.g., turn "Pa." into "PA"
    if re.sub(rgx, "", text) == "":
      text = text.upper().replace(".", "")
    
    #print(init_text, text, country_code_alpha2)
    process_nominatim = False
    if init_text == init_text.upper() and len(init_text)<=4:
      process_nominatim = True
    elif init_text != init_text.upper() and init_text != text and country_code_alpha2 != "-1":
      process_nominatim = True
      
    if process_nominatim:
      time.sleep(1)
      # use Nominatim to get entity names (based on my observations, Nominatim outperforms Geonames and Arcgis geocoders)
      client_nominatim = Nominatim(user_agent="geoapi")
      #client_nominatim = Nominatim(user_agent="arinik9")
      nominatim_locations = client_nominatim.geocode(text, exactly_one=False, addressdetails=True, language="en", timeout=20)
      if nominatim_locations is None:
        print("None for ", text)
      if nominatim_locations is not None:
        if len(nominatim_locations)>5: # limit the search with 5 results
          nominatim_locations = nominatim_locations[0:5]
        for loc in nominatim_locations:
          if loc.raw["address"]["country_code"].upper() == country_code_alpha2:
            text = loc.raw['display_name'].split(", ")[0]
            #print(text)
            return text
      return init_text
    return text
  


  def process_spatial_entities_in_sentences_for_all_articles(self):
    filepath = os.path.join(self.out_csv_folder, "title_locations_final_with_geonames" \
                                           + "." + consts.FILE_FORMAT_CSV)
    df_title_geocoding_results = pd.read_csv(filepath, sep=";", keep_default_na=False)
    
    filepath = os.path.join(self.out_csv_folder, "media_source_resolution" + "." + consts.FILE_FORMAT_CSV)
    df_media_source = pd.read_csv(filepath, sep=",", keep_default_na=False)
    #source_to_country_assoc = dict(zip(df_media_source["url"].str.replace("www.",""), df_media_source["pub_country"]))
    self.article_id_to_source_country_code_assoc = dict(zip(df_media_source[consts.COL_ARTICLE_ID], df_media_source["pub_country_code"]))    
    
    self.article_id_to_text_assoc = dict(zip(self.df_articles[consts.COL_ID], self.df_articles[consts.COL_TEXT]))    

    #self.sentence_id_to_article_id = dict(zip(self.df_sentences[consts.COL_ID], self.df_sentences[consts.COL_ARTICLE_ID]))
    self.sentence_id_to_text = dict(zip(self.df_sentences[consts.COL_ID], self.df_sentences[consts.COL_TEXT]))
    #self.article_id_to_title = dict(zip(self.df_articles[consts.COL_ID], self.df_articles[consts.COL_TITLE]))
    #self.sentence_id_to_local_sentence_id = dict(zip(self.df_sentences[consts.COL_ID], self.df_sentences["local_sentence_id"]))



    
    filepath = os.path.join(self.out_csv_folder, consts.PADIWEB_EXT_INFO_EXTR_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    df_ext_info_extr = pd.read_csv(filepath, sep=";", keep_default_na=False)
    # df_ext_info_extr[consts.COL_ARTICLE_CLASSIF_LABEL_ID] = df_ext_info_extr[consts.COL_ARTICLE_CLASSIF_LABEL_ID].replace("", "[]")
    # df_ext_info_extr = df_ext_info_extr[df_ext_info_extr[consts.COL_CLASSIF_LABEL_ID] != '[]']
    # df_ext_info_extr[consts.COL_CLASSIF_LABEL_ID] = [eval(x) for x in df_ext_info_extr[consts.COL_CLASSIF_LABEL_ID]]
    # filter aggregated sentences by classif label
    drop_vals = [k for k in df_ext_info_extr.index \
                 if str(consts.CLASSIF_LABEL_CURR_EVENT_ID)+" "+str(consts.CLASSIF_LABEL_DESCR_EPID_ID)\
                  != df_ext_info_extr.loc[k, consts.COL_SENTENCE_CLASSIF_LABEL_ID]]
    df_ext_info_extr_only_outbreak = df_ext_info_extr.drop(drop_vals)
    
    filepath = os.path.join(self.out_csv_folder, "relevant_extended_extracted_information" + "." + consts.FILE_FORMAT_CSV)
    df_ext_info_extr_only_outbreak.to_csv(filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    
    #df_sentence_spatial_entities = df_ext_info_extr_only_outbreak[~((df_ext_info_extr_only_outbreak["geonames_id"] == 'NULL') | (df_ext_info_extr_only_outbreak["geonames_id"] == '-1'))]
    #print(df_sentence_spatial_entities["geonames_id"])
    
    filepath = os.path.join(self.out_csv_folder, "geonames_results_in_batch" + "." + consts.FILE_FORMAT_CSV)
    self.df_batch_geonames_results = pd.read_csv(filepath, sep=";", keep_default_na=False)
    self.inverted_indices_for_df_batch_geonames_results = dict(zip(self.df_batch_geonames_results["text"], self.df_batch_geonames_results.index))
    
    
    # ----------------------------------------------------------------------------
    
    ##### df_sentence_spatial_entities = df_ext_info_extr_only_outbreak[~((df_ext_info_extr_only_outbreak["geonames_id"] == 'NULL') | (df_ext_info_extr_only_outbreak["geonames_id"] == '-1'))]


    df_list = []

    article_id_list = np.unique(df_ext_info_extr_only_outbreak[consts.COL_ARTICLE_ID_RENAMED].to_numpy())
    # avian influenza specific article ids
    #article_id_list = ["3UFCKR6BOV", "RA419Z60HU", "7QZJNVLHTS","L7CG25TT3N", "RA419Z60HU", "JMO1JRJDJW", "5PF9FM6U2Q", "MDQA110VY0", "4SJJ0Y1X0D", "WAYOPZHA5J", "03WD3TAW5C", "02TL7DYIWC", "3UFCKR6BOV", "3TB9FO1ENQ", "3Z3PDHGCJX", "7V4FWGZU0D", "3SJZPBCWT8"]
    # west nile specific article ids
    #article_id_list = ["KYW5N3E75G", "1SPERC4WN6", "1SXLMRP77B", "2C6ITS4U0O", "2KXMGAV0UH", "3WL8NSPWZC", "4VR9F44JTM", "6BJ46LALGH", "6BLBA7NUGW", "8DIUIT7I4X", "97S27F2LGU", "CCJZ7ZR4GG", "BKO2QPN8XN", "D4WCIUFQ5P", "DW9NS0ZRPL", "FDUPJDMXBM", "G1FNCLNMLF", "GN026JLHE3", "GOC7W8PWAY", "JFRJW5MVYN", "KIJ81LWVBO", "MDMW68AOF5", "NE32DE5J4X", "P0WZ0UZZPZ", "RMC98UXBMP", "V4E3TKDJKM", "VU805K23Y6", "X8DZJUPSX0", "Z4JJD8RFHJ", "ZAP5DFBPEG", "ZSR2QTMK2O"]
    for indx, article_id in enumerate(article_id_list):
      print("----------- ", indx, article_id)
      
      df_ext_info_by_article = df_ext_info_extr_only_outbreak[df_ext_info_extr_only_outbreak[consts.COL_ARTICLE_ID_RENAMED] == article_id]
      print(df_ext_info_by_article.shape)
      
      df_title_geocoding_results_by_article = df_title_geocoding_results[df_title_geocoding_results["article_id"] == article_id]
      df_title_geocoding_results_by_article = df_title_geocoding_results_by_article.drop_duplicates(subset=['final_value'])
      
      df_title_geocoding_results_by_article = df_title_geocoding_results_by_article[df_title_geocoding_results_by_article["final_geonames_json"] != json.dumps("-1", ensure_ascii=False)]
      # if there is any valid geocoding/JSON info
      if df_title_geocoding_results_by_article.shape[0]>0:
        # if df_title_country_by_article.shape[0]<=1 and df_title_country_by_article.shape[0]>0:
        df_spatial_entities_by_article = self.process_spatial_entities_in_sentences_by_article(article_id, \
                                                   df_ext_info_by_article, df_title_geocoding_results_by_article)
        df_list.append(df_spatial_entities_by_article)
        #print(df_spatial_entities_by_article)
      
    df_all_spatial_entities = pd.concat(df_list)
    filepath = os.path.join(self.out_csv_folder, "extr_spatial_entities_info" + "." + consts.FILE_FORMAT_CSV)
    df_all_spatial_entities.to_csv(filepath, index=True, sep=";", quoting=csv.QUOTE_NONNUMERIC)









  # it returns a list of geonames_ids and a list of geonames data
  # test these padiweb articles:
  #  W0ABEZRTYZ
  #  W18BGBUEDV
  #  W58ED3YOXR
  #  WB9JINONGI
  #  WLU21U85VO
  #  WU18S45M79 >> good
  #  XOR6MDO3WW >> good
  #  Y7LP0XDJEV >> good
  #  DCS19Q8BAR >> good
  #
  # note that the sentences in 'df_ext_info_by_article' are already ordered from the beginning to the end 
  def process_spatial_entities_in_sentences_by_article(self, article_id, df_ext_info_by_article,\
                                                       df_title_geocoding_results_by_article):
    sentence_ids = np.unique(df_ext_info_by_article[consts.COL_SENTENCE_ID].to_numpy())
    local_sentence_ids = np.unique(df_ext_info_by_article["local_sentence_id"].to_numpy())
    title_geonames_json_list = [json.loads(geo_json) for geo_json in df_title_geocoding_results_by_article["final_geonames_json"].to_list() if geo_json != "-1"]
    title_country_codes = [geo_json["countryCode"] for geo_json in title_geonames_json_list]
    title_names = [value for value in df_title_geocoding_results_by_article["final_value"]]
    #title_geonames_id_list = [geo_json["geonameId"] for geo_json in title_geonames_json_list]

    media_source_spatial_entity_entry = []
    media_source_country_code_alpha3 = self.article_id_to_source_country_code_assoc[article_id]
    if media_source_country_code_alpha3 != '-1':
      media_source_country_code_alpha2 = countries.get(media_source_country_code_alpha3).alpha2
      media_source_country_name = countries.get(media_source_country_code_alpha3).apolitical_name
      if media_source_country_code_alpha3 == "KOR":
        media_source_country_name = "South Korea"
      if media_source_country_code_alpha3 == "GBR":
        media_source_country_name = "United Kingdom"
      if media_source_country_code_alpha3 == "RUS":
        media_source_country_name = "Russia"
      if media_source_country_code_alpha3 == "IRN":
        media_source_country_name = "Iran"
      if media_source_country_code_alpha3 == "PRK":
        media_source_country_name = "North Korea"
      media_source_spatial_entity_entry = [0, media_source_country_code_alpha2, media_source_country_name]
      

    #------------------------------------------------------------------
    # columns: id  token_index  type  label  value  id_articleweb   geonames_id  geonames_json  sentence_id  classificationlabel_id  local_sentence_id
    # Note that the sentences in 'df_ext_info_by_article' are already ordered from the beginning to the end
    SLIDING_WINDOW_LENGTH = 4 # 4 previous sentences 
    sliding_window_for_history = [] # e.g. [(local_sentence_id1, country_code1, entity_name1), etc.]
    sliding_window_for_all_history = [] # (for booster) the only difference with 'sliding_window_for_history': we do not delete any items
    for i in range(df_title_geocoding_results_by_article.shape[0]):
      sliding_window_for_history.append((0, title_country_codes[i], title_names[i]))
      sliding_window_for_all_history.append((0, title_country_codes[i], title_names[i]))
    if len(sliding_window_for_history) == 0 and len(media_source_spatial_entity_entry)>0:
      sliding_window_for_history.append(tuple(media_source_spatial_entity_entry))
      sliding_window_for_all_history.append(tuple(media_source_spatial_entity_entry))
      
    df_list = []
    for indx in range(len(local_sentence_ids)):
      local_sentence_id = local_sentence_ids[indx]
      sentence_id = sentence_ids[indx]
      
      # remove the old entries based on time limit
      sliding_window_for_history = [entry for entry in sliding_window_for_history if (local_sentence_id-entry[0])<=SLIDING_WINDOW_LENGTH]

      # =================================================
      # booster step: when avian influenza or west nile related key words appear in some sentences
      if len(sliding_window_for_history) == 0 and len(sliding_window_for_all_history)>0:
        sentence_text = self.sentence_id_to_text[sentence_id]
        disease_info = retrieve_disease_from_raw_sentence(sentence_text, self.disease_name)
        if disease_info is not None: # TODO: we might extent this with: "tested positive", "confirmed"
          sliding_window_for_history = sliding_window_for_all_history
          
      #print(local_sentence_id, sliding_window_for_history)
      df_ext_info_by_sentence = df_ext_info_by_article[df_ext_info_by_article["local_sentence_id"] == local_sentence_id]
      # in the list of spatial entities to check in the next iteration,
      # if we have already seen them (even though they are in the current window),
      # then add the country code into sliding window
      for entity_name in df_ext_info_by_sentence["value"].to_list():
        print(entity_name)
        if entity_name not in [entry[2] for entry in sliding_window_for_history]:
          for entry in sliding_window_for_all_history:
            if entity_name == entry[2]: 
              sliding_window_for_history.append(entry)
      
      df_new_sentence_spatial_entities = self.process_spatial_entities_in_sentence(article_id, local_sentence_id,\
                                     df_ext_info_by_sentence, sliding_window_for_history)
      df_list.append(df_new_sentence_spatial_entities)
      # -------------------------------------------------
      # update sliding_window_for_history
      for index, row in df_new_sentence_spatial_entities.iterrows():
        sliding_window_values = [entry[2] for entry in sliding_window_for_history]
        if row["value"] not in sliding_window_values:
          sliding_window_for_history.append((local_sentence_id, row["country_code"], row["value"]))
          sliding_window_for_all_history.append((local_sentence_id, row["country_code"], row["value"]))
    
    
    df_spatial_entities_by_article = pd.concat(df_list)
    
    # finally, add the title entities, if not included from the news content
    #print(df_spatial_entities_by_article["geonames_id"].to_list())
    for index, row in df_title_geocoding_results_by_article.iterrows():
      name = row["final_value"]
      geonames_json = json.loads(row["final_geonames_json"])
      geonameId = geonames_json["geonameId"]
      country_code_alpha2 = geonames_json["countryCode"]
      country_code_alpha3 = countries.get(country_code_alpha2).alpha3
      feature_code =  geonames_json["fcode"]
      country_bias_alpha3 = row["media_source_country_code"]
      country_bias_alpha2 = '-1'
      if country_bias_alpha3 != '-1':
        country_bias_alpha2 = countries.get(country_bias_alpha3).alpha2
      if str(geonameId) not in df_spatial_entities_by_article["geonames_id"].to_list():
        new_row = pd.DataFrame({"id_articleweb":[article_id], "value": [name], "geonames_id": [geonameId],\
                                "geonames_json": [json.dumps(geonames_json, ensure_ascii=False)], "sentence_id": ['-1'], "token_index": ['-1'],\
                                "local_sentence_id": ['-1'], "country_code": [country_code_alpha3], "type": ['GPE'], \
                                "feature_code": [feature_code], "ref_countries_for_geocoding": [country_bias_alpha2]})
        df_spatial_entities_by_article = df_spatial_entities_by_article.append(new_row)
      
    return(df_spatial_entities_by_article)
  
  

  # TODO you can find nationalities
  def process_spatial_entities_in_sentence(self, article_id, local_sentence_id, df_ext_info_by_sentence, \
                                           sliding_window_for_history):
    df_sentence_spatial_entities = df_ext_info_by_sentence[~((df_ext_info_by_sentence["geonames_id"] == 'NULL') | (df_ext_info_by_sentence["geonames_id"] == '-1'))]
    
    # ----------------------------------------
    # sometimes, geoname contains the key "country_code", sometimes the key "countryCode"
    # if the key "country_code" does not exist, create this key from the key "countryCode"
    geoname_json_list= []
    for index, row in df_sentence_spatial_entities.iterrows():
      geoname_json = json.loads(row["geonames_json"])
      if "country_code" not in geoname_json and "countryCode" in geoname_json:
        geoname_json["country_code"] = geoname_json["countryCode"]
      geoname_json_list.append(json.dumps(geoname_json))
    df_sentence_spatial_entities["geonames_json"] = geoname_json_list
    # ----------------------------------------
    
    drop_vals = [k for k in df_sentence_spatial_entities.index \
             if "country_code" not in json.loads(df_sentence_spatial_entities.loc[k, "geonames_json"])]


    drop_vals = [k for k in df_sentence_spatial_entities.index \
                 if "country_code" not in json.loads(df_sentence_spatial_entities.loc[k, "geonames_json"])]
    # it can be ocean, continent, etc.
    df_sentence_spatial_entities = df_sentence_spatial_entities.drop(drop_vals)
    print(df_sentence_spatial_entities["value"])
    # remove GPEs named "turkey", if often conflicted
    df_sentence_spatial_entities = df_sentence_spatial_entities[df_sentence_spatial_entities["value"] != "turkey"]
    
    df_sentence_spatial_entities["feature_code"] = df_sentence_spatial_entities["geonames_json"].apply(lambda x: json.loads(x)["code"] if "code" in json.loads(x) else json.loads(x)["fcode"])
    df_sentence_spatial_entities = df_sentence_spatial_entities[df_sentence_spatial_entities["type"].isin(["GPE", "outbreak-related-location"])]
    json_country_codes_alpah2 = df_sentence_spatial_entities["geonames_json"].apply(lambda x: json.loads(x)["country_code"])
    df_sentence_spatial_entities["country_code"] = [countries.get(country_code_alpha2).alpha3 for country_code_alpha2 in json_country_codes_alpah2]
    
    df_sentence_spatial_entities = df_sentence_spatial_entities.drop_duplicates(subset=['position', 'geonames_id'])

    df_sentence_spatial_entities["updated"] = 0
    df_sentence_spatial_entities["prev_geonames_json"] = -1
    df_sentence_spatial_entities["ref_countries_for_geocoding"] = -1
    df_sentence_spatial_entities["prev_value"] = df_sentence_spatial_entities["value"]
    
    df_sentence_spatial_entities_copy = df_sentence_spatial_entities.copy()
    print("!!!___!!!", article_id)
    print(df_sentence_spatial_entities_copy)
    
    # ----------------------------------------------
    for index, row in df_sentence_spatial_entities_copy.iterrows():
      text = row["value"]
      text_init = row["value"]
      media_source_country_code_alpha3 = self.article_id_to_source_country_code_assoc[article_id]
      media_source_country_code_alpha2 = "-1"
      if media_source_country_code_alpha3 != "-1":
        media_source_country_code_alpha2 = countries.get(media_source_country_code_alpha3).alpha2
      text = self.process_spatial_entity_abbreviation(text, media_source_country_code_alpha2) # TODO: I am not sure if requiring country code for obtaining the name of a gievn entity is too restrictive.
      print(text_init, "==>", text)
      #if row["value"] != text:
      #  print(row["value"], " ==> ", text)
      if ".." in text:
        rgx = r"(?i)[.]{2,}$" # remove multiple dots in the end
        text = re.sub(rgx, "", text)
      rgx = "(?i)(^the )"
      text = re.sub(rgx, "", text)
      rgx = "(?i)( region)"
      text = re.sub(rgx, "", text)
      rgx = "(?i)(^upper)|(^Upper)|(^lower)|(^Lower)" # >> for Upper and Lower Rhine => it is a complicated situation 
      text = re.sub(rgx, "", text)
      rgx = "(^-)|(-$)|(:$)"
      text = re.sub(rgx, "", text)
      rgx = "(^and )|( and$)"
      text = re.sub(rgx, "", text)
      # if text != text.upper(): # remove a dot point from the end, e.g. Pa. >> we do not treat U.K.
      #   rgx = "(\.$)"
      #   text = re.sub(rgx, "", text)
      text = text.strip()
      #print(text_init, text)
      if text_init != text:
        # print(text, text_init)
        df_sentence_spatial_entities.loc[index, "value"] = text
                # ==================================================
        # retrieve the results from our database
        locs = None
        print(text)
        if text in self.inverted_indices_for_df_batch_geonames_results:
          print("DB ACCESS!!!!")
          gindx = self.inverted_indices_for_df_batch_geonames_results[text]
          row_locs = self.df_batch_geonames_results.iloc[gindx]
          locs = [json.loads(row_locs[str(i)]) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] # we access the columns
        # ==================================================
        else:
          locs = geocode_raw_with_geonames(text)
        if text not in self.inverted_indices_for_df_batch_geonames_results:  # update DB
          df_batch, inverted_indices = self.update_geocoding_results_in_DB(self.out_csv_folder, text)
          self.df_batch_geonames_results = df_batch
          self.inverted_indices_for_df_batch_geonames_results = inverted_indices
        #
        if len(locs) == 0 or "countryCode" not in locs[0]:
          df_sentence_spatial_entities.loc[index, "value"] = "-1" # to remove
        else:
          df_sentence_spatial_entities.loc[index, "feature_code"] = locs[0]["fcode"]
          df_sentence_spatial_entities.loc[index, "geonames_id"] = str(locs[0]["geonameId"])
          df_sentence_spatial_entities.loc[index, "updated"] = 0
          df_sentence_spatial_entities.loc[index, "country_code"] = countries.get(locs[0]["countryCode"]).alpha3
          df_sentence_spatial_entities.loc[index, "value"] = text
          df_sentence_spatial_entities.loc[index, "prev_value"] = text
          df_sentence_spatial_entities.loc[index, "geonames_json"] = json.dumps(locs[0], ensure_ascii=False)
    
    df_sentence_spatial_entities = df_sentence_spatial_entities[df_sentence_spatial_entities["value"] != "-1"]
    print(df_sentence_spatial_entities[["geonames_id", "value", "country_code", "feature_code"]])
    
    # ---------------------------------------
    # first, resolve the general disambiguity
    df_sentence_spatial_entities_copy = df_sentence_spatial_entities.copy()
    media_source_country_code = self.article_id_to_source_country_code_assoc[article_id]
    article_text = self.article_id_to_text_assoc[article_id]
    for index, row in df_sentence_spatial_entities_copy.iterrows():
      spatial_entity = row["value"]
        
      processed, loc_type, country_code_alpha3, fcode = self.resolve_manually_loc_name_ambiguity(spatial_entity, media_source_country_code, article_text)  
      print(spatial_entity, processed, loc_type, country_code_alpha3)
      if processed and loc_type != "continent":
        only_country = False
        country_code_alpha2 = countries.get(country_code_alpha3).alpha2
        country_bias_list = []
        country_limit = None
        text = spatial_entity
        if loc_type != "country":
          country_limit = countries.get(country_code_alpha3).apolitical_name
          if country_code_alpha3 == "KOR":
            country_limit = "South Korea"
          if country_code_alpha3 == "GBR":
            country_limit = "United Kingdom"
          if country_code_alpha3 == "RUS":
            country_limit = "Russia"
          if country_code_alpha3 == "IRN":
            country_limit = "Iran"
          if country_code_alpha3 == "PRK":
            country_limit = "North Korea"
          text = spatial_entity+", "+country_limit
        else: # = country
          ####country_bias_list = [country_code_alpha2]
          only_country = True
          
          
        # ==================================================
        # retrieve the results from our database
        locs = None
        if text in self.inverted_indices_for_df_batch_geonames_results:
          #print("DB ACCESS!!!!")
          gindx = self.inverted_indices_for_df_batch_geonames_results[text]
          row_locs = self.df_batch_geonames_results.iloc[gindx]
          locs = [json.loads(row_locs[str(i)]) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] # we access the columns
        # ==================================================
        res = geocode_with_geonames(spatial_entity, country_bias_list, locs, country_limit, only_country)
        #print(res)
        if text not in self.inverted_indices_for_df_batch_geonames_results: # and res["geonamesId"] != '-1':  # update DB
          text = spatial_entity
          if country_limit is not None:
            text = spatial_entity+", "+country_limit
          df_batch, inverted_indices = self.update_geocoding_results_in_DB(self.out_csv_folder, text)
          self.df_batch_geonames_results = df_batch
          self.inverted_indices_for_df_batch_geonames_results = inverted_indices
        df_sentence_spatial_entities.loc[index, "feature_code"] = res["raw_data"]["fcode"]
        df_sentence_spatial_entities.loc[index, "geonames_id"] = str(res["geonamesId"])
        df_sentence_spatial_entities.loc[index, "updated"] = 1
        df_sentence_spatial_entities.loc[index, "prev_geonames_json"] = row["geonames_json"]
        df_sentence_spatial_entities.loc[index, "country_code"] = res["country_code"]
        df_sentence_spatial_entities.loc[index, "value"] = res["name"]
        df_sentence_spatial_entities.loc[index, "geonames_json"] = json.dumps(res["raw_data"], ensure_ascii=False)

    # PCLS: semi-independent political entity >> Hong Kong and Macao
    df_sentence_country = df_sentence_spatial_entities[(df_sentence_spatial_entities["feature_code"] == "PCLI")\
                                                        | (df_sentence_spatial_entities["feature_code"] == "PCLS")\
                                                        | (df_sentence_spatial_entities["updated"] == 1)]
    df_sentence_other = df_sentence_spatial_entities[(df_sentence_spatial_entities["feature_code"] != "PCLI")\
                                                      & (df_sentence_spatial_entities["feature_code"] != "PCLS")\
                                                       & (df_sentence_spatial_entities["feature_code"] != "CONT")\
                                                       & (df_sentence_spatial_entities["updated"] == 0)]
    df_sentence_country_copy = df_sentence_country.copy()
    print(df_sentence_spatial_entities["feature_code"])
    print(df_sentence_country)
    #print(df_sentence_other)
    
   # ---------------------------------------
    # second, resolve the other (state, city) disambiguity     
    #print("!!!!!!!!!!!!!! process_spatial_entities_in_sentence => local sentence id: ", local_sentence_id)
    #print("nb countries: ", df_sentence_country.shape[0])
    #print("nb others: ", df_sentence_other.shape[0])
    #print(df_sentence_other[["geonames_id", "feature_code", "value"]])
    
    #print(sliding_window_for_history)
    prev_sentence_country_codes = list(np.unique([entry[1] for entry in sliding_window_for_history]))
    #print("prev_sentence_country_codes: ", prev_sentence_country_codes)
    ref_country_codes = prev_sentence_country_codes
    # print("ref_country_codes_from_title: ", ref_country_codes_from_title)
    ref_country_codes = list(np.unique([countries.get(val).alpha2 for val in ref_country_codes if val != '-1'])) # from alpha2 to alpha3
    # sentence_country_values = np.unique(df_sentence_country["value"].to_numpy())
    if df_sentence_country_copy.shape[0]>0:
      sentence_country_codes = list(np.unique(df_sentence_country_copy["geonames_json"].apply(lambda x: \
            json.loads(x)["countryCode"] if "countryCode" in json.loads(x) else json.loads(x)["country_code"]).to_numpy()))
      if len(sentence_country_codes)>0:
        ref_country_codes = list(np.unique(ref_country_codes + sentence_country_codes))
    #  
    #print(ref_country_codes)
    #print(df_sentence_country_copy["value"])
    if len(ref_country_codes) == 0:
      df_sentence_other = pd.DataFrame()
      
    df_sentence_other_copy = df_sentence_other.copy()
    for index, row in df_sentence_other.iterrows():
      spatial_entity = row["value"].strip()
      
      process = False
      #print("'"+spatial_entity.lower()+"'")
      if spatial_entity.lower().replace(",","").strip() not in consts.BAN_LIST_FOR_SPATIAL_ENTITY_EXTRACTION:
        spatial_entity = spatial_entity.replace(",","").strip()
        if "west nile" not in spatial_entity.lower():
          if not ((spatial_entity.lower() == "dc" and "IN" in ref_country_codes) or (spatial_entity.lower() == "cm" and "IN" in ref_country_codes)): # >> 'DC' stands for "Deputy commissioner", CM: Chief minister
            #if not (is_relative_spatial_entity(ent.text) and idenitfy_relative_spatial_cardinal(ent.text) == "-1"):
            # print(spatial_entity, idenitfy_relative_spatial_cardinal(spatial_entity))
            if not (idenitfy_relative_spatial_cardinal(spatial_entity) == "-1"):
              if spatial_entity != "" and len(spatial_entity)>1 and not spatial_entity.isnumeric():
                process = True
      
      #print("process:", process, spatial_entity)
      res = {"geonamesId": "-1", "name": "-1", "country_code": "-1", "raw_data": "-1"}
      if process:
        # ==================================================
        # retrieve the results from our database
        locs = None
        if spatial_entity in self.inverted_indices_for_df_batch_geonames_results:
          #print("DB ACCESS!!!!")
          gindx = self.inverted_indices_for_df_batch_geonames_results[spatial_entity]
          row_locs = self.df_batch_geonames_results.iloc[gindx]
          locs = [json.loads(row_locs[str(i)]) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] # we access the columns
        # ==================================================
        init_geonamesId = str(row["geonames_id"])
        res = geocode_with_geonames(spatial_entity, ref_country_codes, locs)
        if spatial_entity not in self.inverted_indices_for_df_batch_geonames_results: # and res["geonamesId"] != '-1':  # update DB
          df_batch, inverted_indices = self.update_geocoding_results_in_DB(self.out_csv_folder, spatial_entity)
          self.df_batch_geonames_results = df_batch
          self.inverted_indices_for_df_batch_geonames_results = inverted_indices
        # ---------------------------------------------------
        # If we do not get any results, and if there is only one ref country, then get this country as spatial entity
        # e.g.spatial entity: "Sylvie" in id='0D2LGTCUQ0' ==> France
        if res["geonamesId"] == '-1' and len(ref_country_codes) == 1:
          country_spatial_entity = countries.get(ref_country_codes[0]).apolitical_name
          locs = None
          if country_spatial_entity in self.inverted_indices_for_df_batch_geonames_results:
            #print("DB ACCESS!!!!")
            gindx = self.inverted_indices_for_df_batch_geonames_results[country_spatial_entity]
            row_locs = self.df_batch_geonames_results.iloc[gindx]
            locs = [json.loads(row_locs[str(i)]) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] # we access the columns
          res = geocode_with_geonames(country_spatial_entity, [], locs, None, True) # only country
          #print(res)
          if country_spatial_entity not in self.inverted_indices_for_df_batch_geonames_results: # and res["geonamesId"] != '-1': # update DB
            df_batch, inverted_indices = self.update_geocoding_results_in_DB(self.out_csv_folder, country_spatial_entity)
            self.df_batch_geonames_results = df_batch
            self.inverted_indices_for_df_batch_geonames_results = inverted_indices
        # ---------------------------------------------------
        if "fcode" not in res["raw_data"]: # if feature code is not known from geonames, just omit it
          res = {"geonamesId": "-1", "name": "-1", "country_code": "-1", "raw_data": "-1"}
        res_geonamesId = str(res["geonamesId"])
        df_sentence_other_copy.loc[index, "ref_countries_for_geocoding"] = ", ".join(ref_country_codes)
        if res_geonamesId != '-1' and init_geonamesId != res_geonamesId:
          #print("---------------------- article_id: ", article_id, ", local_sentence_id: ", local_sentence_id)
          #print(spatial_entity, " => ", init_geonamesId, " / ", res_geonamesId)
          #print(spatial_entity, res["raw_data"])
          df_sentence_other_copy.loc[index, "feature_code"] = res["raw_data"]["fcode"]
          df_sentence_other_copy.loc[index, "geonames_id"] = str(res_geonamesId)
          df_sentence_other_copy.loc[index, "updated"] = 1
          df_sentence_other_copy.loc[index, "prev_geonames_json"] = row["geonames_json"]
        
      df_sentence_other_copy.loc[index, "country_code"] = res["country_code"]
      df_sentence_other_copy.loc[index, "value"] = res["name"]
      df_sentence_other_copy.loc[index, "geonames_json"] = json.dumps(res["raw_data"], ensure_ascii=False)
        
    if df_sentence_other_copy.shape[0]>0:
      df_sentence_other_copy = df_sentence_other_copy[df_sentence_other_copy["value"] != "-1"]
    if df_sentence_country_copy.shape[0]>0:
      df_sentence_country_copy = df_sentence_country_copy[df_sentence_country_copy["value"] != "-1"]
    df_new_sentence_spatial_entities = pd.concat([df_sentence_country_copy.reset_index(), df_sentence_other_copy.reset_index()])
    
    df_new_sentence_spatial_entities = df_new_sentence_spatial_entities.drop_duplicates(subset=['position', 'geonames_id'])

    return(df_new_sentence_spatial_entities)









  # def check_url_accessibility(self, df):
  #   status_list = []
  #   shortened_url_list = []
  #   for index, row in df.iterrows():
  #     url = row[consts.COL_URL]
  #     status, shortened_url = is_website_up(url)
  #     status_list.append(int(status))
  #     shortened_url_list.append(shortened_url)
  #     print(index, " => ", shortened_url, " => ", status)
  #   df["url_accessibility"] = status_list
  #   df["shortened_url"] = url
  #   return df
  
  
  

  # finding spatial entities in text is called "geotagging"
  # Also: geoparsing: geotagging + geocoding
  # The major error that arises in geotagging is the geo/non-geo confusion, where a non-geographical object,
  #    e.g. a person, may be detected as a geographical entity
  # Jacques Fize lists 4 disambiguation algorithms:
  #  - MOST_COMMON: . Paris is mostly associated with Paris, France.
  #        A spatial entity is selected based on a specific criterion that quantify the commonness of an association, 
  #        e.g.  the commonness of each entity is a page rank Pagerank
  #  - SHAREDPROP: it promotes spatial consistency between the entities identified in a document
  #  - WIKICOOC: It relies solely on the frequency of co-occurrences between spatial entities
  #          First, it creates a co-occurrence graph. Then, the candidate spatial entity with the highest weighted degree is selected.
  #  - MORDECAI: It is based on Spacy and so on.
  #      A. Halterman, Mordecai: Full Text Geoparsing and Event Geocoding, The Journal of Open Source Software 2(9) (2017)
  
  def retrieve_location_info_from_title_with_knowledge_base(self):
    title_locations_filepath = os.path.join(self.out_csv_folder, "title_locations_spacy" + "." + consts.FILE_FORMAT_CSV)
    df_title_locations = pd.read_csv(title_locations_filepath, sep=";", keep_default_na=False, encoding='utf8')
    
    df_title_failure_locations = self.df_articles[~self.df_articles[consts.COL_ID].isin(df_title_locations[consts.COL_ARTICLE_ID])]
    title_locations_failure_filepath = os.path.join(self.out_csv_folder, "title_locations_failure" + "." + consts.FILE_FORMAT_CSV)
    #df_title_failure_locations = pd.read_csv(title_locations_failure_filepath, sep=";", keep_default_na=False, encoding='utf8')
    df_title_failure_locations.to_csv(title_locations_failure_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)  
    
    filepath = os.path.join(self.out_csv_folder, "media_source_resolution" + "." + consts.FILE_FORMAT_CSV)
    df_media_source = pd.read_csv(filepath, sep=",", keep_default_na=False)
    #source_to_country_assoc = dict(zip(df_media_source["url"].str.replace("www.",""), df_media_source["pub_country"]))
    article_id_to_source_country_code_assoc = dict(zip(df_media_source[consts.COL_ARTICLE_ID], df_media_source["pub_country_code"]))

    df_title_failure_locations["media_source_country_code"] = df_title_failure_locations["id"].apply(lambda x: article_id_to_source_country_code_assoc[x] if x in article_id_to_source_country_code_assoc else "-1")
    
    # ===========================================
    # SPACY
    extr_spacy_spatial_entity_dict = dict(zip(df_title_locations["name"], df_title_locations["country_code"]))
    extr_spacy_spatial_entity_list = np.unique(df_title_locations["name"].to_list())
    
    # ===========================================
    # LOOKUP TABLE
    extr_lookup_spatial_entity_dict, lookup_country_to_entites_dict  = build_spatial_entity_lookup_table(self.data_folder)
    extr_lookup_spatial_entity_list = extr_lookup_spatial_entity_dict.keys()
    extr_lookup_spatial_entity_list = [ent.split("(")[0] if "(" in ent else ent for ent in extr_lookup_spatial_entity_list]
    extr_lookup_spatial_entity_list = [ent.replace(")","") if ")" in ent else ent for ent in extr_lookup_spatial_entity_list]
    extr_lookup_spatial_entity_list = [ent.replace("\\", " ") if "\\" in ent else ent for ent in extr_lookup_spatial_entity_list]
    
    # ===========================================
    # COUNTRY/NATIONALITY LIST
    filepath = os.path.join(self.data_folder, "nationalities", "Nationality_to_Country_Assoc.csv")
    df_country_nationality = pd.read_csv(filepath, sep=";", keep_default_na=False)
    extr_country_nationality_spatial_entity_dict = dict(zip(df_country_nationality["nationality"], df_country_nationality["country_code"]))
    extr_country_nationality_spatial_entity_dict2 = dict(zip(df_country_nationality["country_name"], df_country_nationality["country_code"]))
    extr_country_nationality_spatial_entity_dict.update(extr_country_nationality_spatial_entity_dict2)
    extr_country_nationality_spatial_entity_list = extr_country_nationality_spatial_entity_dict.keys()
    
    # -------------------------------------------
    nlp_trf = spacy.load("en_core_web_trf")
    
    punctuations = string.punctuation
    nltk.download('punkt')
    stop_words = set(stopwords.words('english'))
    
    new_spacy_spatial_entity_list = []
    new_spacy_spatial_country_list = []
    new_lookup_spatial_entity_list = []
    new_lookup_spatial_country_list = []
    new_country_nationality_spatial_entity_list = []
    new_country_nationality_spatial_country_list = []
    

    for index, row in df_title_failure_locations.iterrows():
      title = row[consts.COL_TITLE]
      source_media_country_code = row["media_source_country_code"]
      # print(index, source_media_country_code)
      
      curr_extr_lookup_spatial_entity_list = []
      if source_media_country_code in lookup_country_to_entites_dict:
        curr_extr_lookup_spatial_entity_list = lookup_country_to_entites_dict[source_media_country_code]
      
      w_tokens = word_tokenize(title)
      rgx = "(^["+punctuations+"])|(["+punctuations+"]$)"
      tokens_adj = [re.sub(rgx, "", w) for w in w_tokens]
      tokens = []
      for t in w_tokens:
        r = re.compile(r'[\s{}]+'.format(re.escape(punctuations)))
        tokens.extend(r.split(t))
      filtered_tokens = [w for w in tokens if not w.lower() in stop_words]
      bigram = [" ".join(x) for x in ngrams(filtered_tokens, 2)]
      trigram = [" ".join(x) for x in ngrams(filtered_tokens, 3)]
      grams1 = tokens_adj + filtered_tokens + bigram + trigram
      grams2 = tokens_adj + filtered_tokens + bigram + trigram
      
      # ===========================================
      # SPACY
      curr_spacy_spatial_entity_list = [t for t in grams1 if t in extr_spacy_spatial_entity_list]
      curr_spacy_spatial_entity_list_str = "-1"
      if len(curr_spacy_spatial_entity_list) > 0:
        curr_spacy_spatial_entity_list_str = ", ".join(np.unique(curr_spacy_spatial_entity_list))
      new_spacy_spatial_entity_list.append(curr_spacy_spatial_entity_list_str)
      
      curr_spacy_spatial_country_list = [extr_spacy_spatial_entity_dict[t] for t in grams1 if t in extr_spacy_spatial_entity_list]
      curr_spacy_spatial_country_list_str = "-1"
      if len(curr_spacy_spatial_country_list) > 0:
        curr_spacy_spatial_country_list_str = ", ".join(np.unique(curr_spacy_spatial_country_list))
      new_spacy_spatial_country_list.append(curr_spacy_spatial_country_list_str)
        
      # ===========================================
      # LOOKUP TABLE
      curr_lookup_spatial_entity_list = [t for t in grams2 if t in curr_extr_lookup_spatial_entity_list]
      curr_lookup_spatial_entity_list_str = "-1"
      if len(curr_lookup_spatial_entity_list) > 0:
        curr_lookup_spatial_entity_list_str = ", ".join(np.unique(curr_lookup_spatial_entity_list))
      new_lookup_spatial_entity_list.append(curr_lookup_spatial_entity_list_str)
      
      curr_lookup_spatial_country_list = [source_media_country_code for t in grams2 if t in curr_extr_lookup_spatial_entity_list]
      curr_lookup_spatial_country_list_str = "-1"
      if len(curr_lookup_spatial_country_list) > 0:
        curr_lookup_spatial_country_list_str = ", ".join(np.unique(curr_lookup_spatial_country_list))
      new_lookup_spatial_country_list.append(curr_lookup_spatial_country_list_str)
      
      # ===========================================
      # COUNTRY/NATIONALITY LIST
      curr_country_nationality_spatial_entity_list = [t for t in grams2 if t in extr_country_nationality_spatial_entity_list]
      curr_country_nationality_spatial_entity_list_str = "-1"
      if len(curr_country_nationality_spatial_entity_list) > 0:
        curr_country_nationality_spatial_entity_list_str = ", ".join(np.unique(curr_country_nationality_spatial_entity_list))
      new_country_nationality_spatial_entity_list.append(curr_country_nationality_spatial_entity_list_str)
      
      curr_country_nationality_spatial_country_list = [extr_country_nationality_spatial_entity_dict[t] for t in grams2 if t in extr_country_nationality_spatial_entity_list]
      curr_country_nationality_spatial_country_list_str = "-1"
      if len(curr_country_nationality_spatial_country_list) > 0:
        curr_country_nationality_spatial_country_list_str = ", ".join(np.unique(curr_country_nationality_spatial_country_list))
      new_country_nationality_spatial_country_list.append(curr_country_nationality_spatial_country_list_str)      
            
    # --------------------------------------------
    
    df_title_failure_locations["spacy_spatial_entity_name"] = new_spacy_spatial_entity_list
    df_title_failure_locations["spacy_spatial_country_code"] = new_spacy_spatial_country_list
    df_title_failure_locations["lookup_spatial_entity_name"] = new_lookup_spatial_entity_list
    df_title_failure_locations["lookup_spatial_country_code"] = new_lookup_spatial_country_list
    df_title_failure_locations["country_nationality_spatial_entity_name"] = new_country_nationality_spatial_entity_list
    df_title_failure_locations["country_nationality_spatial_country_code"] = new_country_nationality_spatial_country_list
    
    
    temp_filepath = os.path.join(self.out_csv_folder, \
                                      "title_locations_auxiliary_from_knowledge_base" + "." + consts.FILE_FORMAT_CSV)
    df_title_failure_locations.to_csv(temp_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    
    
    # =============================================
    # construct the final data structure
    
    article_id_list = []
    title_list = []
    label_list = []
    type_list = []
    name_list = []
    country_code_list = []
    media_source_country_code_list = []
    for index, row in df_title_failure_locations.iterrows():
      article_id = row[consts.COL_ID]
      print("-------", index, article_id)
      title = row[consts.COL_TITLE]
      source_media_country_code = row["media_source_country_code"]
      
      spacy_spatial_entity_name_list = []
      spacy_spatial_country_code_list = []
      if row["spacy_spatial_entity_name"] != "-1":
        spacy_spatial_entity_name_list = row["spacy_spatial_entity_name"].split(", ")
        spacy_spatial_country_code_list = row["spacy_spatial_country_code"].split(", ")
        if row["spacy_spatial_country_code"] == "N/A" and len(spacy_spatial_entity_name_list) != len(spacy_spatial_country_code_list):
          spacy_spatial_country_code_list = ["N/A"]*len(spacy_spatial_entity_name_list)
        elif row["spacy_spatial_country_code"] == "-1" and len(spacy_spatial_entity_name_list) != len(spacy_spatial_country_code_list):
          spacy_spatial_country_code_list = ["N/A"]*len(spacy_spatial_entity_name_list)
      print(row["spacy_spatial_country_code"])
      print(spacy_spatial_entity_name_list)
      print(spacy_spatial_country_code_list)
      lookup_spatial_entity_name_list = []
      lookup_spatial_country_code_list = []
      if row["lookup_spatial_entity_name"] != "-1":
        lookup_spatial_entity_name_list = row["lookup_spatial_entity_name"].split(", ")
        lookup_spatial_country_code_list = row["lookup_spatial_country_code"].split(", ")
        if len(lookup_spatial_entity_name_list) != len(lookup_spatial_country_code_list):
          lookup_spatial_country_code_list = lookup_spatial_country_code_list*len(lookup_spatial_entity_name_list)
          
      country_nationality_spatial_entity_name_list = []
      country_nationality_spatial_country_code_list = []
      if row["country_nationality_spatial_entity_name"] != "-1":
        country_nationality_spatial_entity_name_list = row["country_nationality_spatial_entity_name"].split(", ")
        country_nationality_spatial_country_code_list = row["country_nationality_spatial_country_code"].split(", ")
        if len(country_nationality_spatial_entity_name_list) != len(country_nationality_spatial_country_code_list):
          country_nationality_spatial_country_code_list = country_nationality_spatial_country_code_list*len(country_nationality_spatial_entity_name_list)
          
      spatial_entity_name_list = spacy_spatial_entity_name_list + lookup_spatial_entity_name_list\
                               + country_nationality_spatial_entity_name_list
      spatial_entity_country_list = spacy_spatial_country_code_list + lookup_spatial_country_code_list\
                               +country_nationality_spatial_country_code_list
                               
      nb_entity = len(spatial_entity_name_list)
      name_list.extend(spatial_entity_name_list)
      country_code_list.extend(spatial_entity_country_list)
      article_id_list.extend([article_id]*nb_entity)
      title_list.extend([title]*nb_entity)
      label_list.extend(["-1"]*nb_entity)
      type_list.extend(["other"]*len(spacy_spatial_entity_name_list))
      type_list.extend(["state/city"]*len(lookup_spatial_entity_name_list))
      #type_list.extend(["other"]*len(lookup_spatial_entity_name_list))
      type_list.extend(["country"]*len(country_nationality_spatial_entity_name_list))
      media_source_country_code_list.extend([source_media_country_code]*nb_entity)
    
      print(len(article_id_list))
      print(len(name_list))
      print(len(media_source_country_code_list))
      print(len(country_code_list))
    data = {"article_id": article_id_list, "title": title_list, "label": label_list, \
            "name": name_list, "media_source_country_code": media_source_country_code_list, 
            "type": type_list, "country_code": country_code_list}  
      
    df = pd.DataFrame(data)
    loc_filepath = os.path.join(self.out_csv_folder, \
                                      "title_locations_from_knowledge_base" + "." + consts.FILE_FORMAT_CSV)
    df.to_csv(loc_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)




  # finding spatial entities in text is called "geotagging"
  # Also: geoparsing: geotagging + geocoding
  # The major error that arises in geotagging is the geo/non-geo confusion, where a non-geographical object,
  #    e.g. a person, may be detected as a geographical entity
  # Jacques Fize lists 4 disambiguation algorithms:
  #  - MOST_COMMON: . Paris is mostly associated with Paris, France.
  #        A spatial entity is selected based on a specific criterion that quantify the commonness of an association, 
  #        e.g.  the commonness of each entity is a page rank Pagerank
  #  - SHAREDPROP: it promotes spatial consistency between the entities identified in a document
  #  - WIKICOOC: It relies solely on the frequency of co-occurrences between spatial entities
  #          First, it creates a co-occurrence graph. Then, the candidate spatial entity with the highest weighted degree is selected.
  #  - MORDECAI: It is based on Spacy and so on.
  #      A. Halterman, Mordecai: Full Text Geoparsing and Event Geocoding, The Journal of Open Source Software 2(9) (2017)
  
  def retrieve_location_info_from_title(self):
    ### named entity types: https://www.dataknowsall.com/ner.html
    nlp_lg = spacy.load("en_core_web_lg")
    nlp_trf = spacy.load("en_core_web_trf")
    
    
    filepath = os.path.join(self.out_csv_folder, "media_source_resolution" + "." + consts.FILE_FORMAT_CSV)
    df_media_source = pd.read_csv(filepath, sep=",", keep_default_na=False)
    #source_to_country_assoc = dict(zip(df_media_source["url"].str.replace("www.",""), df_media_source["pub_country"]))
    article_id_to_source_country_code_assoc = dict(zip(df_media_source[consts.COL_ARTICLE_ID], df_media_source["pub_country_code"]))
    
    #df_articles_failure = pd.read_csv("/home/nejat/eclipse/workspace/news2hin/out-AI-2021/csv/padiweb/title_failure.csv", sep=";", keep_default_na=False) # .iloc[0:500]
    
    loc_name_list = []
    loc_label_list = []
    article_id_list = []
    title_list = []
    for index, row in self.df_articles.iterrows():
    #for index, row in df_articles_failure.iterrows():
      article_id = row[consts.COL_ID]
      print(index, article_id)
      title = row[consts.COL_TITLE]
      source_country_code = article_id_to_source_country_code_assoc[article_id]
      source_country_code_alpha2 = "-1"
      if source_country_code != "-1":
        source_country_code_alpha2 = countries.get(source_country_code).alpha2
      article_text = row[consts.COL_TEXT]
      # --------------
      # apply a simple preprocessing for Japan
      title = title.replace("pref.", "prefecture")
      title = title.replace("Pref.", "Prefecture")
      title = title.replace("APTOPIX", "") # APTOPIX SERBIA AND MONTENEGRO BIRD FLU
      title = title.replace("aptopix", "") # APTOPIX SERBIA AND MONTENEGRO BIRD FLU
      # --------------
    
      ent_found = False
      ents = nlp_trf(title).ents # we find more spatial entities with 'trf' model
      # but, it is possible it misses some texts, so we add additional model 'lg'
      for ent in ents: # iterate over the named entities of the text
        if ent.label_ == "GPE" or ent.label_ == "LOC" or ent.label_ == "FAC":
          ent_found = True
          break
      if not ent_found: 
        ents = nlp_lg(title).ents
        for ent in ents: # iterate over the named entities of the text
          if ent.label_ == "GPE" or ent.label_ == "LOC" or ent.label_ == "FAC":
            ent_found = True
            break
      if ent_found: 
        for ent in ents: # iterate over the named entities of the text
          if ent.label_ == "GPE" or ent.label_ == "LOC" or ent.label_ == "FAC":
            text = ent.text
            if ".." in text:
              rgx = r"(?i)[.]{2,}$" # remove multiple dots in the end
              text = re.sub(rgx, "", text)
            rgx = "(?i)(^the )"
            text = re.sub(rgx, "", text)
            rgx = "(?i)( region)"
            text = re.sub(rgx, "", text)
            rgx = "(?i)(^upper)|(^lower)" # >> for Upper and Lower Rhine => it is a complicated situation 
            text = re.sub(rgx, "", text)
            rgx = "(^-)|(-$)|(:$)"
            text = re.sub(rgx, "", text)
            # if text != text.upper(): # remove a dot point from the end, e.g. Pa. >> we do not treat U.K.
            #   rgx = "(\.$)"
            #   text = re.sub(rgx, "", text)
            text = text.strip()
                  
            process = False
            if text.lower().replace(",","").strip() not in consts.BAN_LIST_FOR_SPATIAL_ENTITY_EXTRACTION:
              text = text.replace(",","").strip()
              if "west nile" not in text.lower():
                if not ((text.lower() == "dc" and source_country_code == "IND") or (text.lower() == "cm" and source_country_code == "IND")): # >> 'DC' stands for "Deputy commissioner", CM: Chief minister
                  #if not (is_relative_spatial_entity(ent.text) and idenitfy_relative_spatial_cardinal(ent.text) == "-1"):
                  #print(text, idenitfy_relative_spatial_cardinal(text))
                  if not (idenitfy_relative_spatial_cardinal(text) == "-1"):
                    if text != "" and len(text)>1 and not text.isnumeric():
                      process = True
            if process:
              # for abbreviations: we do it here, because we check above the ban list
              text = self.process_spatial_entity_abbreviation(text, source_country_code_alpha2)
              rgx = "(?i)('s|’s)" # good example: "Africa's", bad example: "St John's"
              text = re.sub(rgx, "", text)
              # -------
              # to prevent from having "North of France"
              if is_relative_spatial_entity(text) and " of " in text:
                text = text.split(" of ")[1]
              # -------
              # to prevent from having "West Africa" or "East Europe"
              ent_tokens = text.split(" ")
              if is_relative_spatial_entity(text) and len(ent_tokens) == 2: # if there are 2 words/tokens 
                if not (text == "North America" or text == "South America" or text == "South Africa"): 
                  if ent_tokens[1] in consts.CONTINENTS_NAME or ent_tokens[1] in consts.CONTINENTS_CODE:
                    text = ent_tokens[1]
              
              #text = idenitfy_relative_spatial_cardinal(text)
              #print("!!!! ", ent.text, " => ", text)
              loc_name_list.append(text)
              loc_label_list.append(ent.label_)
              title_list.append(title)
              article_id_list.append(article_id)

    source_country_code_list = [article_id_to_source_country_code_assoc[a_id] for a_id in article_id_list]
            
            
    df = pd.DataFrame(data={consts.COL_ARTICLE_ID: article_id_list, consts.COL_TITLE: title_list,\
                          "label": loc_label_list,  "name": loc_name_list, "media_source_country_code": source_country_code_list})
  
    title_locations_filepath = os.path.join(self.out_csv_folder, \
                                      "title_locations_init" + "." + consts.FILE_FORMAT_CSV)
    df.to_csv(title_locations_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)

    # goenames_extr_filepath = os.path.join(self.out_csv_folder, \
    #                                   "title_locations" + "." + consts.FILE_FORMAT_CSV)              
    # df_temp = pd.read_csv(goenames_extr_filepath, sep=";")
    # article_id_list = df_temp["article_id"]
    # title_list = df_temp["title"]
    # loc_label_list = df_temp["label"]
    # loc_name_list = df_temp["name"]
    

    
    
    # ------------------------------------------------------
    client_nominatim = Nominatim(user_agent="geoapi")
    
    loc_type_list = []
    country_code_list = []
    for i in range(len(loc_name_list)):
      loc_name = loc_name_list[i]
      source_country_code = source_country_code_list[i]
      print("----- ", i, loc_name, source_country_code)
      
      
      
      processed = False
      # ----------------------   
      processed, loc_type, country_code, fcode = self.resolve_manually_loc_name_ambiguity(loc_name, source_country_code, article_text)      
      #loc_type = "other" >> by default
      #country_code = "N/A" >> by default
      if not processed and loc_name != "-1" and len(loc_name)>1: # it has at least 2 characters
        try:
          time.sleep(1)
          res = client_nominatim.geocode(loc_name, exactly_one=True, addressdetails=True, language="en", timeout=20)
          if res is not None:
            if "country" not in res.raw["address"]:
              loc_type = "continent or sea"
              country_code = "-1"
            else:
              details = res.raw["address"].copy()
              del details["country"]
              del details["country_code"]
              if len(details) == 0:
                loc_type = "country"
                country_code = countries.get(res.raw["address"]["country_code"]).alpha3
                
                # check if it is a country abbreviation
                if len(loc_name) <= 3:
                  rgx = "^([A-Z])+"
                  if re.sub(rgx, "", loc_name) != "": # it is not a country abbreviation, since there are some lowercase letter
                    loc_type = "other"
                    country_code = "N/A"
                  else: # if it is a country abbreviation
                    # check if the text country's continent is the same as the source media's continent
                    # e.g. "France" > "EU" and "Italy" > "EU"
                    if source_country_code != "-1" and retrieve_continent_from_country_code(country_code) != retrieve_continent_from_country_code(source_country_code):
                      loc_type = "other"
                      country_code = "N/A"
                    
            
            
        except:
          print("error in retrieve_location_info_from_title() with name=", loc_name)
          pass
      loc_type_list.append(loc_type)
      country_code_list.append(country_code)
    # ------------------------------------------------------
      
    df = pd.DataFrame(data={consts.COL_ARTICLE_ID: article_id_list, consts.COL_TITLE: title_list,\
                          "label": loc_label_list,  "name": loc_name_list, "media_source_country_code": source_country_code_list, \
                          "type": loc_type_list, "country_code": country_code_list})
  
    title_locations_filepath = os.path.join(self.out_csv_folder, \
                                      "title_locations_spacy" + "." + consts.FILE_FORMAT_CSV)
    df.to_csv(title_locations_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    
    


  def resolve_manually_loc_name_ambiguity(self, loc_name, source_country_code, article_text):
    loc_type = "N/A"
    country_code = "N/A"
    processed = False
    fcode = "N/A"
    # ----------------------   
    if loc_name.lower() == "eu":
      loc_type = "continent"
      country_code = "-1"
      fcode = "-1"
      processed = True   
    if loc_name == "UK":
      loc_type = "country"
      country_code = "GBR"
      fcode = "PCLI"
      processed = True    
    # ----------------------      
    if loc_name == "US" or loc_name == "USA":
      loc_type = "country"
      country_code = "USA"
      fcode = "PCLI"
      processed = True
    # ----------------------
    if loc_name == "Northern Ireland" or loc_name == "Scotland":
      loc_type = "state"
      country_code = "GBR"
      fcode = "ADM1"
      processed = True
    # ---------------------- 
    if (loc_name == "NI" or loc_name == "Northern Ireland") and (source_country_code == "GBR" or source_country_code == "IRL"):
      loc_type = "state"
      country_code = "GBR"
      fcode = "ADM1"
      processed = True
    # ----------------------   
    if loc_name == "Hong Kong":
      loc_type = "country"
      country_code = "HK"
      fcode = "PCLS"
      processed = True
    # ----------------------   
    if loc_name == "Macao" or loc_name == "Macau":
      loc_type = "country"
      country_code = "MO"
      fcode = "PCLS"
      processed = True      
    # ----------------------   
    if loc_name == "Niger" and source_country_code == "NGA":
      loc_type = "state"
      country_code = "NGA"
      fcode = "ADM1"
      processed = True    
    # ----------------------  
    if loc_name == "SA":
      processed = True
      if source_country_code != "-1" and retrieve_continent_from_country_code(source_country_code) == "AF":
        loc_type = "country"
        country_code = "ZAF"
      elif loc_name == "SA" and source_country_code == "-1":
        if "South Africa" in article_text:
          loc_type = "country"
          country_code = "ZAF"
        elif "Saudi Arabia" in article_text:
          loc_type = "country"
          country_code = "SAU"
    # ----------------------      
    if loc_name == "Zim" and source_country_code != "-1" and source_country_code == "ZWE":
      loc_type = "country"
      country_code = "ZWE"
      processed = True
    # ----------------------      
    if loc_name == "MA":
      processed = True
      if source_country_code != "-1" and source_country_code == "MAR":
        loc_type = "country"
        country_code = "MAR"
        fcode = "PCLI"
      elif source_country_code == "-1" and "Morocco" in article_text:
        loc_type = "country"
        country_code = "MAR"
        fcode = "PCLI"
    # ----------------------  
    if loc_name == "TN" and source_country_code == "-1":
      processed = True
      if "Tunisia" in article_text:
          loc_type = "country"
          country_code = "TUN"
          fcode = "PCLI"
    # ----------------------  
    if loc_name == "PA":
      processed = True
      if source_country_code != "-1" and source_country_code == "PAN":
        loc_type = "country"
        country_code = "PAN"
        fcode = "PCLI"
      elif source_country_code == "-1" and "Panama" in article_text:
        loc_type = "country"
        country_code = "PAN"
        fcode = "PCLI"
    # ----------------------  
    if loc_name == "Meuse": # there is a bug in Nominatim about it
      if source_country_code != "-1" and source_country_code == "EGY":
        loc_type = "country"
        country_code = "EGY"
        fcode = "PCLI"
        processed = True
      elif source_country_code == "-1" and "Egypt" in article_text:
        loc_type = "country"
        country_code = "EGY"
        fcode = "PCLI"
        processed = True
  
    return(processed, loc_type, country_code, fcode)
    
    
      
  def apply_gecoding_for_title_locations(self):
    title_locations_spacy_filepath = os.path.join(self.out_csv_folder, \
                                      "title_locations_spacy" + "." + consts.FILE_FORMAT_CSV)
    df_title_locs_spacy = pd.read_csv(title_locations_spacy_filepath, sep=";", keep_default_na=False) # .iloc[0:500]
    title_locations_from_knowledge_base_filepath = os.path.join(self.out_csv_folder, \
                                      "title_locations_from_knowledge_base" + "." + consts.FILE_FORMAT_CSV)
    df_title_locs_from_knowledge_base = pd.read_csv(title_locations_from_knowledge_base_filepath, sep=";", keep_default_na=False) # .iloc[0:500]
    
    df_title_locs = pd.concat([df_title_locs_spacy.reset_index(), df_title_locs_from_knowledge_base.reset_index()])
    print(df_title_locs_spacy.shape, df_title_locs_from_knowledge_base.shape, df_title_locs.shape)
    filepath = os.path.join(self.out_csv_folder, "title_locations_final" + "." + consts.FILE_FORMAT_CSV)
    df_title_locs.to_csv(filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    
    
    # =========
    filepath = os.path.join(self.out_csv_folder, "media_source_resolution" + "." + consts.FILE_FORMAT_CSV)
    df_media_source = pd.read_csv(filepath, sep=",", keep_default_na=False)
    #source_to_country_assoc = dict(zip(df_media_source["url"].str.replace("www.",""), df_media_source["pub_country"]))
    article_id_to_source_country_code_assoc = dict(zip(df_media_source[consts.COL_ARTICLE_ID], df_media_source["pub_country_code"]))

    df_title_failure_locations = self.df_articles[~self.df_articles[consts.COL_ID].isin(df_title_locs[consts.COL_ARTICLE_ID])]
    df_title_failure_locations["media_source_country_code"] = df_title_failure_locations[consts.COL_ID].apply(lambda x: article_id_to_source_country_code_assoc[x])
    title_locations_failure_filepath = os.path.join(self.out_csv_folder, "title_failure_locations_final" + "." + consts.FILE_FORMAT_CSV)
    df_title_failure_locations.to_csv(title_locations_failure_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    # =========
    
    
    df_title_locs["country_bias"] = df_title_locs["media_source_country_code"] # temporary column
    df_title_locs_na = df_title_locs[df_title_locs["country_code"] == "N/A"]
    df_title_locs_ok = df_title_locs[df_title_locs["country_code"] != "N/A"]
    
    print(df_title_locs_ok.shape)
    print(df_title_locs_na.shape)
    
    df_title_locs_na.reset_index(drop=True, inplace=True)
    df_title_locs_ok.reset_index(drop=True, inplace=True)

    article_id_list = np.unique(df_title_locs[consts.COL_ARTICLE_ID].to_numpy())
    
    print(df_title_locs_na["country_bias"])
    # ---------------------------------------------------------
    # adjust country bias column in 'df_title_locs_na'
    for article_id in article_id_list:
      #print("----------- ", article_id)
      res = df_title_locs_ok[df_title_locs_ok[consts.COL_ARTICLE_ID] == article_id]
      if res.shape[0] == 1: # there is one country mention in the title
        # TODO: counter example: B.1.525 variant, linked to Nigeria, now found in Telangana >> Telangana is in India, not in Nigeria
        #print(res)
        country_code = res.iloc[0]["country_code"]
        if country_code != "-1":
          indx = (df_title_locs_na[consts.COL_ARTICLE_ID] == article_id)
          #if df_title_locs_na.loc[indx].shape[0] > 0: # if there is any other spatial entities other than country and continent
          df_title_locs_na.loc[indx, "country_bias"] = country_code
          # workaround: we put the country name, after a comma, instread of limiting directly the search within a country
          df_title_locs_na.loc[indx, "name"] = df_title_locs_na.loc[indx, "name"] + ", " + countries.get(country_code).name
    # ---------------------------------------------------------
    
    spatial_entity_list = df_title_locs_na["name"].to_numpy()
    country_bias_list = df_title_locs_na["country_bias"].to_numpy()

    print("--------- geonames ")
    geonames_result_list = geocode_batch_with_geonames(spatial_entity_list, country_bias_list)
    df_geonames = pd.DataFrame(geonames_result_list)
    df_geonames.columns= ["name_geonames", "country_code_geonames", "raw_data_json_geonames"]
    df_geonames["raw_data_json_geonames"] = df_geonames["raw_data_json_geonames"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    print(df_geonames.shape)
    
    print("--------- arcgis ")
    arcgis_result_list = geocode_batch_with_arcgis(spatial_entity_list, country_bias_list)
    df_arcgis = pd.DataFrame(arcgis_result_list)
    df_arcgis.columns= ["name_arcgis", "country_code_arcgis", "raw_data_json_arcgis"]
    df_arcgis["raw_data_json_arcgis"] = df_arcgis["raw_data_json_arcgis"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    print(df_arcgis.shape)
    
    print("--------- nominatim ")
    nominatim_result_list = geocode_batch_with_nominatim(spatial_entity_list, country_bias_list)
    df_nominatim = pd.DataFrame(nominatim_result_list)
    df_nominatim.columns= ["name_nominatim", "country_code_nominatim", "raw_data_json_nominatim"]
    df_nominatim["raw_data_json_nominatim"] = df_nominatim["raw_data_json_nominatim"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    print(df_nominatim.shape)


    # for i in range(len(spatial_entity_list)):
    #   spatial_entity_name = spatial_entity_list[i]
    #   country_bias = country_bias_list[i]
    #   # ==================================================
    #   # retrieve the results from our database
    #   locs = None
    #   if spatial_entity_name in self.inverted_indices_for_df_batch_geonames_results:
    #     #print("DB ACCESS!!!!")
    #     gindx = self.inverted_indices_for_df_batch_geonames_results[spatial_entity_name]
    #     row_locs = self.df_batch_geonames_results.iloc[gindx]
    #     locs = [json.loads(row_locs[str(i)]) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] # we access the columns
    #   # ==================================================
    #   res = geocode_with_geonames(spatial_entity_name, [country_bias], locs, None)
    #   if locs is None and res["geonamesId"] != '-1':  # update DB
    #     self.update_geocoding_results_in_DB(spatial_entity_name)      
    #   geonames_json_list.append(json.dumps(res["raw_data"], ensure_ascii=False))
      
      
    
    # df_geocoders = pd.DataFrame({"name_nominatim": df_nominatim["name_nominatim"], 
    #                              "country_code_nominatim": df_nominatim["country_code_nominatim"],
    #                              "name_arcgis": df_arcgis["name_arcgis"], "country_code_arcgis": df_arcgis["country_code_arcgis"],
    #                              "name_geonames": df_geonames["name_geonames"], "country_code_geonames": df_geonames["country_code_geonames"]
    #                              })
    
    df_title_locs_na["name"] = df_title_locs_na["name"].apply(lambda x: x.split(", ")[0]) # return back from workaround solution
    title_locations_new = pd.concat([df_title_locs_na, df_nominatim, df_arcgis, df_geonames], axis=1)
        
    df_title_locs_ok["name_nominatim"] = "-1"
    df_title_locs_ok["country_code_nominatim"] = "-1"
    df_title_locs_ok["raw_data_json_nominatim"] = "-1"
    df_title_locs_ok["name_arcgis"] = "-1"
    df_title_locs_ok["country_code_arcgis"] = "-1"
    df_title_locs_ok["raw_data_json_arcgis"] = "-1"
    df_title_locs_ok["name_geonames"] = "-1"
    df_title_locs_ok["country_code_geonames"] = "-1"
    df_title_locs_ok["raw_data_json_geonames"] = "-1"
    
    title_locations_new_total = pd.concat([title_locations_new, df_title_locs_ok], axis=0)
    title_locations_new_total = title_locations_new_total.drop("country_bias", axis=1)
    
    filepath = os.path.join(self.out_csv_folder, "title_locations_final_with_geocoding_results" \
                                           + "." + consts.FILE_FORMAT_CSV)
    title_locations_new_total.to_csv(filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)
  



  def deduce_from_title_geocoding_results(self):
    filepath = os.path.join(self.out_csv_folder, "title_locations_final_with_geocoding_results" \
                                           + "." + consts.FILE_FORMAT_CSV)
    df_title_geocoding_results = pd.read_csv(filepath, sep=";", keep_default_na=False) # .iloc[0:500]
    
    filepath = os.path.join(self.out_csv_folder, "geonames_results_in_batch" + "." + consts.FILE_FORMAT_CSV)
    if not os.path.exists(filepath):
      # create an empty file 
      columns = [str(i) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] + ["text"]
      df = pd.DataFrame(columns=columns)
      df.to_csv(filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    #
    self.df_batch_geonames_results = pd.read_csv(filepath, sep=";", keep_default_na=False)
    self.inverted_indices_for_df_batch_geonames_results = dict(zip(self.df_batch_geonames_results["text"], self.df_batch_geonames_results.index))


    article_ids = []
    media_source_country_code_list = []
    value_list = []
    country_bias_list = []
    type_list = []
    for index, row in df_title_geocoding_results.iterrows():
      print("----------", index, "/", df_title_geocoding_results.shape[0])
      article_id = row["article_id"]
      media_source_country_code = row["media_source_country_code"]
      text = row["name"]
      
      if "Co " in text:
        text = text.replace("Co ", "County ").strip()
      if " zoo" in text.lower():
        text = text.replace(" Zoo", "").strip()
        text = text.replace(" zoo", "").strip()
      if " Bird Sanctuary" in text:
        text = text.replace(" Bird Sanctuary", "").strip()
      if " Waterworks" in text:
        text = text.replace(" Waterworks", "").strip()
      if " Country Park" in text:
        text = text.replace(" Country Park", "").strip()
      if " Biological Park" in text:
        text = text.replace(" Biological Park", "").strip()
      if " National Park" in text:
        text = text.replace(" National Park", "").strip()
      if " Park" in text:
        text = text.replace(" Park", "").strip()
      if " Wildlife Sanctuary" in text:
        text = text.replace(" Wildlife Sanctuary", "").strip()
      if " rgn" in text:
        text = text.replace(" rgn", "").strip()
      if " Canal" in text:
        text = text.replace(" Canal", "").strip()
      if " Marina" in text:
        text = text.replace(" Marina", "").strip()
      if " Reserve" in text:
        text = text.replace(" Reserve", "").strip()
      # if "s. " in text.lower():
      #   text = text.replace("s.", "").strip()
      #   text = text.replace("S.", "").strip()
      # if "n. " in text.lower():
      #   text = text.replace("n.", "").strip()
      #   text = text.replace("N.", "").strip()
      if text.upper() == text and len(text)>4:
        text = text.title()
      # --
      rgx = "^[A-Z][A-Z]{0,2}[A-Z]$"
      if re.search(rgx, text) is not None: # if it is an abbreviation
        text = row["name_nominatim"]
        # names = [row["name_nominatim"], row["name_arcgis"], row["name_geonames"]]
        # counter_res = Counter(names)
        # counts_dict = dict(counter_res)
        # maj_name = max(names, key=names.count)
        # if counts_dict[maj_name] > 1:
        #   text = maj_name
      processed = False
      if row["country_code"] != "N/A" and row["country_code"] != "-1":
        # nationalities and country names are mixed up in this category
        country_name = countries.get(row["country_code"]).apolitical_name
        if row["country_code"] == "KOR":
          country_name = "South Korea"
        if row["country_code"] == "GBR":
          country_name = "United Kingdom"
        if row["country_code"] == "RUS":
          country_name = "Russia"
        if row["country_code"] == "IRN":
          country_name = "Iran"
        if row["country_code"] == "PRK":
          country_name = "North Korea"
        value_list.append(country_name)
        country_bias_list.append(row["country_code"])
        type_list.append("country")
        processed = True
      elif idenitfy_relative_spatial_cardinal(text) == "-1":
        pass
      else:
          country_codes = [row["country_code_nominatim"], row["country_code_arcgis"], row["country_code_geonames"]]
          country_codes = [country_code for country_code in country_codes if country_code != "-1" and country_code != ""]
          # this step is really too restrictive in order to get a high precision score
          if len(country_codes)>1 and len(np.unique(country_codes)) == 1: # at least 2, all from the same country
            # counter_res = Counter(country_codes)
            # counts_dict = dict(counter_res)
            # maj_country_code = max(country_codes, key=country_codes.count)
            # max_count = counts_dict[maj_country_code]
            # nb_max = len([val for val in counts_dict.values() if val == max_count])
            # if nb_max == 1: # both country codes point to the same entity
            #country_bias_list.append(maj_country_code)
            value_list.append(text)
            country_bias_list.append(country_codes[0])
            type_list.append("other")
            processed = True

      if processed:
        article_ids.append(article_id)
        media_source_country_code_list.append(media_source_country_code)
        
    # ------------------------------------------------------------------------
    
    geonames_json_list = [] 
    for i, spatial_entity in enumerate(value_list):
      print("----------", i, "/", len(value_list))
      #time.sleep(1)
      country_bias = country_bias_list[i]
      country_limit = None
      spatial_entity_adj = spatial_entity
      if type_list[i] != "country":
        country_limit = countries.get(country_bias).apolitical_name
        if country_bias == "KOR":
          country_limit = "South Korea"
        if country_bias == "GBR":
          country_limit = "United Kingdom"
        if country_bias == "RUS":
          country_limit = "Russia"
        if country_bias == "IRN":
          country_limit = "Iran"
        if country_bias == "PRK":
          country_limit = "North Korea"
        spatial_entity_adj = spatial_entity + ", " + country_limit
      print(spatial_entity_adj)
      # ==================================================
      # retrieve the results from our database
      locs = None
      if spatial_entity_adj in self.inverted_indices_for_df_batch_geonames_results:
        #print("DB ACCESS!!!!")
        gindx = self.inverted_indices_for_df_batch_geonames_results[spatial_entity_adj]
        row_locs = self.df_batch_geonames_results.iloc[gindx]
        locs = [json.loads(row_locs[str(i)]) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] # we access the columns
      # ==================================================
      res = geocode_with_geonames(spatial_entity, [country_bias], locs, country_limit)
      if locs is None and res["geonamesId"] != '-1':  # update DB
        df_batch, inverted_indices = self.update_geocoding_results_in_DB(self.out_csv_folder, spatial_entity_adj)
        self.df_batch_geonames_results = df_batch
        self.inverted_indices_for_df_batch_geonames_results = inverted_indices
      geonames_json_list.append(json.dumps(res["raw_data"], ensure_ascii=False))
      
    df = pd.DataFrame({"article_id": article_ids, "media_source_country_code": media_source_country_code_list,\
         "country_bias": country_bias_list, "final_value": value_list, "final_geonames_json": geonames_json_list}) 
    
    df2 = df[df["final_geonames_json"] != '-1']

    filepath = os.path.join(self.out_csv_folder, "title_locations_final_with_geonames" \
                                           + "." + consts.FILE_FORMAT_CSV)
    df2.to_csv(filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)





  def extract_geonames_hierarchy_info_for_all_spatial_entities(self):
    filepath = os.path.join(self.out_csv_folder, "extr_spatial_entities_info" + "." + consts.FILE_FORMAT_CSV)
    df_info_extr = pd.read_csv(filepath, sep=";", keep_default_na=False)
    geonames_id_list = df_info_extr[consts.COL_GEONAMES_ID].to_list()
    geonames_id_list = [int(a) for a in geonames_id_list]
    unique_geonames_id_list = np.unique(geonames_id_list)
    filepath = os.path.join(self.out_csv_folder, consts.GEONAMES_HIERARCHY_INFO_FILENAME + "." + consts.FILE_FORMAT_CSV)
    enhance_geonames_data_with_hierarchy(unique_geonames_id_list, filepath)
    








  def read_csv_files_as_dataframe(self):
    articles_filepath = os.path.join(self.input_folder, \
                           consts.PADIWEB_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_articles = [consts.COL_ID, consts.COL_TITLE, consts.COL_URL, consts.COL_SOURCE, consts.COL_PUBLISHED_TIME, \
                     consts.COL_TEXT, consts.COL_PROCESSED_TEXT, consts.COL_LANG]
                     # consts.COL_TEXT, consts.COL_DESCR, consts.COL_PROCESSED_TEXT, consts.COL_LANG, consts.COL_RSSFEED_ID
    self.df_articles = pd.read_csv(articles_filepath, usecols=cols_articles, sep=";", keep_default_na=False, encoding='utf8')
    
    sentences_filepath = os.path.join(self.input_folder, \
                           consts.PADIWEB_SENTENCES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_sentences = [consts.COL_ID,consts.COL_TEXT,consts.COL_START,consts.COL_END, \
                      consts.COL_ARTICLE_ID, consts.COL_LANG, consts.COL_SENTENCE_CLASSIF_LABEL_ID]
    self.df_sentences = pd.read_csv(sentences_filepath, usecols=cols_sentences, sep=";", keep_default_na=False, encoding='utf8')
    
    info_extr_filepath = os.path.join(self.input_folder, \
                            consts.PADIWEB_INFO_EXTRAC_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_info_extr = [consts.COL_ID, consts.COl_POS, consts.COl_TOKEN_INDEX, consts.COl_LENGTH, \
                      consts.COL_TYPE, consts.COL_LABEL, consts.COL_VALUE, \
                      consts.COL_ARTICLE_ID_RENAMED, consts.COL_KEYW_ID, consts.COL_FROM_AUTO_EXTR, \
                      consts.COL_GEONAMES_ID, consts.COL_GEONAMS_JSON] 
    self.df_info_extr = pd.read_csv(info_extr_filepath, usecols=cols_info_extr, sep=";", keep_default_na=False, encoding='utf8')
    


  # def filter_csv_files_from_bahdja_data(self): 
  #   df_articles_filtered = self.df_articles[self.df_articles["id"].isin(self.df_bahdja_articles["id"])]
  #   article_id_to_signal_id = dict(zip(self.df_bahdja_articles["id"],self.df_bahdja_articles["signal_id"]))
  #   signal_ids = [article_id_to_signal_id[x] for x in df_articles_filtered["id"]]
  #   df_articles_filtered["signal_id"] = signal_ids
  #   articles_filepath = os.path.join(self.out_csv_folder, \
  #                          consts.PADIWEB_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
  #
  #   df_info_extr_filtered = self.df_info_extr[self.df_info_extr["id_articleweb"].isin(df_articles_filtered["id"])]
  #   info_extr_filepath = os.path.join(self.out_csv_folder, \
  #                          consts.PADIWEB_INFO_EXTRAC_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
  #
  #   df_sentences_filtered = self.df_sentences[self.df_sentences["article_id"].isin(df_articles_filtered["id"])]
  #   sentences_filepath = os.path.join(self.out_csv_folder, \
  #                          consts.PADIWEB_SENTENCES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
  #
  #   df_articles_filtered.to_csv(articles_filepath, index=True, sep=";", quoting=csv.QUOTE_NONNUMERIC)
  #   df_info_extr_filtered.to_csv(info_extr_filepath, index=True, sep=";", quoting=csv.QUOTE_NONNUMERIC)
  #   df_sentences_filtered.to_csv(sentences_filepath, index=True, sep=";", quoting=csv.QUOTE_NONNUMERIC)



  def copy_csv_files_from_input(self):
    articles_curr_filepath = os.path.join(self.input_folder, \
                           consts.PADIWEB_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    articles_new_filepath = os.path.join(self.out_csv_folder, \
                           consts.PADIWEB_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    shutil.copyfile(articles_curr_filepath, articles_new_filepath)
    
    info_extr_curr_filepath = os.path.join(self.input_folder, \
                           consts.PADIWEB_INFO_EXTRAC_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    info_extr_new_filepath = os.path.join(self.out_csv_folder, \
                           consts.PADIWEB_INFO_EXTRAC_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    shutil.copyfile(info_extr_curr_filepath, info_extr_new_filepath)
    
    sentences_curr_filepath = os.path.join(self.input_folder, \
                           consts.PADIWEB_SENTENCES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    sentences_new_filepath = os.path.join(self.out_csv_folder, \
                           consts.PADIWEB_SENTENCES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)    
    shutil.copyfile(sentences_curr_filepath, sentences_new_filepath)

    
    
    
#   def aggregate_sentences_by_sentence_id(self):
#     # group sentence rows by id, and combine "classificationlabel_id" values into a list
#     df_sentences_agg = self.df_sentences.groupby([consts.COL_ID])[consts.COL_CLASSIF_LABEL_ID, \
#                                      consts.COL_ARTICLE_ID, consts.COL_START, consts.COL_END, \
#                                       consts.COL_TEXT].aggregate(lambda x: list(x))
#     #          classificationlabel_id                article_id
#     #id                                                        
#     #4197                    [20, 21]  [DEOL28QKIP, DEOL28QKIP]
#     #4198                    [20, 21]  [DEOL28QKIP, DEOL28QKIP]
#     #18031                   [11, 12]  [MO5GLFVP9X, MO5GLFVP9X]
#     #18032                   [11, 12]  [MO5GLFVP9X, MO5GLFVP9X]
#     #18033                   [11, 12]  [MO5GLFVP9X, MO5GLFVP9X]
#     #...                          ...                       ...
#     #305023356                   [11]              [3EU9K92A0G]
#
#     df_sentences_agg[consts.COL_ARTICLE_ID] = df_sentences_agg[consts.COL_ARTICLE_ID].apply(lambda x: np.unique(x)[0])
#     df_sentences_agg[consts.COL_START] = df_sentences_agg[consts.COL_START].apply(lambda x: np.unique(x)[0])
#     df_sentences_agg[consts.COL_END] = df_sentences_agg[consts.COL_END].apply(lambda x: np.unique(x)[0])
#     df_sentences_agg[consts.COL_TEXT] = df_sentences_agg[consts.COL_TEXT].apply(lambda x: np.unique(x)[0])
#     #          classificationlabel_id  article_id
#     #id                                          
#     #4197                    [20, 21]  DEOL28QKIP
#     #4198                    [20, 21]  DEOL28QKIP
#
#     df_sentences_agg = df_sentences_agg[df_sentences_agg[consts.COL_CLASSIF_LABEL_ID].map(len)>1] # remove the rows having only 1 classification label
# #### TODO    #df_sentences_agg = df_sentences_agg[df_sentences_agg[consts.COL_CLASSIF_LABEL_ID].map(len)>1] # remove the rows having only 1 classification label
#     #filepath = os.path.join(self.input_folder, consts.PADIWEB_AGG_SENTENCES_CSV_FILENAME+"."+consts.FILE_FORMAT_CSV)
#     # df_sentences_agg.to_csv(filepath, index=True, sep=";")
#     return(df_sentences_agg)
  
  

  def exists_article_id_in_sentences(self, article_id):
    if article_id in self.inverted_article_indices_for_sentences:
      return True
    return False
  
  
  def retrieve_sentence_id_by_pos(self, article_id, pos):
    indx = self.inverted_article_indices_for_sentences[article_id]-1 # minus 1, because, we will increment in each iteration of While block
    #print(article_id, pos, indx)
    while True:
      indx = indx + 1
      row = self.df_sentences.iloc[indx]
      #print(indx)
      s_id = row[consts.COL_ID]
      start = int(row[consts.COL_START])
      end = int(row[consts.COL_END])
      if pos>=start and pos<end:
        return s_id
    
    
  # Adding sentence id into Extracted Information
  def extend_info_extr_with_sentence_id_and_geoanmes_and_classif_label(self):
    geonames_ass = dict(zip(self.df_info_extr[consts.COL_GEONAMES_ID],self.df_info_extr[consts.COL_GEONAMS_JSON]))
    #>>> geonames_ass
    #{1668284.0: '{"address": "Taiwan", "class_description": "country, state, region,...", "code": "PCLI", 
    # "country": "Taiwan", "country_code": "TW", "description": "independent political entity",
    # "feature_class": "A", "geonames_id": 1668284, "lat": "24", "lng": "121", "ok": true, "population": 22894384, 
    # "raw": {"adminCode1": "00", "lng": "121", "geonameId": 1668284, "toponymName": "Taiwan", "countryId": "1668284",
    #  "fcl": "A", "population": 22894384, "countryCode": "TW", "name": "Taiwan", "fclName": "country, state, region,...", 
    # "countryName": "Taiwan", "fcodeName": "independent political entity", "adminName1": "", "lat": "24", 
    # "fcode": "PCLI"}, "state_code": "00", "status": "OK"}', ....................

    # we do "reset_index()" because after the groupby operation the grouped column becomes index.
    self.inverted_indices_for_sentences = dict(zip(self.df_sentences[consts.COL_ID], self.df_sentences.index))
    # print("index, ", self.inverted_indices_for_sentences[114641604], df_sentences_agg.iloc[self.inverted_indices_for_sentences[114641604]]) 


    # - collect sentence ids
    # - collect country codes from geonames json
    # - collect classification labels

    s_ids = []
    feature_codes = []
    classif_labels = []
    sentence_local_id_list = []
    for index, row in self.df_info_extr.iterrows():
      article_id = row[consts.COL_ARTICLE_ID_RENAMED]
      pos = row[consts.COl_POS]
      if self.exists_article_id_in_sentences(article_id):
        print("article id: ", article_id)
        s_id = self.retrieve_sentence_id_by_pos(article_id, pos)
        s_ids.append(s_id)
        s_local_id = self.sentence_id_to_local_id_ass[s_id]
        sentence_local_id_list.append(s_local_id)
        # ----
        i = self.inverted_indices_for_sentences[s_id]
        classif_label = self.df_sentences.loc[i,consts.COL_SENTENCE_CLASSIF_LABEL_ID]
        classif_labels.append(classif_label)
      else:
        s_ids.append(-1)
        classif_labels.append(-1)
        sentence_local_id_list.append(-1)
      # --------------------
      code = "-1"
      if row[consts.COL_TYPE] in ["outbreak-related-location", "GPE"]:
        geo_id = row[consts.COL_GEONAMES_ID]
        if not math.isnan(int(geo_id)) and int(geo_id) != -1:
          json_data = json.loads(geonames_ass[geo_id])
          if 'code' in json_data:
            code = json_data['code'] # country code
      feature_codes.append(code)
    
    #print(s_ids)
    self.df_info_extr[consts.COL_SENTENCE_ID] = s_ids
    self.df_info_extr[consts.COL_FEATURE_CODE] = feature_codes
    self.df_info_extr[consts.COL_SENTENCE_CLASSIF_LABEL_ID] = classif_labels
    self.df_info_extr["local_sentence_id"] = sentence_local_id_list
    
    self.df_info_extr = self.df_info_extr.sort_values(by=[consts.COL_ARTICLE_ID_RENAMED, "local_sentence_id", consts.COl_TOKEN_INDEX], ascending=True)
    
    
    
    






class PreprocessingPromed(AbstractPreprocessing):

  def perform_preprocessing(self, disease_name, input_folder, out_csv_folder, data_folder, keep_only_unofficial_data):
    self.disease_name = disease_name
    self.input_folder = input_folder
    self.out_csv_folder = out_csv_folder
    self.data_folder = data_folder
    self.keep_only_unofficial_data = keep_only_unofficial_data

    
    signal_filepath = os.path.join(self.input_folder, "raw", \
                        consts.HEALTHMAP_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    signal_info_exists = os.path.isfile(signal_filepath)

    # ===========================================================================================
    # PART 1: Handle signal ids from Bahdja
    # ===========================================================================================
    
    self.read_raw_csv_files_as_dataframe(signal_info_exists)
    self.update_articles_info()
    
    
    if signal_info_exists:
      self.read_new_articles_as_dataframe(False)
      
      paths_agg_filepath = os.path.join(self.input_folder, "raw", \
                             consts.HEALTHMAP_PATHS_AGG_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
      df_paths_agg = self.write_aggregated_paths_file_into_file(paths_agg_filepath)
      
      articles_final_filepath = os.path.join(self.out_csv_folder, \
                             consts.HEALTHMAP_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
      print(articles_final_filepath)
      super().extend_articles_info_with_signal_ids(df_paths_agg, articles_final_filepath)
    #else:
    #  self.copy_csv_files_from_input()
    
      
    # ===========================================================================================
    # PART 2: Normalize the event entities
    # ===========================================================================================

    self.read_new_articles_as_dataframe(True)
    
    # --------------------------------------------------------
    #  From the retrieved source names, we keep only unofficial ones
    # --------------------------------------------------------
    if self.keep_only_unofficial_data:
      self.df_articles = self.df_articles[~self.df_articles[consts.COL_SOURCE].isin(['wahis.oie.int','who.int'])]
    # --------------------------------------------------------
    
    #output_folder = self.out_csv_folder
    loc_list = self.retrieve_loc_list()
    
    host_list = self.retrieve_all_hosts_from_raw_text()
    
    disease_list = self.retrieve_disease_list()
      
      
      
          
    # ===========================================================================================
    # PART 3: Constructing events from extracted normalized event entities. 
    #          Since an event can be reported from multiple articles, there can be some duplicates.
    #          This is why we call them "event candidates" at this stage.
    # ===========================================================================================
    
    event_canditates = []
    counter = -1
    for index, row in self.df_articles.iterrows():
      counter += 1
      print(index, row[consts.COL_ID])
      loc = loc_list[counter]
      if loc.get_geoname_id() != -1:
        pub_date = row[consts.COL_PUBLISHED_TIME]
        t = temporality.Temporality(pub_date)
        host_info = host_list[counter]
        disease_info = disease_list[counter]
        symptom_info = symptom.Symptom()
        #sym.load_dict_data_from_str(row[consts.COL_SYMPTOM_SUBTYPE], row[consts.COL_SYMPTOM])
        e = event.Event(-1, row[consts.COL_ID], row[consts.COL_URL], \
                        row[consts.COL_SOURCE], loc, t, disease_info, host_info, symptom_info, row[consts.COL_SUMMARY], "")
        event_canditates.append(e)

    # create an empty dataframe with predefined columns
    df_event_candidates = pd.DataFrame(columns=( \
                           consts.COL_ID, consts.COL_ARTICLE_ID, consts.COL_URL, consts.COL_SOURCE, \
                          consts.COL_GEONAMES_ID, "geoname_json", "loc_name", "loc_country_code", consts.COL_LOC_CONTINENT, \
                          consts.COL_LAT, consts.COL_LNG, "hierarchy_data", consts.COL_PUBLISHED_TIME, consts.COL_DISEASE,  \
                         consts.COL_HOST, consts.COL_SYMPTOM_SUBTYPE, consts.COL_SYMPTOM, \
                          consts.COL_TITLE, consts.COL_SENTENCES
                           ))

    for indx in range(0,len(event_canditates)):
      e = event_canditates[indx]
      print(e)
      df_event_candidates.loc[indx] = e.get_event_entry() + ["", ""]
    df_event_candidates[consts.COL_ID] = df_event_candidates.index+1
    

    if signal_info_exists:
      article_id_to_signal_ids = dict(zip(self.df_articles[consts.COL_ID], self.df_articles[consts.COL_SIGNAL_ID]))
      print(article_id_to_signal_ids)
      signal_ids = [article_id_to_signal_ids[a_id] for a_id in df_event_candidates[consts.COL_ARTICLE_ID]]
      df_event_candidates[consts.COL_SIGNAL_ID] = signal_ids
    
    # REMARK: alert id is not unique, so consts.COL_ID cannot be article id
    # I perform this here, and not before, because of how I handle signal ids
    #df_event_candidates[consts.COL_ARTICLE_ID] = [str(i) for i in range(0,self.df_articles.shape[0])]  
    
    
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
                       consts.HEALTHMAP_EVENT_CANDIDATES + "." + consts.FILE_FORMAT_CSV)
    df_event_candidates.to_csv(event_candidates_filepath, index=True, sep=";", quoting=csv.QUOTE_NONNUMERIC)


    
    
    
    
    
      
  
  def read_raw_csv_files_as_dataframe(self, signal_info_exists):
    articles_filepath = os.path.join(self.input_folder, "raw", \
                           consts.PADIWEB_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_articles = ['alert_id','place_name','country','disease_name','species_name','summary', \
                 'href','issue_date','long_name','lon','lat','alert_tag','reviewed','sc','sd','cc','cd','ro']
        
    only_cols = list(pd.read_csv(articles_filepath, nrows=1, header=None, sep=";").to_numpy()[0])
    if "content" in only_cols:
      cols_articles.append("content")
    # 
    self.df_articles = pd.read_csv(articles_filepath, usecols=cols_articles, sep=";")
    
    if signal_info_exists:
      paths_filepath = os.path.join(self.input_folder, "raw", \
                             consts.HEALTHMAP_PATHS_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
      cols_paths = [consts.COL_ID,consts.COL_PATH_ID_RENAMED,'source_1','source_2','id_from_source_1','date_edge','cp_spatial', \
                    'cp_host','cp_serotype','cp_date','cp_report_date','cp_nb_of_cases','cp_nb_suc_kill']
      self.df_paths = pd.read_csv(paths_filepath, usecols=cols_paths, sep=";")
      self.df_paths = self.df_paths.rename(columns={"id_from_source_1": consts.COL_ARTICLE_ID})
    
      signal_filepath = os.path.join(self.input_folder, "raw", \
                             consts.HEALTHMAP_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
      cols_signal = [consts.COL_SIGNAL_ID_RENAMED,'ref_empresi']
      self.df_signal = pd.read_csv(signal_filepath, usecols=cols_signal, sep=";")
      
      paths_signal_filepath = os.path.join(self.input_folder, "raw", \
                             consts.HEALTHMAP_PATHS_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
      cols_paths_signal = [consts.COL_ID,consts.COL_PATH_ID_RENAMED,consts.COL_SIGNAL_ID_RENAMED]
      self.df_paths_signal = pd.read_csv(paths_signal_filepath, usecols=cols_paths_signal, sep=";")    



  def read_new_articles_as_dataframe(self, read_signal_id_column):
    signal_filepath = os.path.join(self.input_folder, "raw", \
                    consts.HEALTHMAP_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    signal_info_exists = os.path.isfile(signal_filepath)
    
    articles_filepath = os.path.join(self.out_csv_folder, \
                           consts.HEALTHMAP_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_articles = ['id','place_name','country','disease_name','species_name','summary', \
                     'url','published_at','alert_tag','reviewed','sc','sd','cc','cd','ro','source','lon','lat']
    if read_signal_id_column and signal_info_exists:
      cols_articles.append('signal_id')
      
    only_cols = list(pd.read_csv(articles_filepath, nrows=1, header=None, sep=";").to_numpy()[0])
    if "content" in only_cols:
      cols_articles.append("content")
      
    self.df_articles = pd.read_csv(articles_filepath, usecols=cols_articles, sep=";")
    
    
  def read_initial_articles_as_dataframe(self):
    signal_filepath = os.path.join(self.input_folder, \
                    consts.PADIWEB_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    signal_info_exists = os.path.isfile(signal_filepath)
    
    articles_filepath = os.path.join(self.input_folder, "raw", \
                           consts.PADIWEB_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_articles = ['alert_id','place_name','country','disease_name','species_name','summary', \
                     'href','issue_date','alert_tag','reviewed','sc','sd','cc','cd','ro','long_name','lon','lat']
    if signal_info_exists:
      cols_articles.append('signal_id')
      
    only_cols = list(pd.read_csv(articles_filepath, nrows=1, header=None, sep=";").to_numpy()[0])
    if "content" in only_cols:
      cols_articles.append("content")
      
    self.df_articles = pd.read_csv(articles_filepath, usecols=cols_articles, sep=";")
    #print(self.df_articles['issue_date'])



  def copy_csv_files_from_input(self):
    articles_curr_filepath = os.path.join(self.input_folder, \
                           consts.HEALTHMAP_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    articles_new_filepath = os.path.join(self.out_csv_folder, \
                           consts.HEALTHMAP_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    shutil.copyfile(articles_curr_filepath, articles_new_filepath)
    
    
    
  def update_articles_info(self):
    self.read_initial_articles_as_dataframe()
    
    # self.df_articles = self.df_articles[self.df_articles["alert_tag"] == "Breaking"] # TODO
    self.df_articles = self.df_articles[self.df_articles["species_name"] != "Humans"]
    
    self.df_articles = self.df_articles.rename(columns={"alert_id": consts.COL_ID})
    self.df_articles = self.df_articles.rename(columns={"href": consts.COL_URL})
    self.df_articles = self.df_articles.rename(columns={"long_name": consts.COL_SOURCE})
    self.df_articles = self.df_articles.rename(columns={"issue_date": consts.COL_PUBLISHED_TIME})
    # self.df_articles = self.df_articles[self.df_articles["disease_name"].str.contains("avian influenza", case=False)]
    
    self.df_articles = self.df_articles[self.df_articles["place_name"] != "Worldwide"]
    self.df_articles = self.df_articles[self.df_articles["place_name"] != "Europe"]

    
    # retrieve source names from the corresponding urls
    # --------------------------------------------------------
    urls = self.retrieve_all_url_from_raw_text()
    self.df_articles[consts.COL_URL] = urls
    for url in urls:
      print(url)
      print(url, self.trim_url_custom(url))
    source_names = [self.trim_url_custom(url) for url in urls]
    self.df_articles[consts.COL_SOURCE] = source_names
    
    articles_filepath = os.path.join(self.out_csv_folder, \
                               consts.HEALTHMAP_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    self.df_articles.to_csv(articles_filepath, index=True, sep=";", quoting=csv.QUOTE_NONNUMERIC)

  
  
  
  def retrieve_loc_list(self):
    filepath = os.path.join(self.out_csv_folder, "geonames_results_in_batch" + "." + consts.FILE_FORMAT_CSV)
    if not os.path.exists(filepath):
      # create an empty file 
      columns = [str(i) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] + ["text"]
      df = pd.DataFrame(columns=columns)
      df.to_csv(filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    #
    df_batch_geonames_results = pd.read_csv(filepath, sep=";", keep_default_na=False)
    inverted_indices_for_df_batch_geonames_results = dict(zip(df_batch_geonames_results["text"], df_batch_geonames_results.index))
  
  
    filepath = os.path.join(self.out_csv_folder, consts.GEONAMES_HIERARCHY_INFO_FILENAME + "." + consts.FILE_FORMAT_CSV)
    if not os.path.exists(filepath):
      # create an empty file 
      columns = [consts.COL_GEONAMES_ID, "hierarchy_name", "hierarchy_geoname_id"] 
      df = pd.DataFrame(columns=columns)
      df.to_csv(filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    #
    df_geonames_hierarchy = pd.read_csv(filepath, sep=";", keep_default_na=False)
    geonames_hierarchy_ass = dict(zip(df_geonames_hierarchy[consts.COL_GEONAMES_ID], df_geonames_hierarchy["hierarchy_geoname_id"]))
  
    # ----------------------------
    location_list = []
    place_names = self.df_articles["place_name"].to_list()
    for index, place_name in enumerate(place_names):
      #print("---------------", index, place_name)
      res = {"geonamesId": "-1", "name": "-1", "country_code": "-1", "raw_data": "-1"}
      # ==================================================
      # retrieve the results from our database
      if place_name not in inverted_indices_for_df_batch_geonames_results:
        df_batch, inverted_indices = self.update_geocoding_results_in_DB(self.out_csv_folder, place_name)
        df_batch_geonames_results = df_batch
        inverted_indices_for_df_batch_geonames_results = inverted_indices
  
      gindx = inverted_indices_for_df_batch_geonames_results[place_name]
      row_geonames_results = df_batch_geonames_results.iloc[gindx]
      # print(row_geonames_results["0"])
      # {"adminCode1": "00", "lng": "34.75", "geonameId": 294640, "toponymName": "State of Israel", "countryId": "294640", "fcl": "A", "population": 8883800, "countryCode": "IL", "name": "Israel", "fclName": "country, state, region,...", "countryName": "Israel", "fcodeName": "independent political entity", "adminName1": "", "lat": "31.5", "fcode": "PCLI"}
  
  
      loc = location.Location(place_name, -1, -1, -1, -1, -1, -1, {})
      if row_geonames_results["0"] == "-1":
        print("Error in geonames with place name=",place_name)
      if row_geonames_results["0"] != "-1":
        geonames_results = [json.loads(row_geonames_results[str(i)]) for i in range(consts.MAX_NB_LOCS_PER_GEONAMES_REQUEST)] # we access the columns
        geoname_json = geonames_results[0] # get the first result from geonames
        if "countryCode" in geoname_json: # it is not a continent or sea
          geoname_id = geoname_json["geonameId"]
          # ---------------------------------------
          if geoname_id not in geonames_hierarchy_ass:
              df_geonames_hierarchy, geonames_hierarchy_ass = self.update_geonames_hierarhcy_results_in_DB(self.out_csv_folder, geoname_id)
          #print("DB ACCESS!!!!")
          hierarchy_data = eval(geonames_hierarchy_ass[geoname_id])
          # ---------------------------------------
          name = geoname_json["toponymName"]
          lat = geoname_json["lat"]
          lng = geoname_json["lng"]
          fcode = geoname_json["fcode"]
          country_code_alpha2 = geoname_json["countryCode"]
          country_code_alpha3 = countries.get(country_code_alpha2).alpha3
          continent = retrieve_continent_from_country_code(country_code_alpha3)
          loc = location.Location(name, geoname_id, geoname_json, lat, lng, country_code_alpha3, continent, hierarchy_data)
  
      location_list.append(loc)
    return(location_list)




  

  def retrieve_disease_list(self):
    disease_list = []
    disease_detail_list = self.df_articles["disease_name"].to_list()
    for disease_detail_text in disease_detail_list:
      disease_info = retrieve_disease_from_raw_sentence(disease_detail_text, self.disease_name) # it can be None
      disease_list.append(disease_info)
    return(disease_list)
      



  def retrieve_all_url_from_raw_text(self):
    size = self.df_articles.shape[0]
    unique_result = np.unique(self.df_articles["summary"], return_index=True, return_counts=True)
    unique_indexes = unique_result[1]
    unique_counts = unique_result[2]
    nb_unique = len(unique_indexes)
    source_list = np.empty_like(self.df_articles["summary"])
    counter = -1
    for i in range(nb_unique):
      index = unique_indexes[i]
      count = unique_counts[i]
      #print("index: ",index,", count: ",count)
      row = self.df_articles.iloc[index]
      #print("index", index)
      # res = re.sub('Avian Influenza', '', d_name, count=1).strip()
      place_name = row["place_name"]
      summary = row["summary"]
      #print(summary)
      content = row["content"]
      content_language = "EN"
      #if "ProMED-RUS" in content or content.count("Источник:")>0:
      if content.count("Источник:")>0:
        content_language = "RU"
      #elif "ProMED-PORT" in content or content.count("Fonte:")>0:
      elif content.count("Fonte:")>0:
        content_language = "PT"
      #elif "ProMED-ESP" in content or content.count("Fuente:")>0:
      elif content.count("Fuente:")>0:
        content_language = "ESP"
      content_lines = content.split("\n")
      nb_outbreaks = self.retrieve_nb_outbreaks_from_raw_text(content, content_language)
      urls = self.retrieve_urls_from_raw_text(content_lines, content_language)
      #print(nb_outbreaks)
      if nb_outbreaks >= count or (nb_outbreaks < count and nb_outbreaks == 1):
        source_list[np.arange(index,index+count)] = list(np.array(urls)[0:count])
      if nb_outbreaks < count and ("] ECDC update" in content or "] ECDC situation update" in content): # mostly for west nile virus
        # in the news, where "ECDC update" appears, usually another source reports another outbreak
        # so, here, we want to identify the order of these two sources
        if "[1] ECDC update" in content or "[1] ECDC situation update" in content:
          source_list[np.arange(index,index+count-1)] = urls[0]
          source_list[np.arange(index+count-1,index+count)] = urls[-1]
        else:
          source_list[np.arange(index,index+1)] = urls[0]
          source_list[np.arange(index+1,index+count)] = urls[1]
      # if summary == "PRO/RUS> Лихорадка Западного Нила - Россия (Воронежская область)":
      #   print("index", index)
      #   print(nb_outbreaks)
      #   print(urls)
      # else:
      #   print("index", index)
      #   print(count)
      #   print(nb_outbreaks)
      #   print(urls)
      # sources  = self.todo(urls)
      #print(nb_outbreaks)
    return(list(source_list))


      
      
  def retrieve_nb_outbreaks_from_raw_text(self, content, language):
    if language == "EN":
      nb_outbreaks = len(re.findall("\nSource: [0-9a-zA-Z]", content)) # sentences starting with "Source:", followed by some characters
    elif language == "RU":
      nb_outbreaks = len(re.findall("\nИсточник: [0-9a-zA-ZА-Яа-яЁё]", content))
      if nb_outbreaks == 0:
        nb_outbreaks = len(re.findall("\n\([0-9]\) Источник: [0-9a-zA-ZА-Яа-яЁё]", content))
    elif language == "PT":
      nb_outbreaks = len(re.findall("\nFonte: [0-9a-zA-Z]", content))
    elif language == "ESP":
      nb_outbreaks = len(re.findall("\nFuente: [0-9a-zA-Z]", content))
    return(nb_outbreaks)
  
  

  def retrieve_urls_from_raw_text(self, lines, language):
    do = False
    urls = []
    for line in lines:
      if do and line.count("http")>0:
        res = re.sub('<', '', line, count=1).strip()
        url = re.sub('>', '', res, count=1).strip()
        urls.append(url)
        do = False
        
      if do == False and language == "EN" and len(re.findall("^Source: [0-9a-zA-Z]", line))>0:
        do = True
      elif do == False and language == "RU" and (len(re.findall("^\([0-9]\) Источник: [0-9a-zA-ZА-Яа-яЁё]", line))>0 or len(re.findall("^Источник: [a-zA-ZА-Яа-яЁё]", line))>0):
        do = True
      elif do == False and language == "PT" and len(re.findall("^Fonte: [0-9a-zA-Z]", line))>0:
        do = True
      elif do == False and language == "ESP" and len(re.findall("^Fuente: [0-9a-zA-Z]", line))>0:
        do = True
    
    # post-processing for shortened url such as tiny url
    urls2 = []
    for url in urls:
      if "tinyurl" in url or "cutt.ly" in url or "bit.ly" in url:
        urls2.append(requests.head(url).headers['location'])
      else:
        urls2.append(url)
    return(urls2)  



  def retrieve_host_list(self):
    host_list = []
    host_detail_list = self.df_articles["species_name"].to_list()
    print(host_detail_list)
    for host_detail_text in host_detail_list:
      host_info = self.retrieve_hosts_from_raw_text_for_single_outbreak(host_detail_text, False) # it can be None
      host_list.append(host_info)
    return(host_list)  
  
  
  
  def retrieve_all_hosts_from_raw_text(self):
    size = self.df_articles.shape[0]
    unique_result = np.unique(self.df_articles["summary"], return_index=True, return_counts=True)
    unique_indexes = unique_result[1]
    unique_counts = unique_result[2]
    nb_unique = len(unique_indexes)
    host_list = np.empty_like(self.df_articles["summary"])
    counter = -1
    for i in range(nb_unique):
      index = unique_indexes[i]
      count = unique_counts[i]
      #print("index: ",index,", count: ",count)
      row = self.df_articles.iloc[index]
      #print("index", index)
      # res = re.sub('Avian Influenza', '', d_name, count=1).strip()
      country = row["country"]
      summary = row["summary"]
      print("------------", i, row[consts.COL_ID], summary)
      url = row["url"]
      #print(summary)
      content = row["content"]
      content_language = "EN"
      #if "ProMED-RUS" in content or content.count("Источник:")>0:
      if content.count("Источник:")>0:
        content_language = "RU"
      #elif "ProMED-PORT" in content or content.count("Fonte:")>0:
      elif content.count("Fonte:")>0:
        content_language = "PT"
      #elif "ProMED-ESP" in content or content.count("Fuente:")>0:
      elif content.count("Fuente:")>0:
        content_language = "ESP"
      content_lines = content.split("\n")
      hosts = []
      nb_outbreaks = self.retrieve_nb_outbreaks_from_raw_text(content, content_language)
      if nb_outbreaks>1:
        # get the list of countries from the rows
        country_list = list(self.df_articles.iloc[np.arange(index,index+count)]["country"].to_numpy())
        #print(country_list)
        country_list = [c.lower() for c in country_list if not pd.isnull(c)]
        hosts = self.retrieve_hosts_from_raw_text_for_multiple_outbreaks(content_lines, country_list, content_language, nb_outbreaks)
        #print("----", count, nb_outbreaks, hosts)
      else:
        host_info = self.retrieve_hosts_from_raw_text_for_single_outbreak(summary)
        if host_info.is_host_info_empty() or \
          (not host_info.is_host_info_empty() and host_info.get_max_hierarchy_level() == 0):
          host_info = self.retrieve_hosts_from_first_line_after_source_url(content_lines, content_language)
          if not host_info.is_host_info_empty():
            hosts = [host_info]
      if len(hosts) == 0: # the worst case (1)
        host_info = self.retrieve_hosts_from_raw_text_for_single_outbreak(url, False)
        nb_outbreaks = 1
        if not host_info.is_host_info_empty():
          hosts = [host_info]
      if len(hosts) == 0: # the worst case (2)
        host_info = self.retrieve_hosts_from_raw_text_for_single_outbreak(summary)
        nb_outbreaks = 1
        if not host_info.is_host_info_empty():
          hosts = [host_info]
      if len(hosts) == 0: # the worst case (3)
        host_info = self.retrieve_hosts_from_raw_text_for_single_outbreak(content)
        if not host_info.is_host_info_empty():
          hosts = [host_info]
      if len(hosts) == 0 and self.disease_name == consts.DISEASE_WEST_NILE_VIRUS: # the worst case (4) >> by deafult: human
        host_info = self.retrieve_hosts_from_raw_text_for_single_outbreak("human", False)
        if not host_info.is_host_info_empty():
          hosts = [host_info]
          
      #print(count)
      #print(hosts)
      if len(hosts)>0 and (nb_outbreaks >= count or (nb_outbreaks < count and nb_outbreaks == 1)):
        #print(hosts)
        host_list[np.arange(index,index+count)] = list(np.array(hosts)[0:count])
      elif nb_outbreaks>1 and nb_outbreaks < count and ("] ECDC update" in content or "] ECDC situation update" in content): # mostly for west nile virus
        # in the news, where "ECDC update" appears, usually another source reports another outbreak
        # so, here, we want to identify the order of these two sources
        if "[1] ECDC update" in content or "[1] ECDC situation update" in content:
          host_list[np.arange(index,index+count-1)] = hosts[0]
          host_list[np.arange(index+count-1,index+count)] = hosts[-1]
        else:
          host_list[np.arange(index,index+1)] = hosts[0]
          host_list[np.arange(index+1,index+count)] = hosts[1]
          
    return(list(host_list))
  
  
          

  def retrieve_hosts_from_raw_text_for_single_outbreak(self, text, add_space_for_search=True):
    host_info = retrieve_host_from_raw_sentence(text, add_space_for_search)
    #print(text, " ==> ", host_info)
    return host_info
  
  
  
  def retrieve_hosts_from_raw_text_for_multiple_outbreaks(self, lines, country_list, language, nb_outbreaks):
    # even though the csv file contains x oubtreak information, in the raw text there can be less or more than x.
    # this is why we work with country_list and we try to bind the information from the csv file with the information in the raw text
    # The variable 'count' indicates how many times such a country is repeated in the csv file for the same news
    #
    at_least_one_info = False # in some cases, the host info can be found on some outbreaks, but not for all
    default_host_info = self.retrieve_hosts_from_raw_text_for_single_outbreak(lines[0]) # from first line, which corresponds to the summary
    if default_host_info.is_host_info_empty():
      if self.disease_name == consts.DISEASE_WEST_NILE_VIRUS:
        default_host_info = self.retrieve_hosts_from_raw_text_for_single_outbreak("human", False)
      elif self.disease_name == consts.DISEASE_AVIAN_INFLUENZA:
        default_host_info = self.retrieve_hosts_from_raw_text_for_single_outbreak("bird", False)
    do = False
    hosts = []
    for line in lines:
      if do == True:
        #print(line)
        country = self.retrieve_country_info_from_line(line, country_list)
        count = 1
        if country is not None and nb_outbreaks != len(country_list):
          count = country_list.count(country)
          #print(country, count)
        host_info = self.retrieve_hosts_from_raw_text_for_single_outbreak(line)
        if not host_info.is_host_info_empty():
          hosts = hosts + [host_info]*count 
          at_least_one_info = True
        else:
          hosts = hosts + [default_host_info]*count 
        #print(hosts)
          
      if do == True and line == "":
        do = False
      
      if do == False and line == "In this update:" or line == "In this post:" or line == "In this posting:":
        do = True

    if at_least_one_info:
      return hosts
    return []
      
      
  def retrieve_country_info_from_line(self, line, country_list):
    line = line.lower()
    for country in country_list:
      res = re.findall(country, line)
      if len(res)>0:
        return country
    return None
  
  
  
  # For now, it retrieves only a single host
  # in this code, we suppose that there is an empty line between the url and the first sentence
  def retrieve_hosts_from_first_line_after_source_url(self, lines, language):
    #print("\n!!!!!!!!!!!!!-------------!!!!!!!!!!!!!!--------------------!!!!!!!!!!!!!")
    #print("retrieve_hosts_from_first_line_after_source_url()")
    urls = self.retrieve_urls_from_raw_text(lines, language)
    url = urls[0] # it is supposed to be a single item
    do = False
    h = host.Host() # by default
    skip = True
    for line in lines:
      if do == True:
        #print("------")
        #print(line)
        if not skip and line != "":
          #print(line)
          h = self.retrieve_hosts_from_raw_text_for_single_outbreak(line)
          break
        
        if line == "": # we seek for an empty line between the url and the first sentence
          skip = False
          
      if do == False and url in line:
        do = True
    
    return h
   
    
    
  
  
  
class PreprocessingEmpresi(AbstractPreprocessing):

  
  def perform_preprocessing(self, disease_name, input_folder, out_csv_folder, data_folder):
    self.disease_name = disease_name
    self.input_folder = input_folder
    self.out_csv_folder = out_csv_folder
    self.data_folder = data_folder
    
    input_events_filepath = os.path.join(self.input_folder, "outbreaks.csv")
    event_candidates_filepath = os.path.join(self.out_csv_folder, \
                                consts.EMPRESI_EVENT_CANDIDATES + "." + consts.FILE_FORMAT_CSV)
    cols_event_candidates = [consts.COL_ID, consts.COL_LOC_CITY, consts.COL_LOC_REGION, consts.COL_LOC_COUNTRY, "Region", \
                           "ReportDate", "Serotype", consts.COL_DISEASE, consts.COL_HOST, consts.COL_LAT, consts.COL_LNG]
    self.df_raw_events = pd.read_csv(input_events_filepath, usecols=cols_event_candidates, sep=";", keep_default_na=False)
    self.df_raw_events = self.df_raw_events.rename(columns={"Region": consts.COL_LOC_CONTINENT})
    self.df_raw_events = self.df_raw_events.rename(columns={"ReportDate": consts.COL_PUBLISHED_TIME})
    self.df_raw_events = self.df_raw_events.rename(columns={"Serotype": consts.COL_DISEASE_SUBTYPE})
    
    
    # ===========================================================================================
    # PART 2: Normalize the event entities
    # ===========================================================================================

    self.search_geonames_for_admin1_loc_list(self.out_csv_folder, self.df_raw_events)
    loc_list = self.retrieve_loc_list(self.out_csv_folder, self.df_raw_events)
    
    host_list = self.retrieve_host_list()
    
    disease_list = self.retrieve_disease_list()
        
        
        
    # ===========================================================================================
    # PART 3: Constructing events from extracted normalized event entities. 
    #          Since an event can be reported from multiple articles, there can be some duplicates.
    #          This is why we call them "event candidates" at this stage.
    # ===========================================================================================

    events = []
    counter = -1
    for index, row in self.df_raw_events.iterrows():
      counter += 1
      print(index, row[consts.COL_ID])
      loc = loc_list[counter]
      if loc.get_geoname_id() != -1:
        host_info = host_list[counter]
        if not host_info.is_host_info_empty():
          url = "-1"
          source = "-1"
          pub_date = row[consts.COL_PUBLISHED_TIME]
          t = temporality.Temporality(pub_date)
          
          disease_info = disease_list[counter]
          symptom_info = symptom.Symptom()
          #sym.load_dict_data_from_str(row[consts.COL_SYMPTOM_SUBTYPE], row[consts.COL_SYMPTOM])
          e = event.Event(-1, row[consts.COL_ID], url, \
                          source, loc, t, disease_info, host_info, symptom_info, "", "")
          events.append(e)


    df_events = pd.DataFrame(columns=( \
                           consts.COL_ID, consts.COL_ARTICLE_ID, consts.COL_URL, consts.COL_SOURCE, \
                          consts.COL_GEONAMES_ID, "geoname_json", "loc_name", "loc_country_code", consts.COL_LOC_CONTINENT, \
                          consts.COL_LAT, consts.COL_LNG, "hierarchy_data", consts.COL_PUBLISHED_TIME, consts.COL_DISEASE,  \
                         consts.COL_HOST, consts.COL_SYMPTOM_SUBTYPE, consts.COL_SYMPTOM, \
                          consts.COL_TITLE, consts.COL_SENTENCES
                           ))

    for indx in range(0,len(events)):
      e = events[indx]
      print(e)
      df_events.loc[indx] = e.get_event_entry() + ["", ""]
    df_events[consts.COL_ID] = df_events.index+1

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
    
    events_filepath = os.path.join(self.out_csv_folder, \
                       consts.EMPRESI_EVENTS + "." + consts.FILE_FORMAT_CSV)
    df_events.to_csv(events_filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    




  def retrieve_host_list(self):
    host_list = []
    host_detail_list = self.df_raw_events["host"].to_list()
    for host_detail_text in host_detail_list:
      disease_info = retrieve_host_from_raw_sentence(host_detail_text, False) # it can be None
      host_list.append(disease_info)
    return(host_list)
  
  
  
  def retrieve_disease_list(self):
    disease_list = []
    self.df_raw_events[consts.COL_DISEASE+"2"] = self.df_raw_events[consts.COL_DISEASE] + " " + self.df_raw_events[consts.COL_DISEASE_SUBTYPE] 
    disease_detail_list = self.df_raw_events[consts.COL_DISEASE+"2"].to_list()
    for disease_detail_text in disease_detail_list:
      disease_info = retrieve_disease_from_raw_sentence(disease_detail_text, self.disease_name) # it can be None
      disease_list.append(disease_info)
    return(disease_list)



        



class PreprocessingBadhjaSignals(PreprocessingEmpresi):


  def perform_preprocessing(self, disease_name, input_folder, out_csv_folder, out_padiweb_csv_folder, data_folder):
    self.disease_name = disease_name
    self.input_folder = input_folder
    self.out_csv_folder = out_csv_folder
    self.out_padiweb_csv_folder = out_padiweb_csv_folder
    self.data_folder = data_folder
    
    # ===================================================
    
    self.read_raw_csv_files_as_dataframe()

    paths_agg_filepath = os.path.join(self.out_csv_folder, \
                           consts.PADIWEB_BAHDJA_PATHS_AGG_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    df_paths_agg = self.write_aggregated_paths_file_into_file(paths_agg_filepath)

    self.df_articles = self.df_bahdja_articles[self.df_bahdja_articles[consts.COL_TYPE] == "relevant"]
    
    articles_final_filepath = os.path.join(self.out_csv_folder, \
                           consts.PADIWEB_BAHDJA_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    super().extend_articles_info_with_signal_ids(df_paths_agg, articles_final_filepath) # >> bahdja articles
    
    articles_filepath = os.path.join(self.out_csv_folder, \
                       consts.PADIWEB_BAHDJA_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_articles = [consts.COL_ID, consts.COL_TITLE, consts.COL_URL, consts.COL_SOURCE, consts.COL_PUBLISHED_TIME, consts.COL_SIGNAL_ID]
                     # consts.COL_TEXT, consts.COL_DESCR, consts.COL_PROCESSED_TEXT, consts.COL_LANG, consts.COL_RSSFEED_ID
    self.df_bahdja_articles = pd.read_csv(articles_filepath, usecols=cols_articles, sep=";")
    
    signal_curr_filepath = os.path.join(self.input_folder, \
                           consts.PADIWEB_BAHDJA_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    signal_new_filepath = os.path.join(self.out_csv_folder, \
                           consts.PADIWEB_BAHDJA_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    shutil.copyfile(signal_curr_filepath, signal_new_filepath)
    
    #self.filter_csv_files_from_bahdja_data() # write the data frames (e.g. articles, etc.) into file
    
    # ======================================================
    
    """
    input_events_filepath = os.path.join(self.out_csv_folder, "signal_bahdja.csv")
    cols_event_candidates = ["id_signal", consts.COL_LOC_CITY, consts.COL_LOC_REGION, consts.COL_LOC_COUNTRY, "Region", \
                           "ReportDate", "Serotype", consts.COL_DISEASE, consts.COL_HOST, consts.COL_LAT, consts.COL_LNG]
    self.df_raw_events = pd.read_csv(input_events_filepath, usecols=cols_event_candidates, sep=";", keep_default_na=False)
    self.df_raw_events = self.df_raw_events.rename(columns={"Region": consts.COL_LOC_CONTINENT})
    self.df_raw_events = self.df_raw_events.rename(columns={"ReportDate": consts.COL_PUBLISHED_TIME})
    self.df_raw_events = self.df_raw_events.rename(columns={"Serotype": consts.COL_DISEASE_SUBTYPE})
    
    
    # ===========================================================================================
    # PART 2: Normalize the event entities
    # ===========================================================================================

    self.search_geonames_for_admin1_loc_list(self.out_csv_folder, self.df_raw_events)
    loc_list = self.retrieve_loc_list(self.out_csv_folder, self.df_raw_events)
    
    host_list = self.retrieve_host_list()
    
    disease_list = self.retrieve_disease_list()
    """    
        
        
        

    # bahdja_articles_filepath = os.path.join(self.out_padiweb_csv_folder, \
    #                          consts.PADIWEB_BAHDJA_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    # df_bahdja_articles = pd.read_csv(bahdja_articles_filepath, sep=";", keep_default_na=False)
    # signal_id_to_article_id = {}
    # for index, row in df_bahdja_articles.iterrows():
    #   article_id = row[consts.COL_ID]
    #   signal_ids = eval(row[consts.COL_SIGNAL_ID])
    #   for signal_id in signal_ids:
    #     if not signal_id in signal_id_to_article_id:
    #       signal_id_to_article_id[signal_id] = []
    #     signal_id_to_article_id[signal_id].append(article_id)
    #
    #
    # input_events_filepath = os.path.join(self.input_folder, consts.PADIWEB_SIGNAL_CSV_FILENAME + "_bahdja" +".csv")
    # cols_events = [consts.COL_SIGNAL_ID_RENAMED, consts.COL_LOC_COUNTRY, \
    #                        consts.COL_SIGNAL_DATE, "details", consts.COL_DISEASE, consts.COL_HOST]
    # self.df_raw_events = pd.read_csv(input_events_filepath, usecols=cols_events, sep=";", keep_default_na=False)
    # self.df_raw_events = self.df_raw_events.rename(columns={"details": consts.COL_DISEASE_SUBTYPE})
    # self.df_raw_events = self.df_raw_events.rename(columns={consts.COL_SIGNAL_ID_RENAMED: consts.COL_ID})
    # self.df_raw_events = self.df_raw_events.rename(columns={consts.COL_SIGNAL_DATE: consts.COL_PUBLISHED_TIME})
    # #self.df_raw_events[consts.COL_DISEASE] = self.df_raw_events[consts.COL_DISEASE] + " " + self.df_raw_events[consts.COL_DISEASE_SUBTYPE]
    #
    # print(self.out_csv_folder)
    # loc_list = self.retrieve_loc_list(self.out_csv_folder, self.df_raw_events)
    # host_list = self.retrieve_host_list()
    # disease_list = self.retrieve_disease_list()
    #
    # events = []
    # counter = -1
    # for index, row in self.df_raw_events.iterrows():
    #   counter += 1
    #   print(index, row[consts.COL_ID])
    #   loc = loc_list[counter]
    #   if loc.get_geoname_id() != -1:
    #     host_info = host_list[counter]
    #     if not host_info.is_host_info_empty():
    #       url = "-1"
    #       source = "-1"
    #       pub_date = row[consts.COL_PUBLISHED_TIME]
    #       t = temporality.Temporality(pub_date)
    #
    #       disease_info = disease_list[counter]
    #       symptom_info = symptom.Symptom()
    #       #sym.load_dict_data_from_str(row[consts.COL_SYMPTOM_SUBTYPE], row[consts.COL_SYMPTOM])
    #       e = event.Event(-1, row[consts.COL_ID], url, \
    #                       source, loc, t, disease_info, host_info, symptom_info, "", "")
    #       events.append(e)
    #
    #
    # df_events = pd.DataFrame(columns=( \
    #                        consts.COL_ID, consts.COL_ARTICLE_ID, consts.COL_URL, consts.COL_SOURCE, \
    #                       consts.COL_GEONAMES_ID, "geoname_json", "loc_name", "loc_country_code", consts.COL_LOC_CONTINENT, \
    #                       consts.COL_LAT, consts.COL_LNG, "hierarchy_data", consts.COL_PUBLISHED_TIME, consts.COL_DISEASE,  \
    #                      consts.COL_HOST, consts.COL_SYMPTOM_SUBTYPE, consts.COL_SYMPTOM, \
    #                       consts.COL_TITLE, consts.COL_SENTENCES
    #                        ))
    #
    # for indx in range(0,len(events)):
    #   e = events[indx]
    #   print(e)
    #   df_events.loc[indx] = e.get_event_entry() + ["", ""]
    #
    # # currently, the column "article_id" contains the signal id
    # df_events[consts.COL_SIGNAL_ID] = df_events[consts.COL_ARTICLE_ID]
    # df_events[consts.COL_ARTICLE_ID] = df_events[consts.COL_ARTICLE_ID].apply(lambda id: str(signal_id_to_article_id[int(id)]) if int(id) in signal_id_to_article_id else "")
    # df_events[consts.COL_ID] = df_events.index+1
    #
    # # --------------------------------
    # # add time relate columns
    # dates = pd.to_datetime(df_events[consts.COL_PUBLISHED_TIME]).to_list()
    # day_no_list, week_no_list, biweek_no_list, month_no_list, year_list, season_list = util.retrieve_time_related_info(dates)
    # df_events["day_no"] = day_no_list
    # df_events["week_no"] = week_no_list
    # df_events["biweek_no"] = biweek_no_list
    # df_events["month_no"] = month_no_list
    # df_events["year"] = year_list
    # df_events["season"] = season_list
    # # --------------------------------
    #
    # events_filepath = os.path.join(self.out_csv_folder, \
    #                             consts.PADIWEB_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    # df_events.to_csv(events_filepath, index=False, sep=";", quoting=csv.QUOTE_NONNUMERIC) 



  def read_raw_csv_files_as_dataframe(self):
    bahdja_articles_filepath = os.path.join(self.input_folder, \
                         consts.PADIWEB_BAHDJA_ARTICLES_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_bahdja_articles = ['type','id','title','url','source','text', 'published_at','treated']
                     # consts.COL_TEXT, consts.COL_DESCR, consts.COL_PROCESSED_TEXT, consts.COL_LANG, consts.COL_RSSFEED_ID
    self.df_bahdja_articles = pd.read_csv(bahdja_articles_filepath, usecols=cols_bahdja_articles, sep=";", keep_default_na=False, encoding='utf8')
    self.df_bahdja_articles = self.df_bahdja_articles.rename(columns={"alert_id": consts.COL_ID})
    
    paths_filepath = os.path.join(self.input_folder, \
                           consts.PADIWEB_BAHDJA_PATHS_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_paths = [consts.COL_ID,consts.COL_PATH_ID_RENAMED,'source_1','source_2','id_from_source_1','date_edge','cp_spatial', \
                  'cp_host','cp_serotype','cp_date','cp_report_date','cp_nb_of_cases','cp_nb_suc_kill']
    self.df_paths = pd.read_csv(paths_filepath, usecols=cols_paths, sep=";", keep_default_na=False, encoding='utf8')
    self.df_paths = self.df_paths.rename(columns={"id_from_source_1": consts.COL_ARTICLE_ID})
  
    signal_filepath = os.path.join(self.input_folder, \
                           consts.PADIWEB_BAHDJA_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_signal = [consts.COL_SIGNAL_ID_RENAMED,'ref_empresi']
    self.df_signal = pd.read_csv(signal_filepath, usecols=cols_signal, sep=";", keep_default_na=False, encoding='utf8')
    
    paths_signal_filepath = os.path.join(self.input_folder, \
                           consts.PADIWEB_BAHDJA_PATHS_SIGNAL_CSV_FILENAME + "." + consts.FILE_FORMAT_CSV)
    cols_paths_signal = [consts.COL_ID,consts.COL_PATH_ID_RENAMED,consts.COL_SIGNAL_ID_RENAMED]
    self.df_paths_signal = pd.read_csv(paths_signal_filepath, usecols=cols_paths_signal, sep=";", keep_default_na=False, encoding='utf8')  
    
