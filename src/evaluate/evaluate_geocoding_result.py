'''
Created on Sep 26, 2022

@author: nejat
'''

import src.event.location as location

import os
import pandas as pd
import numpy as np
import json
import csv

from src.util_gis import retrieve_continent_from_country_code


def prepare_loc_object(geoname_json, geonames_hierarchy_ass):
  name = geoname_json["toponymName"]
  geoname_id = geoname_json["geonameId"]
  lat = geoname_json["lat"]
  lng = geoname_json["lng"]
  fcode = "-1"
  if "fcode" in geoname_json:
    fcode = geoname_json["fcode"]
  elif "code" in geoname_json:
    fcode = geoname_json["code"]
  country_code = geoname_json["countryCode"]
  continent = retrieve_continent_from_country_code(country_code)
  hierarchy_data = eval(geonames_hierarchy_ass[geoname_id])
  loc = location.Location(name, geoname_id, geoname_json, lat, lng, country_code, continent, hierarchy_data)
  return(loc)


def evaluate_spatial_info_extr_result_at_article_level(df_estimated, df_true, geonames_id_json_ass, geonames_hierarchy_ass):
  df_estimated = df_estimated.drop_duplicates(subset=['geonames_id'])
  df_true = df_true.drop_duplicates(subset=['geonamesId'])
  estimated_locs = df_estimated["geonames_json"].apply(lambda geonames_json_str: prepare_loc_object(json.loads(geonames_json_str), geonames_hierarchy_ass)).to_list()
  true_locs = df_true["geonamesId"].apply(lambda geoname_id: prepare_loc_object(json.loads(geonames_id_json_ass[geoname_id]), geonames_hierarchy_ass)).to_list()
  print(estimated_locs)
  print(true_locs)
  count = 0
  # how many true locs are included in our results ?
  for true_loc in true_locs:
    for est_loc in estimated_locs:
      if true_loc.is_identical(est_loc):
        count += 1
  score = 0
  if len(true_locs)>0:
    score = count/len(true_locs)
  #print(df_estimated)
  #print(df_true)
  return count, len(true_locs)



def evaluate_spatial_info_extr_result_at_article_level_country_approx(df_estimated, df_true, geonames_id_json_ass, geonames_hierarchy_ass):
  #df_estimated = df_estimated[df_estimated["type"] != "GPE"]
  df_estimated["countryCode"] = df_estimated["geonames_json"].apply(lambda geonames_json_str: json.loads(geonames_json_str)["countryCode"])
  df_estimated = df_estimated.drop_duplicates(subset=['countryCode'])
  df_true["countryCode"] = df_true["geonamesId"].apply(lambda geoname_id: json.loads(geonames_id_json_ass[geoname_id])["countryCode"])
  df_true = df_true.drop_duplicates(subset=['countryCode'])
  #estimated_locs = df_estimated["geonames_json"].apply(lambda geonames_json_str: prepare_loc_object(json.loads(geonames_json_str), geonames_hierarchy_ass)).to_list()
  #true_locs = df_true["geonamesId"].apply(lambda geoname_id: prepare_loc_object(json.loads(geonames_id_json_ass[geoname_id]), geonames_hierarchy_ass)).to_list()
  estimated_loc_info = df_estimated["countryCode"].to_list()
  true_loc_info = df_true["countryCode"].to_list()
  print(estimated_loc_info)
  print(true_loc_info)
  count = 0
  # how many true locs are included in our results ?
  for true_loc in true_loc_info:
    for est_loc in estimated_loc_info:
      # ['Washington, D.C.']
      # ['Washington']
      if true_loc == est_loc or true_loc in est_loc or est_loc in true_loc:
        count += 1
  score = 0
  if len(true_loc_info)>0:
    score = count/len(true_loc_info)
  print(score)
  return count, len(true_loc_info)



def evaluate_spatial_info_extr_result_at_article_level_admin1_approx(df_estimated, df_true, geonames_id_json_ass, geonames_hierarchy_ass):
  #df_estimated = df_estimated[df_estimated["type"] != "GPE"]
  
  df_estimated["admin_geonames_id"] = df_estimated["geonames_json"].apply(lambda geonames_json_str: 
        json.loads(geonames_json_str)["adminCodes1"]["ISO3166_2"] if "adminName1" in json.loads(geonames_json_str) and 
        "adminCodes1" in json.loads(geonames_json_str) and
        json.loads(geonames_json_str)["adminName1"] != '' else json.loads(geonames_json_str)["countryId"])
  df_estimated = df_estimated.drop_duplicates(subset=['admin_geonames_id'])
  df_true["admin_geonames_id"] = df_true["geonamesId"].apply(lambda geoname_id: 
       json.loads(geonames_id_json_ass[geoname_id])["adminCodes1"]["ISO3166_2"] if "adminName1" in json.loads(geonames_id_json_ass[geoname_id]) and 
       "adminCodes1" in json.loads(geonames_id_json_ass[geoname_id]) and json.loads(geonames_id_json_ass[geoname_id])["adminName1"] != '' else json.loads(geonames_id_json_ass[geoname_id])["countryId"])
  df_true = df_true.drop_duplicates(subset=['admin_geonames_id'])
  #estimated_locs = df_estimated["geonames_json"].apply(lambda geonames_json_str: prepare_loc_object(json.loads(geonames_json_str), geonames_hierarchy_ass)).to_list()
  #true_locs = df_true["geonamesId"].apply(lambda geoname_id: prepare_loc_object(json.loads(geonames_id_json_ass[geoname_id]), geonames_hierarchy_ass)).to_list()
  estimated_loc_info = np.unique(df_estimated["admin_geonames_id"].to_list())
  true_loc_info = np.unique(df_true["admin_geonames_id"].to_list())
  print(estimated_loc_info)
  print(true_loc_info)
  count = 0
  # how many true locs are included in our results ?
  for true_loc in true_loc_info:
    for est_loc in estimated_loc_info:
      # ['Washington, D.C.']
      # ['Washington']
      if true_loc == est_loc: # or true_loc in est_loc or est_loc in true_loc:
        count += 1
  score = 0
  if len(true_loc_info)>0:
    score = count/len(true_loc_info)
  print(score)
  return count, len(true_loc_info)


def evaluate_all_spatial_info_extr_results(df_estimated_spatial_info_extr, true_resuls_folder_path,\
                                            geonames_id_json_ass, geonames_hierarchy_ass, output_folder):
  article_id_list = np.unique(df_estimated_spatial_info_extr["id_articleweb"])
  count = 0
  total = 0
  true_geonames_id_list = []
  score_list = []
  estimated_geonames_id_list = []
  eff_article_id_list = []
  for article_id in article_id_list:
    article_id = int(article_id)
    print("-------------------------", article_id)
    df_estimated = df_estimated_spatial_info_extr[df_estimated_spatial_info_extr["id_articleweb"] == article_id]
    filepath = os.path.join(true_resuls_folder_path, str(article_id)+".csv")
    df_true = pd.read_csv(filepath, sep=",", keep_default_na=False)
    df_true = df_true[df_true["geonamesId"] != '']
    df_true = df_true.astype({"geonamesId":"int"})
    # curr_count, curr_total = evaluate_spatial_info_extr_result_at_article_level(df_estimated, df_true, \
    #                                                             geonames_id_json_ass, geonames_hierarchy_ass)
    curr_count, curr_total = evaluate_spatial_info_extr_result_at_article_level_admin1_approx(df_estimated, df_true, \
                                                               geonames_id_json_ass, geonames_hierarchy_ass)
    if curr_total>0:
      eff_article_id_list.append(article_id)
      true_geonames_id_list.append(list(df_true["geonamesId"].to_numpy().astype(int)))
      estimated_geonames_id_list.append(list(df_estimated["geonames_id"].to_numpy().astype(int)))
      count += curr_count
      total += curr_total
      score_list.append(curr_count/curr_total)
  
  filepath = os.path.join(output_folder, "eval_results.csv")
  df = pd.DataFrame({"article_id": eff_article_id_list, "score": score_list})
  df.to_csv(filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)
    
  print("------------------------------")
  print("final proportion:", count/total)
  # write into "output_folder"  
    

  
  