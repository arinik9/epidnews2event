
import json
import os
from random import randrange
import pycountry_convert as pc

from pyphoton import Photon
from pyphoton.errors import PhotonException
from geopy.geocoders import Nominatim, GeoNames

import pandas as pd
import geopandas as gpd
import pycountry_convert as pc
from shapely.geometry import Point, Polygon

import geocoder # >>> https://geocoder.readthedocs.io/providers/GeoNames.html


import numpy as np
import csv


def retrieve_continent_from_country_code(self, country_code):
  continent_name = ""
  try:
    country_code = pc.country_alpha3_to_country_alpha2(country_code)
    continent_name = pc.country_alpha2_to_continent_code(country_code)
  except KeyError:
    print(country_code, "is a key error")
  return continent_name


def get_loc_hierarchy(geonames_id):
  print("geonames_id: ", geonames_id)
  # in some way, we could use this one, as well
  # g = geocoder.geonames(main.geonames_id, method='details', key='arinik9', language='en')
  res = {"name": [], "geoname_id": []}
  
  geonames_api_username_list = ["arinik9", "arinik1", "arinik2", "arinik10", "arinik11"]
  try:
    # there are some hourly and daily query limit for geonames servers.
    # as a workaround, we randomly pick one account and send our request. 
    # >> in theory, those accounts will be picked approx. uniformly
    rand_index = randrange(len(geonames_api_username_list))
    geonames_api_username = geonames_api_username_list[rand_index]
    print("---- account ", geonames_api_username)
    g = geocoder.geonames(geonames_id, method='hierarchy', key=geonames_api_username)
    if g.status == "OK":
      print("status: ", g.status)
      for result in g: # iterate the hierarchy from upper level to lower level
        if "countryName" in result.raw:
          res["name"].append(result.address)
          res["geoname_id"].append(result.geonames_id)
  except:
    print("error in get_loc_hierarchy() with geonames_id=", geonames_id)
    pass
  #print(res)
  return res
    

def retrieve_unique_geonames_id(filepath):
  df_info_extr = pd.read_csv(filepath, sep=";", keep_default_na=False)
  geonames_id_list = df_info_extr["geonames_id"].to_numpy()
  unique_geonames_id_list = np.unique(geonames_id_list)
  print(2978317 in unique_geonames_id_list)
  return unique_geonames_id_list
  


def enhance_geonames_data_with_hierarchy(geonames_id_list, filepath):
  print("there are ", len(geonames_id_list), " geonames ids.")
  hierarchy_name_list = []
  hierarchy_geoname_id_list = []
  for geonames_id in geonames_id_list:
    hierarchy_results = get_loc_hierarchy(geonames_id)
    hierarchy_name_list.append(str(hierarchy_results["name"]))
    hierarchy_geoname_id_list.append(str(hierarchy_results["geoname_id"]))
  df = pd.DataFrame(data={"geonames_id": geonames_id_list, "hierarchy_name": hierarchy_name_list, "hierarchy_geoname_id": hierarchy_geoname_id_list})

  df.to_csv(filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)


####################################



def retrieve_geonames_hierarchy_for_ground_truth(articles_filepath, ground_truth_folder_path, geonames_hierarchy_data_filepath):

  df = pd.read_csv(articles_filepath, sep=";", keep_default_na=False)
  article_id_list = df["id"]
  geonames_id_list = []
  for article_id in article_id_list:
      article_id = int(article_id)
      print("-------------------------", article_id)
      filepath = os.path.join(ground_truth_folder_path, str(article_id)+".csv")
      df_true = pd.read_csv(filepath, sep=",", keep_default_na=False)
      geonames_id_list = geonames_id_list + df_true['geonamesId'].to_list()
  
  geonames_id_list = np.unique(geonames_id_list)
  enhance_geonames_data_with_hierarchy(geonames_id_list, geonames_hierarchy_data_filepath)

