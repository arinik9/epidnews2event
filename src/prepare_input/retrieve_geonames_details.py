
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




def get_geonames_data(geonames_id):
  print("geonames_id: ", geonames_id)
  # in some way, we could use this one, as well
  # g = geocoder.geonames(main.geonames_id, method='details', key='arinik9', language='en')
  res = {"geoname_id": None, "geonames_json": None}
  
  geonames_api_username_list = ["arinik9", "arinik1", "arinik2", "arinik10", "arinik11"]
  try:
    # there are some hourly and daily query limit for geonames servers.
    # as a workaround, we randomly pick one account and send our request. 
    # >> in theory, those accounts will be picked approx. uniformly
    rand_index = randrange(len(geonames_api_username_list))
    geonames_api_username = geonames_api_username_list[rand_index]
    print("---- account ", geonames_api_username)
    g = geocoder.geonames(int(geonames_id), method='details', key=geonames_api_username)
    if g.status == "OK":
      geoname_json = g[0].raw
      res["geoname_id"] = geonames_id
      res["geonames_json"] = json.dumps(geoname_json)
  except:
    print("error in get_geonames_data() with geonames_id=", geonames_id)
    pass
  #print(res)
  return res
    



def retrieve_geonames_data(geonames_id_list, filepath):
  print("there are ", len(geonames_id_list), " geonames ids.")
  json_list = []
  geoname_id_list = []
  for geonames_id in geonames_id_list:
    result = get_geonames_data(geonames_id)
    json_list.append(result["geonames_json"])
    geoname_id_list.append(str(result["geoname_id"]))
  df = pd.DataFrame(data={"geonames_id": geonames_id_list, "geonames_json": json_list})

  df.to_csv(filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)


####################################




def retrieve_geonames_details_for_ground_truth(articles_filepath, ground_truth_folder_path, geonames_data_filepath):

  df = pd.read_csv(articles_filepath, sep=";", keep_default_na=False)
  article_id_list = df["id"]
  geonames_id_list = []
  for article_id in article_id_list:
      article_id = int(article_id)
      print("-------------------------", article_id)
      filepath = os.path.join(ground_truth_folder_path, str(article_id)+".csv")
      df_true = pd.read_csv(filepath, sep=",", keep_default_na=False)
      if len(df_true['geonamesId'].to_list())>0:
          geonames_id_list = geonames_id_list + df_true['geonamesId'].to_list()
  
  geonames_id_list = np.unique(geonames_id_list)
  #print(geonames_id_list)
  retrieve_geonames_data(geonames_id_list, geonames_data_filepath)

