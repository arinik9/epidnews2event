import os
import pandas as pd
import numpy as np
import json

import pandas as pd
import csv

import spacy
import re

from geopy.geocoders import Nominatim, GeoNames, ArcGIS



def clean_spatial_entity_text(text):
    text = text.lower()
    rgx = "(?i)(^the )"
    text = re.sub(rgx, "", text)
    rgx = "(?i)('s|’s)" # good example: "Africa's", bad example: "St John's"
    text = re.sub(rgx, "", text)
    if text == "horsarieux":
        text = "horsarrieu"
    elif text == "united states":
        text = "united states of america"
    elif text == "u.s.":
        text = "united states of america"
    elif text == "us":
        text = "united states of america"
    elif text == "uk":
        text = "united kingdom"
    elif text == "u.k.":
        text = "united kingdom"

    if "village" in text:
        text = text.replace("village", "").strip()
    if "department  " in text:
        text = text.replace("department", "").strip()
    if "municipality  " in text:
        text = text.replace("municipality", "").strip()

    return(text)





#articles_filepath = "articlesweb.csv" # we retrieve GPEs from the whole article content, because we need the position info of the tokens
#extr_info_filepath= "extracted_information.csv"

def retrieve_gpe_from_all_raw_texts(articles_filepath, extr_info_filepath):

  df = pd.read_csv(articles_filepath, sep=";", keep_default_na=False)
  
  nlp_lg = spacy.load("en_core_web_lg")
  nlp_trf = spacy.load("en_core_web_trf")
  
  client_geonames = GeoNames(username="arinik9")
  
  counter = 0
  token_id_list = []
  article_id_list = []
  token_pos_list = []
  token_length_list = []
  token_label_list = []
  token_value_list = []
  geonames_id_list = []
  geonames_json_list = []
  for index, row in df.iterrows():
      print("--------", index)
      article_id = row["id"]
      text = row["text"]
      doc = nlp_trf(text)
      if_any = False
      for ent in doc.ents: # iterate over the named entities of the text
          if ent.label_ == "GPE" or ent.label_ == "LOC" or ent.label_ == "FAC":
              if_any = True
              break
      if not if_any:
          doc = nlp_lg(text)
  
      if index>30 and index<60:
          client_geonames = GeoNames(username="arinik14")
      elif index>=60:
          client_geonames = GeoNames(username="arinik10")
  
      for ent in doc.ents: # iterate over the named entities of the text
          if ent.label_ == "GPE" or ent.label_ == "LOC" or ent.label_ == "FAC":
              text = clean_spatial_entity_text(ent.text)
              print(ent.text, text)
              article_id_list.append(article_id)
              token_id_list.append(counter)
              token_pos_list.append(ent.start_char)
              token_value_list.append(text)
              token_length_list.append(ent.end_char-ent.start_char+1)
              print(text, ent.start_char, ent.end_char, ent.label_)
              counter += 1
              geonames_id = -1
              geonames_json = "-1"
              geonames_locations = client_geonames.geocode(text, exactly_one=False, country_bias=None)
              if geonames_locations is not None:
                  first_loc = geonames_locations[0].raw
                  geonames_id = first_loc["geonameId"]
                  geonames_json = first_loc
              geonames_id_list.append(geonames_id)
              geonames_json_list.append(json.dumps(geonames_json))
  print("end")
  df_extr_info = pd.DataFrame({"id_articleweb": article_id_list, "id": token_id_list, "value": token_value_list, "position": token_pos_list, "length": token_length_list, "geonames_json": geonames_json_list, "geonames_id": geonames_id_list})
  print(df_extr_info)
  df_extr_info["keyword_id"] = "-1"
  df_extr_info["keyword_type_id"] = "-1"
  df_extr_info["a_classificationlabel_id"] = "1 6"
  df_extr_info["from_automatic_extraction"] = "-1"
  df_extr_info["a_classificationlabel_id"] = "1 6"
  df_extr_info["type"] = "outbreak-related-location"
  df_extr_info["label"] = "correct"
  df_extr_info["token_index"] = "-1"
  
  
  df_extr_info.to_csv(extr_info_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)


