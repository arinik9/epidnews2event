'''
Created on Sep 1, 2022

@author: nejat
'''

import src.consts as consts
import re
import unicodedata
import json

from src.event.event import Event
from src.event.temporality import Temporality
from src.event.disease import Disease
from src.event.symptom import Symptom
from src.event.host import Host
from src.event.location import Location

import pandas as pd


def read_df_events(events_filepath):
  cols_events = [consts.COL_ID, consts.COL_ARTICLE_ID, consts.COL_URL, consts.COL_SOURCE, \
                    consts.COL_GEONAMES_ID, "geoname_json", "loc_name", "loc_country_code", consts.COL_LOC_CONTINENT, \
                    consts.COL_LAT, consts.COL_LNG, "hierarchy_data", consts.COL_PUBLISHED_TIME, consts.COL_DISEASE,  \
                   consts.COL_HOST, consts.COL_SYMPTOM_SUBTYPE, consts.COL_SYMPTOM, \
                    consts.COL_TITLE, consts.COL_SENTENCES, \
                    "day_no", "week_no", "month_no", "month_no", "year", "season"
                   ]
  df_events = pd.read_csv(events_filepath, usecols=cols_events, sep=";", keep_default_na=False)
  return df_events


def read_events_from_df(df_events):
  
  # df_events = read_df_events(events_filepath)

  events = []
  for index, row in df_events.iterrows():
    loc = Location(row["loc_name"], row[consts.COL_GEONAMES_ID], json.loads(row["geoname_json"]), \
                   row[consts.COL_LAT], row[consts.COL_LNG], row["loc_country_code"], row[consts.COL_LOC_CONTINENT], \
                   eval(row["hierarchy_data"]))
    t = Temporality(row[consts.COL_PUBLISHED_TIME])
    disease_tuple = eval(row[consts.COL_DISEASE])
    dis = Disease(disease_tuple[0], disease_tuple[1])
    h = Host(json.loads(row[consts.COL_HOST]))
    sym = Symptom()
    # sym.load_dict_data_from_str(row[consts.COL_SYMPTOM_SUBTYPE], row[consts.COL_SYMPTOM])
    e = Event(int(row[consts.COL_ID]), row[consts.COL_ARTICLE_ID], row[consts.COL_URL], \
                    row[consts.COL_SOURCE], loc, t, dis, h, sym, row[consts.COL_TITLE], row[consts.COL_SENTENCES])
    events.append(e)
    
  return events




def retrieve_disease_from_raw_sentence(sentence_text, disease_name):
  sentence_text = sentence_text.lower()
  clean_text = unicodedata.normalize("NFKD",sentence_text)

  # "hpai" and "lpai" are the first two elements in DISEASE_KEYWORDS_DICT >> intentionally
  disease_keywords = consts.DISEASE_KEYWORDS_DICT[disease_name].keys()
  dis_type = ""
  for kw in disease_keywords:
    #print(kw)
    parts = kw.split(" ")
    kw_pattern = ' '.join([p+".{0,2}" for p in parts])
    # I dont add a space in the beginning, maybe a phrase can start with the keyword
    #kw_pattern = " "+ kw_pattern # to ensure that our keyword is not contained in a string >> the found string should start with our keyword
    res = re.findall(kw_pattern, clean_text)
    if len(res)>0:
      dis_type = disease_name #consts.DISEASE_KEYWORDS_DICT[disease_name][kw]
      if kw == "hpai":
        dis_type = "HPAI"
      elif kw == "lpai":
        dis_type = "LPAI"
      break
      
  dis_subtype = ""
  if disease_name == consts.DISEASE_AVIAN_INFLUENZA:
    res = re.findall("h[0-9]{1,2}n[0-9]{1,2}", clean_text)
    res = [r.strip() for r in res]
    res2 = re.findall("h[0-9]{1,2}", clean_text)
    res2 = [r2.strip() for r2 in res2]
    # an event must have a single AI subtype
    # for now, we do not treat complex sentences
    if len(res)>0:
      dis_subtype = ','.join(res).strip()
      if len(res) > 1:
        dis_subtype = ""
      #dis_subtype = res[0] # take the first one, if there are multiple candidates # >> TODO we can improve it
      dis_type = disease_name
    elif len(res2)>0:
      dis_subtype = ','.join(res2).strip()
      if len(res2) > 1:
        dis_subtype = ""
      #dis_subtype = res2[0] # take the first one, if there are multiple candidates # >> TODO we can improve it
      dis_type = disease_name

  dis = None
  if dis_subtype != "" or dis_type != "":
    if disease_name == consts.DISEASE_AVIAN_INFLUENZA and dis_type == disease_name:
      dis_type = dis_type+"-unknown"
      if dis_subtype != "" and dis_subtype not in ["h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9", "h10"]:
        if dis_subtype in ["h5n1", "h7n9", "h5n6", "h5n8"]:
          dis_type = "HPAI"
        else:
          dis_type = "LPAI"
    dis = Disease(dis_subtype, dis_type)
  return(dis)
  
  

def retrieve_host_from_raw_sentence(sentence_text, add_space_for_search=True):
  h = Host()
  sentence_text = sentence_text.lower()
  
  for host_type in list(consts.HOST_KEYWORDS_HIERARCHY_DICT.keys()): # hierarchy level 0
    host_subtype_data = consts.HOST_KEYWORDS_HIERARCHY_DICT[host_type] # it is a list of dicts with two keys: "text" and "hierarchy"
    # each entry in 'host_subtype_data["hierarchy"]' is organized from general to specialized tuple_data
    for lvl in [4,3,2,1,0]:
      if h.is_host_info_empty(): #if a human case already found, or another host type, we do not treat a new one
        for dict_entry in host_subtype_data:
          curr_lvl = dict_entry["level"]
          if lvl == curr_lvl:
            kw = dict_entry["text"]
            #print(kw)
            parts = kw.split(" ")
            kw_pattern = ' '.join([p+".{0,2}" for p in parts])
            if add_space_for_search: 
              kw_pattern = " "+ kw_pattern # to ensure that our keyword is not contained in a string >> the found string should start with our keyword
            res = re.findall(kw_pattern, sentence_text)
            if len(res)>0:
              #print("host:", host_type)
              # each 'd' is organized from general to specialized tuple_data
              d = dict_entry["hierarchy"]
              h_temp = Host()
              h_temp.add_host_info(d)
              if h.is_host_info_empty() or (not h.is_host_info_empty() and list(h.get_entry().keys())[0] == list(h_temp.get_entry().keys())[0]):
                h.add_host_info(d)
            
  
  # if len(h.get_entry()) == 0:
  #   return None   
  return(h)



def contains_ban_related_keyword(text):
  ban = False
  for kw in consts.BAN_KEYWORDS_LIST:
    parts = kw.split(" ")
    kw_pattern = ' '.join([" "+p+".{0,2}" for p in parts])
    if len(re.findall(kw_pattern, text))>0:
      ban = True
      break
  return ban


