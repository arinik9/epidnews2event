import os
import pandas as pd
import numpy as np
import json

import pandas as pd
import csv

import re

filepath = "articlesweb.csv"
df = pd.read_csv(filepath, sep=";", keep_default_na=False)
article_ids = df["id"].to_list()
article_id_to_text = dict(zip(df["id"], df["text"]))


filepath = "geonames_data.csv"
df_geonames = pd.read_csv(filepath, sep=";", keep_default_na=False)
geonames_id_json_ass = dict(zip(df_geonames["geonames_id"], df_geonames["geonames_json"]))


folder_path = "padiweb_disambiguation"

counter = 0
token_id_list = []
article_id_list = []
sentence_id_list = []
token_pos_list = []
token_length_list = []
token_label_list = []
token_value_list = []
geonames_id_list = []
geonames_json_list = []
for article_id in article_ids:
    print("-------------------------", article_id)
    article_text = article_id_to_text[article_id]
    filepath = os.path.join(folder_path, str(article_id)+".csv")
    df = pd.read_csv(filepath, sep=",", keep_default_na=False)
    for index, row in df.iterrows():
        entity_text = row["text"]
        geonames_id = row["geonamesId"]
        for m in re.finditer(entity_text, article_text):
            token_pos_list.append(m.start(0))
            token_value_list.append(entity_text)
            article_id_list.append(article_id)
            token_id_list.append(counter)
            counter += 1
            geonames_id_list.append(geonames_id)
            geonames_json = geonames_id_json_ass[geonames_id]
            geonames_json_list.append(geonames_json)

df_extr_info = pd.DataFrame({"id_articleweb": article_id_list, "id": token_id_list, "value": token_value_list, "position": token_pos_list, "geonames_json": geonames_json_list, "geonames_id": geonames_id_list})
df_extr_info["keyword_id"] = "-1"
df_extr_info["keyword_type_id"] = "-1"
df_extr_info["a_classificationlabel_id"] = "1 6"
df_extr_info["from_automatic_extraction"] = "-1"
df_extr_info["a_classificationlabel_id"] = "1 6"
df_extr_info["type"] = "outbreak-related-location"
df_extr_info["label"] = "correct"
df_extr_info["token_index"] = "-1"
df_extr_info["length"] = "-1"

df_extr_info = df_extr_info.drop_duplicates(subset=['id_articleweb','position','geonames_id'])
df_extr_info = df_extr_info.sort_values(by=['id_articleweb','position'], ascending=True)

df_extr_info.to_csv("extracted_information2.csv", sep=";", quoting=csv.QUOTE_NONNUMERIC)




