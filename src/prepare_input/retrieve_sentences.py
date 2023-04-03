import pandas as pd
import csv

import spacy

#articles_filepath = "articlesweb.csv"
#sentences_filepath = "sentences_with_labels.csv"

def retrieve_sentences_from_all_raw_texts(articles_filepath, sentences_filepath):
  df = pd.read_csv(articles_filepath, sep=";", keep_default_na=False)
  
  nlp_lg = spacy.load("en_core_web_lg")
  nlp_trf = spacy.load("en_core_web_trf")
  
  counter = 0
  sentence_id_list = []
  article_id_list = []
  sentence_start_pos_list = []
  sentence_end_pos_list = []
  sentence_list = []
  for index, row in df.iterrows():
      print("--------", index)
      article_id = row["id"]
      text = row["text"]
      doc = nlp_lg(text)
      for sentence in doc.sents:
          counter += 1
          if sentence.text.strip() != "":
              print(sentence.text)
              article_id_list.append(article_id)
              sentence_id_list.append(counter)
              sentence_start_pos_list.append(sentence.start_char)
              sentence_end_pos_list.append(sentence.end_char)
              sentence_list.append(sentence.text.strip())
  
  df_sentences = pd.DataFrame({"article_id": article_id_list, "id": sentence_id_list, "text": sentence_list, "start": sentence_start_pos_list, "end": sentence_end_pos_list})
  df_sentences["s_classificationlabel_id"] = "11 12"
  df_sentences["a_classificationlabel_id"] = "1 6"
  df_sentences["lang"] = "EN"
  
  df_sentences.to_csv(sentences_filepath, sep=";", quoting=csv.QUOTE_NONNUMERIC)


