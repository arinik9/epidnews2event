# '''
# Created on Nov 21, 2021
#
# @author: nejat
# '''
#
#
# from __future__ import annotations
# from abc import ABC, abstractmethod
# import pandas as pd
# import src.consts as consts
#
# import copy
# from typing import List
#
#
# class SentenceIdentificationStrategy(ABC):
#   """
#   The Strategy interface declares operations common to all supported versions
#   of some algorithm.
#
#   The Context uses this interface to call the algorithm defined by Concrete
#   Strategies.
#   """
#   @abstractmethod
#   def get_description(self) -> str:
#       pass
#
#   @abstractmethod
#   def estimate(self, row:pd.Series, sentence_id_to_article_id:dict) -> List:
#       pass
#
#
# """
# Concrete Strategies implement the algorithm while following the base Strategy
# interface. The interface makes them interchangeable in the Context.
# """
#
#
# class SentenceIdentificationStrategyCurrentSentence(SentenceIdentificationStrategy):
#
#   def get_description(self) -> str:
#     return(consts.SENTENCE_IDEN_STRATEGY_CURRENT)
#
#   # pick the first sentence (in order) having a spatial entity associated with a geonames feature code
#   # note that the last sentence id is the first sentence having a spatial entity >> 'outbreak-related-location'
#   def estimate(self, row:pd.Series, sentence_id_to_article_id:dict) -> List:
#     country_codes = copy.deepcopy(eval(row[consts.COL_COUNTRY_FEATURE_CODE])) # turn str_list into list
#     code = '-1'
#     indx = len(country_codes)
#     #print(country_codes)
#     while (code == '-1' or code=="RGN" or code=="CONT" or code=="PEN") and len(country_codes)>0:
#       indx = indx-1
#       code = country_codes.pop() # retrieve the last element
#     #
#     if len(country_codes)>0 and code != '-1':
#       val = int(row[consts.COL_SENTENCE_ID][indx])
#   #    return [val]
#
#      # it is sure that 'val' corresponds to 'outbreak related location'
#      # but, the variables 'next_val' and 'prev_val' do not have to
#
#       res_sentence_ids = [val]
#       return(res_sentence_ids)
#     return(-1) 
#
#
#
#
# class SentenceIdentificationStrategyCurrentPrevNextSentences(SentenceIdentificationStrategy):
#
#
#   def get_description(self) -> str:
#     return(consts.SENTENCE_IDEN_STRATEGY_CURRENT_PREV_NEXT)
#
#
#   def search_neighbor_sentence_ids(self, val, sentence_id_to_article_id):
#     res_sentence_ids = [val]
#     next_val = val + 1
#     prev_val = val - 1
#
#     for i in range(50000):
#       if next_val in sentence_id_to_article_id:
#         break
#       next_val = next_val + 1
#     next_val_found = (next_val in sentence_id_to_article_id)
#     if not next_val_found:
#       sentence_id_to_article_id[next_val] = "NOTFOUND1"
#
#     for i in range(50000):
#       if prev_val in sentence_id_to_article_id:
#         break
#       prev_val = prev_val - 1
#
#     prev_val_found = (prev_val in sentence_id_to_article_id)
#     if not prev_val_found:
#       sentence_id_to_article_id[prev_val] = "NOTFOUND2"
#
#     if sentence_id_to_article_id[val] == sentence_id_to_article_id[next_val] \
#          and sentence_id_to_article_id[val] == sentence_id_to_article_id[prev_val]:
#       res_sentence_ids = [val,next_val,prev_val]
#     elif sentence_id_to_article_id[val] == sentence_id_to_article_id[next_val] \
#          and sentence_id_to_article_id[val] != sentence_id_to_article_id[prev_val]:
#       res_sentence_ids = [val,next_val]
#     elif sentence_id_to_article_id[val] != sentence_id_to_article_id[next_val] \
#          and sentence_id_to_article_id[val] == sentence_id_to_article_id[prev_val]:
#       res_sentence_ids = [val,prev_val]
#
#     return(res_sentence_ids) # current sentence id is always at the beginning
#
#
#
#
#   def estimate(self, row:pd.Series, sentence_id_to_article_id:dict) -> List:
#     country_codes = copy.deepcopy(eval(row[consts.COL_COUNTRY_FEATURE_CODE])) # turn str_list into list
#     code = '-1'
#     indx = len(country_codes)
#     #print(country_codes)
#     while (code == '-1' or code=="RGN" or code=="CONT" or code=="PEN") and len(country_codes)>0:
#       indx = indx-1
#       code = country_codes.pop() # retrieve the last element
#
#     if len(country_codes)>0 and code != '-1':
#       val = int(row[consts.COL_SENTENCE_ID][indx])
#   #    return [val]
#
#      # it is sure that 'val' corresponds to 'outbreak related location'
#      # but, the variables 'next_val' and 'prev_val' do not have to
#
#       return self.search_neighbor_sentence_ids(val, sentence_id_to_article_id)
#     return(-1) 
#
#
#
#
# class SentenceIdentificationStrategyCurrentAndTwoNextSentences(SentenceIdentificationStrategy):
#
#
#   def get_description(self) -> str:
#     return(consts.SENTENCE_IDEN_STRATEGY_CURRENT_AND_TWO_NEXT)
#
#
#   def estimate(self, row:pd.Series, sentence_id_to_article_id:dict) -> List:
#     country_codes = copy.deepcopy(eval(row[consts.COL_COUNTRY_FEATURE_CODE])) # turn str_list into list
#     code = '-1'
#     indx = len(country_codes)
#     #print(country_codes)
#     while (code == '-1' or code=="RGN" or code=="CONT" or code=="PEN") and len(country_codes)>0:
#       indx = indx-1
#       code = country_codes.pop() # retrieve the last element
#     #
#     if len(country_codes)>0 and code != '-1':
#       val = int(row[consts.COL_SENTENCE_ID][indx])
#   #    return [val]
#
#      # it is sure that 'val' corresponds to 'outbreak related location'
#      # but, the variables 'next_val' and 'prev_val' do not have to
#
#       res_sentence_ids = [val]
#
#       next_val = val + 1
#
#       while next_val not in sentence_id_to_article_id:
#         next_val = next_val + 1
#       subsequent_val = next_val + 1
#       while subsequent_val not in sentence_id_to_article_id:
#         subsequent_val = subsequent_val + 1
#       if sentence_id_to_article_id[val] == sentence_id_to_article_id[next_val] \
#            and sentence_id_to_article_id[val] == sentence_id_to_article_id[subsequent_val]:
#         res_sentence_ids = [val,next_val,subsequent_val]
#       elif sentence_id_to_article_id[val] == sentence_id_to_article_id[next_val] \
#            and sentence_id_to_article_id[val] != sentence_id_to_article_id[subsequent_val]:
#         res_sentence_ids = [val,next_val]
#
#       return(res_sentence_ids) # current sentence id is always at the beginning
#     return(-1) 
