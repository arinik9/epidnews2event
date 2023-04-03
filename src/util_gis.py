'''
Created on Sep 1, 2022

@author: nejat
'''




from math import radians, cos, sin, asin, sqrt
import pycountry_convert as pc
import geopandas as gpd
from shapely.geometry import Point, Polygon

from src.event.location import Location

from pyphoton import Photon
from pyphoton.errors import PhotonException


  
  
def retrieve_continent_from_country_code(country_code):
  continent_name = ""
  try:
    if len(country_code) == 3: # alpha3
      country_code = pc.country_alpha3_to_country_alpha2(country_code)
    continent_name = pc.country_alpha2_to_continent_code(country_code)
  except KeyError:
    print(country_code, "is a key error")
  return continent_name


