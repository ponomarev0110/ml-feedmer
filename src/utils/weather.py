import time

import requests

from datetime import datetime as dt

class TimeMachine:
  url = "https://api.darksky.net/forecast/{0}/{1},{2},{3}"
  def __init__(self, API_Key, lat, lon, time, units="si", lang="ru"):
    self.API_Key = API_Key
    self.lat = lat
    self.lon = lon
    self.time = time
    self.units = units
    self.lang = lang
  
  def execute(self):
    path = TimeMachine.url.format(self.API_Key, self.lat, self.lon, self.time)
    return requests.get(path, params = {"units" : self.units, "lang" : self.lang}).json()

class DarkSky:
  def __init__(self, API_Key):
    self.API_Key = API_Key

  def BuildTimeMachine(self, lat, lon, time):
    return TimeMachine(self.API_Key, lat, lon, time)