from functools import lru_cache
import logging
import datetime as dt

import urllib
import requests
from config.constants import Constants

from repository.internal.weather import WeatherRepository
from service.user import UserService
from utils.background import background
from utils.weather import DarkSky

class WeatherService:
    def __init__(
        self, 
        weatherRepository : WeatherRepository,
        darkSky : DarkSky
        ) -> None:
        self.weatherRepository = weatherRepository
        self.darkSky = darkSky

    @lru_cache(maxsize=5000)
    def callDarkSky(self, lat, lon, date_s):
        return self.darkSky.BuildTimeMachine(lat, lon, date_s).execute()

    def get_weather(self, formaladdr, strdate):
        try: 
            lat, lon = Constants.DEFAULT_LAT, Constants.DEFAULT_LON
            try:
                #TODO move to a variable
                url = 'http://192.168.1.14:8081/search.php?q=' + urllib.parse.quote(formaladdr) +'&format=json'
                response = requests.get(url).json()
                lat, lon = response[0]["lat"], response[0]["lon"]
            except Exception as exc:
                logging.info("Failed to access Nominatim, setting default coordinates")
            logging.debug(f'requesting: {lat} {lon}')
            date_s = strdate
            data = self.callDarkSky(lat, lon, date_s)
            weather = {
                'formaladdr' : formaladdr,
                'strdate' : strdate,
                'latitude' : lat,
                'longitude' : lon,
                'apparentTemperatureHigh' : data['daily']['data'][0]['apparentTemperatureHigh'],
                'apparentTemperatureLow' : data['daily']['data'][0]['apparentTemperatureLow'],
                'temperatureLow' : data['daily']['data'][0]['temperatureLow'],
                'temperatureHigh' : data['daily']['data'][0]['temperatureHigh'],
                'cloudCover' : data['daily']['data'][0]['cloudCover'],
                'humidity' : data['daily']['data'][0]['humidity'],
                'precipIntensity' : data['daily']['data'][0].get('precipIntensity', "NULL"),
                'precipAccumulation' : data['daily']['data'][0].get('precipAccumulation', "NULL"),
                'precipProbability' : data['daily']['data'][0].get('precipProbability', "NULL"),
                'precipType' : data['daily']['data'][0].get('precipType', "NULL"),
                'pressure' : data['daily']['data'][0]['pressure'],
                'windSpeed' : data['daily']['data'][0]['windSpeed'],
                'windBearing' : data['daily']['data'][0].get('windBearing', "NULL"),
                'moonPhase' : data['daily']['data'][0]['moonPhase']
            }
            self.weatherRepository.save_all(weather)
        except Exception as exc:
            logging.info(f'Caught exception for {formaladdr} on {strdate}: {exc}')

    def update_weather(self, limit = None):
        data = self.weatherRepository.get_new_addresses()
        logging.info(f'Inserting {len(data)} objects')
        i = 0
        for formaladdr, strdate in data:
            self.get_weather(formaladdr, strdate)
            if limit is not None:
                if i > limit:
                    break
                else:
                    i += 1