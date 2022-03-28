import logging
import requests
import urllib.parse

from sqlalchemy import create_engine

from datetime import datetime as dt
from sqlalchemy.engine import Engine
from sqlalchemy import text

from entity.weather import Weather
from utils.weather import DarkSky
from utils.background import background

class WeatherRepository:
    def __init__(self, engine : Engine) -> None:
        self.engine = engine

    def get(self, date, formaladdr):
        return None

    def save(self, weather : Weather):
        self.engine.execute(text('''
        INSERT INTO public.weather VALUES(
            :formaladdr, 
            :strdate, 
            :latitude, 
            :longitude, 
            :apparentTemperatureHigh, 
            :apparentTemperatureLow, 
            :temperatureLow, 
            :temperatureHigh, 
            :cloudCover, 
            :humidity, 
            :precipIntensity, 
            :precipProbability, 
            :precipType, 
            :pressure, 
            :windSpeed,
            :windBearing, 
            :moonPhase)
        '''),
            formaladdr = weather.formaladdr,
            strdate = weather.weatherDate,
            latitude = weather.latitude,
            longitude = weather.longitude,
            apparentTemperatureHigh = weather.apparentTemperatureHigh,
            apparentTemperatureLow = weather.apparentTemperatureLow,
            temperatureLow = weather.temperatureLow,
            temperatureHigh = weather.temperatureHigh,
            cloudCover = weather.cloudcover,
            humidity = weather.humidity,
            precipIntensity = weather.precipIntensity,
            precipAccumulation = weather.precipAccumulation,
            precipProbability = weather.precipProbability,
            precipType = weather.precipType,
            pressure = weather.pressure,
            windSpeed = weather.windSpeed,
            windBearing = weather.windBearing,
            moonPhase = weather.moonPhase
        )

    def save_all(self, weather):
        self.engine.execute(text('''
        INSERT INTO public.weather VALUES(
            :formaladdr, 
            :strdate, 
            :latitude, 
            :longitude, 
            :apparentTemperatureHigh, 
            :apparentTemperatureLow, 
            :temperatureLow, 
            :temperatureHigh, 
            :cloudCover, 
            :humidity, 
            :precipIntensity, 
            :precipProbability, 
            :precipType, 
            :pressure, 
            :windSpeed,
            :windBearing, 
            :moonPhase)
        '''),
            weather
        )

    def get_new_addresses(self):
        data = self.engine.execute('''
        SELECT DISTINCT users.formaladdr, user_history.strdate
        FROM public.user_history
        INNER JOIN public.users ON user_history.userid = users.userid
        LEFT JOIN public.weather ON weather.formaladdr = users.formaladdr AND weather.strdate = user_history.strdate
        WHERE weather.formaladdr IS NULL
        ''').fetchall()
        return data